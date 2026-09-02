from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError, KafkaException
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv

from demand_sense.ingestion.cdc import RETAIL_TABLES

CDC_TOPICS = tuple(f"demand_sense.{table}" for table in RETAIL_TABLES)
DEFAULT_BRONZE_TABLE_URI = "s3://demand-sense/bronze/debezium_events"
BRONZE_SCHEMA = pa.schema(
    [
        pa.field("ingest_id", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("ingest_date", pa.date32()),
        pa.field("topic", pa.string()),
        pa.field("kafka_partition", pa.int32()),
        pa.field("kafka_offset", pa.int64()),
        pa.field("source_schema", pa.string()),
        pa.field("source_table", pa.string()),
        pa.field("operation", pa.string()),
        pa.field("event_ts_ms", pa.int64()),
        pa.field("source_lsn", pa.int64()),
        pa.field("kafka_key", pa.string()),
        pa.field("debezium_value", pa.string()),
    ]
)


@dataclass(frozen=True)
class BronzeSettings:
    kafka_bootstrap_servers: str
    table_uri: str
    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str
    aws_region: str
    consumer_group: str = "demand-sense-bronze-landing"
    batch_size: int = 5_000
    max_messages: int = 10_000
    idle_timeout_seconds: float = 10.0
    mode: str = "append"


def main() -> None:
    load_dotenv()
    args = parse_args()
    settings = settings_from_env(args)

    try:
        if args.command == "land":
            summary = land_bronze_events(settings)
        elif args.command == "inspect":
            summary = inspect_bronze_table(settings)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except BronzeLandingError as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Land Kafka CDC events into the bronze lakehouse.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    land = subparsers.add_parser(
        "land", help="Consume Debezium topics into the bronze Delta table."
    )
    land.add_argument("--max-messages", type=int, default=None)
    land.add_argument("--batch-size", type=int, default=None)
    land.add_argument("--idle-timeout-seconds", type=float, default=None)
    land.add_argument("--mode", choices=["append", "overwrite"], default=None)
    land.add_argument("--consumer-group", default=None)
    land.add_argument(
        "--replay",
        action="store_true",
        help="Use a fresh consumer group so Kafka topics are read from the beginning.",
    )

    subparsers.add_parser("inspect", help="Show bronze Delta table metadata.")
    return parser.parse_args()


def settings_from_env(args: argparse.Namespace) -> BronzeSettings:
    base_consumer_group = getattr(args, "consumer_group", None) or os.getenv(
        "BRONZE_CONSUMER_GROUP", "demand-sense-bronze-landing"
    )
    consumer_group = (
        f"{base_consumer_group}-replay-{int(datetime.now(UTC).timestamp())}"
        if getattr(args, "replay", False)
        else base_consumer_group
    )

    return BronzeSettings(
        kafka_bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        table_uri=os.getenv("BRONZE_TABLE_URI", DEFAULT_BRONZE_TABLE_URI),
        minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        consumer_group=consumer_group,
        batch_size=getattr(args, "batch_size", None) or int(os.getenv("BRONZE_BATCH_SIZE", "5000")),
        max_messages=getattr(args, "max_messages", None)
        if getattr(args, "max_messages", None) is not None
        else int(os.getenv("BRONZE_MAX_MESSAGES", "10000")),
        idle_timeout_seconds=getattr(args, "idle_timeout_seconds", None)
        if getattr(args, "idle_timeout_seconds", None) is not None
        else float(os.getenv("BRONZE_IDLE_TIMEOUT_SECONDS", "10")),
        mode=getattr(args, "mode", None) or os.getenv("BRONZE_WRITE_MODE", "append"),
    )


def land_bronze_events(settings: BronzeSettings) -> dict[str, Any]:
    consumer = build_consumer(settings)
    rows: list[dict[str, Any]] = []
    total_landed = 0
    mode = settings.mode
    started_at = datetime.now(UTC)
    last_message_at = started_at

    try:
        consumer.subscribe(list(CDC_TOPICS))
        while total_landed < settings.max_messages:
            message = consumer.poll(timeout=1.0)
            now = datetime.now(UTC)

            if message is None:
                if (now - last_message_at).total_seconds() >= settings.idle_timeout_seconds:
                    break
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise BronzeLandingError(f"Kafka consumer error: {message.error()}")

            rows.append(
                bronze_row_from_kafka_message(
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    key_bytes=message.key(),
                    value_bytes=message.value(),
                    ingested_at=now,
                )
            )
            last_message_at = now

            if len(rows) >= settings.batch_size:
                write_bronze_rows(rows, settings=settings, mode=mode)
                consumer.commit(asynchronous=False)
                total_landed += len(rows)
                rows = []
                mode = "append"

        if rows:
            write_bronze_rows(rows, settings=settings, mode=mode)
            consumer.commit(asynchronous=False)
            total_landed += len(rows)
    except KafkaException as exc:
        raise BronzeLandingError(f"Kafka consumer failed: {exc}") from exc
    finally:
        consumer.close()

    return {
        "table_uri": settings.table_uri,
        "topics": CDC_TOPICS,
        "landed_rows": total_landed,
        "mode": settings.mode,
        "started_at": started_at,
        "finished_at": datetime.now(UTC),
    }


def build_consumer(settings: BronzeSettings) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def bronze_row_from_kafka_message(
    *,
    topic: str,
    partition: int,
    offset: int,
    key_bytes: bytes | None,
    value_bytes: bytes | None,
    ingested_at: datetime,
) -> dict[str, Any]:
    key_text = decode_json_bytes(key_bytes)
    value_text = decode_json_bytes(value_bytes)
    envelope = json.loads(value_text) if value_text else {}
    source = envelope.get("source") or {}
    event_ts_ms = envelope.get("ts_ms") or source.get("ts_ms")
    source_lsn = source.get("lsn")

    return {
        "ingest_id": stable_ingest_id(topic=topic, partition=partition, offset=offset),
        "ingested_at": ingested_at,
        "ingest_date": ingested_at.date(),
        "topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
        "source_schema": source.get("schema"),
        "source_table": source.get("table"),
        "operation": envelope.get("op"),
        "event_ts_ms": int(event_ts_ms) if event_ts_ms is not None else None,
        "source_lsn": int(source_lsn) if source_lsn is not None else None,
        "kafka_key": key_text,
        "debezium_value": value_text,
    }


def write_bronze_rows(
    rows: Iterable[dict[str, Any]],
    *,
    settings: BronzeSettings,
    mode: str,
) -> None:
    rows = list(rows)
    if not rows:
        return

    table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
    write_deltalake(
        settings.table_uri,
        table,
        mode=mode,
        partition_by=["source_table", "ingest_date"],
        storage_options=delta_storage_options(settings),
    )


def inspect_bronze_table(settings: BronzeSettings) -> dict[str, Any]:
    try:
        table = DeltaTable(settings.table_uri, storage_options=delta_storage_options(settings))
    except Exception as exc:
        raise BronzeLandingError(
            f"Could not open bronze Delta table at {settings.table_uri}"
        ) from exc

    add_actions = table.get_add_actions(flatten=True)
    record_counts = add_actions.column("num_records").to_pylist()
    source_tables = add_actions.column("partition.source_table").to_pylist()
    source_table_counts: dict[str, int] = {}
    for source_table, record_count in zip(source_tables, record_counts, strict=True):
        source_table_counts[source_table] = source_table_counts.get(source_table, 0) + record_count

    return {
        "table_uri": settings.table_uri,
        "version": table.version(),
        "rows": sum(record_counts),
        "files": add_actions.num_rows,
        "source_table_counts": source_table_counts,
    }


def delta_storage_options(settings: BronzeSettings) -> dict[str, str]:
    return {
        "AWS_ACCESS_KEY_ID": settings.minio_access_key,
        "AWS_SECRET_ACCESS_KEY": settings.minio_secret_key,
        "AWS_ENDPOINT_URL": settings.minio_endpoint_url,
        "AWS_REGION": settings.aws_region,
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_S3_ADDRESSING_STYLE": "path",
    }


def stable_ingest_id(*, topic: str, partition: int, offset: int) -> str:
    natural_key = f"{topic}:{partition}:{offset}"
    return hashlib.sha256(natural_key.encode("utf-8")).hexdigest()


def decode_json_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8")


class BronzeLandingError(RuntimeError):
    """Raised when Kafka CDC events cannot be landed to the bronze Delta table."""


if __name__ == "__main__":
    main()
