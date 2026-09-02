from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv

from demand_sense.lakehouse.bronze import DEFAULT_BRONZE_TABLE_URI, delta_storage_options
from demand_sense.quality.bronze_to_silver import DataQualityError, validate_bronze_events

DEFAULT_SILVER_TABLE_URI = "s3://demand-sense/silver/retail_cdc_events"
OPERATION_NAMES = {
    "r": "snapshot_read",
    "c": "create",
    "u": "update",
    "d": "delete",
}
SILVER_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("record_key", pa.string()),
        pa.field("source_schema", pa.string()),
        pa.field("source_table", pa.string()),
        pa.field("operation", pa.string()),
        pa.field("operation_name", pa.string()),
        pa.field("is_snapshot", pa.bool_()),
        pa.field("is_deleted", pa.bool_()),
        pa.field("event_at", pa.timestamp("us", tz="UTC")),
        pa.field("event_date", pa.date32()),
        pa.field("source_lsn", pa.int64()),
        pa.field("kafka_topic", pa.string()),
        pa.field("kafka_partition", pa.int32()),
        pa.field("kafka_offset", pa.int64()),
        pa.field("before_data", pa.string()),
        pa.field("record_data", pa.string()),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        pa.field("silver_loaded_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class SilverSettings:
    bronze_table_uri: str
    silver_table_uri: str
    minio_endpoint_url: str
    minio_access_key: str
    minio_secret_key: str
    aws_region: str
    mode: str = "overwrite"


def main() -> None:
    load_dotenv()
    args = parse_args()
    settings = settings_from_env(args)

    try:
        if args.command == "build":
            summary = build_silver_events(settings)
        elif args.command == "inspect":
            summary = inspect_silver_table(settings)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except (SilverTransformError, DataQualityError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the silver CDC event table.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Validate bronze and write cleaned silver events.")
    build.add_argument("--mode", choices=["append", "overwrite"], default=None)

    subparsers.add_parser("inspect", help="Show silver Delta table metadata.")
    return parser.parse_args()


def settings_from_env(args: argparse.Namespace) -> SilverSettings:
    return SilverSettings(
        bronze_table_uri=os.getenv("BRONZE_TABLE_URI", DEFAULT_BRONZE_TABLE_URI),
        silver_table_uri=os.getenv("SILVER_TABLE_URI", DEFAULT_SILVER_TABLE_URI),
        minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        mode=getattr(args, "mode", None) or os.getenv("SILVER_WRITE_MODE", "overwrite"),
    )


def build_silver_events(settings: SilverSettings) -> dict[str, Any]:
    bronze_df = read_delta_as_pandas(settings.bronze_table_uri, settings)
    quality_result = validate_bronze_events(bronze_df)
    silver_df = transform_bronze_to_silver(bronze_df)
    write_silver_events(silver_df, settings=settings)

    return {
        "bronze_table_uri": settings.bronze_table_uri,
        "silver_table_uri": settings.silver_table_uri,
        "bronze_rows": len(bronze_df),
        "silver_rows": len(silver_df),
        "mode": settings.mode,
        "quality_gate": quality_result.to_dict(),
        "source_table_counts": silver_df["source_table"].value_counts().sort_index().to_dict(),
    }


def read_delta_as_pandas(table_uri: str, settings: SilverSettings) -> pd.DataFrame:
    try:
        table = DeltaTable(table_uri, storage_options=silver_storage_options(settings))
    except Exception as exc:
        raise SilverTransformError(f"Could not open Delta table at {table_uri}") from exc

    return table.to_pyarrow_table().to_pandas()


def transform_bronze_to_silver(bronze_df: pd.DataFrame) -> pd.DataFrame:
    if bronze_df.empty:
        return empty_silver_dataframe()

    rows = [silver_row_from_bronze_record(record) for record in bronze_df.to_dict("records")]
    silver_df = pd.DataFrame(rows)
    silver_df = silver_df.drop_duplicates(
        subset=["kafka_topic", "kafka_partition", "kafka_offset"],
        keep="last",
    )
    silver_df = silver_df.sort_values(
        ["event_at", "kafka_topic", "kafka_partition", "kafka_offset"],
        kind="stable",
    ).reset_index(drop=True)
    return silver_df


def silver_row_from_bronze_record(record: dict[str, Any]) -> dict[str, Any]:
    envelope = json.loads(record["debezium_value"])
    before = envelope.get("before")
    after = envelope.get("after")
    source = envelope.get("source") or {}
    operation = envelope.get("op")
    event_ts_ms = envelope.get("ts_ms") or source.get("ts_ms")
    event_at = datetime.fromtimestamp(event_ts_ms / 1000, tz=UTC)
    record_data = before if operation == "d" else after

    return {
        "event_id": record["ingest_id"],
        "record_key": canonical_json(record["kafka_key"]),
        "source_schema": source.get("schema") or record["source_schema"],
        "source_table": source.get("table") or record["source_table"],
        "operation": operation,
        "operation_name": OPERATION_NAMES[operation],
        "is_snapshot": str(source.get("snapshot", "")).lower() in {"true", "first", "last"},
        "is_deleted": operation == "d",
        "event_at": event_at,
        "event_date": event_at.date(),
        "source_lsn": int(source["lsn"]) if source.get("lsn") is not None else None,
        "kafka_topic": record["topic"],
        "kafka_partition": int(record["kafka_partition"]),
        "kafka_offset": int(record["kafka_offset"]),
        "before_data": canonical_json(before),
        "record_data": canonical_json(record_data),
        "ingested_at": ensure_utc_timestamp(record["ingested_at"]),
        "silver_loaded_at": datetime.now(UTC),
    }


def write_silver_events(silver_df: pd.DataFrame, *, settings: SilverSettings) -> None:
    table = pa.Table.from_pylist(silver_df.to_dict("records"), schema=SILVER_SCHEMA)
    write_deltalake(
        settings.silver_table_uri,
        table,
        mode=settings.mode,
        partition_by=["source_table", "event_date"],
        storage_options=silver_storage_options(settings),
    )


def inspect_silver_table(settings: SilverSettings) -> dict[str, Any]:
    try:
        table = DeltaTable(
            settings.silver_table_uri, storage_options=silver_storage_options(settings)
        )
    except Exception as exc:
        raise SilverTransformError(
            f"Could not open silver Delta table at {settings.silver_table_uri}"
        ) from exc

    add_actions = table.get_add_actions(flatten=True)
    record_counts = add_actions.column("num_records").to_pylist()
    source_tables = add_actions.column("partition.source_table").to_pylist()
    source_table_counts: dict[str, int] = {}
    for source_table, record_count in zip(source_tables, record_counts, strict=True):
        source_table_counts[source_table] = source_table_counts.get(source_table, 0) + record_count

    return {
        "table_uri": settings.silver_table_uri,
        "version": table.version(),
        "rows": sum(record_counts),
        "files": add_actions.num_rows,
        "source_table_counts": source_table_counts,
    }


def silver_storage_options(settings: SilverSettings) -> dict[str, str]:
    return delta_storage_options(settings)


def canonical_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = json.loads(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def ensure_utc_timestamp(value: Any) -> datetime:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(UTC)
    else:
        timestamp = timestamp.tz_convert(UTC)
    return timestamp.to_pydatetime()


def empty_silver_dataframe() -> pd.DataFrame:
    return pd.DataFrame({name: pd.Series(dtype="object") for name in SILVER_SCHEMA.names})


class SilverTransformError(RuntimeError):
    """Raised when bronze events cannot be transformed into silver events."""


if __name__ == "__main__":
    main()
