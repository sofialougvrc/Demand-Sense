from datetime import UTC, datetime

from demand_sense.ingestion.cdc import RETAIL_TABLES
from demand_sense.lakehouse.bronze import (
    CDC_TOPICS,
    bronze_row_from_kafka_message,
    stable_ingest_id,
)


def test_cdc_topics_align_with_retail_tables() -> None:
    assert CDC_TOPICS == tuple(f"demand_sense.{table}" for table in RETAIL_TABLES)


def test_bronze_row_preserves_raw_debezium_envelope() -> None:
    ingested_at = datetime(2026, 9, 3, 8, 0, tzinfo=UTC)
    key = b'{"store_id":"STORE-001"}'
    value = (
        b'{"before":null,"after":{"store_id":"STORE-001"},'
        b'"source":{"schema":"retail","table":"stores","lsn":12345,"ts_ms":1788237572499},'
        b'"op":"r","ts_ms":1788237572650}'
    )

    row = bronze_row_from_kafka_message(
        topic="demand_sense.retail.stores",
        partition=0,
        offset=12,
        key_bytes=key,
        value_bytes=value,
        ingested_at=ingested_at,
    )

    assert row["ingest_id"] == stable_ingest_id(
        topic="demand_sense.retail.stores", partition=0, offset=12
    )
    assert row["ingested_at"] == ingested_at
    assert row["ingest_date"] == ingested_at.date()
    assert row["source_schema"] == "retail"
    assert row["source_table"] == "stores"
    assert row["operation"] == "r"
    assert row["event_ts_ms"] == 1788237572650
    assert row["source_lsn"] == 12345
    assert row["kafka_key"] == key.decode()
    assert row["debezium_value"] == value.decode()


def test_stable_ingest_id_changes_by_offset() -> None:
    first = stable_ingest_id(topic="demand_sense.retail.stores", partition=0, offset=1)
    second = stable_ingest_id(topic="demand_sense.retail.stores", partition=0, offset=2)

    assert first != second
    assert first == stable_ingest_id(topic="demand_sense.retail.stores", partition=0, offset=1)
