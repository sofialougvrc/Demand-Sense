from datetime import UTC, date, datetime

import pandas as pd

from demand_sense.lakehouse.silver import (
    canonical_json,
    silver_row_from_bronze_record,
    transform_bronze_to_silver,
)


def bronze_record(offset: int, store_name: str = "Northeast Urban Express 001") -> dict:
    return {
        "ingest_id": f"event-{offset}",
        "ingested_at": datetime(2026, 9, 3, 8, 0, tzinfo=UTC),
        "ingest_date": date(2026, 9, 3),
        "topic": "demand_sense.retail.stores",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "source_schema": "retail",
        "source_table": "stores",
        "operation": "r",
        "event_ts_ms": 1788237572650,
        "source_lsn": 12345,
        "kafka_key": '{"store_id":"STORE-001"}',
        "debezium_value": (
            '{"before":null,'
            f'"after":{{"store_id":"STORE-001","store_name":"{store_name}"}},'
            '"source":{"schema":"retail","table":"stores","lsn":12345,'
            '"snapshot":"first"},'
            '"op":"r","ts_ms":1788237572650}'
        ),
    }


def test_silver_row_from_bronze_record_types_common_metadata() -> None:
    row = silver_row_from_bronze_record(bronze_record(12))

    assert row["event_id"] == "event-12"
    assert row["record_key"] == '{"store_id":"STORE-001"}'
    assert row["source_schema"] == "retail"
    assert row["source_table"] == "stores"
    assert row["operation"] == "r"
    assert row["operation_name"] == "snapshot_read"
    assert row["is_snapshot"] is True
    assert row["is_deleted"] is False
    assert row["event_at"] == datetime.fromtimestamp(1788237572650 / 1000, tz=UTC)
    assert row["event_date"] == datetime.fromtimestamp(1788237572650 / 1000, tz=UTC).date()
    assert row["source_lsn"] == 12345
    assert row["record_data"] == (
        '{"store_id":"STORE-001","store_name":"Northeast Urban Express 001"}'
    )


def test_transform_bronze_to_silver_deduplicates_kafka_offsets() -> None:
    df = pd.DataFrame(
        [
            bronze_record(1, store_name="Original"),
            bronze_record(1, store_name="Replacement"),
            bronze_record(2, store_name="Next"),
        ]
    )

    silver_df = transform_bronze_to_silver(df)

    assert len(silver_df) == 2
    assert silver_df.iloc[0]["record_data"] == '{"store_id":"STORE-001","store_name":"Replacement"}'
    assert silver_df.iloc[1]["record_data"] == '{"store_id":"STORE-001","store_name":"Next"}'


def test_canonical_json_sorts_keys_and_compacts_output() -> None:
    assert canonical_json({"b": 2, "a": 1}) == '{"a":1,"b":2}'
