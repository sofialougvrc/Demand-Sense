from datetime import UTC, date, datetime

import pandas as pd
import pytest

from demand_sense.quality.bronze_to_silver import (
    BRONZE_TO_SILVER_SUITE_NAME,
    DataQualityError,
    build_bronze_to_silver_suite,
    validate_bronze_events,
)


def valid_bronze_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ingest_id": "abc",
                "ingested_at": datetime(2026, 9, 3, tzinfo=UTC),
                "ingest_date": date(2026, 9, 3),
                "topic": "demand_sense.retail.stores",
                "kafka_partition": 0,
                "kafka_offset": 1,
                "source_schema": "retail",
                "source_table": "stores",
                "operation": "r",
                "event_ts_ms": 1788237572650,
                "source_lsn": 12345,
                "kafka_key": '{"store_id":"STORE-001"}',
                "debezium_value": (
                    '{"before":null,"after":{"store_id":"STORE-001"},'
                    '"source":{"schema":"retail","table":"stores","lsn":12345,'
                    '"snapshot":"first"},"op":"r","ts_ms":1788237572650}'
                ),
            }
        ]
    )


def test_bronze_to_silver_suite_is_named() -> None:
    suite = build_bronze_to_silver_suite()

    assert suite.name == BRONZE_TO_SILVER_SUITE_NAME
    assert len(suite.expectations) > 10


def test_validate_bronze_events_passes_valid_dataframe() -> None:
    result = validate_bronze_events(valid_bronze_dataframe())

    assert result.success is True
    assert result.unsuccessful_expectations == 0


def test_validate_bronze_events_blocks_invalid_operation() -> None:
    df = valid_bronze_dataframe()
    df.loc[0, "operation"] = "x"

    with pytest.raises(DataQualityError, match="expect_column_values_to_be_in_set"):
        validate_bronze_events(df)
