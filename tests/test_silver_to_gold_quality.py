from datetime import UTC, date, datetime

import pandas as pd
import pytest

from demand_sense.quality.results import DataQualityError
from demand_sense.quality.silver_to_gold import (
    SILVER_TO_GOLD_SUITE_NAME,
    build_silver_to_gold_suite,
    validate_silver_events,
)


def valid_silver_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "event_id": "abc",
                "record_key": '{"transaction_id":"txn-1"}',
                "source_schema": "retail",
                "source_table": "sales_transactions",
                "operation": "r",
                "operation_name": "snapshot_read",
                "is_snapshot": True,
                "is_deleted": False,
                "event_at": datetime(2026, 9, 1, 4, 0, tzinfo=UTC),
                "event_date": date(2026, 9, 1),
                "source_lsn": 12345,
                "kafka_topic": "demand_sense.retail.sales_transactions",
                "kafka_partition": 0,
                "kafka_offset": 1,
                "before_data": None,
                "record_data": (
                    '{"business_date":20089,"gross_revenue":"10.00",'
                    '"is_stockout":false,"net_revenue":"9.00","sale_ts":"2025-01-01T10:00:00Z",'
                    '"sku_id":"SKU-0001","store_id":"STORE-001","transaction_id":"txn-1",'
                    '"unit_price":"10.00","discount_pct":"0.10","inventory_on_hand":5,"units":1}'
                ),
                "ingested_at": datetime(2026, 9, 1, 4, 1, tzinfo=UTC),
                "silver_loaded_at": datetime(2026, 9, 1, 4, 2, tzinfo=UTC),
            }
        ]
    )


def test_silver_to_gold_suite_is_named() -> None:
    suite = build_silver_to_gold_suite()

    assert suite.name == SILVER_TO_GOLD_SUITE_NAME
    assert len(suite.expectations) > 10


def test_validate_silver_events_passes_valid_dataframe() -> None:
    result = validate_silver_events(valid_silver_dataframe())

    assert result.success is True
    assert result.unsuccessful_expectations == 0


def test_validate_silver_events_blocks_bad_source_table() -> None:
    df = valid_silver_dataframe()
    df.loc[0, "source_table"] = "unknown"

    with pytest.raises(DataQualityError, match="expect_column_values_to_be_in_set"):
        validate_silver_events(df)
