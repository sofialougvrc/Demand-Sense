from datetime import UTC, date, datetime

import pandas as pd

from demand_sense.lakehouse.gold import (
    aggregate_store_sku_daily_demand,
    current_sales_transactions_from_silver,
    debezium_date_to_date,
)
from demand_sense.lakehouse.silver import canonical_json


def silver_sales_event(
    *,
    transaction_id: str,
    offset: int,
    units: int,
    net_revenue: str,
    operation: str = "r",
    is_deleted: bool = False,
    promotion_id: str | None = None,
    event_at: datetime | None = None,
) -> dict:
    event_at = event_at or datetime(2026, 9, 1, 4, 0, tzinfo=UTC)
    payload = {
        "business_date": 20089,
        "discount_pct": "0.10" if promotion_id else "0.00",
        "gross_revenue": "12.00",
        "inventory_on_hand": 20,
        "is_stockout": units > 3,
        "net_revenue": net_revenue,
        "promotion_id": promotion_id,
        "sale_ts": "2025-01-01T10:30:00.000000Z",
        "sku_id": "SKU-0001",
        "store_id": "STORE-001",
        "transaction_id": transaction_id,
        "unit_price": "4.00",
        "units": units,
    }
    return {
        "event_id": f"event-{offset}",
        "record_key": canonical_json({"transaction_id": transaction_id}),
        "source_schema": "retail",
        "source_table": "sales_transactions",
        "operation": operation,
        "operation_name": "delete" if operation == "d" else "snapshot_read",
        "is_snapshot": operation == "r",
        "is_deleted": is_deleted,
        "event_at": event_at,
        "event_date": event_at.date(),
        "source_lsn": 100 + offset,
        "kafka_topic": "demand_sense.retail.sales_transactions",
        "kafka_partition": 0,
        "kafka_offset": offset,
        "before_data": None,
        "record_data": canonical_json(payload),
        "ingested_at": event_at,
        "silver_loaded_at": event_at,
    }


def test_debezium_date_to_date_converts_epoch_days() -> None:
    assert debezium_date_to_date(20089) == date(2025, 1, 1)


def test_current_sales_transactions_keeps_latest_event_and_excludes_deletes() -> None:
    silver_df = pd.DataFrame(
        [
            silver_sales_event(transaction_id="txn-1", offset=1, units=1, net_revenue="4.00"),
            silver_sales_event(
                transaction_id="txn-1",
                offset=2,
                units=3,
                net_revenue="12.00",
                event_at=datetime(2026, 9, 1, 4, 5, tzinfo=UTC),
            ),
            silver_sales_event(transaction_id="txn-2", offset=3, units=2, net_revenue="8.00"),
            silver_sales_event(
                transaction_id="txn-2",
                offset=4,
                units=2,
                net_revenue="8.00",
                operation="d",
                is_deleted=True,
                event_at=datetime(2026, 9, 1, 4, 10, tzinfo=UTC),
            ),
        ]
    )

    current = current_sales_transactions_from_silver(silver_df)

    assert current["transaction_id"].tolist() == ["txn-1"]
    assert current.iloc[0]["units"] == 3


def test_aggregate_store_sku_daily_demand_sums_daily_metrics() -> None:
    silver_df = pd.DataFrame(
        [
            silver_sales_event(
                transaction_id="txn-1",
                offset=1,
                units=2,
                net_revenue="8.00",
                promotion_id="PROMO-1",
            ),
            silver_sales_event(transaction_id="txn-2", offset=2, units=4, net_revenue="16.00"),
        ]
    )
    current = current_sales_transactions_from_silver(silver_df)

    gold_df = aggregate_store_sku_daily_demand(current)

    assert len(gold_df) == 1
    row = gold_df.iloc[0]
    assert row["store_id"] == "STORE-001"
    assert row["sku_id"] == "SKU-0001"
    assert row["business_date"] == date(2025, 1, 1)
    assert row["units_sold"] == 6
    assert row["transaction_count"] == 2
    assert row["net_revenue"] == 24.0
    assert row["stockout_transaction_count"] == 1
    assert row["stockout_rate"] == 0.5
    assert row["promo_transaction_count"] == 1
    assert row["promo_units"] == 2
