from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import Any

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from dotenv import load_dotenv

from demand_sense.lakehouse.silver import DEFAULT_SILVER_TABLE_URI, silver_storage_options
from demand_sense.quality.results import DataQualityError
from demand_sense.quality.silver_to_gold import validate_silver_events

DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI = "s3://demand-sense/gold/store_sku_daily_demand"
POSTGRES_EPOCH = date(1970, 1, 1)
GOLD_DAILY_DEMAND_SCHEMA = pa.schema(
    [
        pa.field("store_id", pa.string()),
        pa.field("sku_id", pa.string()),
        pa.field("business_date", pa.date32()),
        pa.field("units_sold", pa.int64()),
        pa.field("transaction_count", pa.int64()),
        pa.field("gross_revenue", pa.float64()),
        pa.field("net_revenue", pa.float64()),
        pa.field("avg_unit_price", pa.float64()),
        pa.field("avg_discount_pct", pa.float64()),
        pa.field("stockout_transaction_count", pa.int64()),
        pa.field("stockout_rate", pa.float64()),
        pa.field("promo_transaction_count", pa.int64()),
        pa.field("promo_units", pa.int64()),
        pa.field("first_sale_at", pa.timestamp("us", tz="UTC")),
        pa.field("last_sale_at", pa.timestamp("us", tz="UTC")),
        pa.field("source_event_count", pa.int64()),
        pa.field("gold_loaded_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass(frozen=True)
class GoldSettings:
    silver_table_uri: str
    gold_daily_demand_table_uri: str
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
            summary = build_gold_daily_demand(settings)
        elif args.command == "inspect":
            summary = inspect_gold_daily_demand(settings)
        else:
            raise ValueError(f"Unsupported command: {args.command}")
    except (GoldTransformError, DataQualityError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc

    print(json.dumps(summary, indent=2, sort_keys=True, default=str))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build gold store/SKU/day demand aggregates.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    build = subparsers.add_parser("build", help="Validate silver and write gold aggregates.")
    build.add_argument("--mode", choices=["append", "overwrite"], default=None)

    subparsers.add_parser("inspect", help="Show gold Delta table metadata.")
    return parser.parse_args()


def settings_from_env(args: argparse.Namespace) -> GoldSettings:
    return GoldSettings(
        silver_table_uri=os.getenv("SILVER_TABLE_URI", DEFAULT_SILVER_TABLE_URI),
        gold_daily_demand_table_uri=os.getenv(
            "GOLD_DAILY_DEMAND_TABLE_URI", DEFAULT_GOLD_DAILY_DEMAND_TABLE_URI
        ),
        minio_endpoint_url=os.getenv("MINIO_ENDPOINT_URL", "http://localhost:9000"),
        minio_access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        aws_region=os.getenv("AWS_REGION", "us-east-1"),
        mode=getattr(args, "mode", None) or os.getenv("GOLD_WRITE_MODE", "overwrite"),
    )


def build_gold_daily_demand(settings: GoldSettings) -> dict[str, Any]:
    silver_df = read_delta_as_pandas(settings.silver_table_uri, settings)
    quality_result = validate_silver_events(silver_df)
    current_transactions = current_sales_transactions_from_silver(silver_df)
    gold_df = aggregate_store_sku_daily_demand(current_transactions)
    write_gold_daily_demand(gold_df, settings=settings)

    return {
        "silver_table_uri": settings.silver_table_uri,
        "gold_daily_demand_table_uri": settings.gold_daily_demand_table_uri,
        "silver_rows": len(silver_df),
        "current_transaction_rows": len(current_transactions),
        "gold_rows": len(gold_df),
        "mode": settings.mode,
        "quality_gate": quality_result.to_dict(),
        "date_range": {
            "min": gold_df["business_date"].min() if not gold_df.empty else None,
            "max": gold_df["business_date"].max() if not gold_df.empty else None,
        },
    }


def read_delta_as_pandas(table_uri: str, settings: GoldSettings) -> pd.DataFrame:
    try:
        table = DeltaTable(table_uri, storage_options=gold_storage_options(settings))
    except Exception as exc:
        raise GoldTransformError(f"Could not open Delta table at {table_uri}") from exc
    return table.to_pyarrow_table().to_pandas()


def current_sales_transactions_from_silver(silver_df: pd.DataFrame) -> pd.DataFrame:
    sales_df = silver_df[silver_df["source_table"] == "sales_transactions"].copy()
    if sales_df.empty:
        return empty_current_transactions_dataframe()

    sales_df = sales_df.sort_values(
        ["event_at", "source_lsn", "kafka_topic", "kafka_partition", "kafka_offset"],
        kind="stable",
    )
    rows = [
        sales_transaction_row_from_silver_event(record) for record in sales_df.to_dict("records")
    ]
    transactions = pd.DataFrame(rows)
    transactions = transactions.drop_duplicates(subset=["transaction_id"], keep="last")
    transactions = transactions[~transactions["is_deleted"]].reset_index(drop=True)
    return transactions


def sales_transaction_row_from_silver_event(record: dict[str, Any]) -> dict[str, Any]:
    data = json.loads(record["record_data"])
    return {
        "transaction_id": data["transaction_id"],
        "store_id": data["store_id"],
        "sku_id": data["sku_id"],
        "business_date": debezium_date_to_date(data["business_date"]),
        "sale_ts": pd.Timestamp(data["sale_ts"]).to_pydatetime(),
        "units": int(data["units"]),
        "unit_price": decimal_to_float(data["unit_price"]),
        "discount_pct": decimal_to_float(data["discount_pct"]),
        "gross_revenue": decimal_to_float(data["gross_revenue"]),
        "net_revenue": decimal_to_float(data["net_revenue"]),
        "inventory_on_hand": int(data["inventory_on_hand"]),
        "is_stockout": bool(data["is_stockout"]),
        "promotion_id": data.get("promotion_id"),
        "source_event_count": 1,
        "event_at": record["event_at"],
        "source_lsn": record["source_lsn"],
        "kafka_topic": record["kafka_topic"],
        "kafka_partition": record["kafka_partition"],
        "kafka_offset": record["kafka_offset"],
        "is_deleted": bool(record["is_deleted"]),
    }


def aggregate_store_sku_daily_demand(transactions: pd.DataFrame) -> pd.DataFrame:
    if transactions.empty:
        return empty_gold_dataframe()

    transactions = transactions.copy()
    transactions["promo_units_source"] = transactions["units"].where(
        transactions["promotion_id"].notna(), 0
    )

    grouped = transactions.groupby(["store_id", "sku_id", "business_date"], as_index=False).agg(
        units_sold=("units", "sum"),
        transaction_count=("transaction_id", "nunique"),
        gross_revenue=("gross_revenue", "sum"),
        net_revenue=("net_revenue", "sum"),
        avg_unit_price=("unit_price", "mean"),
        avg_discount_pct=("discount_pct", "mean"),
        stockout_transaction_count=("is_stockout", "sum"),
        promo_transaction_count=("promotion_id", lambda values: values.notna().sum()),
        promo_units=("promo_units_source", "sum"),
        first_sale_at=("sale_ts", "min"),
        last_sale_at=("sale_ts", "max"),
        source_event_count=("source_event_count", "sum"),
    )
    grouped["stockout_rate"] = (
        grouped["stockout_transaction_count"] / grouped["transaction_count"]
    ).astype(float)
    grouped["gold_loaded_at"] = datetime.now(UTC)

    ordered_columns = [field.name for field in GOLD_DAILY_DEMAND_SCHEMA]
    grouped = grouped[ordered_columns]
    grouped = grouped.sort_values(
        ["business_date", "store_id", "sku_id"], kind="stable"
    ).reset_index(drop=True)
    return grouped


def write_gold_daily_demand(gold_df: pd.DataFrame, *, settings: GoldSettings) -> None:
    table = pa.Table.from_pylist(gold_df.to_dict("records"), schema=GOLD_DAILY_DEMAND_SCHEMA)
    write_deltalake(
        settings.gold_daily_demand_table_uri,
        table,
        mode=settings.mode,
        partition_by=["business_date"],
        storage_options=gold_storage_options(settings),
    )


def inspect_gold_daily_demand(settings: GoldSettings) -> dict[str, Any]:
    try:
        table = DeltaTable(
            settings.gold_daily_demand_table_uri, storage_options=gold_storage_options(settings)
        )
    except Exception as exc:
        raise GoldTransformError(
            f"Could not open gold Delta table at {settings.gold_daily_demand_table_uri}"
        ) from exc

    add_actions = table.get_add_actions(flatten=True)
    record_counts = add_actions.column("num_records").to_pylist()
    return {
        "table_uri": settings.gold_daily_demand_table_uri,
        "version": table.version(),
        "rows": sum(record_counts),
        "files": add_actions.num_rows,
    }


def gold_storage_options(settings: GoldSettings) -> dict[str, str]:
    return silver_storage_options(settings)


def debezium_date_to_date(value: int | str | date) -> date:
    if isinstance(value, date):
        return value
    if isinstance(value, int):
        return POSTGRES_EPOCH + timedelta(days=value)
    return date.fromisoformat(value)


def decimal_to_float(value: str | int | float | Decimal) -> float:
    return float(Decimal(str(value)))


def empty_current_transactions_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "transaction_id",
            "store_id",
            "sku_id",
            "business_date",
            "sale_ts",
            "units",
            "unit_price",
            "discount_pct",
            "gross_revenue",
            "net_revenue",
            "inventory_on_hand",
            "is_stockout",
            "promotion_id",
            "source_event_count",
            "event_at",
            "source_lsn",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "is_deleted",
        ]
    )


def empty_gold_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {field.name: pd.Series(dtype="object") for field in GOLD_DAILY_DEMAND_SCHEMA}
    )


class GoldTransformError(RuntimeError):
    """Raised when silver events cannot be transformed into gold aggregates."""


if __name__ == "__main__":
    main()
