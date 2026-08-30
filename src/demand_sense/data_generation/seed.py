from __future__ import annotations

import argparse
import os
from datetime import date
from importlib.resources import files

import psycopg
from dotenv import load_dotenv

from demand_sense.data_generation.generator import (
    GeneratorConfig,
    ProductProfile,
    Promotion,
    SalesTransaction,
    StoreProfile,
    SyntheticRetailDataset,
    generate_dataset,
)


def main() -> None:
    args = parse_args()
    load_dotenv()

    config = GeneratorConfig(
        start_date=date.fromisoformat(args.start_date),
        days=args.days,
        store_count=args.stores,
        sku_count=args.skus,
        seed=args.seed,
    )
    dataset = generate_dataset(config)
    seed_database(
        dataset, database_url=args.database_url or database_url_from_env(), reset=not args.append
    )

    print(
        "Seeded retail schema with "
        f"{len(dataset.stores)} stores, "
        f"{len(dataset.products)} products, "
        f"{len(dataset.promotions)} promotions, and "
        f"{len(dataset.transactions)} sales transactions."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed synthetic Demand-Sense retail data.")
    parser.add_argument("--database-url", help="Postgres connection URL. Defaults to .env values.")
    parser.add_argument(
        "--start-date", default="2025-01-01", help="First business date to generate."
    )
    parser.add_argument(
        "--days", type=int, default=180, help="Number of business days to generate."
    )
    parser.add_argument("--stores", type=int, default=8, help="Number of stores to generate.")
    parser.add_argument("--skus", type=int, default=30, help="Number of SKUs to generate.")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible data.")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing retail tables instead of truncating them first.",
    )
    return parser.parse_args()


def database_url_from_env() -> str:
    user = os.getenv("POSTGRES_USER", "demand_sense")
    password = os.getenv("POSTGRES_PASSWORD", "demand_sense")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "demand_sense")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def seed_database(
    dataset: SyntheticRetailDataset, *, database_url: str, reset: bool = True
) -> None:
    with psycopg.connect(database_url) as conn:
        apply_schema(conn)
        if reset:
            truncate_retail_tables(conn)

        insert_stores(conn, dataset.stores)
        insert_products(conn, dataset.products)
        insert_promotions(conn, dataset.promotions)
        insert_transactions(conn, dataset.transactions)
        conn.commit()


def apply_schema(conn: psycopg.Connection) -> None:
    schema_path = files("demand_sense.data_generation").joinpath("schema.sql")
    conn.execute(schema_path.read_text())


def truncate_retail_tables(conn: psycopg.Connection) -> None:
    conn.execute(
        """
        TRUNCATE TABLE
            retail.sales_transactions,
            retail.promotions,
            retail.products,
            retail.stores
        RESTART IDENTITY CASCADE
        """
    )


def insert_stores(conn: psycopg.Connection, stores: list[StoreProfile]) -> None:
    rows = [
        (
            store.store_id,
            store.store_name,
            store.region,
            store.store_format,
            store.square_feet,
            store.opened_on,
            store.demand_multiplier,
        )
        for store in stores
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO retail.stores (
                store_id,
                store_name,
                region,
                store_format,
                square_feet,
                opened_on,
                demand_multiplier
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def insert_products(conn: psycopg.Connection, products: list[ProductProfile]) -> None:
    rows = [
        (
            product.sku_id,
            product.product_name,
            product.category,
            product.brand,
            product.unit_cost,
            product.unit_price,
            product.shelf_life_days,
            product.base_daily_demand,
            product.seasonality_strength,
            product.seasonal_phase,
            product.price_elasticity,
        )
        for product in products
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO retail.products (
                sku_id,
                product_name,
                category,
                brand,
                unit_cost,
                unit_price,
                shelf_life_days,
                base_daily_demand,
                seasonality_strength,
                seasonal_phase,
                price_elasticity
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def insert_promotions(conn: psycopg.Connection, promotions: list[Promotion]) -> None:
    rows = [
        (
            promotion.promotion_id,
            promotion.sku_id,
            promotion.store_id,
            promotion.start_date,
            promotion.end_date,
            promotion.discount_pct,
            promotion.feature_display,
            promotion.campaign_name,
        )
        for promotion in promotions
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO retail.promotions (
                promotion_id,
                sku_id,
                store_id,
                start_date,
                end_date,
                discount_pct,
                feature_display,
                campaign_name
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


def insert_transactions(conn: psycopg.Connection, transactions: list[SalesTransaction]) -> None:
    rows = [
        (
            transaction.transaction_id,
            transaction.store_id,
            transaction.sku_id,
            transaction.sale_ts,
            transaction.business_date,
            transaction.units,
            transaction.unit_price,
            transaction.discount_pct,
            transaction.gross_revenue,
            transaction.net_revenue,
            transaction.inventory_on_hand,
            transaction.is_stockout,
            transaction.promotion_id,
        )
        for transaction in transactions
    ]
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO retail.sales_transactions (
                transaction_id,
                store_id,
                sku_id,
                sale_ts,
                business_date,
                units,
                unit_price,
                discount_pct,
                gross_revenue,
                net_revenue,
                inventory_on_hand,
                is_stockout,
                promotion_id
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
        )


if __name__ == "__main__":
    main()
