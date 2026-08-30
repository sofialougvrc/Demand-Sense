from collections import defaultdict
from datetime import date

import pytest

from demand_sense.data_generation.generator import (
    GeneratorConfig,
    ProductProfile,
    Promotion,
    StoreProfile,
    expected_daily_demand,
    generate_dataset,
)


def test_generate_dataset_is_reproducible_for_same_seed() -> None:
    config = GeneratorConfig(
        start_date=date(2025, 1, 6), days=14, store_count=3, sku_count=5, seed=7
    )

    first = generate_dataset(config)
    second = generate_dataset(config)

    assert first.stores == second.stores
    assert first.products == second.products
    assert first.promotions == second.promotions
    assert first.transactions == second.transactions
    assert len(first.transactions) > 0


def test_generator_config_rejects_non_positive_dimensions() -> None:
    with pytest.raises(ValueError, match="days must be positive"):
        GeneratorConfig(days=0)

    with pytest.raises(ValueError, match="store_count must be positive"):
        GeneratorConfig(store_count=0)

    with pytest.raises(ValueError, match="sku_count must be positive"):
        GeneratorConfig(sku_count=0)


def test_expected_demand_reflects_weekly_pattern_and_promotion_lift() -> None:
    store = StoreProfile(
        store_id="STORE-001",
        store_name="Test Store",
        region="West",
        store_format="suburban",
        square_feet=45_000,
        opened_on=date(2020, 1, 1),
        demand_multiplier=1.0,
    )
    product = ProductProfile(
        sku_id="SKU-0001",
        product_name="Test Product",
        category="beverages",
        brand="Northstar",
        unit_cost=2.00,
        unit_price=4.00,
        shelf_life_days=180,
        base_daily_demand=10.0,
        seasonality_strength=0.0,
        seasonal_phase=0,
        price_elasticity=2.0,
    )
    promotion = Promotion(
        promotion_id="PROMO-00001",
        sku_id=product.sku_id,
        store_id=store.store_id,
        start_date=date(2025, 1, 6),
        end_date=date(2025, 1, 12),
        discount_pct=0.20,
        feature_display=True,
        campaign_name="Test Campaign",
    )

    monday = expected_daily_demand(
        business_date=date(2025, 1, 6),
        day_offset=5,
        store=store,
        product=product,
        promotion=None,
    )
    saturday = expected_daily_demand(
        business_date=date(2025, 1, 11),
        day_offset=10,
        store=store,
        product=product,
        promotion=None,
    )
    promoted = expected_daily_demand(
        business_date=date(2025, 1, 7),
        day_offset=6,
        store=store,
        product=product,
        promotion=promotion,
    )
    unpromoted = expected_daily_demand(
        business_date=date(2025, 1, 7),
        day_offset=6,
        store=store,
        product=product,
        promotion=None,
    )

    assert saturday > monday
    assert promoted > unpromoted * 1.5


def test_transactions_are_capped_by_available_inventory() -> None:
    dataset = generate_dataset(
        GeneratorConfig(start_date=date(2025, 1, 1), days=45, store_count=4, sku_count=8, seed=11)
    )
    units_by_store_sku_day: defaultdict[tuple[str, str, date], int] = defaultdict(int)
    inventory_by_store_sku_day: dict[tuple[str, str, date], int] = {}

    for transaction in dataset.transactions:
        key = (transaction.store_id, transaction.sku_id, transaction.business_date)
        units_by_store_sku_day[key] += transaction.units
        inventory_by_store_sku_day[key] = transaction.inventory_on_hand

    assert units_by_store_sku_day
    assert all(
        units <= inventory_by_store_sku_day[key] for key, units in units_by_store_sku_day.items()
    )
    assert any(transaction.is_stockout for transaction in dataset.transactions)
