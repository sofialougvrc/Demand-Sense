from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from math import pi, sin
from uuid import NAMESPACE_URL, uuid5

import numpy as np


@dataclass(frozen=True)
class GeneratorConfig:
    start_date: date = date(2025, 1, 1)
    days: int = 180
    store_count: int = 8
    sku_count: int = 30
    seed: int = 42

    def __post_init__(self) -> None:
        if self.days <= 0:
            raise ValueError("days must be positive")
        if self.store_count <= 0:
            raise ValueError("store_count must be positive")
        if self.sku_count <= 0:
            raise ValueError("sku_count must be positive")


@dataclass(frozen=True)
class StoreProfile:
    store_id: str
    store_name: str
    region: str
    store_format: str
    square_feet: int
    opened_on: date
    demand_multiplier: float


@dataclass(frozen=True)
class ProductProfile:
    sku_id: str
    product_name: str
    category: str
    brand: str
    unit_cost: float
    unit_price: float
    shelf_life_days: int
    base_daily_demand: float
    seasonality_strength: float
    seasonal_phase: int
    price_elasticity: float


@dataclass(frozen=True)
class Promotion:
    promotion_id: str
    sku_id: str
    store_id: str
    start_date: date
    end_date: date
    discount_pct: float
    feature_display: bool
    campaign_name: str

    def is_active_on(self, business_date: date) -> bool:
        return self.start_date <= business_date <= self.end_date


@dataclass(frozen=True)
class SalesTransaction:
    transaction_id: str
    store_id: str
    sku_id: str
    sale_ts: datetime
    business_date: date
    units: int
    unit_price: float
    discount_pct: float
    gross_revenue: float
    net_revenue: float
    inventory_on_hand: int
    is_stockout: bool
    promotion_id: str | None


@dataclass(frozen=True)
class SyntheticRetailDataset:
    stores: list[StoreProfile]
    products: list[ProductProfile]
    promotions: list[Promotion]
    transactions: list[SalesTransaction]


REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]
STORE_FORMATS = {
    "urban express": (12_000, 0.75),
    "neighborhood": (28_000, 1.00),
    "suburban": (45_000, 1.25),
    "club": (72_000, 1.55),
}
CATEGORIES = {
    "beverages": ("Sparkling Water", "Cold Brew", "Sports Drink", "Iced Tea"),
    "pantry": ("Pasta Sauce", "Granola", "Rice Bowl", "Nut Butter"),
    "household": ("Paper Towels", "Dish Soap", "Laundry Pods", "Trash Bags"),
    "personal care": ("Shampoo", "Body Wash", "Toothpaste", "Hand Soap"),
    "fresh": ("Yogurt", "Salad Kit", "Berry Pack", "Hummus"),
}
BRANDS = ["Northstar", "Harbor", "Fieldday", "Blue Mesa", "Evergreen", "Canyon"]
WEEKDAY_MULTIPLIERS = {
    0: 0.86,
    1: 0.90,
    2: 0.96,
    3: 1.05,
    4: 1.20,
    5: 1.34,
    6: 1.15,
}


def generate_dataset(config: GeneratorConfig | None = None) -> SyntheticRetailDataset:
    config = config or GeneratorConfig()
    rng = np.random.default_rng(config.seed)
    stores = generate_stores(config, rng)
    products = generate_products(config, rng)
    promotions = generate_promotions(config, stores, products, rng)
    transactions = generate_transactions(config, stores, products, promotions, rng)
    return SyntheticRetailDataset(stores, products, promotions, transactions)


def generate_stores(config: GeneratorConfig, rng: np.random.Generator) -> list[StoreProfile]:
    stores: list[StoreProfile] = []
    formats = list(STORE_FORMATS)

    for idx in range(1, config.store_count + 1):
        store_format = formats[(idx - 1) % len(formats)]
        base_square_feet, base_multiplier = STORE_FORMATS[store_format]
        region = REGIONS[(idx - 1) % len(REGIONS)]
        local_multiplier = float(rng.lognormal(mean=0.0, sigma=0.12))

        stores.append(
            StoreProfile(
                store_id=f"STORE-{idx:03d}",
                store_name=f"{region} {store_format.title()} {idx:03d}",
                region=region,
                store_format=store_format,
                square_feet=int(base_square_feet * rng.uniform(0.9, 1.12)),
                opened_on=date(2018, 1, 1) + timedelta(days=int(rng.integers(0, 1800))),
                demand_multiplier=round(base_multiplier * local_multiplier, 4),
            )
        )

    return stores


def generate_products(config: GeneratorConfig, rng: np.random.Generator) -> list[ProductProfile]:
    category_names = list(CATEGORIES)
    products: list[ProductProfile] = []

    for idx in range(1, config.sku_count + 1):
        category = category_names[(idx - 1) % len(category_names)]
        item_type = CATEGORIES[category][int(rng.integers(0, len(CATEGORIES[category])))]
        brand = BRANDS[int(rng.integers(0, len(BRANDS)))]
        unit_cost = round(float(rng.uniform(1.2, 9.5)), 2)
        margin = float(rng.uniform(1.35, 2.25))
        shelf_life_days = 14 if category == "fresh" else int(rng.choice([90, 180, 365, 540]))

        products.append(
            ProductProfile(
                sku_id=f"SKU-{idx:04d}",
                product_name=f"{brand} {item_type}",
                category=category,
                brand=brand,
                unit_cost=unit_cost,
                unit_price=round(unit_cost * margin, 2),
                shelf_life_days=shelf_life_days,
                base_daily_demand=round(float(rng.uniform(4.0, 28.0)), 3),
                seasonality_strength=round(float(rng.uniform(0.06, 0.28)), 3),
                seasonal_phase=int(rng.integers(0, 365)),
                price_elasticity=round(float(rng.uniform(1.0, 2.8)), 3),
            )
        )

    return products


def generate_promotions(
    config: GeneratorConfig,
    stores: list[StoreProfile],
    products: list[ProductProfile],
    rng: np.random.Generator,
) -> list[Promotion]:
    promotions: list[Promotion] = []
    date_window = max(config.days - 21, 1)
    candidate_count = max(1, int(len(stores) * len(products) * 0.08))

    for idx in range(1, candidate_count + 1):
        store = stores[int(rng.integers(0, len(stores)))]
        product = products[int(rng.integers(0, len(products)))]
        start_offset = int(rng.integers(0, date_window))
        duration = int(rng.integers(7, 22))
        start = config.start_date + timedelta(days=start_offset)
        end = min(
            config.start_date + timedelta(days=config.days - 1), start + timedelta(days=duration)
        )

        promotions.append(
            Promotion(
                promotion_id=f"PROMO-{idx:05d}",
                sku_id=product.sku_id,
                store_id=store.store_id,
                start_date=start,
                end_date=end,
                discount_pct=round(float(rng.choice([0.10, 0.15, 0.20, 0.25, 0.30])), 2),
                feature_display=bool(rng.random() < 0.35),
                campaign_name=f"{product.category.title()} Demand Builder",
            )
        )

    return promotions


def generate_transactions(
    config: GeneratorConfig,
    stores: list[StoreProfile],
    products: list[ProductProfile],
    promotions: list[Promotion],
    rng: np.random.Generator,
) -> list[SalesTransaction]:
    transactions: list[SalesTransaction] = []
    promotions_by_key = _index_promotions(promotions)

    for day_offset in range(config.days):
        business_date = config.start_date + timedelta(days=day_offset)

        for store in stores:
            for product in products:
                promotion = _active_promotion(
                    promotions_by_key.get((store.store_id, product.sku_id), []),
                    business_date,
                )
                expected = expected_daily_demand(
                    business_date=business_date,
                    day_offset=day_offset,
                    store=store,
                    product=product,
                    promotion=promotion,
                    noise_multiplier=float(rng.lognormal(mean=0.0, sigma=0.18)),
                )
                latent_units = int(rng.poisson(lam=max(expected, 0.1)))
                inventory_on_hand = int(
                    max(0, round(expected * rng.uniform(0.65, 1.95) + rng.normal(2.0, 3.0)))
                )
                observed_units = min(latent_units, inventory_on_hand)
                is_stockout = latent_units > inventory_on_hand

                if observed_units == 0:
                    continue

                transactions.extend(
                    split_daily_sales_into_transactions(
                        business_date=business_date,
                        store=store,
                        product=product,
                        units=observed_units,
                        inventory_on_hand=inventory_on_hand,
                        is_stockout=is_stockout,
                        promotion=promotion,
                        rng=rng,
                    )
                )

    return transactions


def expected_daily_demand(
    *,
    business_date: date,
    day_offset: int,
    store: StoreProfile,
    product: ProductProfile,
    promotion: Promotion | None,
    noise_multiplier: float = 1.0,
) -> float:
    weekly = WEEKDAY_MULTIPLIERS[business_date.weekday()]
    annual = 1.0 + product.seasonality_strength * sin(
        2 * pi * ((business_date.timetuple().tm_yday - product.seasonal_phase) / 365)
    )
    trend = 1.0 + min(day_offset * 0.00045, 0.18)
    payday = 1.08 if business_date.day in {1, 15, 30, 31} else 1.0
    promotion_lift = 1.0

    if promotion is not None:
        display_lift = 0.18 if promotion.feature_display else 0.0
        promotion_lift += promotion.discount_pct * product.price_elasticity + display_lift

    demand = (
        product.base_daily_demand
        * store.demand_multiplier
        * weekly
        * annual
        * trend
        * payday
        * promotion_lift
        * noise_multiplier
    )
    return max(demand, 0.05)


def split_daily_sales_into_transactions(
    *,
    business_date: date,
    store: StoreProfile,
    product: ProductProfile,
    units: int,
    inventory_on_hand: int,
    is_stockout: bool,
    promotion: Promotion | None,
    rng: np.random.Generator,
) -> list[SalesTransaction]:
    if units <= 0:
        return []

    transaction_count = int(min(units, max(1, rng.poisson(lam=max(units / 3.5, 1.0)))))
    allocations = [
        int(value) for value in rng.multinomial(units, [1 / transaction_count] * transaction_count)
    ]
    non_zero_allocations = [allocation for allocation in allocations if allocation > 0]
    discount_pct = promotion.discount_pct if promotion is not None else 0.0
    effective_unit_price = round(product.unit_price * (1 - discount_pct), 2)

    transactions: list[SalesTransaction] = []
    for idx, allocation in enumerate(non_zero_allocations, start=1):
        sale_ts = _random_sale_timestamp(business_date, rng)
        gross_revenue = round(allocation * product.unit_price, 2)
        net_revenue = round(allocation * effective_unit_price, 2)
        transaction_key = (
            f"{business_date.isoformat()}:{store.store_id}:{product.sku_id}:"
            f"{idx}:{allocation}:{sale_ts.isoformat()}"
        )

        transactions.append(
            SalesTransaction(
                transaction_id=str(uuid5(NAMESPACE_URL, transaction_key)),
                store_id=store.store_id,
                sku_id=product.sku_id,
                sale_ts=sale_ts,
                business_date=business_date,
                units=allocation,
                unit_price=product.unit_price,
                discount_pct=discount_pct,
                gross_revenue=gross_revenue,
                net_revenue=net_revenue,
                inventory_on_hand=inventory_on_hand,
                is_stockout=is_stockout,
                promotion_id=promotion.promotion_id if promotion is not None else None,
            )
        )

    return transactions


def _index_promotions(promotions: Iterable[Promotion]) -> dict[tuple[str, str], list[Promotion]]:
    indexed: dict[tuple[str, str], list[Promotion]] = {}
    for promotion in promotions:
        indexed.setdefault((promotion.store_id, promotion.sku_id), []).append(promotion)
    return indexed


def _active_promotion(promotions: list[Promotion], business_date: date) -> Promotion | None:
    return next(
        (promotion for promotion in promotions if promotion.is_active_on(business_date)), None
    )


def _random_sale_timestamp(business_date: date, rng: np.random.Generator) -> datetime:
    hour = int(rng.choice(np.arange(8, 22), p=_hourly_sales_weights()))
    minute = int(rng.integers(0, 60))
    second = int(rng.integers(0, 60))
    return datetime.combine(business_date, time(hour, minute, second), tzinfo=UTC)


def _hourly_sales_weights() -> np.ndarray:
    hours = np.arange(8, 22)
    lunch_peak = np.exp(-0.5 * ((hours - 12) / 2.2) ** 2)
    evening_peak = np.exp(-0.5 * ((hours - 18) / 2.6) ** 2)
    weights = lunch_peak + 1.35 * evening_peak + 0.15
    return weights / weights.sum()
