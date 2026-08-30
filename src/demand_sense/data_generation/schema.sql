CREATE SCHEMA IF NOT EXISTS retail;

CREATE TABLE IF NOT EXISTS retail.stores (
    store_id TEXT PRIMARY KEY,
    store_name TEXT NOT NULL,
    region TEXT NOT NULL,
    store_format TEXT NOT NULL,
    square_feet INTEGER NOT NULL CHECK (square_feet > 0),
    opened_on DATE NOT NULL,
    demand_multiplier NUMERIC(8, 4) NOT NULL CHECK (demand_multiplier > 0)
);

CREATE TABLE IF NOT EXISTS retail.products (
    sku_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    brand TEXT NOT NULL,
    unit_cost NUMERIC(10, 2) NOT NULL CHECK (unit_cost >= 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= unit_cost),
    shelf_life_days INTEGER NOT NULL CHECK (shelf_life_days > 0),
    base_daily_demand NUMERIC(10, 3) NOT NULL CHECK (base_daily_demand > 0),
    seasonality_strength NUMERIC(6, 3) NOT NULL CHECK (seasonality_strength >= 0),
    seasonal_phase INTEGER NOT NULL CHECK (seasonal_phase BETWEEN 0 AND 364),
    price_elasticity NUMERIC(6, 3) NOT NULL CHECK (price_elasticity > 0)
);

CREATE TABLE IF NOT EXISTS retail.promotions (
    promotion_id TEXT PRIMARY KEY,
    sku_id TEXT NOT NULL REFERENCES retail.products (sku_id),
    store_id TEXT NOT NULL REFERENCES retail.stores (store_id),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    discount_pct NUMERIC(5, 2) NOT NULL CHECK (discount_pct >= 0 AND discount_pct < 1),
    feature_display BOOLEAN NOT NULL,
    campaign_name TEXT NOT NULL,
    CHECK (end_date >= start_date)
);

CREATE TABLE IF NOT EXISTS retail.sales_transactions (
    transaction_id UUID PRIMARY KEY,
    store_id TEXT NOT NULL REFERENCES retail.stores (store_id),
    sku_id TEXT NOT NULL REFERENCES retail.products (sku_id),
    sale_ts TIMESTAMPTZ NOT NULL,
    business_date DATE NOT NULL,
    units INTEGER NOT NULL CHECK (units > 0),
    unit_price NUMERIC(10, 2) NOT NULL CHECK (unit_price >= 0),
    discount_pct NUMERIC(5, 2) NOT NULL CHECK (discount_pct >= 0 AND discount_pct < 1),
    gross_revenue NUMERIC(12, 2) NOT NULL CHECK (gross_revenue >= 0),
    net_revenue NUMERIC(12, 2) NOT NULL CHECK (net_revenue >= 0),
    inventory_on_hand INTEGER NOT NULL CHECK (inventory_on_hand >= 0),
    is_stockout BOOLEAN NOT NULL,
    promotion_id TEXT REFERENCES retail.promotions (promotion_id),
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sales_transactions_store_sku_date
    ON retail.sales_transactions (store_id, sku_id, business_date);

CREATE INDEX IF NOT EXISTS idx_promotions_store_sku_dates
    ON retail.promotions (store_id, sku_id, start_date, end_date);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'demand_sense_publication') THEN
        CREATE PUBLICATION demand_sense_publication FOR ALL TABLES;
    END IF;
END
$$;
