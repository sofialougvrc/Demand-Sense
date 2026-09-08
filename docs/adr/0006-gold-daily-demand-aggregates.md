# ADR 0006: Gold Daily Demand Aggregates

## Status

Accepted

## Context

Forecasting and inventory optimization need a stable business-grain table rather than raw
CDC events. Silver contains cleaned event records, but downstream models should consume
store/SKU/day facts with demand, revenue, stockout, and promotion signals.

## Decision

Build a Delta Lake gold table at `s3://demand-sense/gold/store_sku_daily_demand` with one
row per store, SKU, and business date. The build command runs a blocking Great
Expectations suite named `silver_to_gold_retail_cdc_events` before writing gold.

## Rationale

- Store/SKU/day is the natural grain for the first forecasting and inventory policy
  milestones.
- The transform interprets CDC semantics by keeping the latest event per transaction and
  excluding transactions whose latest event is a delete.
- A second Great Expectations suite keeps the silver-to-gold boundary explicit and
  independently testable.

## Consequences

- Current gold demand uses observed sales, so stockout-censored demand still needs care
  during forecasting and backtesting.
- Product, store, and promotion dimension enrichment can be added to gold or feature
  tables once modeling requirements are clearer.
- Future Dagster assets should call this same validation and aggregation path.
