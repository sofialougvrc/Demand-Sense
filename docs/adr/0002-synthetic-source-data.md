# ADR 0002: Synthetic Source Data Shape

## Status

Accepted

## Context

Demand-Sense needs source data that is rich enough to support forecasting, interval calibration, CDC, data quality checks, and inventory optimization without depending on a proprietary retail dataset.

## Decision

Generate deterministic synthetic store/SKU transaction data in Python and write it to normalized Postgres source tables under the `retail` schema.

## Rationale

- Transaction-level sales give the CDC and lakehouse layers realistic event volume.
- Explicit store, product, and promotion dimensions make later model features explainable.
- Deterministic seeded generation keeps tests and demos reproducible.
- Inventory-capped observed sales and stockout flags preserve the distinction between latent demand and what the store actually sold.

## Consequences

- The data is realistic enough for portfolio engineering and modeling workflows, but it is still simulated and should not be presented as real sales history.
- Future backtesting should account for censored demand when stockouts occur.
- The generator must stay documented as source-data simulation, not as the production forecasting logic.
