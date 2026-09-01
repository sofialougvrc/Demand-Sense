# Demand-Sense

Demand forecasting isn't the hard part. Knowing how much to order is. Demand-Sense is a
retail/CPG demand forecasting platform that produces **calibrated uncertainty intervals**,
not just point forecasts, and feeds them directly into an **inventory optimization layer**
so the output of the pipeline is a defensible order quantity per SKU/store, with a
quantified dollar impact versus a naive baseline.

It's built as a full data science + data engineering system: real event ingestion, a
medallion lakehouse, orchestrated transforms, automated data quality gates, a
from-scratch conformal forecasting model, and a decision layer on top, not a notebook
that assumes clean input.

## Current Status

Milestone 3 is complete: the repository has a runnable local platform stack,
deterministic synthetic retail source data in Postgres, and a Debezium Postgres
connector that publishes CDC events into Kafka. Lakehouse tables, forecasting models,
optimizer, and dashboard have not been implemented yet.

## Why This Exists

Most demand-forecasting projects stop at a forecast and report MAPE. That's not the
decision retailers actually have to make. Given uncertain demand, how many units do you
order? Order too few and you stock out; order too many and you eat holding cost. This
project treats the forecast interval as an input to that decision, using a
newsvendor-style optimization, and evaluates the whole pipeline on the metric that
actually matters: simulated dollar cost of stockouts + overstock compared against a naive
ordering policy.

## Architecture

```text
                      ┌─────────────┐
  synthetic sales  →  │  Postgres   │
  generator           │ (source DB) │
                      └──────┬──────┘
                             │ CDC (Debezium)
                             ▼
                      ┌─────────────┐
                      │    Kafka    │
                      └──────┬──────┘
                             ▼
                 ┌───────────────────────┐
                 │  Lakehouse (MinIO/S3)  │
                 │  bronze → silver → gold│
                 │  (Iceberg/Delta)       │
                 └──────────┬────────────┘
                             │ orchestrated by Dagster
                             │ gated by Great Expectations
                             ▼
                 ┌───────────────────────┐
                 │  Forecasting (CQR)     │
                 │  calibrated intervals  │
                 └──────────┬────────────┘
                             ▼
                 ┌───────────────────────┐
                 │  Newsvendor optimizer  │
                 │  order qty per SKU     │
                 └──────────┬────────────┘
                             ▼
                 ┌───────────────────────┐
                 │  Backtest + Dashboard  │
                 │  $ impact vs. baseline │
                 └───────────────────────┘
```

Raw sales events are captured off Postgres via change-data-capture and streamed through
Kafka into a medallion lakehouse, with each transform gated by automated data quality
checks so bad upstream data can't silently reach the model. A conformalized quantile
regression model will produce calibrated demand intervals from the gold layer, which a
newsvendor-style optimizer will convert into an order quantity per SKU/store. A backtest
framework and dashboard will compare that policy against a naive baseline in terms of
simulated dollar cost.

## Stack

| Layer | Tool | Why |
| --- | --- | --- |
| Source DB | Postgres | Realistic transactional source for CDC |
| CDC | Debezium → Kafka | Production-shape change-data-capture, not batch polling |
| Object storage | MinIO (S3-compatible) | Local dev stand-in for S3 |
| Table format | Iceberg / Delta Lake | Medallion architecture with schema evolution |
| Orchestration | Dagster | Asset-based, testable pipeline definitions |
| Data quality | Great Expectations | Gates bronze→silver→gold, fails loudly on bad data |
| Forecasting | LightGBM + custom CQR | Calibrated prediction intervals, not just point estimates |
| Decision layer | Custom newsvendor optimizer | Turns forecast uncertainty into an order quantity |
| Dashboard | Streamlit | Stakeholder-readable view of forecast + $ impact |

See [`docs/adr/`](docs/adr/) for the reasoning behind these choices, including any place
where a local-dev simplification was made instead of the full production-shape tool.

## Project Structure

```text
Demand-Sense/
├── src/                  # Python package: generator, pipeline, model, optimizer
├── docker-compose.yml    # Postgres, Kafka, Debezium Connect, MinIO
├── docs/
│   └── adr/              # Architecture decision records
├── tests/
├── Makefile              # setup, check, run targets
└── README.md
```

## Getting Started

```bash
git clone https://github.com/sofialougvrc/Demand-Sense.git
cd Demand-Sense
cp .env.example .env
make setup
make up
make ps
```

Seed synthetic retail data into Postgres:

```bash
make seed
```

Or customize the generated footprint:

```bash
make seed SEED_ARGS="--start-date 2025-01-01 --days 365 --stores 12 --skus 50 --seed 42"
```

Register the Debezium CDC connector and inspect Kafka topics:

```bash
make cdc-register
make cdc-status
make cdc-topics
```

Run local checks:

```bash
make check
```

Stop services:

```bash
make down
```

If a service fails during startup, run `docker compose ps` and
`docker compose logs <service-name>`. The first startup can take a few minutes while
Docker downloads images and health checks wait for dependent services.

## Synthetic Source Data

The milestone 2 generator creates source data under the `retail` schema in Postgres:

| Table | Grain | Purpose |
| --- | --- | --- |
| `retail.stores` | One row per store | Store region, format, size, and demand multiplier |
| `retail.products` | One row per SKU | Product category, brand, price, cost, shelf life, baseline demand, and elasticity |
| `retail.promotions` | One row per SKU/store campaign | Discount windows and display flags used to lift demand |
| `retail.sales_transactions` | One row per synthetic checkout transaction | Observed unit sales, revenue, inventory cap, stockout flag, and promotion reference |

Generation is deterministic for a fixed seed and includes weekly seasonality, annual
seasonality, payday effects, promotion lift, store-format effects, multiplicative noise,
and inventory-capped observed sales. The seed command applies the schema and
truncates/reloads the retail tables by default; pass `--append` only when intentionally
adding another generated slice.

## CDC Event Pipeline

Milestone 3 uses Debezium's Postgres connector against the same local Docker network as
Postgres. The connector reads the `retail` schema through Postgres logical replication
and publishes one topic per captured source table:

| Source table | Kafka topic |
| --- | --- |
| `retail.stores` | `demand_sense.retail.stores` |
| `retail.products` | `demand_sense.retail.products` |
| `retail.promotions` | `demand_sense.retail.promotions` |
| `retail.sales_transactions` | `demand_sense.retail.sales_transactions` |

Useful commands:

```bash
make cdc-register
make cdc-status
make cdc-delete
make cdc-topics
```

The connector is configured for an initial snapshot plus ongoing WAL changes. No
simulated CDC fallback is used in the current implementation.

## Local Services

| Service | Purpose | Local URL |
| --- | --- | --- |
| Postgres | Transactional source database with logical WAL settings for CDC | `localhost:5432` |
| Kafka | Event streaming backbone | `localhost:9092` |
| Debezium Connect | CDC connector runtime | `http://localhost:8083` |
| MinIO | Local S3-compatible object store for lakehouse tables | `http://localhost:9000` |
| MinIO Console | Object-store admin UI | `http://localhost:9001` |

## Milestones

1. Repo scaffolding: folder structure, README stub, license, `.gitignore`, Docker Compose, contributing notes. Done.
2. Synthetic data generator and Postgres schema. Done.
3. CDC/event pipeline into Kafka, with documented fallback only if Debezium is too heavy for local development. Done.
4. Bronze landing in the lakehouse.
5. Silver transform and first Great Expectations suite.
6. Gold aggregation and second Great Expectations suite.
7. Dagster orchestration for steps 4-6.
8. Seasonal-naive forecast and evaluation harness.
9. CQR-based forecasting model with calibrated intervals.
10. Newsvendor optimization layer.
11. Backtest framework with dollar-cost comparison.
12. Dashboard.
13. Business and model documentation.
14. Basic CI.

## Known Simplifications

This section tracks any place where a local-dev shortcut was taken instead of the full
production-shape approach, so the gap between what this demonstrates and what a real
production deployment would need stays honest and visible.

- No simulated CDC fallback is currently used; Debezium is running locally.
- Iceberg or Delta Lake has not been selected yet; that decision belongs to the bronze
  lakehouse milestone.
