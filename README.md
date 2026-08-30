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

## Why this exists

Most demand-forecasting projects stop at a forecast and report MAPE. That's not the
decision retailers actually have to make. Given uncertain demand, how many units do you
order? Order too few and you stock out; order too many and you eat holding cost. This
project treats the forecast interval as an input to that decision (a newsvendor-style
optimization), and evaluates the whole pipeline on the metric that actually matters:
simulated dollar cost of stockouts + overstock, compared against a naive ordering policy.

## Architecture

```
                      ┌─────────────┐
  synthetic sales  →  │  Postgres   │
  generator            │ (source DB) │
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
Kafka into a medallion lakehouse (bronze → silver → gold), with each transform gated by
automated data quality checks so bad upstream data can't silently reach the model. A
conformalized quantile regression model produces calibrated demand intervals from the
gold layer, which a newsvendor-style optimizer converts into an order quantity per
SKU/store. A backtest framework and dashboard compare that policy against a naive
baseline in terms of simulated dollar cost.

## Stack

| Layer | Tool | Why |
|---|---|---|
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

## Project structure

```
Demand-Sense/
├── src/                  # Python package: generator, pipeline, model, optimizer
├── docker-compose.yml    # Postgres, Kafka, Debezium Connect, MinIO
├── docs/
│   └── adr/              # Architecture decision records
├── tests/
├── Makefile              # setup, check, run targets
└── README.md
```

## Getting started

```bash
git clone https://github.com/sofialougvrc/Demand-Sense.git
cd Demand-Sense
make setup      # installs dependencies, sets up dev environment
docker compose up -d   # brings up Postgres, Kafka, Debezium, MinIO
make check      # formatting, linting, tests
```

## Known simplifications

This section tracks any place where a local-dev shortcut was taken instead of the full
production-shape approach (e.g. simulated CDC instead of real Debezium), so the gap
between what this demonstrates and what a real production deployment would need stays
honest and visible.
