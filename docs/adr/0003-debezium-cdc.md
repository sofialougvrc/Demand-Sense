# ADR 0003: Debezium CDC for Retail Source Tables

## Status

Accepted

## Context

Demand-Sense needs a credible source-to-stream path so later lakehouse milestones can consume change events rather than batch-copying source tables. The milestone 1 stack already runs Postgres, Kafka, and Debezium Connect locally.

## Decision

Use Debezium's Postgres connector with `pgoutput`, the `demand_sense_publication` publication, and a dedicated replication slot named `demand_sense_slot`. Capture all current `retail` source tables into Kafka topics prefixed with `demand_sense`.

## Rationale

- Debezium is the target architecture's intended CDC technology and works with the existing local services.
- Capturing all source tables keeps product, store, promotion, and transaction events available for bronze landing.
- An initial snapshot makes local demos reproducible after seeding Postgres, while WAL streaming keeps later changes flowing.
- The Python CDC command keeps connector registration testable and avoids relying on manual REST calls.

## Consequences

- Local development requires the Debezium Connect service to be healthy before `make cdc-register`.
- Reseeding source data after connector registration will emit delete/truncate and insert activity into Kafka.
- No Python simulated CDC fallback is implemented because Debezium is currently viable in the local stack.
