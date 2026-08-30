# ADR 0001: Local Development Stack

## Status

Accepted

## Context

Demand-Sense needs a realistic local platform for source transactions, streaming events, and lakehouse storage. The stack should support Debezium CDC in a later milestone without forcing a redesign.

## Decision

Use Docker Compose with Postgres 16, Kafka, Debezium Connect, and MinIO.

## Rationale

- Postgres with logical WAL settings mirrors the expected transactional source.
- Kafka plus Debezium Connect keeps the CDC path explicit instead of replacing it with a simulated producer too early.
- MinIO provides an S3-compatible target for future Iceberg or Delta tables while staying laptop-friendly.

## Consequences

- The local stack has more moving pieces than a pure Python demo.
- Milestone 3 will still need an actual Debezium connector configuration and validation path.
- If Debezium proves too heavy later, the fallback producer must be documented as a simplification rather than hidden behind the same interface.
