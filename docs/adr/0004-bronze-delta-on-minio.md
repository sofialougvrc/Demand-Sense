# ADR 0004: Bronze Delta Table on MinIO

## Status

Accepted

## Context

The lakehouse layer needs an actual table format on local object storage so downstream
silver and gold assets can rely on transaction-log metadata, schema tracking, and
repeatable reads. Iceberg would also be a good fit, but a realistic Iceberg setup usually
adds a catalog service or Spark configuration before it becomes pleasant for local
development.

## Decision

Use Delta Lake via `delta-rs` for the bronze table and store it on MinIO at
`s3://demand-sense/bronze/debezium_events`.

## Rationale

- Delta Lake gives this milestone a real lakehouse table format without introducing a
  separate catalog service yet.
- `delta-rs` can write from Python directly, which keeps the bronze landing path small
  and independently testable.
- The bronze table stores the raw Debezium envelope as JSON plus Kafka/source metadata,
  preserving the event as received while making later transforms easier to filter.

## Consequences

- Future silver transforms should treat bronze as append-only raw CDC events and perform
  typing, de-duplication, and delete/update semantics downstream.
- If the project later chooses Iceberg for production parity, this ADR should be revised
  and the bronze writer swapped behind the same logical interface.
- Local MinIO needs S3-compatible Delta storage options, including path-style addressing
  and unsafe rename support for local development.
