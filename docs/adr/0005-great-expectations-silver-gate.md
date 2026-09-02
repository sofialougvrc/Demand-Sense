# ADR 0005: Great Expectations Gate for Bronze to Silver

## Status

Accepted

## Context

Silver should be the first trustworthy lakehouse layer. Bronze intentionally preserves raw
Debezium events, so the bronze-to-silver boundary is where required metadata, valid CDC
operations, JSON parseability, and duplicate Kafka coordinates must be enforced.

## Decision

Use a Great Expectations suite named `bronze_to_silver_cdc_events` as a blocking gate
inside the silver build command. If the suite fails, the command raises an error and does
not write the silver Delta table.

## Rationale

- The validation is part of the transform path rather than a separate report, so bad
  bronze data cannot silently flow downstream.
- Expectations run against a pandas dataframe loaded from the bronze Delta table, which
  keeps the milestone small and independently testable.
- The silver table remains a cleaned CDC event table rather than business aggregates;
  aggregate modeling features belong in the gold milestone.

## Consequences

- Large bronze tables may eventually need chunked or distributed validation.
- Future Dagster assets should call the same quality gate before materializing silver.
- Silver consumers can trust Kafka coordinates to be unique and common CDC metadata to be
  present and typed.
