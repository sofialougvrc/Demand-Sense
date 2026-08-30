# Contributing

Demand-Sense is being built milestone by milestone. Each milestone should leave the repository runnable, tested where applicable, committed, and pushed before the next milestone begins.

## Local workflow

1. Copy `.env.example` to `.env` and adjust ports or credentials if needed.
2. Start the local platform services with `make up`.
3. Run validation checks with `make check`.
4. Stop local services with `make down`.

## Engineering standards

- Keep the README aligned with what the project can actually do today.
- Add focused tests for non-trivial logic as features are introduced.
- Prefer small commits that map to a single milestone or cohesive change.
- Document meaningful tradeoffs in `docs/adr/` when a decision affects architecture.
- Treat generated data, object-store files, and local service state as disposable runtime artifacts.

## Commit style

Use imperative, specific commit messages. Examples:

- `Add local platform scaffold for milestone 1`
- `Implement synthetic sales generator and seed command`
- `Add newsvendor optimizer tests`
