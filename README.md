# ETL Server Cloudberry

`etl-server-cloudberry` mirrors the current row state of supported PostgreSQL
18 tables into Apache Cloudberry. It supports standalone PostgreSQL and a
validation-gated Citus topology. The delivery contract is at-least-once with
idempotent primary-key application and eventual convergence.

The project is intentionally scoped to PostgreSQL as the source and
Cloudberry as the destination. See [docs/architecture.md](docs/architecture.md)
and [docs/source-contract.md](docs/source-contract.md) before deploying a
pipeline.

## Workspace

- `crates/core`: protocol-independent domain types and invariants.
- `crates/config`: bootstrap configuration and validation.
- `crates/metadata`: control-plane persistence, encryption, and leases.
- `crates/source-postgres`: PostgreSQL catalog, snapshot, WAL, DDL, and Citus.
- `crates/target-cloudberry`: target schema, staging, apply, and checkpoints.
- `crates/engine`: pipeline lifecycle, batching, recovery, and reconciliation.
- `crates/api`: authenticated management HTTP API.
- `crates/app`: command-line entry point and process lifecycle.
- `web`: Svelte management interface.

## Development

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

The integration environment uses WSL Docker on Windows. Exact image versions
and lifecycle commands are documented under `tests/integration`.

