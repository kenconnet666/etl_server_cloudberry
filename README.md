# ETL Server Cloudberry

`etl-server-cloudberry` mirrors the current row state of supported PostgreSQL
18 tables into Apache Cloudberry. The V3 runtime currently executes standalone
PostgreSQL pipelines only. Physical-HA and Citus topology values fail explicitly
at startup; Citus catalog discovery and its opt-in integration environment are
present as validation work, not as an end-to-end replication capability.

The delivery contract is at-least-once with idempotent primary-key application
and eventual convergence. A selected table that violates the source contract
currently prevents that pipeline from starting. It is never silently skipped.

The accepted final direction is broader than the current V3 boundary. Large
transactions will use bounded memory plus a versioned disk spool instead of a
transaction-size failure. Safe DDL will be applied online; changes that cannot
be proven safe will rebuild the affected table or dependency group while WAL
ingest continues. Standalone, physical HA, and Citus will share the same
node-stream, spool, schema-transition, and checkpoint-completion model.

The project is intentionally scoped to PostgreSQL as the source and
Cloudberry as the destination. See [docs/architecture.md](docs/architecture.md)
and [docs/source-contract.md](docs/source-contract.md) before deploying a
pipeline. The phased implementation and old-path removal gates are tracked in
[docs/delivery-plan.md](docs/delivery-plan.md).

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

## Bootstrap configuration

[`etl-server-cloudberry.toml`](etl-server-cloudberry.toml) is the checked-in
bootstrap example. Engine durations are expressed in seconds:

```toml
[engine]
reconcile_interval_seconds = 2
lease_ttl_seconds = 30
lease_renew_interval_seconds = 10
restart_backoff_initial_seconds = 1
restart_backoff_max_seconds = 60
restart_backoff_reset_seconds = 300
```

`lease_renew_interval_seconds` must be shorter than `lease_ttl_seconds`; all six
values must be positive, and the initial restart backoff must not exceed its
maximum. Connection and master-key secrets are read from the environment names
configured under `[control]` and `[security]`.

## Development

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

The integration environment uses WSL Docker on Windows. Exact image versions
and lifecycle commands are documented under `tests/integration`.

Functional tests may cross the Windows/WSL virtual network using the reachable
`10.144.144.4` and `10.144.144.5` addresses. Peak throughput and latency tests
run PostgreSQL, the service, and Cloudberry in one Linux/WSL instance to remove
virtual-network noise; cross-boundary benchmarks are reported separately.
