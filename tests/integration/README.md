# Integration Test Environment

This directory contains disposable database containers for integration testing.

## Quick Start

### Basic PG18 + Cloudberry Environment

Start the test environment:
```bash
cd tests/integration
docker compose up -d
docker compose ps
```

Wait for health checks:
```bash
docker compose ps
# Both containers should show "healthy"
```

Connect to databases:
```bash
# PostgreSQL 18 source
psql "postgresql://postgres:pg2cb_test@127.0.0.1:55432/source"

# Cloudberry 2.1 target
psql "postgresql://postgres:pg2cb_test@127.0.0.1:55433/target"
```

Stop and clean up:
```bash
docker compose down -v
```

## Container Details

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| pg18-source | postgres:18-alpine | 55432 | Source database with logical replication |
| cloudberry-target | apache/cloudberry:2.1.0-incubating | 55433 | Target data warehouse |

> **Cloudberry image caveat:** Apache Cloudberry 2.1 does not yet publish an
> official ready-to-run server image on Docker Hub — only build/test environment
> images exist (`apache/incubator-cloudberry:cbdb-build-*`). The
> `cloudberry-target` image above is a placeholder; running target integration
> tests requires a Cloudberry built from source or the upstream sandbox
> `run.sh`, or a CI job that provisions one. The PG18 source service runs as-is
> and all `source-postgres` `--ignored` integration tests pass against it.

### Running integration tests (WSL)

Windows cannot reach the WSL Docker port bindings directly, so run the DB
integration tests **inside WSL** where `127.0.0.1:5543x` resolves. Use a WSL
target dir (compiling on `/mnt/c` is slow):

```bash
source $HOME/.cargo/env
cd /mnt/c/code/etl_server_cloudberry
export PG2CB_TEST_SOURCE_DSN="postgres://postgres:pg2cb_test@127.0.0.1:55432/source"
export CARGO_TARGET_DIR=/tmp/pg2cb-target
cargo test -p cloudberry-etl-source-postgres --test snapshot_page_pg18 -- --ignored --nocapture
```

## Test Data

The PG18 container is initialized with sample tables:
- `integration.test_simple` - basic table with PK
- `integration.test_composite_pk` - composite primary key
- `integration.test_types` - all supported PostgreSQL types

## Citus Environment

For Citus-specific tests, see `citus/` subdirectory:
```bash
cd citus
./verify.sh  # Starts coordinator + 2 workers, runs verification
```

## Environment Variables

Both containers use default test credentials:
- Username: `postgres`
- Password: `pg2cb_test`
- Database: `source` (PG18), `target` (Cloudberry)

**DO NOT use these credentials in production.**
