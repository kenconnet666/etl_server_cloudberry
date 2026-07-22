# Integration Test Environment

This directory contains disposable database containers for integration testing.

## Quick Start

### Basic PG18 + Cloudberry Environment

Start PostgreSQL 18:
```bash
cd tests/integration
docker compose up -d pg18-source
```

Build and start the runnable Cloudberry 2.1 single-node cluster:
```bash
bash cloudberry/build-local-image.sh
docker ps
```

Connect to databases:
```bash
# PostgreSQL 18 source
psql "postgresql://postgres:pg2cb_test@127.0.0.1:55432/source"

# Cloudberry 2.1 target
psql "postgresql://gpadmin@127.0.0.1:55433/target"
```

Stop and clean up:
```bash
docker compose down -v
docker rm -f cbdb
```

## Container Details

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| pg18-source | postgres:18-alpine | 55432 | Source database with logical replication |
| Cloudberry 2.1 | locally built `cbdb-local:2.1.0` | 55433 | Target data warehouse |

> **Cloudberry image caveat:** Apache Cloudberry 2.1 does not publish a
> ready-to-run server image. `cloudberry/build-local-image.sh` installs the
> official 2.1.0 EL9 RPM into Rocky Linux 9 and initializes a single-node demo
> cluster with `gpinitsystem`. The `cloudberry-target` compose service remains a
> historical placeholder and is not part of the supported startup path.

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

Test credentials:
- PostgreSQL 18: `postgres` / `pg2cb_test`, database `source`.
- Cloudberry 2.1: `gpadmin`, database `target` (local trust authentication in the disposable cluster).

**DO NOT use these credentials in production.**
