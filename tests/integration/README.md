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
