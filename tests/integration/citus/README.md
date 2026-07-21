# PostgreSQL 18 + Citus integration cluster

This directory owns a pinned, three-node Citus environment for mainline integration work:

- PostgreSQL 18.4 and Citus 14.1-1
- one coordinator and two workers
- logical WAL and Citus change data capture enabled on every node
- eight hash shards for a table whose primary key includes the distribution column

The official image does not publish a `14.1.0-pg18` tag. PostgreSQL 18 is the default build for
`citusdata/citus:14.1.0`, pinned here to digest
`sha256:100bb457c798d7b50de1ece819fad7109744c0ed812dc63fb76a90e99767602f`.

## Run and verify

The WSL Docker engine and Docker Compose v2 must be available. From PowerShell:

```powershell
.\tests\integration\citus\verify.ps1
```

From WSL:

```sh
bash ./tests/integration/citus/verify.sh
```

The verifier checks every node's PostgreSQL/Citus versions and logical replication settings, then
checks worker registration, shard placement, CRUD results, and tenant routing across both workers.
A successful cluster is deliberately left running.

## Run the Rust adapter integration test

After the verifier leaves the cluster running, execute the opt-in source adapter test from WSL:

```sh
cd /mnt/c/code/etl_server_cloudberry
export PG2CB_TEST_CITUS_COORDINATOR_DSN='postgresql://postgres:pg2cb_citus_test_only@127.0.0.1:55440/source?sslmode=disable'
export PG2CB_TEST_CITUS_WORKER1_DSN='postgresql://postgres:pg2cb_citus_test_only@127.0.0.1:55441/source?sslmode=disable'
export PG2CB_TEST_CITUS_WORKER2_DSN='postgresql://postgres:pg2cb_citus_test_only@127.0.0.1:55442/source?sslmode=disable'
cargo test -p cloudberry-etl-source-postgres --test citus14_pg18 -- --ignored --nocapture --test-threads=1
```

The test reads all three nodes and limits catalog discovery to the `integration` schema. It verifies
the official Citus coordinator role API, physical system identities, CDC configuration, active
primary topology, and the `tenant_id` distribution key on `integration.accounts`. It does not
create or validate a publication or logical replication slot, and does not claim a cluster-wide
CDC order.

| Node | Address |
| --- | --- |
| Coordinator | `127.0.0.1:55440` |
| Worker 1 | `127.0.0.1:55441` |
| Worker 2 | `127.0.0.1:55442` |

Test credentials are `postgres` / `pg2cb_citus_test_only`; the database is `source`. These
credentials and ports are only for this loopback-bound integration environment.

On failure, complete Compose logs and status are retained under `artifacts/`. The verifier removes
containers, volumes, and the network only when the failed project did not exist before that run. It
never removes images or unrelated Docker resources.

## Stop and remove test data

Run this only when the retained integration data is no longer needed:

```sh
cd tests/integration/citus
docker-compose --project-name pg2cb-citus-it --file compose.yaml down --volumes --remove-orphans
```

The command permanently removes this test cluster's three named data volumes. It does not affect
other Compose projects or Docker images.
