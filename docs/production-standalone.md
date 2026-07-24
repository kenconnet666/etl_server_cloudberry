# Standalone production runbook

This runbook covers one active `etl-server-cloudberry` process replicating PostgreSQL 18 into
Apache Cloudberry 2.1. It does not claim Citus support or logical-slot continuity across a
PostgreSQL primary failover. A source identity/timeline change stops safely and requires a new
snapshot generation.

## Supported behavior

- Source tables need a stable non-null primary key and all selected types must pass the source
  contract. Text primary keys need `COLLATE "C"` or `COLLATE "POSIX"`.
- The target storage default is Cloudberry AOCO (`ao_column`). PAX remains experimental.
- DML is at-least-once with idempotent primary-key application and target-durable checkpoints.
- Large transactions spill to a bounded durable volume. The spool is not the recovery authority;
  source WAL plus the target checkpoint are authoritative.
- DDL and reconciliation mismatches use a table-local shadow reload. Online ALTER is not part of
  this production scope, so affected tables trade temporary availability for proven correctness.
- In-memory administrator sessions expire on an application restart. Pipeline ownership and data
  recovery do not depend on those sessions.

Read [source-contract.md](source-contract.md) before onboarding a database.

## Host and database prerequisites

- A Linux host with Docker Engine 26+ and Compose v2, persistent SSD storage, DNS, and inbound
  TCP 80/443 for certificate issuance.
- PostgreSQL 18 with `wal_level=logical` and reserved `max_replication_slots` and
  `max_wal_senders`. Size `max_slot_wal_keep_size` above the largest accepted target outage and
  monitor the service WAL-retention metrics.
- A source identity that can replicate, read selected tables/catalogs, create the publication and
  slot, and install the database-level event triggers plus `pg2cb_meta`. Installing event triggers
  normally requires a controlled privileged bootstrap.
- Cloudberry 2.1 with a dedicated database and role able to create target schemas, AOCO tables,
  and `pg2cb_meta` metadata. Do not use the repository's local demo image in production.
- Source and target DSNs using `sslmode=verify-full` and trusted CA roots whenever traffic leaves
  a host-local trusted network.

Reserve spool capacity of at least:

```text
peak source WAL bytes/second * maximum target outage seconds + largest expected transaction
```

Keep additional filesystem headroom above `transaction.minimum_free_disk_bytes`. Defaults are a
256 GiB high-water mark and 8 GiB minimum free space; lower them explicitly when the volume is
smaller. Crossing either threshold pauses reads and acknowledgements rather than dropping data.

## Deployment

The production Compose bundle is in [`deploy/standalone`](../deploy/standalone). It provides a
private PostgreSQL 18 control database, one active app, automatic migrations, durable named
volumes, Caddy TLS termination, and optional internal Prometheus scraping. `/metrics` and
`/health/ready` are reachable only on the private Compose network; Caddy returns 404 for them.

The control database volume is mounted at `/var/lib/postgresql`, as required by the PostgreSQL 18
official image's major-version directory layout. Do not reuse a PostgreSQL 17 volume by changing
the image tag; perform an explicit `pg_upgrade` or logical migration into a fresh volume.

1. Copy `.env.example` to `.env`, set a real DNS name and ACME contact, and point the DNS record at
   the host.
2. Create the four files named by `compose.yaml` under `secrets/`, owned by the deploy account and
   mode `0600`. Never use the checked-in `.example` values.
3. Generate a URL-safe control password with `openssl rand -hex 32`. Put it in
   `control_db_password`, and use the same value in `control_database_url`.
4. Generate `master_key` with `openssl rand -base64 32`.
5. Generate `admin_password_hash` with `etl-server-cloudberry hash-password`; the administrator
   password must contain at least 12 characters.
6. Validate and start:

```bash
cd deploy/standalone
docker compose config --quiet
docker compose build app
docker compose run --rm migrate check-config --config /etc/pg2cb/etl-server-cloudberry.toml
docker compose up -d
docker compose ps
```

The image runs as UID 10001, with a read-only root filesystem, dropped capabilities, explicit CPU
and memory limits, and `/var/lib/pg2cb` as the only writable persistent app volume. Change the
defaults in `.env` only after measuring peak RSS and recovery throughput.

Start internal Prometheus when an external scraper is not available:

```bash
docker compose --profile monitoring up -d prometheus
```

Prometheus is intentionally not published on a host port. Attach an authenticated monitoring
gateway or use a host-local tunnel; do not expose it directly.

## Release and rollback

Before every upgrade, record the current image digest and take a control database backup. Build or
pull the candidate image, run `migrate` as a one-shot job, then recreate the app. Migrations are
forward-only; rolling the binary back after a schema migration is allowed only when that release's
notes explicitly say it accepts the new control schema. Otherwise restore the control backup and
the previous image together while the app is stopped.

Use the checked-in backup helper so the archive is written atomically, kept mode `0600`, and
verified before it is reported successful. Set `PG2CB_BACKUP_DIR` to a filesystem outside the
Compose volumes (preferably a mounted backup target):

```bash
export PG2CB_BACKUP_DIR=/srv/backups/pg2cb
./control-db-backup.sh backup
./control-db-backup.sh verify /srv/backups/pg2cb/pg2cb-control-20260723T120000Z.dump
./control-db-backup.sh restore-drill /srv/backups/pg2cb/pg2cb-control-20260723T120000Z.dump
```

`restore-drill` creates a temporary database inside the control container, restores the custom
archive with `pg_restore --exit-on-error`, checks that the control schema exists, and drops the
temporary database on success or failure. It does not overwrite the live `pg2cb_control` database.

For a planned rollback, retain the backup created immediately before the upgrade together with the
immutable digest of the preceding release. `rollback-drill.sh` is intentionally destructive to the
live control database: it stops the app and proxy, restores that archive, runs the old image's
migration command once, starts that exact image, and verifies both health and image identity. It
emits the backup SHA-256, backup age at rollback start (the control-plane RPO bound), and measured
RTO when `PG2CB_ROLLBACK_RESULT_FILE` is set.

```bash
export PG2CB_PREVIOUS_IMAGE='registry.example/etl-server-cloudberry@sha256:<digest>'
export PG2CB_CONTROL_BACKUP=/srv/backups/pg2cb/pg2cb-control-20260723T120000Z.dump
export PG2CB_ROLLBACK_RESULT_FILE=/srv/backups/pg2cb/rollback-20260723T120500Z.json
./rollback-drill.sh
```

Do not use a mutable image tag for a production rollback. The script rejects it unless the
explicit `PG2CB_ALLOW_MUTABLE_IMAGE=1` override is set for a disposable local drill.

```bash
./control-db-backup.sh backup
docker compose build app
docker compose run --rm migrate
docker compose up -d --no-deps app
docker compose up -d --no-deps caddy
```

Back up Cloudberry business tables and `pg2cb_meta` with the site's Cloudberry procedure. The spool
volume is transient recovery state and must not be restored independently from an older source
slot/target checkpoint pair.

## Incident checks

Alert immediately when the app is down, a desired-running pipeline has no owner, the source slot
approaches its retention limit, or apply remains stalled with lag. A short `RESOURCE_WAIT` is
recoverable; if it lasts five minutes, restore target connectivity or disk capacity before WAL
retention reaches the rebuild threshold.

For a process or host crash:

1. Preserve the source replication slot and the Cloudberry target metadata.
2. Restore the same master key and control database.
3. Mount an empty or surviving writable spool volume and start exactly one app instance.
4. Confirm `/health/ready`, `pg2cb_pipelines_running`, checkpoint movement, and eventual
   reconciliation success before clearing the incident.

For source identity/timeline change, lost slot, WAL invalidation, or target metadata loss, stop the
pipeline and request a new generation. Never edit an LSN, fencing token, snapshot manifest, or
chunk ledger by hand.

## Routine operations

- Daily: inspect pipeline state, WAL safety margin, lag, last apply/ACK timestamps, spool bytes, and
  disk free space.
- Weekly: verify the control backup can be listed/restored and review quarantined table retention.
- Monthly: run an exact reconciliation outside peak hours and review source/target certificate
  expiry.
- Before schema changes: verify the DDL is within the source contract and budget for a table-local
  reload. Keep enough WAL and spool capacity for the reload duration.

## Long-run release gate

The ignored external-process soak is the release evidence path, not a production service mode. It
must run from Linux/WSL against disposable PostgreSQL 18 and Cloudberry 2.1 endpoints, with the
source and target containers dedicated to the run. The process E2E binary serializes its fault
injection cases because container restarts and TCP cuts are host-wide operations.

For a 24-hour candidate gate:

```bash
export PG2CB_TEST_SOURCE_DSN='postgres://postgres:<password>@127.0.0.1:55432/source'
export PG2CB_TEST_TARGET_DSN='postgres://gpadmin@127.0.0.1:55433/target'
export PG2CB_TEST_SOURCE_CONTAINER=pg2cb-it-pg18
export PG2CB_TEST_TARGET_CONTAINER=cbdb
export PG2CB_SOAK_SECONDS=86400
export PG2CB_SOAK_SAMPLE_INTERVAL_SECONDS=30
export PG2CB_SOAK_SAMPLE_FILE="$PWD/target/standalone-soak-24h.csv"
export PG2CB_SOAK_RESULT_FILE="$PWD/target/standalone-soak-24h.json"
cargo test -p etl-server-cloudberry --test standalone_process_e2e \
  standalone_mixed_workload_soak --all-features -- --ignored --nocapture --test-threads=1
bash tests/integration/standalone-soak-gate.sh \
  "$PG2CB_SOAK_RESULT_FILE" 86400
```

The gate rejects missing samples, null lag percentiles, sampling errors, an unrepaired injected
reconciliation mismatch, or a run shorter than the requested observation window. The CSV is for
time-series review; the JSON is the machine-readable release artifact. A 30/120-second run is a
regression signal only and must not be reported as 24-hour or 72-hour stability evidence.
