# Integration tests

Integration tests use disposable databases and are opt-in locally. They never use a DSN unless
the corresponding `PG2CB_TEST_*` environment variable is set.

## PostgreSQL 18

Start a dedicated source instance:

```text
docker run --rm -d --name pg2cb-it-pg18 \
  -e POSTGRES_PASSWORD=pg2cb_test_only -e POSTGRES_DB=source \
  -p 127.0.0.1:55432:5432 postgres:18.4-alpine \
  -c wal_level=logical -c max_replication_slots=16 -c max_wal_senders=16
```

Then run the ignored suite with `PG2CB_TEST_SOURCE_DSN` set to that database:

```text
cargo test -p cloudberry-etl-source-postgres --test postgres18 -- --ignored --test-threads=1
```

Stop the disposable container with `docker stop pg2cb-it-pg18`. The container uses `--rm`, so its
database is removed and cannot be recovered after it stops.

Cloudberry and Citus suites remain validation-gated until their pinned environments and full
correctness matrices are checked in here.
