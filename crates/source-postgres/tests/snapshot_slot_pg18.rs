//! Opt-in PostgreSQL 18 coverage for the initial snapshot consistency boundary.
//!
//! Run explicitly with a disposable PG18 instance:
//! `PG2CB_TEST_SOURCE_DSN=postgres://... cargo test -p cloudberry-etl-source-postgres --test snapshot_slot_pg18 -- --ignored --nocapture`

use cloudberry_etl_source_postgres::{
    SourceError, SourceResult,
    snapshot::set_snapshot_sql,
    snapshot_slot::{SnapshotSlotGuard, SnapshotSlotState},
};
use tokio_postgres::{Client, IsolationLevel, NoTls, Transaction};
use uuid::Uuid;

struct TestObjects {
    schema: String,
    table: String,
    slot: String,
}

impl TestObjects {
    fn new() -> Self {
        let suffix = Uuid::now_v7().simple().to_string();
        Self {
            schema: format!("pg2cb_snapshot_s_{suffix}"),
            table: format!("items_{suffix}"),
            slot: format!("pg2cb_snapshot_slot_{suffix}"),
        }
    }

    fn table_sql(&self) -> String {
        format!(
            "{}.{}",
            quote_identifier(&self.schema),
            quote_identifier(&self.table)
        )
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 18 instance and PG2CB_TEST_SOURCE_DSN"]
async fn exported_slot_snapshot_holds_concurrent_write_boundary() -> SourceResult<()> {
    let dsn = std::env::var("PG2CB_TEST_SOURCE_DSN").map_err(|_| {
        SourceError::Contract(
            "PG2CB_TEST_SOURCE_DSN is required for the ignored integration test".to_owned(),
        )
    })?;
    let (admin, admin_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let admin_task = spawn_connection(admin_connection, "admin");
    let objects = TestObjects::new();

    let result = run_test(&dsn, &admin, &objects).await;
    cleanup(&admin, &objects).await;
    admin_task.abort();
    result
}

async fn run_test(dsn: &str, admin: &Client, objects: &TestObjects) -> SourceResult<()> {
    let version: i32 = admin
        .query_one("SELECT current_setting('server_version_num')::int", &[])
        .await?
        .try_get(0)?;
    assert_eq!(version / 10_000, 18);

    let schema = quote_identifier(&objects.schema);
    let table = objects.table_sql();
    admin
        .batch_execute(&format!(
            "CREATE SCHEMA {schema};
             CREATE TABLE {table} (id bigint PRIMARY KEY, payload text NOT NULL);
             INSERT INTO {table} VALUES (1, 'before')"
        ))
        .await?;

    let mut guard = SnapshotSlotGuard::create(dsn, &objects.slot, 2).await?;
    assert_eq!(guard.snapshot().slot_name, objects.slot);
    assert_ne!(guard.snapshot().consistent_point.as_u64(), 0);
    assert!(!guard.snapshot().snapshot_name.is_empty());
    assert_eq!(guard.state(), SnapshotSlotState::Created);
    assert!(matches!(
        guard.release(),
        Err(SourceError::SnapshotReadersPending {
            ready: 0,
            expected: 2
        })
    ));

    // This commit is newer than the replication slot's consistent point.  Both readers must
    // exclude it even though they import the exported snapshot after the write commits.
    admin
        .execute(
            &format!("INSERT INTO {table} VALUES ($1, $2)"),
            &[&2_i64, &"after"],
        )
        .await?;

    let (mut reader_a, reader_a_connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let reader_a_task = spawn_connection(reader_a_connection, "reader-a");
    let (mut reader_b, reader_b_connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let reader_b_task = spawn_connection(reader_b_connection, "reader-b");

    let transaction_a = import_snapshot(&mut reader_a, &guard).await?;
    guard.mark_reader_ready()?;
    let transaction_b = import_snapshot(&mut reader_b, &guard).await?;
    guard.mark_reader_ready()?;
    assert_eq!(guard.state(), SnapshotSlotState::ReadersReady);

    assert_visible_ids(&transaction_a, &table, &[1]).await?;
    assert_visible_ids(&transaction_b, &table, &[1]).await?;
    guard.release()?;
    assert_eq!(guard.state(), SnapshotSlotState::Released);

    // Imported transactions remain pinned after the exporting replication connection closes.
    assert_visible_ids(&transaction_a, &table, &[1]).await?;
    assert_visible_ids(&transaction_b, &table, &[1]).await?;
    transaction_a.rollback().await?;
    transaction_b.rollback().await?;
    reader_a_task.abort();
    reader_b_task.abort();

    let current_ids: Vec<i64> = admin
        .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(current_ids, vec![1, 2]);
    let slot_exists: bool = admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&objects.slot],
        )
        .await?
        .try_get(0)?;
    assert!(slot_exists, "release must not remove the WAL slot");
    Ok(())
}

async fn import_snapshot<'client>(
    client: &'client mut Client,
    guard: &SnapshotSlotGuard,
) -> SourceResult<Transaction<'client>> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    transaction
        .batch_execute(&set_snapshot_sql(&guard.snapshot().snapshot_name)?)
        .await?;
    Ok(transaction)
}

async fn assert_visible_ids(
    transaction: &Transaction<'_>,
    table: &str,
    expected: &[i64],
) -> SourceResult<()> {
    let ids: Vec<i64> = transaction
        .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(ids, expected);
    Ok(())
}

async fn cleanup(client: &Client, objects: &TestObjects) {
    // All names are generated by this test.  Cleanup remains narrowly scoped and best-effort so
    // it never masks the original assertion or protocol error.
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
               FROM pg_replication_slots
              WHERE slot_name = $1 AND NOT active",
            &[&objects.slot],
        )
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&objects.schema)
        ))
        .await;
}

fn spawn_connection<S>(
    connection: tokio_postgres::Connection<tokio_postgres::Socket, S>,
    label: &'static str,
) -> tokio::task::JoinHandle<()>
where
    S: tokio_postgres::tls::TlsStream + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test {label} connection ended: {error}");
        }
    })
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
