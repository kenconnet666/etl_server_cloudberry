//! Opt-in PostgreSQL 18 coverage for online reconciliation source boundaries.
//!
//! Run with a disposable PostgreSQL 18 source:
//! `PG2CB_TEST_SOURCE_DSN=postgres://... cargo test -p cloudberry-etl-source-postgres --test reconciliation_boundary_pg18 -- --ignored --nocapture`

use cloudberry_etl_source_postgres::{
    SourceError, SourceResult,
    connection::canonical_startup_options,
    reconciliation_boundary::{ReconciliationBoundaryGuard, emit_transactional_marker},
    snapshot::set_snapshot_sql,
};
use tokio_postgres::{Client, Config, IsolationLevel, NoTls, Transaction};
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
            schema: format!("pg2cb_reconcile_s_{suffix}"),
            table: format!("items_{suffix}"),
            slot: format!("pg2cb_reconcile_{suffix}"),
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
async fn temporary_boundary_marker_and_cleanup_are_consistent() -> SourceResult<()> {
    let dsn = std::env::var("PG2CB_TEST_SOURCE_DSN").map_err(|_| {
        SourceError::Contract(
            "PG2CB_TEST_SOURCE_DSN is required for the ignored integration test".to_owned(),
        )
    })?;
    let (admin, admin_task) = connect_canonical(&dsn, "admin").await?;
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

    let mut guard = ReconciliationBoundaryGuard::create(dsn, &objects.slot).await?;
    let boundary = guard.boundary().clone();
    let database: String = admin
        .query_one("SELECT current_database()", &[])
        .await?
        .try_get(0)?;
    assert_eq!(boundary.source_database, database);
    assert_eq!(boundary.slot_name, objects.slot);
    assert_ne!(boundary.consistent_point.as_u64(), 0);
    assert!(!boundary.snapshot_name.is_empty());
    assert_ne!(boundary.system_identifier, 0);
    assert_ne!(boundary.timeline, 0);

    let slot = admin
        .query_one(
            "SELECT temporary, active, plugin, database
               FROM pg_replication_slots
              WHERE slot_name = $1",
            &[&objects.slot],
        )
        .await?;
    assert!(slot.try_get::<_, bool>(0)?);
    assert!(slot.try_get::<_, bool>(1)?);
    assert_eq!(slot.try_get::<_, String>(2)?, "pgoutput");
    assert_eq!(
        slot.try_get::<_, Option<String>>(3)?.as_deref(),
        Some(database.as_str())
    );

    admin
        .execute(
            &format!("INSERT INTO {table} VALUES ($1, $2)"),
            &[&2_i64, &"after"],
        )
        .await?;

    let (mut reader, reader_task) = connect_canonical(dsn, "snapshot-reader").await?;
    let snapshot = import_snapshot(&mut reader, &boundary.snapshot_name).await?;
    assert_visible_ids(&snapshot, &table, &[1]).await?;

    let (mut marker_client, marker_task) = connect_canonical(dsn, "marker").await?;
    let marker_id = Uuid::now_v7();
    let emitted = emit_transactional_marker(&mut marker_client, &boundary, marker_id).await?;
    assert_eq!(emitted.marker.marker_id, marker_id);
    assert_eq!(emitted.marker.boundary_lsn, boundary.consistent_point);
    assert!(emitted.message_lsn > boundary.consistent_point);

    guard.cleanup().await?;
    guard.cleanup().await?;
    assert!(guard.is_cleaned());
    let slot_exists: bool = admin
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&objects.slot],
        )
        .await?
        .try_get(0)?;
    assert!(!slot_exists);

    // An imported repeatable-read transaction remains pinned after the temporary slot and its
    // exporting connection are gone.
    assert_visible_ids(&snapshot, &table, &[1]).await?;
    snapshot.rollback().await?;
    reader_task.abort();
    marker_task.abort();

    let ids: Vec<i64> = admin
        .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
        .await?
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(ids, vec![1, 2]);
    Ok(())
}

async fn import_snapshot<'client>(
    client: &'client mut Client,
    snapshot_name: &str,
) -> SourceResult<Transaction<'client>> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    transaction
        .batch_execute(&set_snapshot_sql(snapshot_name)?)
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

async fn connect_canonical(
    dsn: &str,
    label: &'static str,
) -> SourceResult<(Client, tokio::task::JoinHandle<()>)> {
    let mut config: Config = dsn.parse().map_err(|error| {
        SourceError::Contract(format!("invalid PG2CB_TEST_SOURCE_DSN: {error}"))
    })?;
    let options = canonical_startup_options(config.get_options());
    config.options(options);
    let (client, connection) = config.connect(NoTls).await?;
    let task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test {label} connection ended: {error}");
        }
    });
    Ok((client, task))
}

async fn cleanup(client: &Client, objects: &TestObjects) {
    // A failed guard closes its session and PostgreSQL removes the temporary slot. This query only
    // handles a prior test process that exited after the slot unexpectedly became persistent.
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

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
