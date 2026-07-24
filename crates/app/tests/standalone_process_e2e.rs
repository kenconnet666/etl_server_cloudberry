#![cfg(unix)]

//! External-process recovery proof for the Standalone data plane.
//!
//! The test blocks Cloudberry apply behind an ACCESS EXCLUSIVE lock, waits until the committed
//! source transaction is durably spooled, sends SIGKILL to the real service binary, expires the
//! durable lease, and starts a fresh process. Recovery must converge exactly and retire the spool.

use std::{
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::OnceLock,
    time::{Duration, Instant},
};

use chrono::Utc;
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
    schema::QualifiedName,
};
use cloudberry_etl_engine::runtime::{TargetStorage, replication_names};
use cloudberry_etl_metadata::{
    crypto::{MasterKey, source_credential_aad, target_credential_aad},
    migration::migrate_control_database,
    model::{PipelineDefinition, SourceProfile, TargetProfile},
    store::{ControlStore, PostgresControlStore, configure_control_session},
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use secrecy::SecretString;
use serde_json::json;
use tokio::{
    io::copy_bidirectional,
    net::{TcpListener, TcpStream},
    sync::{Mutex, watch},
    task::JoinHandle,
    time::sleep,
};
use tokio_postgres::{Client, NoTls};
use url::Url;
use uuid::Uuid;

const TEST_MASTER_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const TEST_ADMIN_HASH: &str = "$argon2id$v=19$m=16384,t=2,p=1$cGcyY2J0ZXN0c2FsdDEyMw$HJwLH1iZRaNJzPkUWMiK8TfSxXA0nQMcZIw8XJN59wo";
const WAIT_TIMEOUT: Duration = Duration::from_secs(90);
const LEASE_TTL_SECONDS: u64 = 6;
const BULK_ROWS: i64 = 4_096;

static PROCESS_E2E_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

async fn acquire_process_e2e_lock() -> tokio::sync::MutexGuard<'static, ()> {
    PROCESS_E2E_LOCK
        .get_or_init(|| Mutex::const_new(()))
        .lock()
        .await
}

#[derive(Debug, Clone, Copy, Default)]
struct SoakMetadata {
    active_tables: i64,
    quarantined_tables: i64,
    snapshot_groups: i64,
    schema_events: i64,
    table_transitions: i64,
    reconciliation_runs: i64,
    reconciliation_log_rows: i64,
}

impl SoakMetadata {
    fn update_max(&mut self, sample: Self) {
        self.active_tables = self.active_tables.max(sample.active_tables);
        self.quarantined_tables = self.quarantined_tables.max(sample.quarantined_tables);
        self.snapshot_groups = self.snapshot_groups.max(sample.snapshot_groups);
        self.schema_events = self.schema_events.max(sample.schema_events);
        self.table_transitions = self.table_transitions.max(sample.table_transitions);
        self.reconciliation_runs = self.reconciliation_runs.max(sample.reconciliation_runs);
        self.reconciliation_log_rows = self
            .reconciliation_log_rows
            .max(sample.reconciliation_log_rows);
    }

    fn as_json(self) -> serde_json::Value {
        json!({
            "active_tables": self.active_tables,
            "quarantined_tables": self.quarantined_tables,
            "snapshot_groups": self.snapshot_groups,
            "schema_events": self.schema_events,
            "table_transitions": self.table_transitions,
            "reconciliation_runs": self.reconciliation_runs,
            "reconciliation_log_rows": self.reconciliation_log_rows,
        })
    }

    fn delta_from(self, baseline: Self) -> serde_json::Value {
        json!({
            "active_tables": self.active_tables - baseline.active_tables,
            "quarantined_tables": self.quarantined_tables - baseline.quarantined_tables,
            "snapshot_groups": self.snapshot_groups - baseline.snapshot_groups,
            "schema_events": self.schema_events - baseline.schema_events,
            "table_transitions": self.table_transitions - baseline.table_transitions,
            "reconciliation_runs": self.reconciliation_runs - baseline.reconciliation_runs,
            "reconciliation_log_rows": self.reconciliation_log_rows
                - baseline.reconciliation_log_rows,
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CpuTicks {
    process: u64,
    system: u64,
}

#[derive(Debug)]
struct SoakSample {
    timestamp_unix_ms: i64,
    elapsed_ms: u64,
    source_rows: Option<i64>,
    target_rows: Option<i64>,
    lag_bytes: Option<u64>,
    rss_bytes: Option<u64>,
    spool_bytes: u64,
    retained_wal_bytes: Option<u64>,
    cpu_percent: Option<f64>,
    metadata: Option<SoakMetadata>,
}

impl SoakSample {
    fn write_csv(&self, file: &mut File) -> io::Result<()> {
        let metadata = self.metadata.unwrap_or_default();
        writeln!(
            file,
            "{},{},{},{},{},{},{},{},{:.3},{},{},{},{},{},{},{}",
            self.timestamp_unix_ms,
            self.elapsed_ms,
            display_optional(self.source_rows),
            display_optional(self.target_rows),
            display_optional(self.lag_bytes),
            display_optional(self.rss_bytes),
            self.spool_bytes,
            display_optional(self.retained_wal_bytes),
            self.cpu_percent.unwrap_or_default(),
            metadata.active_tables,
            metadata.quarantined_tables,
            metadata.snapshot_groups,
            metadata.schema_events,
            metadata.table_transitions,
            metadata.reconciliation_runs,
            metadata.reconciliation_log_rows,
        )
    }
}

struct SoakEvidence {
    sample_file: Option<File>,
    cpu_ticks: Option<CpuTicks>,
    lag_samples: Vec<u64>,
    cpu_samples: Vec<f64>,
    max_rss_bytes: u64,
    max_spool_bytes: u64,
    max_retained_wal_bytes: u64,
    metadata_max: SoakMetadata,
    sample_count: u64,
    sample_query_errors: u64,
}

impl SoakEvidence {
    fn new(sample_file: Option<File>, metadata: SoakMetadata) -> Self {
        Self {
            sample_file,
            cpu_ticks: None,
            lag_samples: Vec::new(),
            cpu_samples: Vec::new(),
            max_rss_bytes: 0,
            max_spool_bytes: 0,
            max_retained_wal_bytes: 0,
            metadata_max: metadata,
            sample_count: 0,
            sample_query_errors: 0,
        }
    }

    fn record(&mut self, sample: SoakSample) -> io::Result<()> {
        if sample.source_rows.is_none()
            || sample.target_rows.is_none()
            || sample.lag_bytes.is_none()
            || sample.rss_bytes.is_none()
            || sample.retained_wal_bytes.is_none()
            || sample.metadata.is_none()
        {
            self.sample_query_errors += 1;
        }
        if let Some(lag_bytes) = sample.lag_bytes {
            self.lag_samples.push(lag_bytes);
        }
        if let Some(cpu_percent) = sample.cpu_percent {
            self.cpu_samples.push(cpu_percent);
        }
        self.max_rss_bytes = self.max_rss_bytes.max(sample.rss_bytes.unwrap_or_default());
        self.max_spool_bytes = self.max_spool_bytes.max(sample.spool_bytes);
        self.max_retained_wal_bytes = self
            .max_retained_wal_bytes
            .max(sample.retained_wal_bytes.unwrap_or_default());
        if let Some(metadata) = sample.metadata {
            self.metadata_max.update_max(metadata);
        }
        if let Some(file) = self.sample_file.as_mut() {
            sample.write_csv(file)?;
            file.flush()?;
        }
        self.sample_count += 1;
        Ok(())
    }
}

fn display_optional<T: std::fmt::Display>(value: Option<T>) -> String {
    value.map_or_else(String::new, |value| value.to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires real PG18/Cloudberry and sends SIGKILL to a child service process"]
async fn standalone_process_sigkill_replays_spool_after_lease_expiry() -> Result<(), Box<dyn Error>>
{
    let _lock = acquire_process_e2e_lock().await;
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let mut fixture = Fixture::setup(&source_dsn, &target_dsn).await?;
    let result = run_process_recovery(&mut fixture).await;
    fixture.cleanup().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires real PG18/Cloudberry and cuts live source/target TCP connections"]
async fn standalone_source_and_target_network_disconnects_recover() -> Result<(), Box<dyn Error>> {
    let _lock = acquire_process_e2e_lock().await;
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let source_proxy = TcpProxy::start(dsn_socket_addr(&source_dsn)?).await?;
    let target_proxy = TcpProxy::start(dsn_socket_addr(&target_dsn)?).await?;
    let proxied_source_dsn = proxy_dsn(&source_dsn, source_proxy.address())?;
    let proxied_target_dsn = proxy_dsn(&target_dsn, target_proxy.address())?;
    let mut fixture = Fixture::setup_with_profiles(
        &source_dsn,
        &target_dsn,
        &proxied_source_dsn,
        &proxied_target_dsn,
    )
    .await?;
    let result = async {
        let mut process = fixture.spawn_service()?;
        wait_for_target_count(&fixture.target, &fixture.target_schema, 1, &mut process).await?;

        target_proxy.cut();
        fixture.insert_rows(2, 512, 128).await?;
        sleep(Duration::from_secs(2)).await;
        process.ensure_running()?;
        target_proxy.restore();
        wait_for_target_count(&fixture.target, &fixture.target_schema, 513, &mut process).await?;

        source_proxy.cut();
        fixture.insert_rows(514, 512, 128).await?;
        sleep(Duration::from_secs(2)).await;
        process.ensure_running()?;
        source_proxy.restore();
        wait_for_target_count(&fixture.target, &fixture.target_schema, 1_025, &mut process).await?;

        assert_source_target_equal(
            &fixture.source,
            &fixture.target,
            &fixture.source_schema,
            &fixture.target_schema,
        )
        .await?;
        let rebuilds: i64 = fixture
            .control
            .query_one(
                "SELECT count(*) FROM cloudberry_etl_control.operations
                  WHERE pipeline_id=$1 AND operation_type='rebuild'",
                &[&fixture.pipeline.id.as_uuid()],
            )
            .await?
            .get(0);
        assert_eq!(
            rebuilds, 0,
            "transient disconnects must not request rebuild"
        );
        let status = process.terminate_and_wait()?;
        assert!(
            status.success(),
            "SIGTERM should complete graceful shutdown"
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    fixture.cleanup().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production matrix: stops and starts explicitly named PG18/Cloudberry containers"]
async fn standalone_source_and_target_container_restarts_recover() -> Result<(), Box<dyn Error>> {
    let _lock = acquire_process_e2e_lock().await;
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let source_container = std::env::var("PG2CB_TEST_SOURCE_CONTAINER")?;
    let target_container = std::env::var("PG2CB_TEST_TARGET_CONTAINER")?;
    let mut fixture = Fixture::setup(&source_dsn, &target_dsn).await?;
    let result = async {
        let mut process = fixture.spawn_service()?;
        wait_for_target_count(&fixture.target, &fixture.target_schema, 1, &mut process).await?;

        let mut stopped_target = StoppedContainer::stop(&target_container)?;
        fixture.insert_rows(2, 256, 128).await?;
        sleep(Duration::from_secs(2)).await;
        process.ensure_running()?;
        stopped_target.start()?;
        fixture.reconnect_target().await?;
        wait_for_target_count(&fixture.target, &fixture.target_schema, 257, &mut process).await?;

        let mut stopped_source = StoppedContainer::stop(&source_container)?;
        sleep(Duration::from_secs(2)).await;
        process.ensure_running()?;
        stopped_source.start()?;
        fixture.reconnect_source_and_control().await?;
        fixture.insert_rows(258, 256, 128).await?;
        wait_for_target_count(&fixture.target, &fixture.target_schema, 513, &mut process).await?;

        assert_source_target_equal(
            &fixture.source,
            &fixture.target,
            &fixture.source_schema,
            &fixture.target_schema,
        )
        .await?;
        let status = process.terminate_and_wait()?;
        assert!(
            status.success(),
            "SIGTERM should complete graceful shutdown"
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    fixture.cleanup().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production soak: set PG2CB_SOAK_SECONDS to run mixed DML/DDL/reconciliation"]
async fn standalone_mixed_workload_soak() -> Result<(), Box<dyn Error>> {
    let _lock = acquire_process_e2e_lock().await;
    let Ok(duration_seconds) = std::env::var("PG2CB_SOAK_SECONDS") else {
        eprintln!("PG2CB_SOAK_SECONDS is unset; skipping long-running soak body");
        return Ok(());
    };
    let duration_seconds: u64 = duration_seconds.parse()?;
    if !(30..=7 * 24 * 60 * 60).contains(&duration_seconds) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "PG2CB_SOAK_SECONDS must be between 30 seconds and 7 days",
        )
        .into());
    }
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let mut fixture = Fixture::setup(&source_dsn, &target_dsn).await?;
    fixture.enable_fast_reconciliation().await?;
    let result = run_soak(&mut fixture, Duration::from_secs(duration_seconds)).await;
    fixture.cleanup().await;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "production DDL matrix: requires real PG18 and Cloudberry 2.1"]
async fn standalone_rapid_ddl_drop_recreate_and_reconciliation_converge()
-> Result<(), Box<dyn Error>> {
    let _lock = acquire_process_e2e_lock().await;
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let mut fixture = Fixture::setup(&source_dsn, &target_dsn).await?;
    fixture.enable_fast_reconciliation().await?;
    let result = run_ddl_matrix(&mut fixture).await;
    fixture.cleanup().await;
    result
}

async fn run_ddl_matrix(fixture: &mut Fixture) -> Result<(), Box<dyn Error>> {
    let mut process = fixture.spawn_service()?;
    wait_for_target_count(&fixture.target, &fixture.target_schema, 1, &mut process).await?;

    let source_table = format!("{}.items", quote_identifier(&fixture.source_schema));
    fixture
        .source
        .batch_execute(&format!(
            "BEGIN;
             ALTER TABLE {source_table} ADD COLUMN churn_note text;
             ALTER TABLE {source_table} RENAME COLUMN churn_note TO churn_note2;
             ALTER TABLE {source_table} DROP COLUMN churn_note2;
             COMMIT;"
        ))
        .await?;
    wait_for_schema_events_settled(&fixture.target, fixture.pipeline.id, 1, &mut process).await?;
    wait_for_target_count(&fixture.target, &fixture.target_schema, 1, &mut process).await?;

    fixture
        .target
        .execute(
            &format!(
                "UPDATE {}.items SET payload='ddl-matrix-corruption' WHERE id=1",
                quote_identifier(&fixture.target_schema)
            ),
            &[],
        )
        .await?;
    fixture
        .source
        .batch_execute(&format!(
            "BEGIN;
             DROP TABLE {source_table};
             CREATE TABLE {source_table} (id bigint PRIMARY KEY, payload text NOT NULL);
             INSERT INTO {source_table} VALUES (1, 'reborn');
             COMMIT;"
        ))
        .await?;
    // The first DDL transaction may already have closed the old generation. If DROP/CREATE
    // commits before the replacement slot's consistent point, the full snapshot absorbs it and
    // there is deliberately no second target schema-event row. Prove the terminal data state
    // instead of requiring an implementation-specific event count.
    wait_for_target_payload(fixture, "reborn", &mut process).await?;
    wait_for_schema_events_settled(&fixture.target, fixture.pipeline.id, 1, &mut process).await?;
    assert_source_target_equal(
        &fixture.source,
        &fixture.target,
        &fixture.source_schema,
        &fixture.target_schema,
    )
    .await?;

    let first = load_rebuild_observation(&fixture.control, fixture.pipeline.id).await?;
    sleep(Duration::from_secs(6)).await;
    process.ensure_running()?;
    let second = load_rebuild_observation(&fixture.control, fixture.pipeline.id).await?;
    assert_eq!(
        first, second,
        "DDL fallback must not retry the same generation indefinitely"
    );
    assert!(
        second.0 <= 2 && second.1 <= 3,
        "two schema transactions may request at most two bounded rebuild generations: {second:?}"
    );
    assert_source_target_equal(
        &fixture.source,
        &fixture.target,
        &fixture.source_schema,
        &fixture.target_schema,
    )
    .await?;

    let status = process.terminate_and_wait()?;
    assert!(
        status.success(),
        "SIGTERM should complete graceful shutdown"
    );
    Ok(())
}

async fn run_soak(fixture: &mut Fixture, duration: Duration) -> Result<(), Box<dyn Error>> {
    let mut process = fixture.spawn_service()?;
    wait_for_target_count(&fixture.target, &fixture.target_schema, 1, &mut process).await?;
    let sample_interval = soak_sample_interval()?;
    let sample_file = open_soak_sample_file()?;
    let metadata_start = load_soak_metadata(&fixture.target, fixture.pipeline.id).await?;
    let mut evidence = SoakEvidence::new(sample_file, metadata_start);
    let started = Instant::now();
    let mut next_id = 2_i64;
    let mut cycles = 0_u64;
    let mut inserted = 0_u64;
    let mut updated = 0_u64;
    let mut deleted = 0_u64;
    let mut ddl_stage = 0_u8;
    let mut corrupted = false;
    let mut corruption_started = None;
    let mut reconciliation_recovery_seconds = None;
    let mut next_sample_at = Duration::ZERO;

    while started.elapsed() < duration {
        fixture.mixed_transaction(next_id, cycles).await?;
        next_id += 20;
        cycles += 1;
        inserted += 20;
        updated += 10;
        deleted += 2;

        let progress = started.elapsed().as_secs_f64() / duration.as_secs_f64();
        if ddl_stage == 0 && progress >= 0.20 {
            fixture.execute_ddl("ADD COLUMN soak_note text").await?;
            ddl_stage = 1;
        } else if ddl_stage == 1 && progress >= 0.40 {
            fixture
                .execute_ddl("RENAME COLUMN soak_note TO soak_note2")
                .await?;
            ddl_stage = 2;
        } else if ddl_stage == 2 && progress >= 0.70 {
            fixture.execute_ddl("DROP COLUMN soak_note2").await?;
            ddl_stage = 3;
        }
        if !corrupted && progress >= 0.50 {
            fixture
                .target
                .execute(
                    &format!(
                        "UPDATE {}.items SET payload='soak-corruption' WHERE id=1",
                        quote_identifier(&fixture.target_schema)
                    ),
                    &[],
                )
                .await?;
            corrupted = true;
            corruption_started = Some(Instant::now());
        }

        process.ensure_running()?;
        if corrupted && reconciliation_recovery_seconds.is_none() {
            let repaired = fixture
                .target
                .query_opt(
                    &format!(
                        "SELECT payload FROM {}.items WHERE id=1",
                        quote_identifier(&fixture.target_schema)
                    ),
                    &[],
                )
                .await
                .ok()
                .and_then(|row| row.map(|row| row.get::<_, String>(0) == "seed"))
                .unwrap_or(false);
            if repaired {
                reconciliation_recovery_seconds =
                    corruption_started.map(|started_at| started_at.elapsed().as_secs_f64());
            }
        }
        let elapsed = started.elapsed();
        if elapsed >= next_sample_at {
            let sample =
                collect_soak_sample(fixture, process.id(), elapsed, &mut evidence.cpu_ticks)
                    .await?;
            evidence.record(sample)?;
            next_sample_at = elapsed.saturating_add(sample_interval);
        }
        sleep(Duration::from_millis(500)).await;
    }

    let drain_started = Instant::now();
    let expected_count: i64 = fixture
        .source
        .query_one(
            &format!(
                "SELECT count(*) FROM {}.items",
                quote_identifier(&fixture.source_schema)
            ),
            &[],
        )
        .await?
        .get(0);
    wait_for_target_count(
        &fixture.target,
        &fixture.target_schema,
        expected_count,
        &mut process,
    )
    .await?;
    wait_for_exact_convergence(fixture, &mut process).await?;
    if reconciliation_recovery_seconds.is_none() {
        reconciliation_recovery_seconds =
            corruption_started.map(|started_at| started_at.elapsed().as_secs_f64());
    }
    wait_for_spool_files(&fixture.spool_root, false).await?;
    let final_sample = collect_soak_sample(
        fixture,
        process.id(),
        started.elapsed(),
        &mut evidence.cpu_ticks,
    )
    .await?;
    let final_source_rows = final_sample.source_rows;
    let final_target_rows = final_sample.target_rows;
    let final_lag_bytes = final_sample.lag_bytes;
    let final_spool_bytes = final_sample.spool_bytes;
    evidence.record(final_sample)?;
    assert_eq!(ddl_stage, 3, "all three DDL stages must execute");
    assert!(corrupted, "soak must exercise reconciliation repair");
    assert!(
        evidence.sample_count > 0,
        "soak must record runtime samples"
    );

    let status = process.terminate_and_wait()?;
    assert!(
        status.success(),
        "SIGTERM should complete graceful shutdown"
    );
    let metadata_end = load_soak_metadata(&fixture.target, fixture.pipeline.id).await?;
    evidence.metadata_max.update_max(metadata_end);
    let result = json!({
        "duration_seconds": started.elapsed().as_secs(),
        "drain_seconds": drain_started.elapsed().as_secs_f64(),
        "cycles": cycles,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "final_rows": expected_count,
        "final_source_rows": final_source_rows,
        "final_target_rows": final_target_rows,
        "final_lag_bytes": final_lag_bytes,
        "final_spool_bytes": final_spool_bytes,
        "source_target_equal": final_source_rows == Some(expected_count)
            && final_target_rows == Some(expected_count),
        "spool_drained": final_spool_bytes == 0,
        "sample_count": evidence.sample_count,
        "sample_query_errors": evidence.sample_query_errors,
        "sample_interval_seconds": sample_interval.as_secs(),
        "lag_p50_bytes": percentile_u64(&evidence.lag_samples, 0.50),
        "lag_p95_bytes": percentile_u64(&evidence.lag_samples, 0.95),
        "lag_p99_bytes": percentile_u64(&evidence.lag_samples, 0.99),
        "cpu_p50_percent": percentile_f64(&evidence.cpu_samples, 0.50),
        "cpu_p95_percent": percentile_f64(&evidence.cpu_samples, 0.95),
        "cpu_p99_percent": percentile_f64(&evidence.cpu_samples, 0.99),
        "max_rss_bytes": evidence.max_rss_bytes,
        "max_spool_bytes": evidence.max_spool_bytes,
        "max_retained_wal_bytes": evidence.max_retained_wal_bytes,
        "metadata_start": metadata_start.as_json(),
        "metadata_end": metadata_end.as_json(),
        "metadata_delta": metadata_end.delta_from(metadata_start),
        "metadata_max": evidence.metadata_max.as_json(),
        "ddl_stages": ddl_stage,
        "reconciliation_corruption_repaired": reconciliation_recovery_seconds.is_some(),
        "reconciliation_recovery_seconds": reconciliation_recovery_seconds,
    });
    if let Ok(path) = std::env::var("PG2CB_SOAK_RESULT_FILE") {
        fs::write(path, serde_json::to_vec_pretty(&result)?)?;
    }
    println!("PG2CB_SOAK_RESULT {result}");
    Ok(())
}

async fn run_process_recovery(fixture: &mut Fixture) -> Result<(), Box<dyn Error>> {
    let mut process = fixture.spawn_service()?;
    wait_for_target_count(&fixture.target, &fixture.target_schema, 1, &mut process).await?;

    let (mut lock_client, lock_connection) = connect(&fixture.target_dsn, "target-lock").await?;
    let lock_transaction = lock_client.transaction().await?;
    lock_transaction
        .batch_execute(&format!(
            "LOCK TABLE {}.items IN ACCESS EXCLUSIVE MODE",
            quote_identifier(&fixture.target_schema)
        ))
        .await?;

    fixture
        .source
        .execute(
            &format!(
                "INSERT INTO {}.items (id, payload)
                 SELECT value, repeat('x', 1024) || '-' || value::text
                   FROM generate_series(2::bigint, $1::bigint + 1) AS value",
                quote_identifier(&fixture.source_schema)
            ),
            &[&BULK_ROWS],
        )
        .await?;
    wait_for_spool_files(&fixture.spool_root, true).await?;

    let killed_status = process.kill_and_wait()?;
    assert!(
        !killed_status.success(),
        "SIGKILL must not look like a clean exit"
    );
    lock_transaction.rollback().await?;
    lock_connection.abort();

    wait_for_expired_lease(&fixture.control, fixture.pipeline.id).await?;
    process = fixture.spawn_service()?;
    wait_for_target_count(
        &fixture.target,
        &fixture.target_schema,
        BULK_ROWS + 1,
        &mut process,
    )
    .await?;
    wait_for_spool_files(&fixture.spool_root, false).await?;
    assert_source_target_equal(
        &fixture.source,
        &fixture.target,
        &fixture.source_schema,
        &fixture.target_schema,
    )
    .await?;

    let status = process.terminate_and_wait()?;
    assert!(
        status.success(),
        "SIGTERM should complete graceful shutdown: {status}"
    );
    Ok(())
}

struct Fixture {
    source: Client,
    control: Client,
    target: Client,
    source_connection: JoinHandle<()>,
    control_connection: JoinHandle<()>,
    target_connection: JoinHandle<()>,
    pipeline: PipelineDefinition,
    source_schema: String,
    target_schema: String,
    control_database: String,
    control_dsn: String,
    source_dsn: String,
    target_dsn: String,
    spool_root: PathBuf,
    fixture_root: PathBuf,
    config_path: PathBuf,
}

impl Fixture {
    async fn setup(source_dsn: &str, target_dsn: &str) -> Result<Self, Box<dyn Error>> {
        Self::setup_with_profiles(source_dsn, target_dsn, source_dsn, target_dsn).await
    }

    async fn setup_with_profiles(
        source_dsn: &str,
        target_dsn: &str,
        source_profile_dsn: &str,
        target_profile_dsn: &str,
    ) -> Result<Self, Box<dyn Error>> {
        let (source, source_connection) = connect(source_dsn, "source").await?;
        let (target, target_connection) = connect(target_dsn, "target").await?;

        let suffix = Uuid::now_v7().simple().to_string();
        let control_database = format!("pg2cb_process_{}", &suffix[..12]);
        source
            .batch_execute(&format!(
                "CREATE DATABASE {}",
                quote_identifier(&control_database)
            ))
            .await?;
        let control_dsn = database_dsn(source_dsn, &control_database)?;
        let (mut control, control_connection) = connect(&control_dsn, "control").await?;
        migrate_control_database(&mut control).await?;
        let source_schema = format!("pg2cb_process_src_{}", &suffix[..12]);
        let target_schema = format!("pg2cb_process_dst_{}", &suffix[..12]);
        source
            .batch_execute(&format!(
                "CREATE SCHEMA {};
                 CREATE TABLE {}.items (id bigint PRIMARY KEY, payload text NOT NULL);
                 INSERT INTO {}.items VALUES (1, 'seed')",
                quote_identifier(&source_schema),
                quote_identifier(&source_schema),
                quote_identifier(&source_schema),
            ))
            .await?;

        let store = build_control_store(&control_dsn)?;
        let master_key = MasterKey::from_base64(&SecretString::from(TEST_MASTER_KEY))?;
        let source_id = SourceId::new();
        let target_id = TargetId::new();
        let pipeline_id = PipelineId::new();
        let now = Utc::now();
        store
            .put_source(&SourceProfile {
                id: source_id,
                name: format!("process-source-{suffix}"),
                prefix: SourcePrefix::new(format!("p{}", &suffix[suffix.len() - 8..]))?,
                database_name: "source".to_owned(),
                topology: SourceTopology::Standalone,
                encrypted_dsn: master_key.encrypt(
                    &SecretString::from(source_profile_dsn.to_owned()),
                    source_credential_aad(source_id).as_bytes(),
                    1,
                )?,
                settings: json!({
                    "include_schemas": [&source_schema],
                    "transaction": {
                        "memory_high_water_changes": 64,
                        "memory_high_water_bytes": 32 * 1024,
                        "segment_target_bytes": 64 * 1024,
                        "disk_high_water_bytes": 1024 * 1024 * 1024_u64,
                        "minimum_free_disk_bytes": 1
                    }
                }),
                enabled: true,
                created_at: now,
                updated_at: now,
            })
            .await?;
        store
            .put_target(&TargetProfile {
                id: target_id,
                name: format!("process-target-{suffix}"),
                database_name: "target".to_owned(),
                encrypted_dsn: master_key.encrypt(
                    &SecretString::from(target_profile_dsn.to_owned()),
                    target_credential_aad(target_id).as_bytes(),
                    1,
                )?,
                settings: json!({"default_table_storage": TargetStorage::AoColumn}),
                enabled: true,
                created_at: now,
                updated_at: now,
            })
            .await?;
        let pipeline = PipelineDefinition {
            id: pipeline_id,
            name: format!("process-pipeline-{suffix}"),
            source_id,
            target_id,
            desired_running: true,
            config_revision: 1,
            snapshot_generation: 1,
            settings: json!({
                "batch": {"max_rows": 10_000, "max_bytes": 1024 * 1024, "max_delay_ms": 50},
                "table_mappings": [{
                    "source": QualifiedName::new(&source_schema, "items")?,
                    "target": QualifiedName::new(&target_schema, "items")?
                }]
            }),
            created_at: now,
            updated_at: now,
        };
        store.put_pipeline(&pipeline).await?;

        let fixture_root = std::env::temp_dir().join(format!("pg2cb-process-{suffix}"));
        let spool_root = fixture_root.join("spool");
        fs::create_dir_all(&spool_root)?;
        let config_path = fixture_root.join("etl-server-cloudberry.toml");
        fs::write(
            &config_path,
            format!(
                r#"[server]
listen = "127.0.0.1:0"
secure_cookies = false
session_ttl_seconds = 3600

[engine]
spool_directory = "{}"
reconcile_interval_seconds = 1
lease_ttl_seconds = {LEASE_TTL_SECONDS}
lease_renew_interval_seconds = 2
restart_backoff_initial_seconds = 1
restart_backoff_max_seconds = 4
restart_backoff_reset_seconds = 30

[admin]
username = "process-test"
password_hash_env = "ETL_ADMIN_PASSWORD_HASH"

[control]
database_url_env = "ETL_CONTROL_DATABASE_URL"

[security]
master_key_env = "ETL_MASTER_KEY"
"#,
                toml_path(&spool_root)
            ),
        )?;

        Ok(Self {
            source,
            control,
            target,
            source_connection,
            control_connection,
            target_connection,
            pipeline,
            source_schema,
            target_schema,
            control_database,
            control_dsn,
            source_dsn: source_dsn.to_owned(),
            target_dsn: target_dsn.to_owned(),
            spool_root,
            fixture_root,
            config_path,
        })
    }

    async fn insert_rows(
        &self,
        first_id: i64,
        rows: i64,
        payload_bytes: i32,
    ) -> Result<(), tokio_postgres::Error> {
        self.source
            .execute(
                &format!(
                    "INSERT INTO {}.items (id, payload)
                     SELECT $1::bigint + value, repeat('n', $3::integer) || '-' || value::text
                       FROM generate_series(0::bigint, $2::bigint - 1) AS value",
                    quote_identifier(&self.source_schema)
                ),
                &[&first_id, &rows, &payload_bytes],
            )
            .await?;
        Ok(())
    }

    async fn enable_fast_reconciliation(&self) -> Result<(), Box<dyn Error>> {
        self.control
            .execute(
                "UPDATE cloudberry_etl_control.pipelines
                    SET settings = jsonb_set(
                            jsonb_set(settings, '{reconciliation}', $2::jsonb),
                            '{batch,max_bytes}',
                            to_jsonb($3::bigint)
                        ),
                        config_revision = config_revision + 1,
                        updated_at = clock_timestamp()
                  WHERE id=$1",
                &[
                    &self.pipeline.id.as_uuid(),
                    &json!({
                        "enabled": true,
                        "interval_seconds": 5,
                        "retry_seconds": 1,
                        "boundary_timeout_seconds": 30,
                        "scan_timeout_seconds": 60,
                        "max_lag_bytes": 64 * 1024 * 1024_u64,
                        "max_row_bytes": 1024 * 1024
                    }),
                    &(64_i64 * 1024),
                ],
            )
            .await?;
        Ok(())
    }

    async fn mixed_transaction(&mut self, first_id: i64, cycle: u64) -> Result<(), Box<dyn Error>> {
        let transaction = self.source.transaction().await?;
        let table = quote_identifier(&self.source_schema);
        transaction
            .execute(
                &format!(
                    "INSERT INTO {table}.items (id, payload)
                     SELECT $1::bigint + value, $3::text || value::text
                       FROM generate_series(0::bigint, $2::bigint - 1) AS value"
                ),
                &[&first_id, &20_i64, &format!("soak-{cycle}-")],
            )
            .await?;
        transaction
            .execute(
                &format!(
                    "UPDATE {table}.items SET payload=$3::text || id::text
                      WHERE id >= $1 AND id < $2"
                ),
                &[&first_id, &(first_id + 10), &format!("update-{cycle}-")],
            )
            .await?;
        transaction
            .execute(
                &format!("DELETE FROM {table}.items WHERE id >= $1 AND id < $2"),
                &[&(first_id + 18), &(first_id + 20)],
            )
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn execute_ddl(&self, action: &str) -> Result<(), Box<dyn Error>> {
        self.source
            .batch_execute(&format!(
                "ALTER TABLE {}.items {}",
                quote_identifier(&self.source_schema),
                action
            ))
            .await?;
        Ok(())
    }

    async fn reconnect_source_and_control(&mut self) -> Result<(), Box<dyn Error>> {
        self.source_connection.abort();
        self.control_connection.abort();
        (self.source, self.source_connection) =
            wait_for_database(&self.source_dsn, "source").await?;
        (self.control, self.control_connection) =
            wait_for_database(&self.control_dsn, "control").await?;
        Ok(())
    }

    async fn reconnect_target(&mut self) -> Result<(), Box<dyn Error>> {
        self.target_connection.abort();
        (self.target, self.target_connection) = wait_for_cloudberry(&self.target_dsn).await?;
        Ok(())
    }

    fn spawn_service(&self) -> io::Result<ServiceProcess> {
        let child = Command::new(env!("CARGO_BIN_EXE_etl-server-cloudberry"))
            .args([
                "serve",
                "--config",
                self.config_path.to_str().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "config path is not UTF-8")
                })?,
                "--web-dir",
                self.fixture_root.to_str().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "web path is not UTF-8")
                })?,
            ])
            .env("ETL_CONTROL_DATABASE_URL", &self.control_dsn)
            .env("ETL_MASTER_KEY", TEST_MASTER_KEY)
            .env("ETL_ADMIN_PASSWORD_HASH", TEST_ADMIN_HASH)
            .env("RUST_LOG", "warn")
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;
        Ok(ServiceProcess { child })
    }

    async fn cleanup(&mut self) {
        let names = replication_names(self.pipeline.id, 0);
        let _ = self
            .source
            .execute("SELECT pg_drop_replication_slot($1)", &[&names.slot])
            .await;
        let _ = self
            .source
            .batch_execute(&format!(
                "DROP PUBLICATION IF EXISTS {}; DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&names.publication),
                quote_identifier(&self.source_schema),
            ))
            .await;
        let _ = self
            .target
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {} CASCADE",
                quote_identifier(&self.target_schema)
            ))
            .await;
        cleanup_target_metadata(&self.target, self.pipeline.id).await;
        let _ = self
            .source
            .batch_execute(&format!(
                "DROP DATABASE IF EXISTS {} WITH (FORCE)",
                quote_identifier(&self.control_database)
            ))
            .await;
        self.source_connection.abort();
        self.control_connection.abort();
        self.target_connection.abort();
        let _ = fs::remove_dir_all(&self.fixture_root);
    }
}

struct StoppedContainer {
    name: String,
    stopped: bool,
}

impl StoppedContainer {
    fn stop(name: &str) -> io::Result<Self> {
        let status = Command::new("docker").args(["stop", name]).status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "docker stop {name} failed: {status}"
            )));
        }
        Ok(Self {
            name: name.to_owned(),
            stopped: true,
        })
    }

    fn start(&mut self) -> io::Result<()> {
        if !self.stopped {
            return Ok(());
        }
        let status = Command::new("docker")
            .args(["start", &self.name])
            .status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "docker start {} failed: {status}",
                self.name
            )));
        }
        self.stopped = false;
        Ok(())
    }
}

impl Drop for StoppedContainer {
    fn drop(&mut self) {
        let _ = self.start();
    }
}

struct TcpProxy {
    address: SocketAddr,
    enabled: watch::Sender<bool>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl TcpProxy {
    async fn start(upstream: SocketAddr) -> io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (enabled, _) = watch::channel(true);
        let connection_state = enabled.clone();
        let (shutdown, mut shutdown_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((inbound, _)) = accepted else {
                            break;
                        };
                        let state = connection_state.subscribe();
                        tokio::spawn(proxy_connection(inbound, upstream, state));
                    }
                }
            }
        });
        Ok(Self {
            address,
            enabled,
            shutdown,
            task,
        })
    }

    const fn address(&self) -> SocketAddr {
        self.address
    }

    fn cut(&self) {
        self.enabled.send_replace(false);
    }

    fn restore(&self) {
        self.enabled.send_replace(true);
    }
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        self.task.abort();
    }
}

async fn proxy_connection(
    mut inbound: TcpStream,
    upstream: SocketAddr,
    mut enabled: watch::Receiver<bool>,
) {
    if !*enabled.borrow() {
        return;
    }
    let Ok(mut outbound) = TcpStream::connect(upstream).await else {
        return;
    };
    if !*enabled.borrow() {
        return;
    }
    tokio::select! {
        _ = enabled.changed() => {}
        _ = copy_bidirectional(&mut inbound, &mut outbound) => {}
    }
}

struct ServiceProcess {
    child: Child,
}

impl ServiceProcess {
    fn id(&self) -> u32 {
        self.child.id()
    }

    fn ensure_running(&mut self) -> io::Result<()> {
        if let Some(status) = self.child.try_wait()? {
            Err(io::Error::other(format!(
                "service process exited before convergence: {status}"
            )))
        } else {
            Ok(())
        }
    }

    fn kill_and_wait(&mut self) -> io::Result<std::process::ExitStatus> {
        self.child.kill()?;
        self.child.wait()
    }

    fn terminate_and_wait(&mut self) -> io::Result<std::process::ExitStatus> {
        let status = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()?;
        if !status.success() {
            return Err(io::Error::other(format!(
                "failed to send SIGTERM to child: {status}"
            )));
        }
        self.child.wait()
    }
}

impl Drop for ServiceProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn build_control_store(dsn: &str) -> Result<PostgresControlStore, Box<dyn Error>> {
    let mut config: tokio_postgres::Config = dsn.parse()?;
    configure_control_session(&mut config);
    let manager = Manager::from_config(
        config,
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Verified,
        },
    );
    let pool = Pool::builder(manager)
        .max_size(8)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(5)))
        .create_timeout(Some(Duration::from_secs(5)))
        .recycle_timeout(Some(Duration::from_secs(5)))
        .build()?;
    Ok(PostgresControlStore::new(pool))
}

async fn connect(
    dsn: &str,
    label: &'static str,
) -> Result<(Client, JoinHandle<()>), Box<dyn Error>> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("{label} process E2E connection ended: {error}");
        }
    });
    Ok((client, task))
}

async fn wait_for_database(
    dsn: &str,
    label: &'static str,
) -> Result<(Client, JoinHandle<()>), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        match connect(dsn, label).await {
            Ok(connection) => return Ok(connection),
            Err(error) if Instant::now() < deadline => {
                eprintln!("waiting for {label} after restart: {error}");
                sleep(Duration::from_secs(1)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn wait_for_cloudberry(dsn: &str) -> Result<(Client, JoinHandle<()>), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        match connect(dsn, "target").await {
            Ok((client, task)) => {
                let ready = client
                    .query_one(
                        "SELECT current_setting('gp_role', true)='dispatch',
                                lower(version()) LIKE '%cloudberry%2.1.%',
                                NOT EXISTS (
                                    SELECT 1 FROM gp_segment_configuration
                                     WHERE role='p' AND status<>'u'
                                )",
                        &[],
                    )
                    .await
                    .is_ok_and(|row| {
                        row.get::<_, bool>(0) && row.get::<_, bool>(1) && row.get::<_, bool>(2)
                    });
                if ready {
                    return Ok((client, task));
                }
                task.abort();
            }
            Err(error) => eprintln!("waiting for target after restart: {error}"),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "Cloudberry did not reach dispatch mode with all primary segments up",
            )
            .into());
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn wait_for_schema_events_settled(
    target: &Client,
    pipeline_id: PipelineId,
    minimum_events: i64,
    process: &mut ServiceProcess,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        process.ensure_running()?;
        let observation = match target
            .query_one(
                "SELECT count(*)::bigint,
                        count(*) FILTER (WHERE state IN ('completed', 'failed'))::bigint
                   FROM pg2cb_meta.schema_events
                  WHERE pipeline_id=$1",
                &[&pipeline_id.as_uuid()],
            )
            .await
        {
            Ok(row) => {
                let total: i64 = row.get(0);
                let terminal: i64 = row.get(1);
                if total >= minimum_events && terminal == total {
                    return Ok(());
                }
                format!("had {total} events, {terminal} terminal")
            }
            Err(error) => format!("query failed: {error}"),
        };
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("schema events did not settle at count {minimum_events}; {observation}"),
            )
            .into());
        }
        sleep(Duration::from_millis(250)).await;
    }
}

async fn load_rebuild_observation(
    control: &Client,
    pipeline_id: PipelineId,
) -> Result<(i64, i64), tokio_postgres::Error> {
    control
        .query_one(
            "SELECT
                (SELECT count(*) FROM cloudberry_etl_control.operations
                  WHERE pipeline_id=$1 AND operation_type='rebuild')::bigint,
                snapshot_generation
               FROM cloudberry_etl_control.pipelines
              WHERE id=$1",
            &[&pipeline_id.as_uuid()],
        )
        .await
        .map(|row| (row.get(0), row.get(1)))
}

async fn wait_for_target_count(
    target: &Client,
    target_schema: &str,
    expected: i64,
    process: &mut ServiceProcess,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let sql = format!(
        "SELECT count(*) FROM {}.items",
        quote_identifier(target_schema)
    );
    let mut last_observation: String;
    loop {
        process.ensure_running()?;
        match target.query_one(&sql, &[]).await {
            Ok(row) => {
                let count = row.get::<_, i64>(0);
                if count == expected {
                    return Ok(());
                }
                last_observation = format!("target had {count} rows");
            }
            Err(error) => last_observation = format!("target query failed: {error}"),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("target did not reach {expected} rows; {last_observation}"),
            )
            .into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_target_payload(
    fixture: &Fixture,
    expected: &str,
    process: &mut ServiceProcess,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let sql = format!(
        "SELECT payload FROM {}.items WHERE id=1",
        quote_identifier(&fixture.target_schema)
    );
    let mut last_observation: String;
    loop {
        process.ensure_running()?;
        match fixture.target.query_opt(&sql, &[]).await {
            Ok(Some(row)) => {
                let payload: String = row.get(0);
                if payload == expected {
                    return Ok(());
                }
                last_observation = format!("target payload was {payload:?}");
            }
            Ok(None) => last_observation = "target row was absent".to_owned(),
            Err(error) => last_observation = format!("target query failed: {error}"),
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("target payload did not become {expected:?}; {last_observation}"),
            )
            .into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_spool_files(root: &Path, expected_present: bool) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        let present = count_files(root)? > 0;
        if present == expected_present {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("spool file presence did not become {expected_present}"),
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

fn count_files(root: &Path) -> io::Result<usize> {
    if !root.exists() {
        return Ok(0);
    }
    let mut files = 0;
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                directories.push(entry.path());
            } else {
                files += 1;
            }
        }
    }
    Ok(files)
}

fn total_file_bytes(root: &Path) -> io::Result<u64> {
    if !root.exists() {
        return Ok(0);
    }
    let mut bytes = 0_u64;
    let mut directories = vec![root.to_owned()];
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                directories.push(entry.path());
            } else {
                bytes = bytes.saturating_add(entry.metadata()?.len());
            }
        }
    }
    Ok(bytes)
}

fn soak_sample_interval() -> Result<Duration, io::Error> {
    const DEFAULT_SECONDS: u64 = 5;
    const MAX_SECONDS: u64 = 60 * 60;

    let seconds = match std::env::var("PG2CB_SOAK_SAMPLE_INTERVAL_SECONDS") {
        Ok(value) => value.parse::<u64>().map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("PG2CB_SOAK_SAMPLE_INTERVAL_SECONDS is invalid: {error}"),
            )
        })?,
        Err(std::env::VarError::NotPresent) => DEFAULT_SECONDS,
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("PG2CB_SOAK_SAMPLE_INTERVAL_SECONDS is invalid: {error}"),
            ));
        }
    };
    if !(1..=MAX_SECONDS).contains(&seconds) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("PG2CB_SOAK_SAMPLE_INTERVAL_SECONDS must be between 1 and {MAX_SECONDS}"),
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn open_soak_sample_file() -> Result<Option<File>, io::Error> {
    let path = match std::env::var("PG2CB_SOAK_SAMPLE_FILE") {
        Ok(path) if !path.trim().is_empty() => PathBuf::from(path),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "PG2CB_SOAK_SAMPLE_FILE cannot be empty",
            ));
        }
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("PG2CB_SOAK_SAMPLE_FILE is invalid: {error}"),
            ));
        }
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)?;
    writeln!(
        file,
        "timestamp_unix_ms,elapsed_ms,source_rows,target_rows,lag_bytes,rss_bytes,spool_bytes,retained_wal_bytes,cpu_percent,active_tables,quarantined_tables,snapshot_groups,schema_events,table_transitions,reconciliation_runs,reconciliation_log_rows"
    )?;
    file.flush()?;
    Ok(Some(file))
}

async fn collect_soak_sample(
    fixture: &Fixture,
    pid: u32,
    elapsed: Duration,
    previous_cpu_ticks: &mut Option<CpuTicks>,
) -> Result<SoakSample, io::Error> {
    let source_rows = table_row_count(&fixture.source, &fixture.source_schema)
        .await
        .ok();
    let target_rows = table_row_count(&fixture.target, &fixture.target_schema)
        .await
        .ok();
    let lag_bytes = checkpoint_lag_bytes(fixture).await.ok().flatten();
    let retained_wal_bytes = retained_wal_bytes(&fixture.source, fixture.pipeline.id)
        .await
        .ok()
        .flatten();
    let metadata = load_soak_metadata(&fixture.target, fixture.pipeline.id)
        .await
        .ok();
    let current_cpu_ticks = process_cpu_ticks(pid)?;
    let cpu_percent = current_cpu_ticks.and_then(|current| {
        let previous = previous_cpu_ticks.as_ref()?;
        let process_delta = current.process.saturating_sub(previous.process);
        let system_delta = current.system.saturating_sub(previous.system);
        if system_delta == 0 {
            return None;
        }
        let processors = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Some(process_delta as f64 * processors as f64 * 100.0 / system_delta as f64)
    });
    *previous_cpu_ticks = current_cpu_ticks;

    Ok(SoakSample {
        timestamp_unix_ms: Utc::now().timestamp_millis(),
        elapsed_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
        source_rows,
        target_rows,
        lag_bytes,
        rss_bytes: process_rss_bytes(pid)?,
        spool_bytes: total_file_bytes(&fixture.spool_root)?,
        retained_wal_bytes,
        cpu_percent,
        metadata,
    })
}

async fn table_row_count(client: &Client, schema: &str) -> Result<i64, tokio_postgres::Error> {
    client
        .query_one(
            &format!("SELECT count(*) FROM {}.items", quote_identifier(schema)),
            &[],
        )
        .await
        .map(|row| row.get(0))
}

async fn checkpoint_lag_bytes(fixture: &Fixture) -> Result<Option<u64>, tokio_postgres::Error> {
    let checkpoint = fixture
        .target
        .query_opt(
            "SELECT applied_lsn::text
               FROM pg2cb_meta.node_checkpoints
              WHERE pipeline_id=$1 AND node_id=0
              ORDER BY topology_generation DESC
              LIMIT 1",
            &[&fixture.pipeline.id.as_uuid()],
        )
        .await?;
    let Some(checkpoint) = checkpoint else {
        return Ok(None);
    };
    let applied_lsn: String = checkpoint.get(0);
    let lag: i64 = fixture
        .source
        .query_one(
            "SELECT GREATEST(
                        pg_wal_lsn_diff(pg_current_wal_lsn(), $1::text::pg_lsn),
                        0
                    )::bigint",
            &[&applied_lsn],
        )
        .await?
        .get(0);
    Ok(Some(lag as u64))
}

async fn load_soak_metadata(
    target: &Client,
    pipeline_id: PipelineId,
) -> Result<SoakMetadata, tokio_postgres::Error> {
    let row = target
        .query_one(
            "SELECT
                (SELECT count(*) FROM pg2cb_meta.managed_tables
                  WHERE pipeline_id=$1 AND state='active')::bigint,
                (SELECT count(*) FROM pg2cb_meta.managed_tables
                  WHERE pipeline_id=$1 AND state='quarantined')::bigint,
                (SELECT count(*) FROM pg2cb_meta.snapshot_groups
                  WHERE pipeline_id=$1)::bigint,
                (SELECT count(*) FROM pg2cb_meta.schema_events
                  WHERE pipeline_id=$1)::bigint,
                (SELECT count(*) FROM pg2cb_meta.table_schema_transitions
                  WHERE pipeline_id=$1)::bigint,
                (SELECT count(*) FROM pg2cb_meta.table_reconciliation_state
                  WHERE pipeline_id=$1)::bigint,
                (SELECT count(*) FROM pg2cb_meta.snapshot_reconciliation_log
                  WHERE pipeline_id=$1)::bigint",
            &[&pipeline_id.as_uuid()],
        )
        .await?;
    Ok(SoakMetadata {
        active_tables: row.get(0),
        quarantined_tables: row.get(1),
        snapshot_groups: row.get(2),
        schema_events: row.get(3),
        table_transitions: row.get(4),
        reconciliation_runs: row.get(5),
        reconciliation_log_rows: row.get(6),
    })
}

fn process_cpu_ticks(pid: u32) -> io::Result<Option<CpuTicks>> {
    let Ok(process_stat) = fs::read_to_string(format!("/proc/{pid}/stat")) else {
        return Ok(None);
    };
    let process = parse_process_cpu_ticks(&process_stat)?;
    let system_stat = fs::read_to_string("/proc/stat")?;
    let system = parse_system_cpu_ticks(&system_stat)?;
    Ok(Some(CpuTicks { process, system }))
}

fn parse_process_cpu_ticks(stat: &str) -> io::Result<u64> {
    let end_name = stat.rfind(')').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "process stat has no command name",
        )
    })?;
    let fields = stat
        .get(end_name + 1..)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "process stat is truncated"))?
        .split_whitespace()
        .collect::<Vec<_>>();
    let user = parse_tick_field(&fields, 11, "utime")?;
    let system = parse_tick_field(&fields, 12, "stime")?;
    Ok(user.saturating_add(system))
}

fn parse_system_cpu_ticks(stat: &str) -> io::Result<u64> {
    let cpu = stat
        .lines()
        .next()
        .filter(|line| line.starts_with("cpu "))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "system stat has no cpu line"))?;
    cpu.split_whitespace()
        .skip(1)
        .try_fold(0_u64, |sum, field| {
            let ticks = field
                .parse::<u64>()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            Ok(sum.saturating_add(ticks))
        })
}

fn parse_tick_field(fields: &[&str], index: usize, name: &str) -> io::Result<u64> {
    fields
        .get(index)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("process stat has no {name} field"),
            )
        })?
        .parse::<u64>()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn percentile_u64(values: &[u64], quantile: f64) -> Option<u64> {
    percentile_index(values.len(), quantile).map(|index| {
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        sorted[index]
    })
}

fn percentile_f64(values: &[f64], quantile: f64) -> Option<f64> {
    percentile_index(values.len(), quantile).map(|index| {
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        sorted[index]
    })
}

fn percentile_index(len: usize, quantile: f64) -> Option<usize> {
    if len == 0 || !quantile.is_finite() {
        return None;
    }
    let quantile = quantile.clamp(0.0, 1.0);
    Some(((len as f64 * quantile).ceil() as usize).max(1) - 1)
}

fn process_rss_bytes(pid: u32) -> io::Result<Option<u64>> {
    let path = format!("/proc/{pid}/status");
    let Ok(status) = fs::read_to_string(path) else {
        return Ok(None);
    };
    let Some(line) = status.lines().find(|line| line.starts_with("VmRSS:")) else {
        return Ok(None);
    };
    let Some(kib) = line.split_whitespace().nth(1) else {
        return Ok(None);
    };
    Ok(Some(
        kib.parse::<u64>()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?
            * 1024,
    ))
}

async fn retained_wal_bytes(
    source: &Client,
    pipeline_id: PipelineId,
) -> Result<Option<u64>, tokio_postgres::Error> {
    let names = replication_names(pipeline_id, 0);
    let row = source
        .query_opt(
            "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::int8
               FROM pg_replication_slots WHERE slot_name=$1",
            &[&names.slot],
        )
        .await?;
    Ok(row.map(|row| row.get::<_, i64>(0).max(0) as u64))
}

async fn wait_for_exact_convergence(
    fixture: &Fixture,
    process: &mut ServiceProcess,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let sql = format!(
        "SELECT payload FROM {}.items WHERE id=1",
        quote_identifier(&fixture.target_schema)
    );
    loop {
        process.ensure_running()?;
        if let Ok(row) = fixture.target.query_one(&sql, &[]).await {
            let payload: String = row.get(0);
            if payload == "seed" {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "reconciliation did not repair the intentionally corrupted row",
            )
            .into());
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn wait_for_expired_lease(
    source: &Client,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + Duration::from_secs(LEASE_TTL_SECONDS + 15);
    loop {
        let expired: bool = source
            .query_one(
                "SELECT COALESCE((SELECT expires_at <= clock_timestamp()
                                    FROM cloudberry_etl_control.pipeline_leases
                                   WHERE pipeline_id=$1), true)",
                &[&pipeline_id.as_uuid()],
            )
            .await?
            .get(0);
        if expired {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "lease did not expire").into());
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn assert_source_target_equal(
    source: &Client,
    target: &Client,
    source_schema: &str,
    target_schema: &str,
) -> Result<(), Box<dyn Error>> {
    let source_rows = source
        .query(
            &format!(
                "SELECT id, payload FROM {}.items ORDER BY id",
                quote_identifier(source_schema)
            ),
            &[],
        )
        .await?;
    let target_rows = target
        .query(
            &format!(
                "SELECT id, payload FROM {}.items ORDER BY id",
                quote_identifier(target_schema)
            ),
            &[],
        )
        .await?;
    let source_values = source_rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    let target_values = target_rows
        .iter()
        .map(|row| (row.get::<_, i64>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(source_values, target_values);
    Ok(())
}

async fn cleanup_target_metadata(target: &Client, pipeline_id: PipelineId) {
    for table in ["snapshot_group_nodes", "snapshot_group_tables"] {
        let _ = target
            .execute(
                &format!(
                    "DELETE FROM pg2cb_meta.{table} WHERE snapshot_group_id IN
                     (SELECT snapshot_group_id FROM pg2cb_meta.snapshot_groups WHERE pipeline_id=$1)"
                ),
                &[&pipeline_id.as_uuid()],
            )
            .await;
    }
    for table in [
        "table_schema_transitions",
        "schema_events",
        "transaction_committed_chunks",
        "transaction_chunk_progress",
        "snapshot_table_progress",
        "table_reconciliation_state",
        "managed_tables",
        "managed_types",
        "snapshot_reconciliation_log",
        "snapshot_groups",
        "node_checkpoints",
        "pipeline_state",
    ] {
        let _ = target
            .execute(
                &format!("DELETE FROM pg2cb_meta.{table} WHERE pipeline_id=$1"),
                &[&pipeline_id.as_uuid()],
            )
            .await;
    }
}

fn dsn_socket_addr(dsn: &str) -> Result<SocketAddr, Box<dyn Error>> {
    let url = Url::parse(dsn)?;
    url.socket_addrs(|| Some(5432))?
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "DSN has no address").into())
}

fn proxy_dsn(dsn: &str, proxy: SocketAddr) -> Result<String, Box<dyn Error>> {
    let mut url = Url::parse(dsn)?;
    let host = proxy.ip().to_string();
    url.set_host(Some(&host))?;
    url.set_port(Some(proxy.port()))
        .map_err(|()| io::Error::new(io::ErrorKind::InvalidInput, "invalid proxy port"))?;
    Ok(url.into())
}

fn database_dsn(source_dsn: &str, database: &str) -> Result<String, url::ParseError> {
    let mut url = Url::parse(source_dsn)?;
    url.set_path(database);
    Ok(url.into())
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn toml_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod soak_evidence_tests {
    use super::*;

    #[test]
    fn process_stat_parser_handles_parentheses_in_command_name() {
        let stat = "42 (etl server (worker)) S 1 2 3 4 5 6 7 8 9 10 11 12 13";
        assert_eq!(parse_process_cpu_ticks(stat).unwrap(), 23);
    }

    #[test]
    fn system_stat_parser_sums_cpu_counters() {
        assert_eq!(
            parse_system_cpu_ticks("cpu 10 20 30 40\ncpu0 1 2 3 4\n").unwrap(),
            100
        );
    }

    #[test]
    fn percentiles_use_nearest_rank_and_return_null_for_empty() {
        assert_eq!(percentile_u64(&[], 0.95), None);
        assert_eq!(percentile_u64(&[9, 1, 5, 3], 0.50), Some(3));
        assert_eq!(percentile_u64(&[9, 1, 5, 3], 0.95), Some(9));
        assert_eq!(percentile_f64(&[2.0, 1.0, 3.0], 0.0), Some(1.0));
    }
}
