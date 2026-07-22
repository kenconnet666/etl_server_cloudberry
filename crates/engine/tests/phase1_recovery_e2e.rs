//! Full-runtime Phase 1 recovery and capacity matrix against PostgreSQL 18 and Cloudberry 2.1.
//!
//! The test destroys and reconstructs the complete pipeline job at each fault boundary. Only the
//! control records, source slot/WAL, local spool directory, and target metadata survive, matching
//! a process restart without relying on timing-sensitive log scraping.

use std::{
    error::Error,
    io,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use chrono::Utc;
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    lsn::PgLsn,
    mapping::SourcePrefix,
    pipeline::SourceTopology,
    schema::QualifiedName,
};
use cloudberry_etl_engine::{
    adapters::{SourceIngestObserver, SourceIngestPoint},
    runtime::{
        PostgresCloudberryJobFactory, reconciler::PipelineJobFactory, settings::replication_names,
    },
    supervisor::SupervisorError,
    telemetry::{PipelineRuntimeState, PipelineTelemetryHandle},
};
use cloudberry_etl_metadata::{
    crypto::{MasterKey, source_credential_aad, target_credential_aad},
    migration::migrate_control_database,
    model::{PipelineDefinition, PipelineLease, SourceProfile, TargetProfile},
    store::{ControlStore, PostgresControlStore, configure_control_session},
};
use cloudberry_etl_target_cloudberry::apply::{
    LedgeredCommitKind, LedgeredCommitObserver, LedgeredCommitPhase,
};
use cloudberry_etl_target_cloudberry::snapshot::SnapshotPageCommitObserver;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use secrecy::SecretString;
use serde_json::json;
use tokio::{
    task::JoinHandle,
    time::{Instant, sleep},
};
use tokio_postgres::{Client, NoTls};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const TEST_MASTER_KEY: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
const WAIT_TIMEOUT: Duration = Duration::from_secs(90);
const LEASE_TTL: Duration = Duration::from_secs(180);

#[derive(Debug, Clone, Copy)]
struct RuntimeProfile {
    memory_high_water_changes: usize,
    memory_high_water_bytes: usize,
    segment_target_bytes: usize,
    disk_high_water_bytes: u64,
    batch_max_rows: usize,
    batch_max_bytes: usize,
    batch_max_delay_ms: u64,
}

struct FixtureSpec<'a> {
    source_dsn: &'a str,
    target_dsn: &'a str,
    source_schema: &'a str,
    target_schema: &'a str,
    suffix: &'a str,
    profile: RuntimeProfile,
}

const RECOVERY_PROFILE: RuntimeProfile = RuntimeProfile {
    memory_high_water_changes: 1,
    memory_high_water_bytes: 1,
    segment_target_bytes: 128,
    disk_high_water_bytes: 16 * 1024 * 1024,
    batch_max_rows: 2,
    batch_max_bytes: 1024 * 1024,
    batch_max_delay_ms: 10,
};

const LARGE_TRANSACTION_PROFILE: RuntimeProfile = RuntimeProfile {
    memory_high_water_changes: 8,
    memory_high_water_bytes: 64 * 1024,
    segment_target_bytes: 256 * 1024,
    disk_high_water_bytes: 128 * 1024 * 1024,
    batch_max_rows: 4_096,
    batch_max_bytes: 8 * 1024 * 1024,
    batch_max_delay_ms: 10,
};

const CAPACITY_PROFILE: RuntimeProfile = RuntimeProfile {
    memory_high_water_changes: 1,
    memory_high_water_bytes: 1,
    segment_target_bytes: 16 * 1024,
    disk_high_water_bytes: 96 * 1024,
    batch_max_rows: 10_000,
    batch_max_bytes: 32 * 1024 * 1024,
    batch_max_delay_ms: 2_000,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryFault {
    SnapshotPageAfterCommit,
    Source(SourceIngestPoint),
    Target {
        phase: LedgeredCommitPhase,
        kind: LedgeredCommitKind,
    },
}

#[derive(Debug, Default)]
struct FaultController {
    armed: Mutex<Option<RecoveryFault>>,
    fired: tokio::sync::Notify,
    spool_commits: AtomicUsize,
}

impl FaultController {
    fn arm(&self, fault: RecoveryFault) {
        let previous = self.armed.lock().unwrap().replace(fault);
        assert!(previous.is_none(), "a recovery fault was already armed");
    }

    fn trigger(&self, fault: RecoveryFault) -> Result<(), String> {
        let mut armed = self.armed.lock().unwrap();
        if *armed != Some(fault) {
            return Ok(());
        }
        *armed = None;
        self.fired.notify_one();
        Err(format!("injected fatal recovery fault at {fault:?}"))
    }

    async fn wait_until_fired(&self) -> Result<(), Box<dyn Error>> {
        tokio::time::timeout(WAIT_TIMEOUT, self.fired.notified())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "recovery fault did not fire"))?;
        Ok(())
    }
}

impl SourceIngestObserver for FaultController {
    fn observe(&self, point: SourceIngestPoint) -> Result<(), String> {
        if point == SourceIngestPoint::AfterSpoolCommit {
            self.spool_commits.fetch_add(1, Ordering::Relaxed);
        }
        self.trigger(RecoveryFault::Source(point))
    }
}

impl LedgeredCommitObserver for FaultController {
    fn observe(&self, phase: LedgeredCommitPhase, kind: LedgeredCommitKind) -> Result<(), String> {
        self.trigger(RecoveryFault::Target { phase, kind })
    }
}

impl SnapshotPageCommitObserver for FaultController {
    fn observe_after_commit(&self) -> Result<(), String> {
        self.trigger(RecoveryFault::SnapshotPageAfterCommit)
    }
}

struct ActiveJob {
    cancellation: CancellationToken,
    handle: Option<JoinHandle<Result<(), SupervisorError>>>,
    lease: Option<PipelineLease>,
    telemetry: PipelineTelemetryHandle,
}

impl ActiveJob {
    async fn expect_fault(&mut self, store: &PostgresControlStore) -> Result<(), Box<dyn Error>> {
        let handle = self
            .handle
            .take()
            .ok_or_else(|| io::Error::other("pipeline job handle is missing"))?;
        let result = tokio::time::timeout(WAIT_TIMEOUT, handle)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "pipeline job did not stop"))??;
        let error = result.expect_err("fault injection must terminate the pipeline job");
        assert!(
            error.to_string().contains("injected fatal recovery fault"),
            "pipeline stopped for an unexpected reason: {error}"
        );
        if let Some(lease) = self.lease.take() {
            store.release_lease(&lease).await?;
        }
        Ok(())
    }

    async fn stop(&mut self, store: &PostgresControlStore) -> Result<(), Box<dyn Error>> {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            let result = tokio::time::timeout(WAIT_TIMEOUT, handle)
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "pipeline did not cancel")
                })??;
            result?;
        }
        if let Some(lease) = self.lease.take() {
            store.release_lease(&lease).await?;
        }
        Ok(())
    }

    async fn expect_successful_exit(
        &mut self,
        store: &PostgresControlStore,
    ) -> Result<(), Box<dyn Error>> {
        let handle = self
            .handle
            .take()
            .ok_or_else(|| io::Error::other("pipeline job handle is missing"))?;
        let result = tokio::time::timeout(WAIT_TIMEOUT, handle)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "pipeline job did not stop"))??;
        result?;
        if let Some(lease) = self.lease.take() {
            store.release_lease(&lease).await?;
        }
        Ok(())
    }
}

impl Drop for ActiveJob {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(handle) = &self.handle {
            handle.abort();
        }
    }
}

struct TestContext {
    source: Client,
    target: Client,
    source_connection: JoinHandle<()>,
    target_connection: JoinHandle<()>,
    store: Arc<PostgresControlStore>,
    master_key: Arc<MasterKey>,
    pipeline: PipelineDefinition,
    source_schema: String,
    target_schema: String,
    spool_root: PathBuf,
    controller: Arc<FaultController>,
}

#[tokio::test]
#[ignore = "requires real PG18, Cloudberry 2.1, and PG2CB_TEST_SOURCE_DSN/PG2CB_TEST_TARGET_DSN"]
async fn full_runtime_proves_phase1_recovery_and_capacity_boundaries() -> Result<(), Box<dyn Error>>
{
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;

    run_snapshot_restart_case(&source_dsn, &target_dsn).await?;
    run_recovery_case(&source_dsn, &target_dsn).await?;
    run_large_transaction_case(&source_dsn, &target_dsn).await?;
    run_capacity_wait_case(&source_dsn, &target_dsn).await?;
    run_schema_fallback_case(&source_dsn, &target_dsn).await
}

async fn run_snapshot_restart_case(
    source_dsn: &str,
    target_dsn: &str,
) -> Result<(), Box<dyn Error>> {
    let mut context = TestContext::setup(source_dsn, target_dsn, RECOVERY_PROFILE).await?;
    let result = run_snapshot_restart(&mut context).await;
    context.cleanup().await;
    result
}

async fn run_recovery_case(source_dsn: &str, target_dsn: &str) -> Result<(), Box<dyn Error>> {
    let mut context = TestContext::setup(source_dsn, target_dsn, RECOVERY_PROFILE).await?;

    let result = run_recovery_matrix(&mut context).await;
    context.cleanup().await;
    result
}

async fn run_large_transaction_case(
    source_dsn: &str,
    target_dsn: &str,
) -> Result<(), Box<dyn Error>> {
    let mut context = TestContext::setup(source_dsn, target_dsn, LARGE_TRANSACTION_PROFILE).await?;
    let result = run_large_transaction(&mut context).await;
    context.cleanup().await;
    result
}

async fn run_capacity_wait_case(source_dsn: &str, target_dsn: &str) -> Result<(), Box<dyn Error>> {
    let mut context = TestContext::setup(source_dsn, target_dsn, CAPACITY_PROFILE).await?;
    let result = run_capacity_wait(&mut context).await;
    context.cleanup().await;
    result
}

async fn run_schema_fallback_case(
    source_dsn: &str,
    target_dsn: &str,
) -> Result<(), Box<dyn Error>> {
    let mut context = TestContext::setup(source_dsn, target_dsn, RECOVERY_PROFILE).await?;
    let result = run_schema_fallback(&mut context).await;
    context.cleanup().await;
    result
}

async fn run_recovery_matrix(context: &mut TestContext) -> Result<(), Box<dyn Error>> {
    let mut active = context.start_job().await?;
    wait_for_target_count(&context.target, &context.target_schema, 1, &active).await?;

    let faults = [
        RecoveryFault::Source(SourceIngestPoint::AfterSourceRead),
        RecoveryFault::Source(SourceIngestPoint::AfterSpoolCommit),
        RecoveryFault::Target {
            phase: LedgeredCommitPhase::BeforeCommit,
            kind: LedgeredCommitKind::DataChunk { final_chunk: false },
        },
        RecoveryFault::Target {
            phase: LedgeredCommitPhase::AfterCommit,
            kind: LedgeredCommitKind::DataChunk { final_chunk: false },
        },
        RecoveryFault::Target {
            phase: LedgeredCommitPhase::AfterCommit,
            kind: LedgeredCommitKind::DataChunk { final_chunk: true },
        },
    ];

    let mut expected_rows = 1_i64;
    for (scenario, fault) in faults.into_iter().enumerate() {
        context.controller.arm(fault);
        insert_source_transaction(
            &mut context.source,
            &context.source_schema,
            i64::try_from(scenario)? * 100 + 10,
            scenario,
        )
        .await?;
        context.controller.wait_until_fired().await?;
        active.expect_fault(context.store.as_ref()).await?;

        expected_rows += 5;
        if matches!(
            fault,
            RecoveryFault::Target {
                phase: LedgeredCommitPhase::AfterCommit,
                kind: LedgeredCommitKind::DataChunk { final_chunk: true }
            }
        ) {
            // The final target commit includes DML, checkpoint, and ledger retirement. It must be
            // visible before restart even though the job reports an ambiguous error before ACK.
            wait_for_target_count(
                &context.target,
                &context.target_schema,
                expected_rows,
                &active,
            )
            .await?;
            assert_eq!(
                target_ledger_rows(&context.target, context.pipeline.id).await?,
                0
            );
        }

        active = context.start_job().await?;
        wait_for_target_count(
            &context.target,
            &context.target_schema,
            expected_rows,
            &active,
        )
        .await?;
        assert_eq!(
            target_ledger_rows(&context.target, context.pipeline.id).await?,
            0
        );
    }

    assert_source_target_equal(
        &context.source,
        &context.target,
        &context.source_schema,
        &context.target_schema,
    )
    .await?;
    active.stop(context.store.as_ref()).await?;
    Ok(())
}

#[derive(Debug)]
struct LoadingSnapshotState {
    group_id: Uuid,
    shadow_schema: String,
    shadow_table: String,
    shadow_relation_oid: i64,
    cursor: Vec<String>,
    pages_copied: i64,
    rows_copied: i64,
    consistent_lsn: PgLsn,
}

#[derive(Debug)]
struct ActiveSnapshotState {
    group_id: Uuid,
    relation_oid: i64,
    consistent_lsn: PgLsn,
    checkpoint_lsn: PgLsn,
}

async fn run_snapshot_restart(context: &mut TestContext) -> Result<(), Box<dyn Error>> {
    context
        .source
        .execute(
            &format!(
                "INSERT INTO {}.items (id, payload) SELECT value, 'snapshot-' || value::text FROM generate_series(2::bigint, 6::bigint) AS value",
                quote_identifier(&context.source_schema)
            ),
            &[],
        )
        .await?;
    context
        .controller
        .arm(RecoveryFault::SnapshotPageAfterCommit);

    let mut active = context.start_job().await?;
    context.controller.wait_until_fired().await?;
    active.expect_fault(context.store.as_ref()).await?;

    let old = load_single_loading_snapshot(&context.target, context.pipeline.id).await?;
    assert_eq!(old.cursor, ["2"]);
    assert_eq!(old.pages_copied, 1);
    assert_eq!(old.rows_copied, 2);
    let old_shadow = format!("{}.{}", old.shadow_schema, old.shadow_table);
    let old_shadow_exists: bool = context
        .target
        .query_one("SELECT to_regclass($1) IS NOT NULL", &[&old_shadow])
        .await?
        .get(0);
    assert!(
        old_shadow_exists,
        "committed first page must leave its shadow"
    );

    let names = replication_names(context.pipeline.id, 0);
    assert_eq!(
        source_slot_count(&context.source, &names.slot).await?,
        0,
        "the failed initial source snapshot must drop S0's logical slot"
    );

    context
        .source
        .execute(
            &format!(
                "INSERT INTO {}.items (id, payload) VALUES (0, 'inserted-before-old-cursor')",
                quote_identifier(&context.source_schema)
            ),
            &[],
        )
        .await?;

    active = context.start_job().await?;
    wait_for_target_count(&context.target, &context.target_schema, 7, &active).await?;

    let new = load_single_active_snapshot(&context.target, context.pipeline.id).await?;
    assert_ne!(
        new.group_id, old.group_id,
        "restart must create a new S1 group"
    );
    assert!(
        new.consistent_lsn > old.consistent_lsn,
        "S1 must be exported after the source write that followed S0"
    );
    assert_eq!(
        new.checkpoint_lsn, new.consistent_lsn,
        "activation must initialize the checkpoint from S1"
    );
    assert_ne!(
        new.relation_oid, old.shadow_relation_oid,
        "the stale shadow must be dropped instead of adopted by S1"
    );
    assert_eq!(
        stale_snapshot_artifact_count(&context.target, context.pipeline.id, old.group_id).await?,
        0,
        "every old group, progress, manifest, and managed-table row must be removed"
    );
    let old_relation_exists: bool = context
        .target
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class WHERE oid::bigint=$1)",
            &[&old.shadow_relation_oid],
        )
        .await?
        .get(0);
    assert!(!old_relation_exists, "the old physical shadow must be gone");
    assert_eq!(
        snapshot_group_count(&context.target, context.pipeline.id, "loading").await?,
        0
    );
    assert_eq!(
        snapshot_group_count(&context.target, context.pipeline.id, "active").await?,
        1
    );
    assert_eq!(source_slot_count(&context.source, &names.slot).await?, 1);
    let sentinel: i64 = context
        .target
        .query_one(
            &format!(
                "SELECT count(*) FROM {}.items WHERE id=0 AND payload='inserted-before-old-cursor'",
                quote_identifier(&context.target_schema)
            ),
            &[],
        )
        .await?
        .get(0);
    assert_eq!(sentinel, 1, "S1 must restart before the old S0 cursor");
    assert_eq!(rebuild_operation_count(context).await?, 0);
    assert_source_target_equal(
        &context.source,
        &context.target,
        &context.source_schema,
        &context.target_schema,
    )
    .await?;
    active.stop(context.store.as_ref()).await?;
    Ok(())
}

async fn run_large_transaction(context: &mut TestContext) -> Result<(), Box<dyn Error>> {
    const ROWS: i64 = 32 * 1024;
    const PAYLOAD_BYTES: i32 = 1024;
    const MAX_RSS_GROWTH_BYTES: u64 = 24 * 1024 * 1024;

    assert!(
        u64::try_from(ROWS)? * u64::try_from(PAYLOAD_BYTES)?
            > u64::try_from(LARGE_TRANSACTION_PROFILE.memory_high_water_bytes)? * 128
    );

    let mut active = context.start_job().await?;
    wait_for_target_count(&context.target, &context.target_schema, 1, &active).await?;
    let sampler = RssSampler::start()?;
    let commits_before = context.controller.spool_commits.load(Ordering::Relaxed);

    insert_bulk_source_transaction(
        &context.source,
        &context.source_schema,
        10_000,
        ROWS,
        PAYLOAD_BYTES,
    )
    .await?;
    wait_for_target_count(&context.target, &context.target_schema, ROWS + 1, &active).await?;
    let rss_growth = sampler.stop().await?;

    assert!(
        context.controller.spool_commits.load(Ordering::Relaxed) > commits_before,
        "the large transaction must use the durable spool path"
    );
    if let Some(rss_growth) = rss_growth {
        eprintln!("large transaction process RSS growth: {rss_growth} bytes");
        assert!(
            rss_growth <= MAX_RSS_GROWTH_BYTES,
            "large transaction grew process RSS by {rss_growth} bytes, limit is {MAX_RSS_GROWTH_BYTES}"
        );
    }
    assert_source_target_equal(
        &context.source,
        &context.target,
        &context.source_schema,
        &context.target_schema,
    )
    .await?;
    active.stop(context.store.as_ref()).await?;
    Ok(())
}

async fn run_capacity_wait(context: &mut TestContext) -> Result<(), Box<dyn Error>> {
    const ROWS_PER_TRANSACTION: i64 = 128;
    const PAYLOAD_BYTES: i32 = 512;

    let mut active = context.start_job().await?;
    wait_for_target_count(&context.target, &context.target_schema, 1, &active).await?;
    insert_bulk_source_transaction(
        &context.source,
        &context.source_schema,
        20_000,
        ROWS_PER_TRANSACTION,
        PAYLOAD_BYTES,
    )
    .await?;
    insert_bulk_source_transaction(
        &context.source,
        &context.source_schema,
        30_000,
        ROWS_PER_TRANSACTION,
        PAYLOAD_BYTES,
    )
    .await?;

    wait_for_resource_wait(&active).await?;
    wait_for_target_count(
        &context.target,
        &context.target_schema,
        ROWS_PER_TRANSACTION * 2 + 1,
        &active,
    )
    .await?;
    assert_ne!(
        active.telemetry.snapshot().state,
        PipelineRuntimeState::ResourceWait,
        "resource wait must clear after committed spool files are retired"
    );
    let generation: i64 = context
        .source
        .query_one(
            "SELECT snapshot_generation FROM cloudberry_etl_control.pipelines WHERE id=$1",
            &[&context.pipeline.id.as_uuid()],
        )
        .await?
        .get(0);
    assert_eq!(generation, context.pipeline.snapshot_generation);
    let rebuilds: i64 = context
        .source
        .query_one(
            "SELECT count(*) FROM cloudberry_etl_control.operations WHERE pipeline_id=$1 AND operation_type='rebuild'",
            &[&context.pipeline.id.as_uuid()],
        )
        .await?
        .get(0);
    assert_eq!(rebuilds, 0, "capacity recovery must not request a rebuild");
    assert_source_target_equal(
        &context.source,
        &context.target,
        &context.source_schema,
        &context.target_schema,
    )
    .await?;
    active.stop(context.store.as_ref()).await?;
    Ok(())
}

async fn run_schema_fallback(context: &mut TestContext) -> Result<(), Box<dyn Error>> {
    let mut active = context.start_job().await?;
    wait_for_target_count(&context.target, &context.target_schema, 1, &active).await?;
    let initial_checkpoint = load_checkpoint_lsn(&context.target, context.pipeline.id).await?;

    context
        .source
        .batch_execute(&format!(
            "BEGIN;
             CREATE TABLE {}.transient_noop (id bigint PRIMARY KEY);
             DROP TABLE {}.transient_noop;
             COMMIT;",
            quote_identifier(&context.source_schema),
            quote_identifier(&context.source_schema),
        ))
        .await?;
    wait_for_checkpoint_after(
        &context.target,
        context.pipeline.id,
        initial_checkpoint,
        &active,
    )
    .await?;
    assert_eq!(rebuild_operation_count(context).await?, 0);
    let noop_states = context
        .target
        .query_one(
            "SELECT
                 count(*) FILTER (WHERE e.state='completed'),
                 count(*) FILTER (WHERE t.state='completed')
               FROM pg2cb_meta.schema_events e
               JOIN pg2cb_meta.table_schema_transitions t
                 ON t.pipeline_id=e.pipeline_id AND t.source_lsn=e.source_lsn
                AND t.source_xid=e.source_xid
              WHERE e.pipeline_id=$1 AND t.action='noop'",
            &[&context.pipeline.id.as_uuid()],
        )
        .await?;
    assert_eq!(noop_states.get::<_, i64>(0), 1);
    assert_eq!(noop_states.get::<_, i64>(1), 1);

    let checkpoint_before = load_checkpoint_lsn(&context.target, context.pipeline.id).await?;

    context
        .source
        .batch_execute(&format!(
            "ALTER TABLE {}.items ADD COLUMN note text",
            quote_identifier(&context.source_schema)
        ))
        .await?;
    active
        .expect_successful_exit(context.store.as_ref())
        .await?;

    assert_eq!(rebuild_operation_count(context).await?, 1);
    assert_eq!(
        load_checkpoint_lsn(&context.target, context.pipeline.id).await?,
        checkpoint_before,
        "schema fallback must not advance the target checkpoint across the DDL"
    );
    let event = context
        .target
        .query_one(
            "SELECT state, failure_reason, transitions FROM pg2cb_meta.schema_events WHERE pipeline_id=$1 AND state='failed'",
            &[&context.pipeline.id.as_uuid()],
        )
        .await?;
    assert_eq!(event.get::<_, String>("state"), "failed");
    assert!(
        event
            .get::<_, Option<String>>("failure_reason")
            .is_some_and(|reason| reason.contains("requires table transition"))
    );
    let transitions: serde_json::Value = event.try_get("transitions")?;
    assert!(transitions["source_xid"].as_u64().is_some());
    let target_has_note: bool = context
        .target
        .query_one(
            "SELECT EXISTS (
                 SELECT 1
                   FROM pg_catalog.pg_attribute AS a
                   JOIN pg_catalog.pg_class AS c ON c.oid=a.attrelid
                   JOIN pg_catalog.pg_namespace AS n ON n.oid=c.relnamespace
                  WHERE n.nspname=$1 AND c.relname='items'
                    AND a.attname='note' AND a.attnum>0 AND NOT a.attisdropped
             )",
            &[&context.target_schema],
        )
        .await?
        .get(0);
    assert!(
        !target_has_note,
        "the fallback must not leave a partially applied target DDL"
    );
    Ok(())
}

impl TestContext {
    async fn setup(
        source_dsn: &str,
        target_dsn: &str,
        profile: RuntimeProfile,
    ) -> Result<Self, Box<dyn Error>> {
        let (mut source, source_connection) = connect(source_dsn, "source").await?;
        let (target, target_connection) = connect(target_dsn, "target").await?;
        migrate_control_database(&mut source).await?;

        let suffix = Uuid::now_v7().simple().to_string();
        let source_schema = format!("pg2cb_recovery_src_{}", &suffix[..12]);
        let target_schema = format!("pg2cb_recovery_dst_{}", &suffix[..12]);
        source
            .batch_execute(&format!(
                "CREATE SCHEMA {source_schema};
                 CREATE TABLE {source_schema}.items (
                     id bigint PRIMARY KEY,
                     payload text NOT NULL
                 );
                 INSERT INTO {source_schema}.items VALUES (1, 'seed');"
            ))
            .await?;

        let mut pool_config: tokio_postgres::Config = source_dsn.parse()?;
        configure_control_session(&mut pool_config);
        let manager = Manager::from_config(
            pool_config,
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
        let store = Arc::new(PostgresControlStore::new(pool));
        let master_key = Arc::new(MasterKey::from_base64(&SecretString::from(
            TEST_MASTER_KEY,
        ))?);
        let pipeline = persist_pipeline(
            store.as_ref(),
            master_key.as_ref(),
            FixtureSpec {
                source_dsn,
                target_dsn,
                source_schema: &source_schema,
                target_schema: &target_schema,
                suffix: &suffix,
                profile,
            },
        )
        .await?;

        Ok(Self {
            source,
            target,
            source_connection,
            target_connection,
            store,
            master_key,
            pipeline,
            source_schema,
            target_schema,
            spool_root: std::env::temp_dir().join(format!("pg2cb-recovery-{suffix}")),
            controller: Arc::new(FaultController::default()),
        })
    }

    async fn start_job(&self) -> Result<ActiveJob, Box<dyn Error>> {
        let lease = self
            .store
            .try_acquire_lease(self.pipeline.id, Uuid::now_v7(), LEASE_TTL)
            .await?
            .ok_or_else(|| io::Error::other("pipeline lease is still owned"))?;
        let factory = PostgresCloudberryJobFactory::new(
            Arc::<PostgresControlStore>::clone(&self.store),
            Arc::clone(&self.master_key),
        )
        .with_spool_root(&self.spool_root)
        .with_source_ingest_observer(Arc::<FaultController>::clone(&self.controller))
        .with_target_commit_observer(Arc::<FaultController>::clone(&self.controller))
        .with_snapshot_page_commit_observer(Arc::<FaultController>::clone(&self.controller));
        let telemetry = PipelineTelemetryHandle::new(self.pipeline.id);
        let job = factory
            .create(&self.pipeline, &lease, telemetry.clone())
            .await?;
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let handle = tokio::spawn(async move { job.run(task_cancellation).await });
        Ok(ActiveJob {
            cancellation,
            handle: Some(handle),
            lease: Some(lease),
            telemetry,
        })
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
        cleanup_control_metadata(&self.source, &self.pipeline).await;
        self.source_connection.abort();
        self.target_connection.abort();
        let _ = std::fs::remove_dir_all(&self.spool_root);
    }
}

async fn persist_pipeline(
    store: &PostgresControlStore,
    master_key: &MasterKey,
    spec: FixtureSpec<'_>,
) -> Result<PipelineDefinition, Box<dyn Error>> {
    let FixtureSpec {
        source_dsn,
        target_dsn,
        source_schema,
        target_schema,
        suffix,
        profile,
    } = spec;
    let source_id = SourceId::new();
    let target_id = TargetId::new();
    let pipeline_id = PipelineId::new();
    let now = Utc::now();
    store
        .put_source(&SourceProfile {
            id: source_id,
            name: format!("recovery-source-{suffix}"),
            prefix: SourcePrefix::new(format!("r{}", &suffix[..8]))?,
            database_name: "source".to_owned(),
            topology: SourceTopology::Standalone,
            encrypted_dsn: master_key.encrypt(
                &SecretString::from(source_dsn.to_owned()),
                source_credential_aad(source_id).as_bytes(),
                1,
            )?,
            settings: json!({
                "include_schemas": [source_schema],
                "transaction": {
                    "memory_high_water_changes": profile.memory_high_water_changes,
                    "memory_high_water_bytes": profile.memory_high_water_bytes,
                    "segment_target_bytes": profile.segment_target_bytes,
                    "disk_high_water_bytes": profile.disk_high_water_bytes,
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
            name: format!("recovery-target-{suffix}"),
            database_name: "target".to_owned(),
            encrypted_dsn: master_key.encrypt(
                &SecretString::from(target_dsn.to_owned()),
                target_credential_aad(target_id).as_bytes(),
                1,
            )?,
            settings: json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
        })
        .await?;
    let pipeline = PipelineDefinition {
        id: pipeline_id,
        name: format!("recovery-pipeline-{suffix}"),
        source_id,
        target_id,
        desired_running: true,
        config_revision: 1,
        snapshot_generation: 1,
        settings: json!({
            "batch": {
                "max_rows": profile.batch_max_rows,
                "max_bytes": profile.batch_max_bytes,
                "max_delay_ms": profile.batch_max_delay_ms
            },
            "table_mappings": [{
                "source": QualifiedName::new(source_schema, "items")?,
                "target": QualifiedName::new(target_schema, "items")?
            }]
        }),
        created_at: now,
        updated_at: now,
    };
    store.put_pipeline(&pipeline).await?;
    Ok(pipeline)
}

async fn connect(
    dsn: &str,
    label: &'static str,
) -> Result<(Client, JoinHandle<()>), Box<dyn Error>> {
    let (client, connection) = tokio_postgres::connect(dsn, NoTls).await?;
    let task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("{label} recovery connection ended: {error}");
        }
    });
    Ok((client, task))
}

async fn insert_source_transaction(
    source: &mut Client,
    source_schema: &str,
    first_id: i64,
    scenario: usize,
) -> Result<(), Box<dyn Error>> {
    let transaction = source.transaction().await?;
    let sql = format!(
        "INSERT INTO {}.items (id, payload) VALUES ($1, $2)",
        quote_identifier(source_schema)
    );
    for offset in 0..5_i64 {
        let id = first_id + offset;
        let payload = format!("scenario-{scenario}-row-{offset}");
        transaction.execute(&sql, &[&id, &payload]).await?;
    }
    transaction.commit().await?;
    Ok(())
}

async fn insert_bulk_source_transaction(
    source: &Client,
    source_schema: &str,
    first_id: i64,
    rows: i64,
    payload_bytes: i32,
) -> Result<(), tokio_postgres::Error> {
    source
        .execute(
            &format!(
                "INSERT INTO {}.items (id, payload) SELECT $1::bigint + value, repeat('x', $3::integer) || '-' || value::text FROM generate_series(0::bigint, $2::bigint - 1) AS value",
                quote_identifier(source_schema)
            ),
            &[&first_id, &rows, &payload_bytes],
        )
        .await?;
    Ok(())
}

async fn wait_for_target_count(
    target: &Client,
    target_schema: &str,
    expected: i64,
    active: &ActiveJob,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    let relation = format!("{}.items", quote_identifier(target_schema));
    loop {
        if active.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return Err(io::Error::other(format!(
                "pipeline stopped before target converged: {:?}",
                active.telemetry.snapshot().last_error
            ))
            .into());
        }
        let exists: bool = target
            .query_one("SELECT to_regclass($1) IS NOT NULL", &[&relation])
            .await?
            .get(0);
        if exists {
            let count: i64 = target
                .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
                .await?
                .get(0);
            if count == expected {
                return Ok(());
            }
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("target did not reach {expected} rows"),
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_checkpoint_after(
    target: &Client,
    pipeline_id: PipelineId,
    previous: PgLsn,
    active: &ActiveJob,
) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if active.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return Err(io::Error::other(format!(
                "pipeline stopped before checkpoint advanced: {:?}",
                active.telemetry.snapshot().last_error
            ))
            .into());
        }
        if load_checkpoint_lsn(target, pipeline_id).await? > previous {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "target checkpoint did not advance after schema no-op",
            )
            .into());
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_resource_wait(active: &ActiveJob) -> Result<(), Box<dyn Error>> {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if active.handle.as_ref().is_some_and(JoinHandle::is_finished) {
            return Err(io::Error::other(format!(
                "pipeline stopped before entering resource wait: {:?}",
                active.telemetry.snapshot().last_error
            ))
            .into());
        }
        let snapshot = active.telemetry.snapshot();
        if snapshot.state == PipelineRuntimeState::ResourceWait {
            assert!(
                snapshot
                    .resource_wait_reason
                    .as_deref()
                    .is_some_and(|reason| reason.contains("spool capacity wait"))
            );
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "pipeline did not enter resource wait",
            )
            .into());
        }
        sleep(Duration::from_millis(10)).await;
    }
}

struct RssSampler {
    baseline: Option<u64>,
    peak: Arc<AtomicU64>,
    cancellation: CancellationToken,
    handle: Option<JoinHandle<()>>,
}

impl RssSampler {
    fn start() -> io::Result<Self> {
        let baseline = resident_set_bytes()?;
        let peak = Arc::new(AtomicU64::new(baseline.unwrap_or(0)));
        let cancellation = CancellationToken::new();
        let handle = baseline.map(|_| {
            let task_peak = Arc::clone(&peak);
            let task_cancellation = cancellation.clone();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        () = task_cancellation.cancelled() => break,
                        () = sleep(Duration::from_millis(5)) => {
                            if let Ok(Some(rss)) = resident_set_bytes() {
                                task_peak.fetch_max(rss, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        });
        Ok(Self {
            baseline,
            peak,
            cancellation,
            handle,
        })
    }

    async fn stop(mut self) -> Result<Option<u64>, tokio::task::JoinError> {
        self.cancellation.cancel();
        if let Some(handle) = self.handle.take() {
            handle.await?;
        }
        Ok(self
            .baseline
            .map(|baseline| self.peak.load(Ordering::Relaxed).saturating_sub(baseline)))
    }
}

fn resident_set_bytes() -> io::Result<Option<u64>> {
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status")?;
        let kibibytes = status
            .lines()
            .find_map(|line| line.strip_prefix("VmRSS:"))
            .and_then(|value| value.split_whitespace().next())
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| io::Error::other("/proc/self/status does not contain VmRSS"))?;
        Ok(Some(kibibytes.saturating_mul(1024)))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(None)
    }
}

async fn target_ledger_rows(
    target: &Client,
    pipeline_id: PipelineId,
) -> Result<i64, tokio_postgres::Error> {
    target
        .query_one(
            "SELECT
                 (SELECT count(*) FROM pg2cb_meta.transaction_chunk_progress WHERE pipeline_id=$1) +
                 (SELECT count(*) FROM pg2cb_meta.transaction_committed_chunks WHERE pipeline_id=$1)",
            &[&pipeline_id.as_uuid()],
        )
        .await
        .map(|row| row.get(0))
}

async fn load_single_loading_snapshot(
    target: &Client,
    pipeline_id: PipelineId,
) -> Result<LoadingSnapshotState, Box<dyn Error>> {
    let rows = target
        .query(
            "SELECT g.snapshot_group_id, t.shadow_schema, t.shadow_table,
                    p.shadow_relation_oid, p.cursor_values, p.pages_copied, p.rows_copied,
                    n.consistent_lsn::text AS consistent_lsn
             FROM pg2cb_meta.snapshot_groups AS g
             JOIN pg2cb_meta.snapshot_group_tables AS t
               ON t.snapshot_group_id=g.snapshot_group_id
             JOIN pg2cb_meta.snapshot_table_progress AS p
               ON p.snapshot_group_id=t.snapshot_group_id
              AND p.target_schema=t.target_schema AND p.target_table=t.target_table
             JOIN pg2cb_meta.snapshot_group_nodes AS n
               ON n.snapshot_group_id=g.snapshot_group_id
             WHERE g.pipeline_id=$1 AND g.state='loading'",
            &[&pipeline_id.as_uuid()],
        )
        .await?;
    assert_eq!(rows.len(), 1, "expected exactly one loading snapshot table");
    let row = &rows[0];
    Ok(LoadingSnapshotState {
        group_id: row.try_get("snapshot_group_id")?,
        shadow_schema: row.try_get("shadow_schema")?,
        shadow_table: row.try_get("shadow_table")?,
        shadow_relation_oid: row.try_get("shadow_relation_oid")?,
        cursor: row.try_get("cursor_values")?,
        pages_copied: row.try_get("pages_copied")?,
        rows_copied: row.try_get("rows_copied")?,
        consistent_lsn: row.try_get::<_, String>("consistent_lsn")?.parse()?,
    })
}

async fn load_single_active_snapshot(
    target: &Client,
    pipeline_id: PipelineId,
) -> Result<ActiveSnapshotState, Box<dyn Error>> {
    let rows = target
        .query(
            "SELECT g.snapshot_group_id, m.relation_oid,
                    n.consistent_lsn::text AS consistent_lsn,
                    c.applied_lsn::text AS checkpoint_lsn
             FROM pg2cb_meta.snapshot_groups AS g
             JOIN pg2cb_meta.snapshot_group_tables AS t
               ON t.snapshot_group_id=g.snapshot_group_id
             JOIN pg2cb_meta.snapshot_group_nodes AS n
               ON n.snapshot_group_id=g.snapshot_group_id
             JOIN pg2cb_meta.node_checkpoints AS c
               ON c.pipeline_id=g.pipeline_id
              AND c.topology_generation=g.topology_generation
              AND c.node_id=n.node_id
             JOIN pg2cb_meta.managed_tables AS m
               ON m.pipeline_id=g.pipeline_id
              AND m.target_schema=t.target_schema AND m.target_table=t.target_table
              AND m.state='active'
             WHERE g.pipeline_id=$1 AND g.state='active'",
            &[&pipeline_id.as_uuid()],
        )
        .await?;
    assert_eq!(rows.len(), 1, "expected exactly one active snapshot table");
    let row = &rows[0];
    Ok(ActiveSnapshotState {
        group_id: row.try_get("snapshot_group_id")?,
        relation_oid: row.try_get("relation_oid")?,
        consistent_lsn: row.try_get::<_, String>("consistent_lsn")?.parse()?,
        checkpoint_lsn: row.try_get::<_, String>("checkpoint_lsn")?.parse()?,
    })
}

async fn stale_snapshot_artifact_count(
    target: &Client,
    pipeline_id: PipelineId,
    group_id: Uuid,
) -> Result<i64, tokio_postgres::Error> {
    target
        .query_one(
            "SELECT
                 (SELECT count(*) FROM pg2cb_meta.snapshot_groups WHERE snapshot_group_id=$1) +
                 (SELECT count(*) FROM pg2cb_meta.snapshot_group_tables WHERE snapshot_group_id=$1) +
                 (SELECT count(*) FROM pg2cb_meta.snapshot_group_nodes WHERE snapshot_group_id=$1) +
                 (SELECT count(*) FROM pg2cb_meta.snapshot_table_progress WHERE snapshot_group_id=$1) +
                 (SELECT count(*) FROM pg2cb_meta.managed_tables WHERE pipeline_id=$2 AND snapshot_group_id=$1)",
            &[&group_id, &pipeline_id.as_uuid()],
        )
        .await
        .map(|row| row.get(0))
}

async fn snapshot_group_count(
    target: &Client,
    pipeline_id: PipelineId,
    state: &str,
) -> Result<i64, tokio_postgres::Error> {
    target
        .query_one(
            "SELECT count(*) FROM pg2cb_meta.snapshot_groups WHERE pipeline_id=$1 AND state=$2",
            &[&pipeline_id.as_uuid(), &state],
        )
        .await
        .map(|row| row.get(0))
}

async fn source_slot_count(source: &Client, slot_name: &str) -> Result<i64, tokio_postgres::Error> {
    source
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_replication_slots WHERE slot_name=$1",
            &[&slot_name],
        )
        .await
        .map(|row| row.get(0))
}

async fn rebuild_operation_count(context: &TestContext) -> Result<i64, tokio_postgres::Error> {
    context
        .source
        .query_one(
            "SELECT count(*) FROM cloudberry_etl_control.operations WHERE pipeline_id=$1 AND operation_type='rebuild'",
            &[&context.pipeline.id.as_uuid()],
        )
        .await
        .map(|row| row.get(0))
}

async fn load_checkpoint_lsn(
    target: &Client,
    pipeline_id: PipelineId,
) -> Result<PgLsn, Box<dyn Error>> {
    let value: String = target
        .query_one(
            "SELECT applied_lsn::text FROM pg2cb_meta.node_checkpoints WHERE pipeline_id=$1",
            &[&pipeline_id.as_uuid()],
        )
        .await?
        .get(0);
    Ok(value.parse()?)
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
                    "DELETE FROM pg2cb_meta.{table} WHERE snapshot_group_id IN (SELECT snapshot_group_id FROM pg2cb_meta.snapshot_groups WHERE pipeline_id=$1)"
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

async fn cleanup_control_metadata(source: &Client, pipeline: &PipelineDefinition) {
    let pipeline_id = pipeline.id.as_uuid();
    for table in [
        "pipeline_leases",
        "operations",
        "audit_log",
        "config_revisions",
        "pipelines",
    ] {
        let column = match table {
            "audit_log" => "object_id",
            "pipelines" => "id",
            _ => "pipeline_id",
        };
        if table == "audit_log" {
            let _ = source
                .execute(
                    &format!("DELETE FROM cloudberry_etl_control.{table} WHERE {column}=$1"),
                    &[&pipeline.id.to_string()],
                )
                .await;
        } else {
            let _ = source
                .execute(
                    &format!("DELETE FROM cloudberry_etl_control.{table} WHERE {column}=$1"),
                    &[&pipeline_id],
                )
                .await;
        }
    }
    let _ = source
        .execute(
            "DELETE FROM cloudberry_etl_control.sources WHERE id=$1",
            &[&pipeline.source_id.as_uuid()],
        )
        .await;
    let _ = source
        .execute(
            "DELETE FROM cloudberry_etl_control.targets WHERE id=$1",
            &[&pipeline.target_id.as_uuid()],
        )
        .await;
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
