//! Full-runtime Phase 1 recovery matrix against PostgreSQL 18 and Cloudberry 2.1.
//!
//! The test destroys and reconstructs the complete pipeline job at each fault boundary. Only the
//! control records, source slot/WAL, local spool directory, and target metadata survive, matching
//! a process restart without relying on timing-sensitive log scraping.

use std::{
    error::Error,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
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
    telemetry::PipelineTelemetryHandle,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryFault {
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
        self.trigger(RecoveryFault::Source(point))
    }
}

impl LedgeredCommitObserver for FaultController {
    fn observe(&self, phase: LedgeredCommitPhase, kind: LedgeredCommitKind) -> Result<(), String> {
        self.trigger(RecoveryFault::Target { phase, kind })
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
async fn full_runtime_recovers_at_phase1_durable_boundaries() -> Result<(), Box<dyn Error>> {
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let mut context = TestContext::setup(&source_dsn, &target_dsn).await?;

    let result = run_recovery_matrix(&mut context).await;
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

impl TestContext {
    async fn setup(source_dsn: &str, target_dsn: &str) -> Result<Self, Box<dyn Error>> {
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
            source_dsn,
            target_dsn,
            &source_schema,
            &target_schema,
            &suffix,
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
        .with_target_commit_observer(Arc::<FaultController>::clone(&self.controller));
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
    source_dsn: &str,
    target_dsn: &str,
    source_schema: &str,
    target_schema: &str,
    suffix: &str,
) -> Result<PipelineDefinition, Box<dyn Error>> {
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
                    "memory_high_water_changes": 1,
                    "memory_high_water_bytes": 1,
                    "segment_target_bytes": 128,
                    "disk_high_water_bytes": 16 * 1024 * 1024,
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
                "max_rows": 2,
                "max_bytes": 1024 * 1024,
                "max_delay_ms": 10
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
