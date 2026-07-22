//! Concrete PostgreSQL 18 to Cloudberry pipeline job.

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    str::FromStr as _,
    sync::Arc,
};

use async_trait::async_trait;
use cloudberry_etl_core::{
    id::PipelineId,
    lsn::PgLsn,
    mapping::shorten_identifier,
    pipeline::{PipelinePhase, SourceTopology},
};
use cloudberry_etl_metadata::{
    crypto::{CryptoError, MasterKey, source_credential_aad, target_credential_aad},
    model::{PipelineDefinition, PipelineLease, SourceProfile, TargetProfile},
    store::{ControlStore, StoreError},
};
use cloudberry_etl_source_postgres::{
    SourceError,
    catalog::{CatalogOptions, PreflightOptions, PreflightReport, TableInventory, preflight},
    connection::connect_replication,
    ddl::{CANONICAL_DDL_TRIGGER_NAME, DdlInstallSpec, ensure_ddl_capture},
    publication::{
        LogicalSlotState, PublicationSpec, drop_logical_slot, ensure_publication,
        inspect_logical_slot, validate_publication,
    },
    snapshot::{SnapshotCursor, SnapshotPageLimits, SnapshotSession, begin_at_exported_snapshot},
    snapshot_slot::SnapshotSlotGuard,
    spool::{ChunkLimits, SpoolError, SpoolIdentity, SpoolJournal, SpoolLimits},
    wal::{ReplicationTransport, SourceNodeIdentity, TransactionAssembler, TransactionLimits},
};
use cloudberry_etl_target_cloudberry::{
    apply::LedgeredCommitObserver,
    checkpoint::{
        CheckpointError, CheckpointKey, NodeCheckpoint, PipelineFence, activate_pipeline_fence,
        load_node_checkpoint,
    },
    migration::{MigrationError, migrate_target_database},
    schema_event::{
        SchemaEvent, SchemaEventError, SchemaEventState, advance_schema_event_state_in_transaction,
        fail_schema_event_and_block_transitions, list_unfinished_schema_events, load_schema_event,
    },
    snapshot::{
        ActiveTableRequirement, QuarantineGcPolicy, SnapshotActivationRequest, SnapshotOwnership,
        SnapshotPageCommitObserver, SnapshotTargetError, SnapshotTargetPlan,
        activate_snapshot_group, activate_table_snapshot_group_in_transaction,
        adopt_table_snapshot_replay_group, begin_snapshot_apply, begin_snapshot_group,
        begin_snapshot_group_in_transaction, begin_snapshot_pages, cleanup_stale_snapshot_groups,
        garbage_collect_quarantined_tables, load_snapshot_group_manifest,
        plan_snapshot_target_with_storage, quarantine_active_table_in_transaction,
        reset_interrupted_table_snapshot_group, validate_active_tables,
    },
    storage::{StorageCapabilityError, load_relation_storage, verify_storage_available},
    table_transition::{
        TableTransition, TableTransitionAction, TableTransitionError, TableTransitionState,
        advance_table_transition_state_in_transaction,
        begin_table_snapshot_transition_in_transaction, list_unfinished_table_transitions,
    },
};
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;
use tokio::time::{self, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    adapters::{
        AdapterConfigError, CloudberryTransactionSink, DdlScope, PgOutputTransactionSource,
        SourceIngestObserver, TableBinding, TableBindingRegistry, TableReplayFence,
    },
    batch::{BatchError, BatchLimits, Batcher},
    pipeline::{PipelineError, SchemaEventKey},
    runtime::reconciler::{JobFactoryError, PipelineJobFactory},
    schema_transition::{SchemaAction, SchemaTransactionPlan},
    supervisor::{PipelineJob, SupervisorError},
    telemetry::PipelineTelemetryHandle,
};

use super::{
    EndpointRole, PipelineSettings, PlannedTable, SourceSettings, TablePlanningError,
    TargetPlanningContext, TargetSettings, WalRetentionSettings, connect_sql, plan_tables,
    replication_names,
};

const STANDALONE_NODE_ID: i32 = 0;

#[derive(Debug, Clone, PartialEq, Eq)]
enum WalMonitorOutcome {
    Cancelled,
    Rebuild(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum WalRetentionAssessment {
    Healthy,
    Warning(String),
    Rebuild(String),
}

#[derive(Debug, Error)]
enum RuntimeJobError {
    #[error("source profile is disabled")]
    SourceDisabled,
    #[error("target profile is disabled")]
    TargetDisabled,
    #[error("pipeline snapshot generation must be positive")]
    InvalidSnapshotGeneration,
    #[error("pipeline lease belongs to a different pipeline")]
    LeaseMismatch,
    #[error("source profile {0} does not exist")]
    MissingSource(String),
    #[error("target profile {0} does not exist")]
    MissingTarget(String),
    #[error("pipeline disappeared while requesting a schema rebuild")]
    MissingPipelineForRebuild,
    #[error("persisted schema event {source_lsn}/{source_xid} disappeared before fallback")]
    MissingSchemaEvent { source_lsn: PgLsn, source_xid: u64 },
    #[error("completed schema event {source_lsn}/{source_xid} unexpectedly raised a barrier")]
    CompletedSchemaEventBarrier { source_lsn: PgLsn, source_xid: u64 },
    #[error("table transition for relation {0} has no recoverable snapshot group")]
    MissingTableSnapshotGroup(u32),
    #[error("table transition for relation {relation_id} has invalid plan: {reason}")]
    InvalidTableTransitionPlan { relation_id: u32, reason: String },
    #[error("table transition for relation {0} has no pending generation")]
    MissingPendingTableGeneration(u32),
    #[error("persisted online transition for relation {0} predates the AOCO reload executor")]
    UnsupportedPersistedOnlineTransition(u32),
    #[error("table snapshot group `{group}` has an invalid standalone source boundary: {reason}")]
    InvalidTableSnapshotBoundary { group: Uuid, reason: String },
    #[error("table snapshot group `{0}` must not use the pipeline's main logical slot")]
    TableSnapshotUsesMainSlot(Uuid),
    #[error("{0} topology is validation-gated and cannot run yet")]
    UnsupportedTopology(&'static str),
    #[error("source endpoint is a standby; a writable primary endpoint is required")]
    SourceIsStandby,
    #[error("source profile expects database `{expected}`, server reported `{actual}`")]
    SourceDatabaseMismatch { expected: String, actual: String },
    #[error("source profile is standalone but the endpoint has Citus installed")]
    UnexpectedCitus,
    #[error("target profile expects database `{expected}`, server reported `{actual}`")]
    TargetDatabaseMismatch { expected: String, actual: String },
    #[error("target endpoint is not Apache Cloudberry 2.1")]
    UnsupportedTarget,
    #[error("target table `{table}` uses `{actual}` access method, expected `{expected}`")]
    TargetStorageDrift {
        table: String,
        expected: &'static str,
        actual: String,
    },
    #[error("no source table satisfies the complete source and target contract")]
    NoEligibleTables,
    #[error("source table `{table}` is inside the configured scope but unsupported: {reason}")]
    UnsupportedIncludedTable { table: String, reason: String },
    #[error("source catalog changed while establishing the initial snapshot boundary")]
    InitialCatalogChanged,
    #[error("initial snapshot failed: {original}; unable to clean up slot `{slot}`: {cleanup}")]
    InitialSnapshotCleanup {
        original: String,
        slot: String,
        cleanup: String,
    },
    #[error("logical slot `{slot}` is active while recovery owns no target checkpoint")]
    ActiveOrphanSlot { slot: String },
    #[error("logical slot `{slot}` belongs to database {actual:?}, expected `{expected}`")]
    SlotDatabaseMismatch {
        slot: String,
        actual: Option<String>,
        expected: String,
    },
    #[error("logical slot `{slot}` uses unsupported lifecycle options: {reason}")]
    UnsupportedSlotMode { slot: String, reason: String },
    #[error("logical slot `{slot}` was invalidated: {reason}")]
    SlotInvalidated { slot: String, reason: String },
    #[error("managed publication drifted from its exact runtime contract: {0}")]
    PublicationDrift(String),
    #[error("logical slot `{slot}` uses plugin `{plugin}`, expected pgoutput")]
    WrongSlotPlugin { slot: String, plugin: String },
    #[error("logical slot `{0}` is missing for an existing target checkpoint")]
    MissingSlot(String),
    #[error("source slot confirmed LSN {slot_lsn} is ahead of target checkpoint {target_lsn}")]
    SlotAheadOfTarget { slot_lsn: PgLsn, target_lsn: PgLsn },
    #[error("target checkpoint precedes the source slot restart LSN; retained WAL was lost")]
    WalLost,
    #[error("source logical slot `{slot}` crossed the WAL retention protection limit: {reason}")]
    WalRetentionExceeded { slot: String, reason: String },
    #[error("persisted slot LSN `{0}` is invalid")]
    InvalidPersistedLsn(String),
    #[error("target checkpoint source identity no longer matches the PostgreSQL endpoint")]
    SourceIdentityChanged,
    #[error("target checkpoint slot `{actual}` does not match configured slot `{expected}`")]
    CheckpointSlotMismatch { expected: String, actual: String },
    #[error("pipeline was cancelled")]
    Cancelled,
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    Settings(#[from] super::SettingsError),
    #[error(transparent)]
    Connect(#[from] super::SqlConnectError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Spool(#[from] SpoolError),
    #[error(transparent)]
    TargetMigration(#[from] MigrationError),
    #[error(transparent)]
    StorageCapability(#[from] StorageCapabilityError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    SchemaEvent(#[from] SchemaEventError),
    #[error(transparent)]
    TableTransition(#[from] TableTransitionError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotTargetError),
    #[error(transparent)]
    Planning(#[from] TablePlanningError),
    #[error(transparent)]
    Adapter(#[from] AdapterConfigError),
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
    #[error("target validation query failed: {0}")]
    TargetDatabase(#[from] tokio_postgres::Error),
}

pub struct PostgresCloudberryJobFactory {
    control: Arc<dyn ControlStore>,
    master_key: Arc<MasterKey>,
    spool_root: PathBuf,
    source_ingest_observer: Option<Arc<dyn SourceIngestObserver>>,
    target_commit_observer: Option<Arc<dyn LedgeredCommitObserver>>,
    snapshot_page_commit_observer: Option<Arc<dyn SnapshotPageCommitObserver>>,
}

impl std::fmt::Debug for PostgresCloudberryJobFactory {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresCloudberryJobFactory")
            .finish_non_exhaustive()
    }
}

impl PostgresCloudberryJobFactory {
    #[must_use]
    pub fn new(control: Arc<dyn ControlStore>, master_key: Arc<MasterKey>) -> Self {
        Self {
            control,
            master_key,
            spool_root: PathBuf::from("data/spool"),
            source_ingest_observer: None,
            target_commit_observer: None,
            snapshot_page_commit_observer: None,
        }
    }

    #[must_use]
    pub fn with_spool_root(mut self, spool_root: impl AsRef<Path>) -> Self {
        self.spool_root = spool_root.as_ref().to_owned();
        self
    }

    #[must_use]
    pub fn with_source_ingest_observer(mut self, observer: Arc<dyn SourceIngestObserver>) -> Self {
        self.source_ingest_observer = Some(observer);
        self
    }

    #[must_use]
    pub fn with_target_commit_observer(
        mut self,
        observer: Arc<dyn LedgeredCommitObserver>,
    ) -> Self {
        self.target_commit_observer = Some(observer);
        self
    }

    #[must_use]
    pub fn with_snapshot_page_commit_observer(
        mut self,
        observer: Arc<dyn SnapshotPageCommitObserver>,
    ) -> Self {
        self.snapshot_page_commit_observer = Some(observer);
        self
    }

    async fn resolve(
        &self,
        pipeline: &PipelineDefinition,
        lease: &PipelineLease,
        telemetry: PipelineTelemetryHandle,
    ) -> Result<PostgresCloudberryJob, RuntimeJobError> {
        if pipeline.id != lease.pipeline_id {
            return Err(RuntimeJobError::LeaseMismatch);
        }
        let source = self
            .control
            .list_sources()
            .await?
            .into_iter()
            .find(|source| source.id == pipeline.source_id)
            .ok_or_else(|| RuntimeJobError::MissingSource(pipeline.source_id.to_string()))?;
        let target = self
            .control
            .list_targets()
            .await?
            .into_iter()
            .find(|target| target.id == pipeline.target_id)
            .ok_or_else(|| RuntimeJobError::MissingTarget(pipeline.target_id.to_string()))?;
        if !source.enabled {
            return Err(RuntimeJobError::SourceDisabled);
        }
        if !target.enabled {
            return Err(RuntimeJobError::TargetDisabled);
        }
        let topology_generation = u64::try_from(pipeline.snapshot_generation)
            .map_err(|_| RuntimeJobError::InvalidSnapshotGeneration)?;
        if topology_generation == 0 {
            return Err(RuntimeJobError::InvalidSnapshotGeneration);
        }
        let source_settings = SourceSettings::parse(&source.settings)?;
        let target_settings = TargetSettings::parse(&target.settings)?;
        let pipeline_settings = PipelineSettings::parse(&pipeline.settings)?;
        pipeline_settings.validate_with_source(&source_settings)?;
        let source_dsn = self.master_key.decrypt(
            &source.encrypted_dsn,
            source_credential_aad(source.id).as_bytes(),
        )?;
        let target_dsn = self.master_key.decrypt(
            &target.encrypted_dsn,
            target_credential_aad(target.id).as_bytes(),
        )?;
        Ok(PostgresCloudberryJob {
            control: Arc::clone(&self.control),
            pipeline_id: pipeline.id,
            topology_generation,
            fence: PipelineFence {
                pipeline_id: pipeline.id,
                topology_generation,
                fencing_token: lease.fencing_token,
            },
            source,
            target,
            source_settings,
            target_settings,
            pipeline_settings,
            source_dsn,
            target_dsn,
            spool_root: self.spool_root.clone(),
            source_ingest_observer: self.source_ingest_observer.as_ref().map(Arc::clone),
            target_commit_observer: self.target_commit_observer.as_ref().map(Arc::clone),
            snapshot_page_commit_observer: self
                .snapshot_page_commit_observer
                .as_ref()
                .map(Arc::clone),
            telemetry,
        })
    }
}

#[async_trait]
impl PipelineJobFactory for PostgresCloudberryJobFactory {
    async fn create(
        &self,
        pipeline: &PipelineDefinition,
        lease: &PipelineLease,
        telemetry: PipelineTelemetryHandle,
    ) -> Result<Arc<dyn PipelineJob>, JobFactoryError> {
        self.resolve(pipeline, lease, telemetry)
            .await
            .map(|job| Arc::new(job) as Arc<dyn PipelineJob>)
            .map_err(|error| JobFactoryError::new(error.to_string()))
    }
}

struct PostgresCloudberryJob {
    control: Arc<dyn ControlStore>,
    pipeline_id: PipelineId,
    topology_generation: u64,
    fence: PipelineFence,
    source: SourceProfile,
    target: TargetProfile,
    source_settings: SourceSettings,
    target_settings: TargetSettings,
    pipeline_settings: PipelineSettings,
    source_dsn: SecretString,
    target_dsn: SecretString,
    spool_root: PathBuf,
    source_ingest_observer: Option<Arc<dyn SourceIngestObserver>>,
    target_commit_observer: Option<Arc<dyn LedgeredCommitObserver>>,
    snapshot_page_commit_observer: Option<Arc<dyn SnapshotPageCommitObserver>>,
    telemetry: PipelineTelemetryHandle,
}

impl std::fmt::Debug for PostgresCloudberryJob {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresCloudberryJob")
            .field("pipeline_id", &self.pipeline_id)
            .field("topology_generation", &self.topology_generation)
            .field("source_id", &self.source.id)
            .field("target_id", &self.target.id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl PipelineJob for PostgresCloudberryJob {
    async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError> {
        self.telemetry.set_phase(PipelinePhase::Validating);
        let result = match data_plane_for_topology(self.source.topology) {
            DataPlane::Standalone => self.run_standalone(cancellation).await,
            DataPlane::Gated(name) => Err(RuntimeJobError::UnsupportedTopology(name)),
        };
        match result {
            Ok(()) => Ok(()),
            Err(RuntimeJobError::Cancelled) => {
                self.telemetry.set_phase(PipelinePhase::Stopped);
                Ok(())
            }
            Err(error) => {
                self.telemetry.mark_failed(error.to_string());
                Err(SupervisorError::Task(error.to_string()))
            }
        }
    }
}

struct RuntimeTable {
    planned: PlannedTable,
    snapshot_plan: SnapshotTargetPlan,
    table_generation: u64,
}

struct PreparedRun {
    tables: Vec<RuntimeTable>,
    checkpoint: NodeCheckpoint,
    replay_fences: Vec<TableReplayFence>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableSchemaExecution {
    Completed,
    RequiresPipelineRebuild,
}

const fn transition_action_matches(
    persisted: TableTransitionAction,
    action: &SchemaAction,
) -> bool {
    matches!(
        (persisted, action),
        (TableTransitionAction::Noop, SchemaAction::Noop)
            | (TableTransitionAction::Online, SchemaAction::Online { .. })
            | (TableTransitionAction::Reload, SchemaAction::Reload { .. })
            | (TableTransitionAction::Drop, SchemaAction::Drop)
            | (TableTransitionAction::Add, SchemaAction::Add { .. })
    )
}

/// Which runtime data plane serves a given source topology.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DataPlane {
    /// The single-logical-node data plane (one primary endpoint, one slot, one
    /// node checkpoint). Serves both `Standalone` and `PhysicalHa`.
    Standalone,
    /// Topology whose data plane is not implemented yet; carries the stable name
    /// used in the fail-closed error.
    Gated(&'static str),
}

/// Map a source topology to its data plane.
///
/// `PhysicalHa` shares the `Standalone` data plane: it is a single logical node
/// with a physical standby, so ingest, slot, and checkpoint handling are
/// identical. A failover changes the source timeline, which the resume path
/// rejects through the checkpoint identity check, forcing a safe rebuild rather
/// than continuing across a divergent history — no separate data plane needed.
///
/// `Citus` requires a genuinely different multi-node data plane (per-worker
/// publications, slots, identities, and a per-node checkpoint vector) and stays
/// gated until Phase 4.
const fn data_plane_for_topology(topology: SourceTopology) -> DataPlane {
    match topology {
        SourceTopology::Standalone | SourceTopology::PhysicalHa => DataPlane::Standalone,
        SourceTopology::Citus => DataPlane::Gated("citus"),
    }
}

/// Bridge a source canonical PK key into the target's durable text cursor.
///
/// Canonical snapshot keys are produced through PostgreSQL's text output with a
/// UTF-8 client encoding, so each key column is valid UTF-8 and maps losslessly
/// to the `Vec<String>` cursor that the target digests. A non-UTF-8 key would
/// violate the canonical contract and is rejected rather than silently lossy.
fn cursor_to_text(key: &[bytes::Bytes]) -> Result<Vec<String>, SourceError> {
    key.iter()
        .map(|value| {
            std::str::from_utf8(value).map(str::to_owned).map_err(|_| {
                SourceError::Contract("canonical snapshot key is not valid UTF-8".to_owned())
            })
        })
        .collect()
}

impl PostgresCloudberryJob {
    async fn run_standalone(&self, cancellation: CancellationToken) -> Result<(), RuntimeJobError> {
        let mut source_setup = connect_sql(&self.source_dsn, EndpointRole::Source).await?;
        let mut target = connect_sql(&self.target_dsn, EndpointRole::Target).await?;
        let report = preflight(
            &source_setup,
            &PreflightOptions {
                metadata_schema: self.source_settings.metadata_schema.clone(),
                ..PreflightOptions::default()
            },
        )
        .await?;
        self.validate_source(&report)?;
        self.validate_target(&target).await?;
        self.validate_storage_capabilities(&target).await?;
        migrate_target_database(&mut target).await?;
        activate_pipeline_fence(&target, self.fence).await?;
        let names = replication_names(self.pipeline_id, STANDALONE_NODE_ID);
        self.recover_table_snapshot_transitions(&source_setup, &mut target, &report, &names.slot)
            .await?;
        if !self
            .resume_unfinished_table_schema_events(&mut target, &report, &names, &cancellation)
            .await?
        {
            return Ok(());
        }
        let stale_groups = cleanup_stale_snapshot_groups(&mut target, self.fence).await?;
        for group in &stale_groups {
            tracing::warn!(
                pipeline_id = %self.pipeline_id,
                snapshot_group_id = %group.snapshot_group_id,
                dropped_shadows = group.dropped_shadows.len(),
                "removed stale loading snapshot group before pipeline start"
            );
        }
        let gc_policy = QuarantineGcPolicy::enabled(
            self.target_settings.quarantine_retention(),
            self.target_settings.quarantine_gc_max_tables,
        )?;
        let gc = garbage_collect_quarantined_tables(&mut target, self.fence, gc_policy).await?;
        if !gc.dropped.is_empty() {
            tracing::info!(
                pipeline_id = %self.pipeline_id,
                dropped_quarantines = gc.dropped.len(),
                "garbage-collected expired quarantined target tables"
            );
        }
        self.install_ddl_capture(&source_setup).await?;

        let checkpoint_key = CheckpointKey {
            pipeline_id: self.pipeline_id,
            topology_generation: self.topology_generation,
            node_id: STANDALONE_NODE_ID,
        };
        let preparation = match load_node_checkpoint(&target, checkpoint_key).await? {
            Some(stored) => {
                self.telemetry.set_phase(PipelinePhase::CatchingUp);
                self.prepare_resume(
                    &source_setup,
                    &mut target,
                    &report,
                    &names,
                    stored.checkpoint,
                )
                .await
            }
            None => {
                self.telemetry.set_phase(PipelinePhase::Snapshotting);
                self.prepare_initial_snapshot(
                    &mut source_setup,
                    &mut target,
                    &report,
                    &names,
                    cancellation.clone(),
                )
                .await
            }
        };
        let prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error)
                if matches!(
                    error,
                    RuntimeJobError::MissingSlot(_)
                        | RuntimeJobError::SlotInvalidated { .. }
                        | RuntimeJobError::SlotAheadOfTarget { .. }
                        | RuntimeJobError::WalLost
                        | RuntimeJobError::WalRetentionExceeded { .. }
                        | RuntimeJobError::PublicationDrift(_)
                        | RuntimeJobError::TargetStorageDrift { .. }
                ) =>
            {
                self.request_rebuild(&error.to_string()).await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        if cancellation.is_cancelled() {
            return Err(RuntimeJobError::Cancelled);
        }
        self.telemetry
            .checkpoint_initialized(prepared.checkpoint.applied_lsn);
        let snapshot_generation = i64::try_from(self.topology_generation)
            .map_err(|_| RuntimeJobError::InvalidSnapshotGeneration)?;
        let completed_rebuilds = self
            .control
            .complete_pipeline_rebuilds(self.pipeline_id, snapshot_generation)
            .await?;
        if completed_rebuilds > 0 {
            tracing::info!(
                pipeline_id = %self.pipeline_id,
                snapshot_generation,
                completed_rebuilds,
                "snapshot rebuild operations completed"
            );
        }

        let replication_client = connect_replication(self.source_dsn.expose_secret()).await?;
        let transport = ReplicationTransport::start(
            &replication_client,
            &names.slot,
            &prepared.checkpoint.applied_lsn.to_string(),
            &names.publication,
        )
        .await?;
        let source_identity = SourceNodeIdentity {
            node_id: STANDALONE_NODE_ID,
            system_identifier: report.identity.system_identifier,
            timeline: report.identity.timeline,
        };
        let transaction_settings = self.source_settings.transaction;
        // Run preparation proved the managed slot covers the target checkpoint (or created both
        // from one snapshot), and START_REPLICATION above established replay from that point.
        // Local spool files are not a source of truth, so this exact identity can be regenerated.
        let journal = SpoolJournal::open_after_wal_replay_verified(
            &self.spool_root,
            SpoolIdentity {
                pipeline_id: self.pipeline_id,
                topology_generation: self.topology_generation,
                node_id: source_identity.node_id,
                system_identifier: source_identity.system_identifier,
                timeline: source_identity.timeline,
            },
            SpoolLimits {
                memory_high_water_bytes: transaction_settings.memory_high_water_bytes,
                segment_target_bytes: transaction_settings.segment_target_bytes,
                disk_high_water_bytes: transaction_settings.disk_high_water_bytes,
                minimum_free_disk_bytes: transaction_settings.minimum_free_disk_bytes,
            },
        )?;
        let assembler = TransactionAssembler::with_spool(
            source_identity,
            TransactionLimits {
                max_changes: transaction_settings.memory_high_water_changes,
                max_bytes: transaction_settings.memory_high_water_bytes,
            },
            journal,
        )?;
        let mut source = match &self.source_ingest_observer {
            Some(observer) => PgOutputTransactionSource::new_with_telemetry_and_observer(
                transport,
                assembler,
                prepared.checkpoint.applied_lsn,
                self.telemetry.clone(),
                Arc::clone(observer),
            ),
            None => PgOutputTransactionSource::new_with_telemetry(
                transport,
                assembler,
                prepared.checkpoint.applied_lsn,
                self.telemetry.clone(),
            ),
        };
        let registry = TableBindingRegistry::new(
            prepared
                .tables
                .iter()
                .map(|table| {
                    TableBinding::new(
                        table.planned.source.clone(),
                        table.planned.target.clone(),
                        table.planned.staging_name.clone(),
                        table.planned.storage,
                        table.table_generation,
                        table.planned.schema_fingerprint.clone(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()?,
        )?;
        let mut ddl_scope = DdlScope::from_lists(
            self.source_settings.include_schemas.as_deref(),
            &self.source_settings.exclude_schemas,
        );
        ddl_scope.exclude(self.source_settings.metadata_schema.clone());
        let chunk_limits = ChunkLimits {
            max_records: self.pipeline_settings.batch.max_rows,
            max_bytes: self.pipeline_settings.batch.max_bytes,
        };
        let sink = match &self.target_commit_observer {
            Some(observer) => CloudberryTransactionSink::new_with_chunk_limits_and_observer(
                target,
                self.fence,
                &names.slot,
                registry,
                ddl_scope,
                chunk_limits,
                Arc::clone(observer),
            )?,
            None => CloudberryTransactionSink::new_with_chunk_limits(
                target,
                self.fence,
                &names.slot,
                registry,
                ddl_scope,
                chunk_limits,
            )?,
        }
        .with_replay_fences(prepared.replay_fences.clone())?
        .with_schema_source(source_setup, self.catalog_options())?;
        let batcher = Batcher::new(BatchLimits {
            max_rows: self.pipeline_settings.batch.max_rows,
            max_bytes: self.pipeline_settings.batch.max_bytes,
            max_delay: self.pipeline_settings.batch.max_delay(),
        })?;
        self.telemetry.set_phase(PipelinePhase::CatchingUp);
        self.telemetry.mark_running();
        let monitor_client = connect_sql(&self.source_dsn, EndpointRole::Source).await?;
        let outcome = tokio::select! {
            result = crate::pipeline::replicate_with_telemetry(
                &mut source,
                &sink,
                batcher,
                cancellation.clone(),
                Some(&self.telemetry),
            ) => Ok(result),
            result = self.monitor_wal_retention(
                monitor_client,
                names.slot.clone(),
                cancellation.clone(),
            ) => Err(result),
        };
        drop(source);
        drop(sink);
        drop(replication_client);
        match outcome {
            Ok(Err(PipelineError::SchemaBarrier {
                reason,
                command_tag,
                schema_event,
            })) => {
                if let Some(tag) = &command_tag {
                    tracing::info!(
                        pipeline_id = %self.pipeline_id,
                        command_tag = %tag,
                        "schema barrier raised by DDL; starting table-local transition"
                    );
                }
                if let Some(schema_event) = schema_event {
                    match self
                        .execute_table_schema_event(schema_event, &report, &names, &cancellation)
                        .await?
                    {
                        TableSchemaExecution::Completed => {
                            tracing::info!(
                                pipeline_id = %self.pipeline_id,
                                source_lsn = %schema_event.source_lsn,
                                source_xid = schema_event.source_xid,
                                "table-local schema transition completed; main-slot replay scheduled"
                            );
                            return Ok(());
                        }
                        TableSchemaExecution::RequiresPipelineRebuild => {}
                    }
                    self.telemetry.mark_degraded(reason.clone());
                    self.fail_schema_event_for_rebuild(schema_event, &reason)
                        .await?;
                } else {
                    self.telemetry.mark_degraded(reason.clone());
                }
                self.request_rebuild(&reason).await?;
                Ok(())
            }
            Ok(result) => result.map_err(RuntimeJobError::from),
            Err(Ok(WalMonitorOutcome::Cancelled)) => Err(RuntimeJobError::Cancelled),
            Err(Ok(WalMonitorOutcome::Rebuild(reason))) => {
                self.telemetry.mark_degraded(reason.clone());
                self.request_rebuild(&reason).await?;
                Ok(())
            }
            Err(Err(error)) => Err(error),
        }
    }

    async fn fail_schema_event_for_rebuild(
        &self,
        key: SchemaEventKey,
        reason: &str,
    ) -> Result<(), RuntimeJobError> {
        let mut target = connect_sql(&self.target_dsn, EndpointRole::Target).await?;
        let event = load_schema_event(&target, self.pipeline_id, key.source_lsn, key.source_xid)
            .await?
            .ok_or(RuntimeJobError::MissingSchemaEvent {
                source_lsn: key.source_lsn,
                source_xid: key.source_xid,
            })?;
        match event.state {
            SchemaEventState::Completed => {
                return Err(RuntimeJobError::CompletedSchemaEventBarrier {
                    source_lsn: key.source_lsn,
                    source_xid: key.source_xid,
                });
            }
            SchemaEventState::Pending
            | SchemaEventState::InTransition
            | SchemaEventState::Failed => {
                fail_schema_event_and_block_transitions(
                    &mut target,
                    self.fence,
                    key.source_lsn,
                    key.source_xid,
                    reason,
                )
                .await?;
            }
        }
        Ok(())
    }

    async fn execute_table_schema_event(
        &self,
        key: SchemaEventKey,
        report: &PreflightReport,
        names: &super::ReplicationNames,
        cancellation: &CancellationToken,
    ) -> Result<TableSchemaExecution, RuntimeJobError> {
        let mut target = connect_sql(&self.target_dsn, EndpointRole::Target).await?;
        let event = load_schema_event(&target, self.pipeline_id, key.source_lsn, key.source_xid)
            .await?
            .ok_or(RuntimeJobError::MissingSchemaEvent {
                source_lsn: key.source_lsn,
                source_xid: key.source_xid,
            })?;
        match event.state {
            SchemaEventState::Completed => return Ok(TableSchemaExecution::Completed),
            SchemaEventState::Failed => {
                return Ok(TableSchemaExecution::RequiresPipelineRebuild);
            }
            SchemaEventState::Pending | SchemaEventState::InTransition => {}
        }

        let mut transitions = list_unfinished_table_transitions(&mut target, self.fence)
            .await?
            .into_iter()
            .filter(|transition| {
                transition.key.source_lsn == key.source_lsn
                    && transition.key.source_xid == key.source_xid
            })
            .collect::<Vec<_>>();
        if transitions.is_empty() {
            let plan: SchemaTransactionPlan = serde_json::from_value(event.transitions.clone())
                .map_err(|error| RuntimeJobError::InvalidTableTransitionPlan {
                    relation_id: 0,
                    reason: error.to_string(),
                })?;
            if plan.terminal_relations.is_empty() && !plan.unknown_scope {
                self.cutover_table_schema_event(&mut target, &event, &[], None)
                    .await?;
                return Ok(TableSchemaExecution::Completed);
            }
            return Ok(TableSchemaExecution::RequiresPipelineRebuild);
        }
        if transitions
            .iter()
            .any(|transition| transition.action == TableTransitionAction::Online)
        {
            return Ok(TableSchemaExecution::RequiresPipelineRebuild);
        }
        for transition in &transitions {
            if transition.event_id != event.event_id {
                return Err(RuntimeJobError::InvalidTableTransitionPlan {
                    relation_id: transition.key.source_relation_id,
                    reason: "child event identity does not match its schema event".to_owned(),
                });
            }
            let action: SchemaAction =
                serde_json::from_value(transition.plan.clone()).map_err(|error| {
                    RuntimeJobError::InvalidTableTransitionPlan {
                        relation_id: transition.key.source_relation_id,
                        reason: error.to_string(),
                    }
                })?;
            if !transition_action_matches(transition.action, &action) {
                return Err(RuntimeJobError::InvalidTableTransitionPlan {
                    relation_id: transition.key.source_relation_id,
                    reason: "persisted action tag does not match the typed plan".to_owned(),
                });
            }
        }

        let snapshot_transitions = transitions
            .iter()
            .filter(|transition| {
                matches!(
                    transition.action,
                    TableTransitionAction::Reload | TableTransitionAction::Add
                )
            })
            .collect::<Vec<_>>();
        if snapshot_transitions.is_empty() {
            self.cutover_table_schema_event(&mut target, &event, &transitions, None)
                .await?;
            return Ok(TableSchemaExecution::Completed);
        }

        let existing_group = snapshot_transitions
            .iter()
            .filter_map(|transition| transition.snapshot_group_id)
            .next();
        if let Some(group_id) = existing_group {
            if snapshot_transitions
                .iter()
                .any(|transition| transition.snapshot_group_id != Some(group_id))
            {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: group_id,
                    reason: "schema event references more than one snapshot group".to_owned(),
                });
            }
            let manifest = load_snapshot_group_manifest(&mut target, self.fence, group_id).await?;
            self.cutover_table_schema_event(
                &mut target,
                &event,
                &transitions,
                Some(&manifest.request),
            )
            .await?;
            return Ok(TableSchemaExecution::Completed);
        }

        self.execute_new_table_snapshot(
            &mut target,
            &event,
            &mut transitions,
            report,
            names,
            cancellation,
        )
        .await?;
        Ok(TableSchemaExecution::Completed)
    }

    async fn execute_new_table_snapshot(
        &self,
        target: &mut tokio_postgres::Client,
        event: &SchemaEvent,
        transitions: &mut [TableTransition],
        report: &PreflightReport,
        names: &super::ReplicationNames,
        cancellation: &CancellationToken,
    ) -> Result<(), RuntimeJobError> {
        let source = connect_sql(&self.source_dsn, EndpointRole::Source).await?;
        let inventory = cloudberry_etl_source_postgres::catalog::inspect_tables(
            &source,
            &self.catalog_options(),
        )
        .await?;
        let publication_tables = self.plan_resume_inventory(inventory)?;
        self.ensure_publication(&source, &names.publication, &publication_tables)
            .await?;

        let slot_name = shorten_identifier(&format!("pg2cb_t_{}", event.event_id.simple()));
        if let Some(existing) = inspect_logical_slot(&source, &slot_name).await? {
            if existing.active {
                return Err(RuntimeJobError::ActiveOrphanSlot { slot: slot_name });
            }
            drop_logical_slot(&source, &slot_name).await?;
        }

        let result = async {
            let mut guard =
                SnapshotSlotGuard::create(self.source_dsn.expose_secret(), &slot_name, 1).await?;
            let slot_snapshot = guard.snapshot().clone();
            let mut snapshot_client = connect_sql(&self.source_dsn, EndpointRole::Source).await?;
            let mut snapshot =
                begin_at_exported_snapshot(&mut snapshot_client, &slot_snapshot.snapshot_name)
                    .await?;
            guard.mark_reader_ready()?;
            guard.release()?;

            let snapshot_inventory = snapshot.inspect_tables(&self.catalog_options()).await?;
            let snapshot_tables = self.plan_resume_inventory(snapshot_inventory)?;
            if publication_tables.len() != snapshot_tables.len()
                || publication_tables
                    .iter()
                    .zip(&snapshot_tables)
                    .any(|(before, at_snapshot)| before.planned != at_snapshot.planned)
            {
                snapshot.rollback().await?;
                return Err(RuntimeJobError::InitialCatalogChanged);
            }

            let mut reload_tables = Vec::new();
            for transition in transitions.iter() {
                if !matches!(
                    transition.action,
                    TableTransitionAction::Reload | TableTransitionAction::Add
                ) {
                    continue;
                }
                let pending_generation = transition.pending_table_generation.ok_or(
                    RuntimeJobError::MissingPendingTableGeneration(
                        transition.key.source_relation_id,
                    ),
                )?;
                let action: SchemaAction = serde_json::from_value(transition.plan.clone())
                    .map_err(|error| RuntimeJobError::InvalidTableTransitionPlan {
                        relation_id: transition.key.source_relation_id,
                        reason: error.to_string(),
                    })?;
                let (SchemaAction::Reload {
                    after: expected_after,
                    ..
                }
                | SchemaAction::Add {
                    after: expected_after,
                }) = action
                else {
                    unreachable!("snapshot action checked above")
                };
                let planned = snapshot_tables
                    .iter()
                    .find(|table| {
                        table.planned.source.relation_id == transition.key.source_relation_id
                    })
                    .ok_or(RuntimeJobError::InitialCatalogChanged)?;
                if planned.planned.source != expected_after {
                    return Err(RuntimeJobError::InitialCatalogChanged);
                }
                let mut snapshot_schema = planned.planned.source.clone();
                snapshot_schema.generation = pending_generation;
                let snapshot_plan = plan_snapshot_target_with_storage(
                    &snapshot_schema,
                    planned.planned.target.clone(),
                    planned.planned.shadow.clone(),
                    planned.planned.storage,
                )?;
                reload_tables.push(RuntimeTable {
                    planned: planned.planned.clone(),
                    snapshot_plan,
                    table_generation: pending_generation,
                });
            }

            let snapshot_group_id = Uuid::now_v7();
            let boundary = NodeCheckpoint {
                key: CheckpointKey {
                    pipeline_id: self.pipeline_id,
                    topology_generation: self.topology_generation,
                    node_id: STANDALONE_NODE_ID,
                },
                system_identifier: report.identity.system_identifier,
                timeline: report.identity.timeline,
                slot_name: slot_name.clone(),
                applied_lsn: slot_snapshot.consistent_point,
            };
            let activation_request = SnapshotActivationRequest {
                fence: self.fence,
                snapshot_group_id,
                tables: reload_tables
                    .iter()
                    .map(|table| {
                        table
                            .snapshot_plan
                            .activation_table(&table.planned.schema_fingerprint)
                    })
                    .collect(),
                initial_checkpoints: vec![boundary],
            };

            let transaction = target.transaction().await?;
            begin_snapshot_group_in_transaction(&transaction, &activation_request).await?;
            for transition in transitions.iter_mut().filter(|transition| {
                matches!(
                    transition.action,
                    TableTransitionAction::Reload | TableTransitionAction::Add
                )
            }) {
                begin_table_snapshot_transition_in_transaction(
                    &transaction,
                    self.fence,
                    transition.key,
                    transition.state,
                    snapshot_group_id,
                )
                .await?;
                transition.snapshot_group_id = Some(snapshot_group_id);
                transition.state = TableTransitionState::Snapshotting;
            }
            if event.state == SchemaEventState::Pending {
                advance_schema_event_state_in_transaction(
                    &transaction,
                    self.fence,
                    event.source_lsn,
                    event.source_xid,
                    SchemaEventState::Pending,
                    SchemaEventState::InTransition,
                    None,
                )
                .await?;
            }
            transaction.commit().await?;

            self.telemetry.set_phase(PipelinePhase::Snapshotting);
            for table in &reload_tables {
                let ownership = SnapshotOwnership {
                    fence: self.fence,
                    snapshot_group_id,
                    schema_fingerprint: table.planned.schema_fingerprint.clone(),
                };
                if let Err(error) = self
                    .load_table_snapshot(target, &mut snapshot, table, &ownership, cancellation)
                    .await
                {
                    let _ = snapshot.rollback().await;
                    return Err(error);
                }
            }
            snapshot.commit().await?;
            let mut cutover_event = event.clone();
            cutover_event.state = SchemaEventState::InTransition;
            self.cutover_table_schema_event(
                target,
                &cutover_event,
                transitions,
                Some(&activation_request),
            )
            .await
        }
        .await;

        let cleanup = drop_logical_slot(&source, &slot_name).await;
        match (result, cleanup) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(error), _) => Err(error),
            (Ok(()), Err(error)) => Err(RuntimeJobError::Source(error)),
        }
    }

    async fn cutover_table_schema_event(
        &self,
        target: &mut tokio_postgres::Client,
        event: &SchemaEvent,
        transitions: &[TableTransition],
        activation: Option<&SnapshotActivationRequest>,
    ) -> Result<(), RuntimeJobError> {
        let transaction = target.transaction().await?;
        if event.state == SchemaEventState::Pending {
            advance_schema_event_state_in_transaction(
                &transaction,
                self.fence,
                event.source_lsn,
                event.source_xid,
                SchemaEventState::Pending,
                SchemaEventState::InTransition,
                None,
            )
            .await?;
        }

        for transition in transitions.iter().filter(|transition| {
            matches!(
                transition.action,
                TableTransitionAction::Reload | TableTransitionAction::Add
            )
        }) {
            let mut state = transition.state;
            if state == TableTransitionState::Snapshotting {
                advance_table_transition_state_in_transaction(
                    &transaction,
                    self.fence,
                    transition.key,
                    state,
                    TableTransitionState::CatchingUp,
                    None,
                )
                .await?;
                state = TableTransitionState::CatchingUp;
            }
            if state == TableTransitionState::CatchingUp {
                advance_table_transition_state_in_transaction(
                    &transaction,
                    self.fence,
                    transition.key,
                    state,
                    TableTransitionState::CutoverPending,
                    None,
                )
                .await?;
            } else if state != TableTransitionState::CutoverPending {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: transition.snapshot_group_id.unwrap_or_default(),
                    reason: format!("unexpected cutover state {state:?}"),
                });
            }
        }
        if let Some(activation) = activation {
            activate_table_snapshot_group_in_transaction(&transaction, activation).await?;
        }

        for transition in transitions {
            match transition.action {
                TableTransitionAction::Reload | TableTransitionAction::Add => {
                    advance_table_transition_state_in_transaction(
                        &transaction,
                        self.fence,
                        transition.key,
                        TableTransitionState::CutoverPending,
                        TableTransitionState::Completed,
                        None,
                    )
                    .await?;
                }
                TableTransitionAction::Drop => {
                    advance_table_transition_state_in_transaction(
                        &transaction,
                        self.fence,
                        transition.key,
                        transition.state,
                        TableTransitionState::CutoverPending,
                        None,
                    )
                    .await?;
                    quarantine_active_table_in_transaction(
                        &transaction,
                        self.fence,
                        event.event_id,
                        transition.key.source_relation_id,
                        transition.active_table_generation.ok_or(
                            RuntimeJobError::MissingPendingTableGeneration(
                                transition.key.source_relation_id,
                            ),
                        )?,
                    )
                    .await?;
                    advance_table_transition_state_in_transaction(
                        &transaction,
                        self.fence,
                        transition.key,
                        TableTransitionState::CutoverPending,
                        TableTransitionState::Completed,
                        None,
                    )
                    .await?;
                }
                TableTransitionAction::Noop => {
                    advance_table_transition_state_in_transaction(
                        &transaction,
                        self.fence,
                        transition.key,
                        transition.state,
                        TableTransitionState::Completed,
                        None,
                    )
                    .await?;
                }
                TableTransitionAction::Online => {
                    return Err(RuntimeJobError::UnsupportedPersistedOnlineTransition(
                        transition.key.source_relation_id,
                    ));
                }
            }
        }
        advance_schema_event_state_in_transaction(
            &transaction,
            self.fence,
            event.source_lsn,
            event.source_xid,
            SchemaEventState::InTransition,
            SchemaEventState::Completed,
            None,
        )
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn recover_table_snapshot_transitions(
        &self,
        source: &tokio_postgres::Client,
        target: &mut tokio_postgres::Client,
        report: &PreflightReport,
        main_slot: &str,
    ) -> Result<(), RuntimeJobError> {
        let transitions = list_unfinished_table_transitions(target, self.fence).await?;
        let mut groups = HashMap::<Uuid, Vec<TableTransition>>::new();
        for transition in transitions {
            match transition.snapshot_group_id {
                Some(group) => groups.entry(group).or_default().push(transition),
                None if matches!(
                    transition.state,
                    TableTransitionState::Snapshotting
                        | TableTransitionState::CatchingUp
                        | TableTransitionState::CutoverPending
                ) =>
                {
                    return Err(RuntimeJobError::MissingTableSnapshotGroup(
                        transition.key.source_relation_id,
                    ));
                }
                None => {}
            }
        }

        for (group_id, group_transitions) in groups {
            let manifest = load_snapshot_group_manifest(target, self.fence, group_id).await?;
            let [boundary] = manifest.request.initial_checkpoints.as_slice() else {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: group_id,
                    reason: format!(
                        "expected one source node, found {}",
                        manifest.request.initial_checkpoints.len()
                    ),
                });
            };
            if boundary.key.node_id != STANDALONE_NODE_ID
                || boundary.system_identifier != report.identity.system_identifier
                || boundary.timeline != report.identity.timeline
                || group_transitions
                    .iter()
                    .any(|transition| boundary.applied_lsn < transition.barrier_lsn)
            {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: group_id,
                    reason: format!(
                        "node={}, system_identifier={}, timeline={}, consistent_lsn={}",
                        boundary.key.node_id,
                        boundary.system_identifier,
                        boundary.timeline,
                        boundary.applied_lsn
                    ),
                });
            }
            if boundary.slot_name == main_slot {
                return Err(RuntimeJobError::TableSnapshotUsesMainSlot(group_id));
            }
            if let Some(slot) = inspect_logical_slot(source, &boundary.slot_name).await? {
                if slot.plugin != "pgoutput" {
                    return Err(RuntimeJobError::WrongSlotPlugin {
                        slot: boundary.slot_name.clone(),
                        plugin: slot.plugin,
                    });
                }
                if slot.active {
                    return Err(RuntimeJobError::ActiveOrphanSlot {
                        slot: boundary.slot_name.clone(),
                    });
                }
                if slot.database.as_deref() != Some(self.source.database_name.as_str()) {
                    return Err(RuntimeJobError::SlotDatabaseMismatch {
                        slot: boundary.slot_name.clone(),
                        actual: slot.database,
                        expected: self.source.database_name.clone(),
                    });
                }
                if slot.temporary || slot.two_phase || slot.failover || slot.synced {
                    return Err(RuntimeJobError::UnsupportedSlotMode {
                        slot: boundary.slot_name.clone(),
                        reason: format!(
                            "temporary={}, two_phase={}, failover={}, synced={}",
                            slot.temporary, slot.two_phase, slot.failover, slot.synced
                        ),
                    });
                }
                drop_logical_slot(source, &boundary.slot_name).await?;
            }

            let state = group_transitions[0].state;
            if group_transitions
                .iter()
                .any(|transition| transition.state != state)
            {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: group_id,
                    reason: "group transitions are in different states".to_owned(),
                });
            }
            match state {
                TableTransitionState::Snapshotting => {
                    let outcome =
                        reset_interrupted_table_snapshot_group(target, self.fence, group_id)
                            .await?;
                    tracing::warn!(
                        pipeline_id = %self.pipeline_id,
                        snapshot_group_id = %group_id,
                        dropped_shadows = outcome.dropped_shadows.len(),
                        "reset interrupted table snapshot to a fresh source boundary"
                    );
                }
                TableTransitionState::CatchingUp | TableTransitionState::CutoverPending => {
                    let replay =
                        adopt_table_snapshot_replay_group(target, self.fence, group_id).await?;
                    tracing::info!(
                        pipeline_id = %self.pipeline_id,
                        snapshot_group_id = %group_id,
                        consistent_lsn = %boundary.applied_lsn,
                        status = ?replay.manifest.status,
                        state = ?replay.transition_state,
                        "adopted completed table snapshot for main-slot replay"
                    );
                }
                _ => {
                    return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                        group: group_id,
                        reason: format!("unexpected transition state {state:?}"),
                    });
                }
            }
        }
        Ok(())
    }

    async fn resume_unfinished_table_schema_events(
        &self,
        target: &mut tokio_postgres::Client,
        report: &PreflightReport,
        names: &super::ReplicationNames,
        cancellation: &CancellationToken,
    ) -> Result<bool, RuntimeJobError> {
        let events = list_unfinished_schema_events(target, self.fence).await?;

        for event in events {
            let event = SchemaEventKey {
                source_lsn: event.source_lsn,
                source_xid: event.source_xid,
            };
            match self
                .execute_table_schema_event(event, report, names, cancellation)
                .await?
            {
                TableSchemaExecution::Completed => {
                    tracing::info!(
                        pipeline_id = %self.pipeline_id,
                        source_lsn = %event.source_lsn,
                        source_xid = event.source_xid,
                        "resumed unfinished table-local schema transition"
                    );
                }
                TableSchemaExecution::RequiresPipelineRebuild => {
                    let reason = format!(
                        "unfinished schema event {}/{} cannot be resumed table-locally",
                        event.source_lsn, event.source_xid
                    );
                    self.fail_schema_event_for_rebuild(event, &reason).await?;
                    self.request_rebuild(&reason).await?;
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    async fn request_rebuild(&self, reason: &str) -> Result<(), RuntimeJobError> {
        let rebuild = self
            .control
            .request_pipeline_rebuild(self.pipeline_id)
            .await?
            .ok_or(RuntimeJobError::MissingPipelineForRebuild)?;
        tracing::warn!(
            pipeline_id = %self.pipeline_id,
            operation_id = %rebuild.operation_id,
            snapshot_generation = rebuild.pipeline.snapshot_generation,
            %reason,
            "consistency barrier requested a full snapshot rebuild"
        );
        Ok(())
    }

    async fn monitor_wal_retention(
        &self,
        client: tokio_postgres::Client,
        slot_name: String,
        cancellation: CancellationToken,
    ) -> Result<WalMonitorOutcome, RuntimeJobError> {
        let policy = self.source_settings.wal_retention;
        let mut ticker = time::interval(policy.check_interval());
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_warning = None::<String>;

        loop {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return Ok(WalMonitorOutcome::Cancelled),
                _ = ticker.tick() => {
                    let slot = inspect_logical_slot(&client, &slot_name)
                        .await?
                        .ok_or_else(|| RuntimeJobError::WalRetentionExceeded {
                            slot: slot_name.clone(),
                            reason: "logical slot disappeared while replication was running".to_owned(),
                        })?;
                    match assess_wal_retention(&slot, policy) {
                        WalRetentionAssessment::Healthy => {
                            self.telemetry.wal_retention_observed(
                                slot.retained_wal_bytes,
                                slot.safe_wal_size,
                                false,
                            );
                            last_warning = None;
                        }
                        WalRetentionAssessment::Warning(reason) => {
                            self.telemetry.wal_retention_observed(
                                slot.retained_wal_bytes,
                                slot.safe_wal_size,
                                true,
                            );
                            if last_warning.as_deref() != Some(reason.as_str()) {
                                tracing::warn!(
                                    pipeline_id = %self.pipeline_id,
                                    slot = %slot_name,
                                    retained_wal_bytes = ?slot.retained_wal_bytes,
                                    safe_wal_bytes = ?slot.safe_wal_size,
                                    %reason,
                                    "source WAL retention is approaching the protection limit"
                                );
                                last_warning = Some(reason);
                            }
                        }
                        WalRetentionAssessment::Rebuild(reason) => {
                            self.telemetry.wal_retention_observed(
                                slot.retained_wal_bytes,
                                slot.safe_wal_size,
                                true,
                            );
                            return Ok(WalMonitorOutcome::Rebuild(reason));
                        }
                    }
                }
            }
        }
    }

    fn validate_source(&self, report: &PreflightReport) -> Result<(), RuntimeJobError> {
        if report.identity.database != self.source.database_name {
            return Err(RuntimeJobError::SourceDatabaseMismatch {
                expected: self.source.database_name.clone(),
                actual: report.identity.database.clone(),
            });
        }
        if report.identity.in_recovery {
            return Err(RuntimeJobError::SourceIsStandby);
        }
        if report.citus_version.is_some() {
            return Err(RuntimeJobError::UnexpectedCitus);
        }
        Ok(())
    }

    async fn validate_target(
        &self,
        target: &tokio_postgres::Client,
    ) -> Result<(), RuntimeJobError> {
        let row = target
            .query_one(
                "SELECT current_database(), version(), current_setting('gp_role', true), current_setting('server_encoding')",
                &[],
            )
            .await?;
        let database: String = row.try_get(0)?;
        let version: String = row.try_get(1)?;
        let gp_role: Option<String> = row.try_get(2)?;
        let server_encoding: String = row.try_get(3)?;
        if database != self.target.database_name {
            return Err(RuntimeJobError::TargetDatabaseMismatch {
                expected: self.target.database_name.clone(),
                actual: database,
            });
        }
        let normalized = version.to_ascii_lowercase();
        if gp_role.as_deref() != Some("dispatch")
            || !server_encoding.eq_ignore_ascii_case("UTF8")
            || !normalized.contains("cloudberry")
            || !normalized.contains("2.1.")
        {
            return Err(RuntimeJobError::UnsupportedTarget);
        }
        Ok(())
    }

    async fn validate_storage_capabilities(
        &self,
        target: &tokio_postgres::Client,
    ) -> Result<(), RuntimeJobError> {
        let mut required = HashSet::from([self.target_settings.default_table_storage]);
        required.extend(
            self.pipeline_settings
                .table_mappings
                .iter()
                .filter_map(|mapping| mapping.storage),
        );
        for storage in required {
            verify_storage_available(target, storage).await?;
        }
        Ok(())
    }

    async fn install_ddl_capture(
        &self,
        source: &tokio_postgres::Client,
    ) -> Result<(), RuntimeJobError> {
        let spec = DdlInstallSpec {
            metadata_schema: self.source_settings.metadata_schema.clone(),
            trigger_name: CANONICAL_DDL_TRIGGER_NAME.to_owned(),
            allow_citus_worker_guard: true,
        };
        let installed = ensure_ddl_capture(source, &spec).await?;
        if installed {
            tracing::info!(
                pipeline_id = %self.pipeline_id,
                trigger = %spec.trigger_name,
                "source DDL capture installed"
            );
        }
        Ok(())
    }

    fn catalog_options(&self) -> CatalogOptions {
        CatalogOptions {
            metadata_schema: self.source_settings.metadata_schema.clone(),
            include_schemas: self
                .source_settings
                .include_schemas
                .as_ref()
                .map(|schemas| schemas.iter().cloned().collect()),
            exclude_schemas: self
                .source_settings
                .exclude_schemas
                .iter()
                .cloned()
                .collect(),
            // Partition hierarchies remain blocked until root publication identity is verified.
            include_partitions: false,
        }
    }

    fn plan_inventory(
        &self,
        inventory: TableInventory,
    ) -> Result<Vec<RuntimeTable>, RuntimeJobError> {
        self.plan_inventory_with_mode(inventory, false)
    }

    fn plan_resume_inventory(
        &self,
        inventory: TableInventory,
    ) -> Result<Vec<RuntimeTable>, RuntimeJobError> {
        self.plan_inventory_with_mode(inventory, true)
    }

    fn plan_inventory_with_mode(
        &self,
        inventory: TableInventory,
        allow_missing_explicit_mappings: bool,
    ) -> Result<Vec<RuntimeTable>, RuntimeJobError> {
        if let Some(rejected) = inventory.rejected.into_iter().next() {
            return Err(RuntimeJobError::UnsupportedIncludedTable {
                table: rejected.name.to_string(),
                reason: rejected.reason.to_string(),
            });
        }
        let mut settings = self.pipeline_settings.clone();
        if allow_missing_explicit_mappings {
            let available = inventory
                .supported
                .iter()
                .map(|table| table.name.clone())
                .collect::<HashSet<_>>();
            settings
                .table_mappings
                .retain(|mapping| available.contains(&mapping.source));
        }
        let planned = plan_tables(
            self.pipeline_id,
            self.topology_generation,
            &self.source.prefix,
            &self.source.database_name,
            TargetPlanningContext {
                database: &self.target.database_name,
                default_storage: self.target_settings.default_table_storage,
            },
            &settings,
            inventory.supported,
        )?;
        let mut accepted = Vec::with_capacity(planned.len());
        for planned in planned {
            let mut snapshot_schema = planned.source.clone();
            snapshot_schema.generation = self.topology_generation;
            let snapshot_plan = plan_snapshot_target_with_storage(
                &snapshot_schema,
                planned.target.clone(),
                planned.shadow.clone(),
                planned.storage,
            )?;
            accepted.push(RuntimeTable {
                planned,
                snapshot_plan,
                table_generation: self.topology_generation,
            });
        }
        if accepted.is_empty() && !allow_missing_explicit_mappings {
            Err(RuntimeJobError::NoEligibleTables)
        } else {
            Ok(accepted)
        }
    }

    async fn ensure_publication(
        &self,
        source: &tokio_postgres::Client,
        publication_name: &str,
        tables: &[RuntimeTable],
    ) -> Result<(), RuntimeJobError> {
        let spec = PublicationSpec::new(
            publication_name,
            tables
                .iter()
                .map(|table| table.planned.source.name.clone())
                .collect(),
        )?;
        ensure_publication(source, &spec, None).await?;
        Ok(())
    }

    /// Load one table into its shadow using bounded PK pages.
    ///
    /// The source pager reads one `LIMIT+1` canonical PK page, derives a
    /// `(start, end]` range, and streams it directly through PostgreSQL COPY. The
    /// target driver copies that page and advances a durable cursor in its own
    /// transaction, so an ambiguous per-page commit is reconciled on the next
    /// call via `SnapshotPageApplyOutcome::ResumeAt` within this same live
    /// source snapshot. A process crash instead discards the whole loading group
    /// (a fresh run gets a new slot/group and restarts from the table head), so
    /// the durable cursor is only ever resumed against the session that wrote it.
    ///
    /// Tables without a usable primary key fall back to a single whole-table
    /// COPY because the bounded cursor contract requires PK columns to prove
    /// forward progress.
    async fn load_table_snapshot(
        &self,
        target: &mut tokio_postgres::Client,
        snapshot: &mut SnapshotSession<'_>,
        table: &RuntimeTable,
        ownership: &cloudberry_etl_target_cloudberry::snapshot::SnapshotOwnership,
        cancellation: &CancellationToken,
    ) -> Result<(), RuntimeJobError> {
        let schema = &table.planned.source;
        if schema.primary_key().is_empty() {
            return self
                .load_table_whole(target, snapshot, table, ownership, cancellation)
                .await;
        }

        let limits = SnapshotPageLimits {
            row_limit: self.pipeline_settings.batch.max_rows,
            max_page_bytes: self.pipeline_settings.batch.max_bytes,
        };
        let mut loader =
            begin_snapshot_pages(target, table.snapshot_plan.clone(), ownership).await?;
        if loader.is_completed() {
            tracing::info!(
                pipeline_id = %self.pipeline_id,
                table = %schema.name,
                "snapshot table already complete; skipping load"
            );
            return Ok(());
        }

        // Re-derive the in-session source cursor from the durable target cursor. An empty durable
        // cursor means the scan restarts at the table head. A non-empty one only reappears within
        // this same live snapshot, so the source pager can trust it as a page boundary.
        let mut cursor: Option<SnapshotCursor> = None;
        loop {
            if cancellation.is_cancelled() {
                return Err(RuntimeJobError::Cancelled);
            }
            let page = snapshot
                .read_canonical_pk_page(schema, cursor.as_ref(), limits)
                .await?;
            let has_more = page.has_more;
            let next_cursor_bytes = page.next_cursor();
            let next_cursor = match &next_cursor_bytes {
                Some(cursor) => cursor_to_text(cursor.key())?,
                // An empty tail keeps the current durable cursor rather than inventing one.
                None => loader.cursor().to_vec(),
            };
            let range = page.copy_range()?;
            let completed = !has_more;

            let stream = snapshot.copy_text_pk_range(schema, &range).await?;
            let apply_page = async {
                match &self.snapshot_page_commit_observer {
                    Some(observer) => {
                        loader
                            .apply_page_observed(
                                target,
                                next_cursor,
                                completed,
                                stream,
                                observer.as_ref(),
                            )
                            .await
                    }
                    None => {
                        loader
                            .apply_page(target, next_cursor, completed, stream)
                            .await
                    }
                }
            };
            let outcome = tokio::select! {
                biased;
                () = cancellation.cancelled() => return Err(RuntimeJobError::Cancelled),
                outcome = apply_page => outcome?,
            };
            tracing::debug!(
                pipeline_id = %self.pipeline_id,
                table = %schema.name,
                ?outcome,
                has_more,
                "snapshot page applied"
            );

            if completed {
                break;
            }
            // Advance the source pager. On a reconciled lost commit the target cursor already
            // moved past this page, but re-reading the same boundary is safe and idempotent.
            cursor = next_cursor_bytes;
        }
        tracing::info!(
            pipeline_id = %self.pipeline_id,
            table = %schema.name,
            "snapshot table load committed via bounded pages"
        );
        Ok(())
    }

    /// Whole-table COPY fallback for tables without a primary key. Preparation,
    /// COPY and completion all commit in a single target transaction.
    async fn load_table_whole(
        &self,
        target: &mut tokio_postgres::Client,
        snapshot: &mut SnapshotSession<'_>,
        table: &RuntimeTable,
        ownership: &cloudberry_etl_target_cloudberry::snapshot::SnapshotOwnership,
        cancellation: &CancellationToken,
    ) -> Result<(), RuntimeJobError> {
        let mut apply =
            begin_snapshot_apply(target, table.snapshot_plan.clone(), ownership).await?;
        let stream = snapshot.copy_text_table(&table.planned.source).await?;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => None,
            result = apply.copy_from_stream(stream) => Some(result),
        };
        match result {
            Some(result) => {
                result?;
            }
            None => {
                apply.rollback().await?;
                return Err(RuntimeJobError::Cancelled);
            }
        }
        let outcome = apply.commit().await?;
        tracing::info!(
            pipeline_id = %self.pipeline_id,
            table = %table.planned.source.name,
            ?outcome,
            "snapshot table load committed"
        );
        Ok(())
    }

    async fn prepare_initial_snapshot(
        &self,
        source_setup: &mut tokio_postgres::Client,
        target: &mut tokio_postgres::Client,
        report: &PreflightReport,
        names: &super::ReplicationNames,
        cancellation: CancellationToken,
    ) -> Result<PreparedRun, RuntimeJobError> {
        if let Some(existing) = inspect_logical_slot(source_setup, &names.slot).await? {
            if existing.plugin != "pgoutput" {
                return Err(RuntimeJobError::WrongSlotPlugin {
                    slot: names.slot.clone(),
                    plugin: existing.plugin,
                });
            }
            if existing.active {
                return Err(RuntimeJobError::ActiveOrphanSlot {
                    slot: names.slot.clone(),
                });
            }
            if existing.database.as_deref() != Some(self.source.database_name.as_str()) {
                return Err(RuntimeJobError::SlotDatabaseMismatch {
                    slot: names.slot.clone(),
                    actual: existing.database,
                    expected: self.source.database_name.clone(),
                });
            }
            if existing.temporary || existing.two_phase || existing.failover || existing.synced {
                return Err(RuntimeJobError::UnsupportedSlotMode {
                    slot: names.slot.clone(),
                    reason: format!(
                        "temporary={}, two_phase={}, failover={}, synced={}",
                        existing.temporary, existing.two_phase, existing.failover, existing.synced
                    ),
                });
            }
            // No target checkpoint exists, so this generated slot cannot authorize progress.
            drop_logical_slot(source_setup, &names.slot).await?;
        }

        // Publication membership must exist before the slot's consistent point. Otherwise writes
        // between slot creation and ALTER PUBLICATION can be omitted by pgoutput permanently.
        let publication_inventory = cloudberry_etl_source_postgres::catalog::inspect_tables(
            source_setup,
            &self.catalog_options(),
        )
        .await?;
        let publication_tables = self.plan_inventory(publication_inventory)?;
        self.ensure_publication(source_setup, &names.publication, &publication_tables)
            .await?;

        // Keep the newly-created slot until the target activation attempt has started. Before
        // that point no target checkpoint can refer to it, so every error path must release it;
        // after that point an ambiguous target commit must be recoverable by resume instead of
        // risking a checkpoint/slot split-brain.
        let mut preserve_slot_on_error = false;
        let result = async {
            let mut guard =
                SnapshotSlotGuard::create(self.source_dsn.expose_secret(), &names.slot, 1).await?;
            let slot_snapshot = guard.snapshot().clone();
            let mut snapshot_client = connect_sql(&self.source_dsn, EndpointRole::Source).await?;
            let mut snapshot =
                begin_at_exported_snapshot(&mut snapshot_client, &slot_snapshot.snapshot_name)
                    .await?;
            guard.mark_reader_ready()?;
            guard.release()?;

            let inventory = snapshot.inspect_tables(&self.catalog_options()).await?;
            let tables = self.plan_inventory(inventory)?;
            let catalog_unchanged = publication_tables.len() == tables.len()
                && publication_tables
                    .iter()
                    .zip(&tables)
                    .all(|(before, at_snapshot)| before.planned == at_snapshot.planned);
            if !catalog_unchanged {
                snapshot.rollback().await?;
                return Err(RuntimeJobError::InitialCatalogChanged);
            }

            let checkpoint = NodeCheckpoint {
                key: CheckpointKey {
                    pipeline_id: self.pipeline_id,
                    topology_generation: self.topology_generation,
                    node_id: STANDALONE_NODE_ID,
                },
                system_identifier: report.identity.system_identifier,
                timeline: report.identity.timeline,
                slot_name: names.slot.clone(),
                applied_lsn: slot_snapshot.consistent_point,
            };
            let snapshot_group_id = Uuid::now_v7();
            let activation_request = SnapshotActivationRequest {
                fence: self.fence,
                snapshot_group_id,
                tables: tables
                    .iter()
                    .map(|table| {
                        table
                            .snapshot_plan
                            .activation_table(&table.planned.schema_fingerprint)
                    })
                    .collect(),
                initial_checkpoints: vec![checkpoint.clone()],
            };
            let registration = begin_snapshot_group(target, &activation_request).await?;
            tracing::info!(
                pipeline_id = %self.pipeline_id,
                %snapshot_group_id,
                ?registration,
                "snapshot group manifest registered"
            );

            for table in &tables {
                if cancellation.is_cancelled() {
                    snapshot.rollback().await?;
                    return Err(RuntimeJobError::Cancelled);
                }
                let ownership = cloudberry_etl_target_cloudberry::snapshot::SnapshotOwnership {
                    fence: self.fence,
                    snapshot_group_id,
                    schema_fingerprint: table.planned.schema_fingerprint.clone(),
                };
                if let Err(error) = self
                    .load_table_snapshot(target, &mut snapshot, table, &ownership, &cancellation)
                    .await
                {
                    // The source snapshot transaction is still open on error; close it so the
                    // repeatable-read holdback is released before we propagate. Slot retention is
                    // still governed by preserve_slot_on_error below.
                    let _ = snapshot.rollback().await;
                    return Err(error);
                }
            }

            // Close the source snapshot before target activation. If activation is ambiguous,
            // retaining the slot lets the next run discover either the committed checkpoint or
            // the orphan slot and converge safely.
            snapshot.commit().await?;
            preserve_slot_on_error = true;
            activate_snapshot_group(target, &activation_request).await?;
            Ok(PreparedRun {
                tables,
                checkpoint,
                replay_fences: Vec::new(),
            })
        }
        .await;

        if result.is_err()
            && !preserve_slot_on_error
            && let Err(cleanup) = drop_logical_slot(source_setup, &names.slot).await
        {
            let original = result.as_ref().err().map_or_else(
                || "unknown initial snapshot failure".to_owned(),
                ToString::to_string,
            );
            return Err(RuntimeJobError::InitialSnapshotCleanup {
                original,
                slot: names.slot.clone(),
                cleanup: cleanup.to_string(),
            });
        }
        result
    }

    async fn prepare_resume(
        &self,
        source: &tokio_postgres::Client,
        target: &mut tokio_postgres::Client,
        report: &PreflightReport,
        names: &super::ReplicationNames,
        checkpoint: NodeCheckpoint,
    ) -> Result<PreparedRun, RuntimeJobError> {
        if checkpoint.system_identifier != report.identity.system_identifier
            || checkpoint.timeline != report.identity.timeline
        {
            return Err(RuntimeJobError::SourceIdentityChanged);
        }
        if checkpoint.slot_name != names.slot {
            return Err(RuntimeJobError::CheckpointSlotMismatch {
                expected: names.slot.clone(),
                actual: checkpoint.slot_name,
            });
        }
        let slot = inspect_logical_slot(source, &names.slot)
            .await?
            .ok_or_else(|| RuntimeJobError::MissingSlot(names.slot.clone()))?;
        if slot.plugin != "pgoutput" {
            return Err(RuntimeJobError::WrongSlotPlugin {
                slot: names.slot.clone(),
                plugin: slot.plugin,
            });
        }
        if slot.active {
            return Err(RuntimeJobError::ActiveOrphanSlot {
                slot: names.slot.clone(),
            });
        }
        if slot.database.as_deref() != Some(self.source.database_name.as_str()) {
            return Err(RuntimeJobError::SlotDatabaseMismatch {
                slot: names.slot.clone(),
                actual: slot.database,
                expected: self.source.database_name.clone(),
            });
        }
        if slot.temporary || slot.two_phase || slot.failover || slot.synced {
            return Err(RuntimeJobError::UnsupportedSlotMode {
                slot: names.slot.clone(),
                reason: format!(
                    "temporary={}, two_phase={}, failover={}, synced={}",
                    slot.temporary, slot.two_phase, slot.failover, slot.synced
                ),
            });
        }
        if slot.wal_status.as_deref() == Some("lost") || slot.invalidation_reason.is_some() {
            return Err(RuntimeJobError::SlotInvalidated {
                slot: names.slot.clone(),
                reason: format!(
                    "wal_status={:?}, invalidation_reason={:?}",
                    slot.wal_status, slot.invalidation_reason
                ),
            });
        }
        if slot.restart_lsn.is_none() {
            return Err(RuntimeJobError::WalLost);
        }
        if let Some(confirmed) = parse_optional_lsn(slot.confirmed_flush_lsn)?
            && confirmed > checkpoint.applied_lsn
        {
            return Err(RuntimeJobError::SlotAheadOfTarget {
                slot_lsn: confirmed,
                target_lsn: checkpoint.applied_lsn,
            });
        }
        if let Some(restart) = parse_optional_lsn(slot.restart_lsn)?
            && checkpoint.applied_lsn < restart
        {
            return Err(RuntimeJobError::WalLost);
        }

        let inventory = cloudberry_etl_source_postgres::catalog::inspect_tables(
            source,
            &self.catalog_options(),
        )
        .await?;
        let mut tables = self.plan_resume_inventory(inventory)?;
        for table in &tables {
            if let Some(actual) = load_relation_storage(target, &table.planned.target).await?
                && actual != table.planned.storage.access_method()
            {
                return Err(RuntimeJobError::TargetStorageDrift {
                    table: table.planned.target.to_string(),
                    expected: table.planned.storage.access_method(),
                    actual,
                });
            }
        }
        let publication = PublicationSpec::new(
            &names.publication,
            tables
                .iter()
                .map(|table| table.planned.source.name.clone())
                .collect(),
        )?;
        validate_publication(source, &publication, None)
            .await
            .map_err(|error| RuntimeJobError::PublicationDrift(error.to_string()))?;
        let active_requirements = tables
            .iter()
            .map(|table| ActiveTableRequirement {
                target: table.planned.target.clone(),
                source_relation_id: table.planned.source.relation_id,
                schema_fingerprint: table.planned.schema_fingerprint.clone(),
            })
            .collect::<Vec<_>>();
        let active = validate_active_tables(target, self.fence, &active_requirements).await?;
        let mut replay_fences = Vec::new();
        let mut manifests = HashMap::new();
        for table in &mut tables {
            let metadata = active
                .iter()
                .find(|metadata| metadata.target == table.planned.target)
                .ok_or(RuntimeJobError::NoEligibleTables)?;
            table.table_generation = metadata.table_generation;
            let Some(group_id) = metadata.snapshot_group_id else {
                continue;
            };
            if let std::collections::hash_map::Entry::Vacant(entry) = manifests.entry(group_id) {
                let manifest = load_snapshot_group_manifest(target, self.fence, group_id).await?;
                entry.insert(manifest);
            }
            let manifest = &manifests[&group_id];
            let [boundary] = manifest.request.initial_checkpoints.as_slice() else {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: group_id,
                    reason: format!(
                        "expected one source node, found {}",
                        manifest.request.initial_checkpoints.len()
                    ),
                });
            };
            if boundary.key.node_id != STANDALONE_NODE_ID
                || boundary.system_identifier != report.identity.system_identifier
                || boundary.timeline != report.identity.timeline
            {
                return Err(RuntimeJobError::InvalidTableSnapshotBoundary {
                    group: group_id,
                    reason: "active table snapshot source identity does not match".to_owned(),
                });
            }
            if boundary.applied_lsn > checkpoint.applied_lsn {
                replay_fences.push(TableReplayFence {
                    relation_id: table.planned.source.relation_id,
                    snapshot_lsn: boundary.applied_lsn,
                });
            }
        }
        Ok(PreparedRun {
            tables,
            checkpoint,
            replay_fences,
        })
    }
}

fn parse_optional_lsn(value: Option<String>) -> Result<Option<PgLsn>, RuntimeJobError> {
    value
        .map(|value| {
            PgLsn::from_str(&value).map_err(|_| RuntimeJobError::InvalidPersistedLsn(value))
        })
        .transpose()
}

fn assess_wal_retention(
    slot: &LogicalSlotState,
    policy: WalRetentionSettings,
) -> WalRetentionAssessment {
    if let Some(reason) = &slot.invalidation_reason {
        return WalRetentionAssessment::Rebuild(format!(
            "PostgreSQL invalidated the slot: {reason}"
        ));
    }
    match slot.wal_status.as_deref() {
        Some("lost") => {
            return WalRetentionAssessment::Rebuild(
                "PostgreSQL reports wal_status=lost".to_owned(),
            );
        }
        Some("unreserved") => {
            return WalRetentionAssessment::Rebuild(
                "PostgreSQL reports wal_status=unreserved; required WAL is no longer protected"
                    .to_owned(),
            );
        }
        Some("extended") => {
            // `extended` means the server has exceeded max_wal_size while retaining the slot's
            // required files. Keep the pipeline alive only until the configured hard byte limit.
        }
        Some("reserved") => {}
        Some(other) => {
            return WalRetentionAssessment::Rebuild(format!(
                "unknown PostgreSQL wal_status `{other}`"
            ));
        }
        None => {
            return WalRetentionAssessment::Rebuild(
                "PostgreSQL returned no wal_status for the managed slot".to_owned(),
            );
        }
    }

    if slot
        .retained_wal_bytes
        .is_some_and(|bytes| bytes >= policy.rebuild_bytes)
    {
        return WalRetentionAssessment::Rebuild(format!(
            "retained WAL reached configured hard limit of {} bytes",
            policy.rebuild_bytes
        ));
    }
    if slot
        .safe_wal_size
        .is_some_and(|bytes| bytes <= policy.minimum_safe_bytes)
    {
        return WalRetentionAssessment::Rebuild(format!(
            "safe_wal_size reached configured minimum of {} bytes",
            policy.minimum_safe_bytes
        ));
    }

    let retained_warning = slot
        .retained_wal_bytes
        .is_some_and(|bytes| bytes >= policy.warning_bytes);
    let safe_warning = slot
        .safe_wal_size
        .is_some_and(|bytes| bytes <= policy.minimum_safe_bytes.saturating_mul(2));
    if retained_warning || safe_warning || slot.wal_status.as_deref() == Some("extended") {
        return WalRetentionAssessment::Warning(format!(
            "retained_wal_bytes={:?}, safe_wal_size={:?}, wal_status={:?}",
            slot.retained_wal_bytes, slot.safe_wal_size, slot.wal_status
        ));
    }
    WalRetentionAssessment::Healthy
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot() -> LogicalSlotState {
        LogicalSlotState {
            name: "slot".to_owned(),
            plugin: "pgoutput".to_owned(),
            database: Some("source".to_owned()),
            temporary: false,
            active: true,
            confirmed_flush_lsn: Some("0/10".to_owned()),
            restart_lsn: Some("0/1".to_owned()),
            retained_wal_bytes: Some(100),
            safe_wal_size: Some(1_000),
            wal_status: Some("reserved".to_owned()),
            invalidation_reason: None,
            two_phase: false,
            failover: false,
            synced: false,
        }
    }

    fn policy() -> WalRetentionSettings {
        WalRetentionSettings {
            check_interval_seconds: 1,
            warning_bytes: 100,
            rebuild_bytes: 200,
            minimum_safe_bytes: 50,
        }
    }

    #[test]
    fn physical_ha_shares_the_standalone_data_plane_and_citus_stays_gated() {
        assert_eq!(
            data_plane_for_topology(SourceTopology::Standalone),
            DataPlane::Standalone
        );
        assert_eq!(
            data_plane_for_topology(SourceTopology::PhysicalHa),
            DataPlane::Standalone
        );
        assert_eq!(
            data_plane_for_topology(SourceTopology::Citus),
            DataPlane::Gated("citus")
        );
    }

    #[test]
    fn wal_retention_assessment_is_fail_closed() {
        assert!(matches!(
            assess_wal_retention(&slot(), policy()),
            WalRetentionAssessment::Warning(_)
        ));
        let mut hard = slot();
        hard.retained_wal_bytes = Some(200);
        assert!(matches!(
            assess_wal_retention(&hard, policy()),
            WalRetentionAssessment::Rebuild(_)
        ));
        let mut lost = slot();
        lost.wal_status = Some("lost".to_owned());
        assert!(matches!(
            assess_wal_retention(&lost, policy()),
            WalRetentionAssessment::Rebuild(_)
        ));
        let mut unknown = slot();
        unknown.wal_status = Some("future".to_owned());
        assert!(matches!(
            assess_wal_retention(&unknown, policy()),
            WalRetentionAssessment::Rebuild(_)
        ));
    }
}
