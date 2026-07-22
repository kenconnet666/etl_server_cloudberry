//! Strongly typed, streaming initial snapshot loads into shadow tables.

use std::{fmt::Display, pin::Pin};

use bytes::Bytes;
use cloudberry_etl_core::{
    id::PipelineId,
    schema::{QualifiedName, TableSchema},
};
use futures::{Sink, SinkExt, Stream, StreamExt};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::{Client, Transaction};
use uuid::Uuid;

use crate::{
    checkpoint::{CheckpointError, NodeCheckpoint, PipelineFence, lock_pipeline_fence},
    migration::TARGET_METADATA_SCHEMA,
    schema::{
        CreateTablePlan, SchemaError, UserTypeDefinition, UserTypePlan,
        plan_create_table_with_storage, quote_identifier_list,
    },
    sql::{SqlRenderError, quote_identifier, quote_literal, quote_qualified_name},
    storage::TargetStorage,
};

const RELATION_EXISTS_SQL: &str = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2)";
const TYPE_EXISTS_SQL: &str = "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_type AS t JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace WHERE n.nspname = $1 AND t.typname = $2)";

mod activation;
mod cleanup;
mod manifest;
mod progress;

pub use activation::{
    activate_snapshot_group, activate_table_snapshot_group,
    activate_table_snapshot_group_in_transaction, validate_active_snapshot_group,
    validate_active_tables,
};
pub use cleanup::{
    QuarantineGcOutcome, QuarantineGcPolicy, SnapshotCleanupOutcome, SnapshotGroupCleanupRequest,
    cleanup_loading_snapshot_group, cleanup_stale_snapshot_groups,
    garbage_collect_quarantined_tables, reset_interrupted_table_snapshot_group,
};
pub use manifest::{SnapshotGroupRegistrationDisposition, begin_snapshot_group};
pub use progress::{
    SNAPSHOT_CURSOR_FORMAT_VERSION, SnapshotPageApplyOutcome, SnapshotTableProgress,
    copy_snapshot_page, register_snapshot_table_progress,
};

#[derive(Debug, Error)]
pub enum SnapshotTargetError {
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Sql(#[from] SqlRenderError),
    #[error("target snapshot database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("target table and shadow table must be different")]
    TargetIsShadow,
    #[error("target table `{target}` and shadow table `{shadow}` must use the same schema")]
    CrossSchemaShadowUnsupported { target: String, shadow: String },
    #[error("existing table `{0}` is not owned by pg2cb metadata")]
    ExistingUnmanagedTable(String),
    #[error("table `{table}` is managed by pipeline {actual}, expected pipeline {expected}")]
    ManagedByOtherPipeline {
        table: String,
        expected: PipelineId,
        actual: PipelineId,
    },
    #[error("table `{table}` is managed for source relation {actual}, expected {expected}")]
    ManagedByOtherSource {
        table: String,
        expected: u32,
        actual: u32,
    },
    #[error(
        "managed object `{object}` has fencing token {actual}, newer than current pipeline fence {current}"
    )]
    ManagedByNewerFence {
        object: String,
        current: i64,
        actual: i64,
    },
    #[error("managed relation `{0}` is recorded in metadata but is missing physically")]
    ManagedRelationMissing(String),
    #[error("managed relation `{table}` is in state `{actual}`, expected `{expected}`")]
    UnexpectedManagedTableState {
        table: String,
        expected: &'static str,
        actual: String,
    },
    #[error("shadow table `{table}` has generation {actual}, expected {expected}")]
    ShadowGenerationMismatch {
        table: String,
        expected: u64,
        actual: u64,
    },
    #[error("shadow table `{0}` has a different schema fingerprint")]
    ShadowFingerprintMismatch(String),
    #[error("target table `{0}` is already active for this snapshot generation")]
    TargetAlreadyActive(String),
    #[error("prerequisite type `{0}` already exists and cannot be recreated safely")]
    ExistingPrerequisiteType(String),
    #[error("managed prerequisite type `{0}` is recorded in metadata but is missing physically")]
    ManagedPrerequisiteTypeMissing(String),
    #[error("prerequisite type `{name}` is managed by pipeline {actual}, expected {expected}")]
    PrerequisiteTypeManagedByOtherPipeline {
        name: String,
        expected: PipelineId,
        actual: PipelineId,
    },
    #[error("prerequisite type `{0}` has a different managed definition")]
    PrerequisiteTypeDefinitionMismatch(String),
    #[error("prerequisite type `{name}` cannot be reconciled safely: {reason}")]
    UnsupportedPrerequisiteTypeEvolution { name: String, reason: String },
    #[error("source snapshot stream failed: {0}")]
    SourceStream(String),
    #[error("target COPY sink failed: {0}")]
    CopySink(String),
    #[error("snapshot page commit observer failed: {0}")]
    PageCommitObserver(String),
    #[error("snapshot COPY has already completed")]
    CopyAlreadyCompleted,
    #[error("snapshot transaction cannot commit before COPY completes")]
    CopyNotCompleted,
    #[error("snapshot ownership requires a positive fencing token and non-empty fingerprint")]
    InvalidOwnership,
    #[error("snapshot group id must not be nil")]
    InvalidSnapshotGroupId,
    #[error("snapshot group `{0}` is not registered")]
    SnapshotGroupNotRegistered(Uuid),
    #[error("snapshot group `{0}` has a corrupt or incomplete persisted manifest")]
    CorruptSnapshotGroupManifest(Uuid),
    #[error("snapshot group `{group}` manifest does not exactly match the caller: {difference}")]
    SnapshotGroupManifestMismatch { group: Uuid, difference: String },
    #[error("snapshot group `{0}` is already active and cannot accept more shadows")]
    SnapshotGroupAlreadyActive(Uuid),
    #[error("snapshot group `{0}` is not in the loading state")]
    SnapshotGroupNotLoading(Uuid),
    #[error("snapshot group `{0}` is owned by a table schema transition")]
    SnapshotGroupOwnedByTableTransition(Uuid),
    #[error("snapshot group `{0}` is not owned by a reload/add table transition")]
    SnapshotGroupNotOwnedByTableTransition(Uuid),
    #[error("snapshot group `{0}` cannot be reset because its transition ownership is invalid")]
    SnapshotGroupTransitionMismatch(Uuid),
    #[error("snapshot group cleanup authority and group fence do not match")]
    InvalidSnapshotCleanupFence,
    #[error(
        "snapshot group `{group}` belongs to generation {actual_generation} token {actual_token}, expected generation {expected_generation} token {expected_token}"
    )]
    SnapshotGroupFenceMismatch {
        group: Uuid,
        expected_generation: u64,
        expected_token: i64,
        actual_generation: u64,
        actual_token: i64,
    },
    #[error("snapshot cleanup found an unexpected shadow `{0}`")]
    UnexpectedSnapshotShadow(String),
    #[error("snapshot cleanup cannot verify the physical identity of `{0}`")]
    MissingRelationIdentity(String),
    #[error("physical relation `{table}` has identity {actual}, expected {expected}")]
    RelationIdentityMismatch {
        table: String,
        expected: i64,
        actual: i64,
    },
    #[error("quarantine retention must be positive when enabled")]
    InvalidQuarantineRetention,
    #[error("quarantine cleanup batch size must be positive")]
    InvalidQuarantineBatchSize,
    #[error("quarantine metadata for `{0}` is missing or not quarantined")]
    MissingQuarantineMetadata(String),
    #[error("quarantine metadata for `{0}` does not match its reconciliation record")]
    QuarantineMetadataMismatch(String),
    #[error("quarantine reconciliation record for `{0}` is not uniquely owned")]
    QuarantineRecordMismatch(String),
    #[error("pipeline {pipeline} generation {generation} has no active snapshot group")]
    MissingActiveSnapshotGroup {
        pipeline: PipelineId,
        generation: u64,
    },
    #[error("pipeline {pipeline} generation {generation} has more than one active snapshot group")]
    MultipleActiveSnapshotGroups {
        pipeline: PipelineId,
        generation: u64,
    },
    #[error("table `{table}` is not present in snapshot group `{group}` manifest")]
    SnapshotTableNotInManifest { group: Uuid, table: String },
    #[error("table `{table}` in snapshot group `{group}` does not match its persisted manifest")]
    SnapshotTableManifestMismatch { group: Uuid, table: String },
    #[error("shadow `{table}` belongs to snapshot group {actual:?}, expected {expected}")]
    ShadowSnapshotGroupMismatch {
        table: String,
        expected: Uuid,
        actual: Option<Uuid>,
    },
    #[error("table generation {0} exceeds the target bigint range")]
    GenerationOutOfRange(u64),
    #[error("persisted managed-table field `{field}` has invalid value `{value}` for `{table}`")]
    InvalidManagedTableValue {
        table: String,
        field: &'static str,
        value: String,
    },
    #[error("snapshot activation requires at least one table and one node checkpoint")]
    EmptyActivationGroup,
    #[error(
        "snapshot activation contains duplicate target, shadow, source relation, or node identity"
    )]
    DuplicateActivationIdentity,
    #[error("snapshot activation table `{0}` has an empty schema fingerprint")]
    EmptyActivationFingerprint(String),
    #[error("snapshot activation source relation id must be non-zero")]
    InvalidActivationSourceRelation,
    #[error(
        "snapshot activation for `{table}` must advance table generation beyond active {active}, proposed {proposed}"
    )]
    ActivationGenerationNotNewer {
        table: String,
        active: u64,
        proposed: u64,
    },
    #[error("snapshot activation contains a mixture of pending and already-active tables")]
    MixedActivationState,
    #[error("active managed-table set does not exactly match the requested source inventory")]
    ActiveTableSetMismatch,
    #[error("active managed table `{0}` has a different schema fingerprint")]
    ActiveTableFingerprintMismatch(String),
    #[error("snapshot shadow `{0}` is missing or is not completely owned by this activation")]
    IncompleteShadow(String),
    #[error("active snapshot retry has not reached the required checkpoint for node {0}")]
    ActiveCheckpointIncomplete(i32),
    #[error("snapshot activation checkpoint for node {0} has a zero consistent point")]
    ZeroInitialCheckpoint(i32),
    #[error("deterministic quarantine relation `{0}` already exists")]
    QuarantineNameConflict(String),
    #[error("snapshot metadata write affected {0} rows instead of one")]
    UnexpectedMetadataWriteCount(u64),
    #[error("snapshot progress for `{0}` is missing")]
    MissingSnapshotProgress(String),
    #[error("snapshot group `{0}` does not have one progress identity per manifest table")]
    IncompleteSnapshotProgress(Uuid),
    #[error("snapshot progress for `{0}` has not reached the source snapshot tail")]
    SnapshotProgressIncomplete(String),
    #[error("snapshot progress identity for `{table}` differs in `{field}`")]
    SnapshotProgressIdentityMismatch { table: String, field: &'static str },
    #[error(
        "persisted snapshot progress field `{field}` has invalid value `{value}` for `{table}`"
    )]
    InvalidSnapshotProgressValue {
        table: String,
        field: &'static str,
        value: String,
    },
    #[error("snapshot cursor values cannot contain NUL")]
    InvalidSnapshotCursorValue,
    #[error("snapshot cursor has {actual} fields, expected {expected}")]
    SnapshotCursorArityMismatch { expected: usize, actual: usize },
    #[error("snapshot cursor primary-key arity {0} exceeds the target range")]
    SnapshotCursorArityOutOfRange(usize),
    #[error("snapshot cursor digest for `{0}` does not match its canonical values")]
    SnapshotCursorDigestMismatch(String),
    #[error("bounded snapshot pagination for `{0}` requires a primary key")]
    SnapshotPaginationRequiresPrimaryKey(String),
    #[error("incomplete snapshot page for `{0}` did not advance its cursor")]
    SnapshotCursorDidNotAdvance(String),
    #[error("incomplete snapshot page for `{0}` copied no rows")]
    EmptyIncompleteSnapshotPage(String),
    #[error("snapshot page for `{0}` advanced its cursor without copying rows")]
    SnapshotCursorAdvancedWithoutRows(String),
    #[error("snapshot progress counter overflow for `{0}`")]
    SnapshotProgressCounterOverflow(String),
    #[error("snapshot progress `{field}` value {value} exceeds the target bigint range")]
    SnapshotProgressValueOutOfRange { field: &'static str, value: u64 },
    #[error("snapshot progress for `{0}` changed after it was locked")]
    SnapshotProgressChanged(String),
    #[error("whole-table snapshot progress for `{0}` is not freshly registered")]
    SnapshotProgressNotFresh(String),
    #[error("snapshot group `{group}` uses unsupported progress version {version}")]
    UnsupportedSnapshotProgressVersion { group: Uuid, version: u16 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotOwnership {
    pub fence: PipelineFence,
    pub snapshot_group_id: Uuid,
    pub schema_fingerprint: String,
}

/// Immutable SQL plan for loading one source table into a new typed shadow table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTargetPlan {
    pub target: QualifiedName,
    pub shadow: CreateTablePlan,
    pub copy_sql: String,
    source_relation_id: u32,
    source_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotActivationTable {
    pub target: QualifiedName,
    pub shadow: QualifiedName,
    pub source_relation_id: u32,
    pub table_generation: u64,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotActivationRequest {
    pub fence: PipelineFence,
    pub snapshot_group_id: Uuid,
    pub tables: Vec<SnapshotActivationTable>,
    /// Each LSN is the corresponding logical slot's snapshot consistent point.
    pub initial_checkpoints: Vec<NodeCheckpoint>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotActivationDisposition {
    Activated,
    AlreadyActive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotActivationOutcome {
    pub disposition: SnapshotActivationDisposition,
    pub quarantined: Vec<QualifiedName>,
}

/// Source/target identity required when resuming a pipeline. The persistent table generation is
/// intentionally omitted because target metadata is authoritative for table-local reloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTableRequirement {
    pub target: QualifiedName,
    pub source_relation_id: u32,
    pub schema_fingerprint: String,
}

/// Validated persistent identity used to rebuild the runtime apply binding after restart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveTableMetadata {
    pub target: QualifiedName,
    pub source_relation_id: u32,
    pub table_generation: u64,
    pub schema_fingerprint: String,
    pub snapshot_group_id: Option<Uuid>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotApplyMode {
    Copy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotApplyOutcome {
    Copied { rows: u64 },
}

/// Plan a distinct shadow table with exactly the source column types and column order.
pub fn plan_snapshot_target(
    source: &TableSchema,
    target: QualifiedName,
    shadow: QualifiedName,
) -> Result<SnapshotTargetPlan, SnapshotTargetError> {
    plan_snapshot_target_with_storage(source, target, shadow, TargetStorage::default())
}

/// Plan a shadow using the same explicit storage profile as its eventual active table.
pub fn plan_snapshot_target_with_storage(
    source: &TableSchema,
    target: QualifiedName,
    shadow: QualifiedName,
    storage: TargetStorage,
) -> Result<SnapshotTargetPlan, SnapshotTargetError> {
    if target == shadow {
        return Err(SnapshotTargetError::TargetIsShadow);
    }
    if target.schema != shadow.schema {
        return Err(SnapshotTargetError::CrossSchemaShadowUnsupported {
            target: target.to_string(),
            shadow: shadow.to_string(),
        });
    }
    let shadow = plan_create_table_with_storage(source, shadow, storage)?;
    let columns = shadow
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let copy_sql = format!(
        "COPY {} ({}) FROM STDIN WITH (FORMAT text, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')",
        quote_qualified_name(&shadow.target)?,
        quote_identifier_list(&columns)?,
    );
    Ok(SnapshotTargetPlan {
        target,
        shadow,
        copy_sql,
        source_relation_id: source.relation_id,
        source_generation: source.generation,
    })
}

impl SnapshotTargetPlan {
    #[must_use]
    pub fn activation_table(
        &self,
        schema_fingerprint: impl Into<String>,
    ) -> SnapshotActivationTable {
        SnapshotActivationTable {
            target: self.target.clone(),
            shadow: self.shadow.target.clone(),
            source_relation_id: self.source_relation_id,
            table_generation: self.source_generation,
            schema_fingerprint: schema_fingerprint.into(),
        }
    }
}

/// Create the schema, (re)build the shadow table and register its durable
/// progress row inside a caller-owned transaction that already holds the
/// pipeline fence. Shared by both the whole-table [`begin_snapshot_apply`] and
/// the bounded [`begin_snapshot_pages`] entry points so the shadow identity,
/// rebuild rules and progress registration stay byte-for-byte identical.
///
/// A committed shadow is rebuilt from the registered group's exported snapshot
/// boundary because schema identity alone does not prove which exported
/// snapshot produced the rows.
async fn prepare_shadow_load(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    let manifest = manifest::load_snapshot_group(transaction, ownership.snapshot_group_id).await?;
    manifest::validate_apply_membership(&manifest, plan, ownership)?;
    if manifest.state == manifest::SnapshotGroupState::Active {
        return Err(SnapshotTargetError::SnapshotGroupAlreadyActive(
            ownership.snapshot_group_id,
        ));
    }
    create_schema(transaction, &plan.target.schema).await?;

    let target_state = load_relation_state(transaction, &plan.target).await?;
    validate_target_for_load(&target_state, plan, ownership)?;

    let shadow_state = load_relation_state(transaction, &plan.shadow.target).await?;
    if validate_shadow_for_rebuild(&shadow_state, plan, ownership)? {
        let RelationState::Managed(record) = &shadow_state else {
            unreachable!("only a managed shadow can be rebuilt")
        };
        remove_managed_shadow(transaction, &plan.shadow.target, record).await?;
    }

    for prerequisite in &plan.shadow.prerequisites {
        ensure_prerequisite_type(transaction, prerequisite, ownership).await?;
    }
    transaction.batch_execute(&plan.shadow.create_sql).await?;
    register_shadow(transaction, plan, ownership).await?;
    progress::register_snapshot_table_progress(transaction, plan, ownership).await
}

/// Open the target transaction and create every object needed by the shadow load.
///
/// A committed shadow is rebuilt from the registered group's exported snapshot boundary.
pub async fn begin_snapshot_apply<'client>(
    client: &'client mut Client,
    plan: SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<SnapshotApply<'client>, SnapshotTargetError> {
    validate_ownership(ownership)?;
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, ownership.fence).await?;
    prepare_shadow_load(&transaction, &plan, ownership).await?;
    Ok(SnapshotApply {
        transaction,
        plan,
        ownership: ownership.clone(),
        mode: SnapshotApplyMode::Copy,
        copied_rows: None,
    })
}

/// Prepare a shadow table for a bounded, resumable page-by-page load and commit
/// that preparation so each subsequent page can run in its own transaction.
///
/// Unlike [`begin_snapshot_apply`], which binds shadow creation and the whole
/// COPY into one transaction, the bounded path must survive a process crash
/// between pages. Preparation (schema + shadow + progress row) is therefore
/// committed up front; [`SnapshotPageLoader::apply_page`] then advances the
/// durable cursor one bounded page per transaction, and a lost commit response
/// is reconciled by [`SnapshotPageApplyOutcome::ResumeAt`].
pub async fn begin_snapshot_pages(
    client: &mut Client,
    plan: SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<SnapshotPageLoader, SnapshotTargetError> {
    validate_ownership(ownership)?;
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, ownership.fence).await?;
    let progress = prepare_shadow_load(&transaction, &plan, ownership).await?;
    transaction.commit().await?;
    Ok(SnapshotPageLoader {
        plan,
        ownership: ownership.clone(),
        cursor: progress.cursor,
        completed: progress.completed,
    })
}

/// Durable, resumable driver for a bounded page-by-page shadow load. Holds the
/// last committed cursor so the caller can derive the next source range and
/// prove forward progress. Each [`apply_page`](Self::apply_page) runs in its own
/// transaction and commits the copied rows together with the advanced cursor.
#[derive(Debug, Clone)]
pub struct SnapshotPageLoader {
    plan: SnapshotTargetPlan,
    ownership: SnapshotOwnership,
    cursor: Vec<String>,
    completed: bool,
}

/// Observes the durable boundary after a bounded snapshot page transaction commits.
///
/// Returning an error makes the commit result intentionally ambiguous to the caller. The caller
/// must discard the source snapshot session; a later owner then fences and removes the loading
/// group before starting a fresh source snapshot.
pub trait SnapshotPageCommitObserver: Send + Sync {
    fn observe_after_commit(&self) -> Result<(), String>;
}

#[derive(Debug)]
struct NoopSnapshotPageCommitObserver;

impl SnapshotPageCommitObserver for NoopSnapshotPageCommitObserver {
    fn observe_after_commit(&self) -> Result<(), String> {
        Ok(())
    }
}

static NOOP_SNAPSHOT_PAGE_COMMIT_OBSERVER: NoopSnapshotPageCommitObserver =
    NoopSnapshotPageCommitObserver;

impl SnapshotPageLoader {
    /// The last durably committed cursor. Empty means the scan starts at the
    /// table head; the caller maps this to `None` for the source pager.
    #[must_use]
    pub fn cursor(&self) -> &[String] {
        &self.cursor
    }

    /// Whether the durable progress row is already marked complete. A resumed
    /// loader that finds this true must skip straight to activation.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        self.completed
    }

    #[must_use]
    pub const fn plan(&self) -> &SnapshotTargetPlan {
        &self.plan
    }

    /// Copy one bounded page into the shadow and advance the durable cursor in a
    /// single transaction. `next_cursor` is the source-derived boundary for this
    /// page (the final PK of the page, or the current cursor for an empty tail);
    /// `completed` must mean the source session proved this range is the fixed
    /// tail of its repeatable-read snapshot. Updates the in-memory cursor to the
    /// committed value and returns the outcome so callers can react to a
    /// reconciled lost commit ([`SnapshotPageApplyOutcome::ResumeAt`]).
    pub async fn apply_page<S, E>(
        &mut self,
        client: &mut Client,
        next_cursor: Vec<String>,
        completed: bool,
        stream: S,
    ) -> Result<SnapshotPageApplyOutcome, SnapshotTargetError>
    where
        S: Stream<Item = Result<Bytes, E>>,
        E: Display,
    {
        self.apply_page_observed(
            client,
            next_cursor,
            completed,
            stream,
            &NOOP_SNAPSHOT_PAGE_COMMIT_OBSERVER,
        )
        .await
    }

    /// Deterministic fault-injection entry point for the page commit boundary.
    pub async fn apply_page_observed<S, E>(
        &mut self,
        client: &mut Client,
        next_cursor: Vec<String>,
        completed: bool,
        stream: S,
        observer: &dyn SnapshotPageCommitObserver,
    ) -> Result<SnapshotPageApplyOutcome, SnapshotTargetError>
    where
        S: Stream<Item = Result<Bytes, E>>,
        E: Display,
    {
        let transaction = client.transaction().await?;
        lock_pipeline_fence(&transaction, self.ownership.fence).await?;
        let outcome = progress::copy_snapshot_page(
            &transaction,
            &self.plan,
            &self.ownership,
            &self.cursor,
            next_cursor,
            completed,
            stream,
        )
        .await?;
        transaction.commit().await?;
        observer
            .observe_after_commit()
            .map_err(SnapshotTargetError::PageCommitObserver)?;
        let progress = match &outcome {
            SnapshotPageApplyOutcome::Applied(progress)
            | SnapshotPageApplyOutcome::ResumeAt(progress)
            | SnapshotPageApplyOutcome::AlreadyCompleted(progress) => progress,
        };
        self.cursor = progress.cursor.clone();
        self.completed = progress.completed;
        Ok(outcome)
    }
}

/// Target transaction whose COPY stream must finish before commit is allowed.
pub struct SnapshotApply<'client> {
    transaction: Transaction<'client>,
    plan: SnapshotTargetPlan,
    ownership: SnapshotOwnership,
    mode: SnapshotApplyMode,
    copied_rows: Option<u64>,
}

impl SnapshotApply<'_> {
    #[must_use]
    pub const fn plan(&self) -> &SnapshotTargetPlan {
        &self.plan
    }

    #[must_use]
    pub const fn mode(&self) -> SnapshotApplyMode {
        self.mode
    }

    /// Forward source COPY chunks one at a time. `send().await` supplies target backpressure.
    pub async fn copy_from_stream<S, E>(&mut self, stream: S) -> Result<u64, SnapshotTargetError>
    where
        S: Stream<Item = Result<Bytes, E>>,
        E: Display,
    {
        if self.copied_rows.is_some() {
            return Err(SnapshotTargetError::CopyAlreadyCompleted);
        }
        let sink = self.transaction.copy_in(&self.plan.copy_sql).await?;
        let mut sink = std::pin::pin!(sink);
        forward_chunks(stream, &mut sink).await?;
        let copied_rows = sink
            .finish()
            .await
            .map_err(|error| SnapshotTargetError::CopySink(error.to_string()))?;
        self.copied_rows = Some(copied_rows);
        Ok(copied_rows)
    }

    /// Commit the DDL and copied rows together.
    pub async fn commit(self) -> Result<SnapshotApplyOutcome, SnapshotTargetError> {
        let outcome = match self.copied_rows {
            Some(rows) => {
                progress::complete_full_snapshot_copy(
                    &self.transaction,
                    &self.plan,
                    &self.ownership,
                    rows,
                )
                .await?;
                SnapshotApplyOutcome::Copied { rows }
            }
            None => {
                self.transaction.rollback().await?;
                return Err(SnapshotTargetError::CopyNotCompleted);
            }
        };
        self.transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn rollback(self) -> Result<(), SnapshotTargetError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

async fn forward_chunks<S, E, K, SinkError>(
    stream: S,
    sink: &mut Pin<&mut K>,
) -> Result<(), SnapshotTargetError>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Display,
    K: Sink<Bytes, Error = SinkError>,
    SinkError: Display,
{
    let mut stream = Box::pin(stream);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| SnapshotTargetError::SourceStream(error.to_string()))?;
        sink.as_mut()
            .send(chunk)
            .await
            .map_err(|error| SnapshotTargetError::CopySink(error.to_string()))?;
    }
    Ok(())
}

async fn create_schema(
    transaction: &Transaction<'_>,
    schema: &str,
) -> Result<(), SnapshotTargetError> {
    transaction
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}",
            quote_identifier(schema)?
        ))
        .await?;
    Ok(())
}

pub(super) async fn relation_exists(
    transaction: &Transaction<'_>,
    table: &QualifiedName,
) -> Result<bool, SnapshotTargetError> {
    Ok(transaction
        .query_one(RELATION_EXISTS_SQL, &[&table.schema, &table.name])
        .await?
        .try_get(0)?)
}

/// Returns the stable catalog identity of a physical table.
///
/// Names alone are not sufficient for destructive cleanup: an operator could drop a quarantined
/// table and create an unrelated table with the same name while the service is stopped. OIDs are
/// captured in the same transaction as table creation/rename and checked before every purge.
pub(super) async fn relation_oid(
    transaction: &Transaction<'_>,
    table: &QualifiedName,
) -> Result<Option<i64>, SnapshotTargetError> {
    Ok(transaction
        .query_opt(
            "SELECT c.oid::bigint AS relation_oid
               FROM pg_catalog.pg_class AS c
               JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
              WHERE n.nspname = $1
                AND c.relname = $2
                AND c.relkind IN ('r', 'p', 'f')",
            &[&table.schema, &table.name],
        )
        .await?
        .map(|row| row.try_get("relation_oid"))
        .transpose()?)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ManagedTableState {
    Shadow,
    Active,
    Blocked,
    Quarantined,
}

impl ManagedTableState {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Shadow => "shadow",
            Self::Active => "active",
            Self::Blocked => "blocked",
            Self::Quarantined => "quarantined",
        }
    }

    fn parse(table: &QualifiedName, value: String) -> Result<Self, SnapshotTargetError> {
        match value.as_str() {
            "shadow" => Ok(Self::Shadow),
            "active" => Ok(Self::Active),
            "blocked" => Ok(Self::Blocked),
            "quarantined" => Ok(Self::Quarantined),
            _ => Err(SnapshotTargetError::InvalidManagedTableValue {
                table: table.to_string(),
                field: "state",
                value,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ManagedTableRecord {
    pub(super) pipeline_id: PipelineId,
    pub(super) snapshot_group_id: Option<Uuid>,
    /// PostgreSQL object identity captured at registration time.  Legacy V4 rows may be `None`;
    /// destructive cleanup deliberately refuses to operate on those rows.
    pub(super) relation_oid: Option<i64>,
    pub(super) source_relation_id: u32,
    pub(super) table_generation: u64,
    pub(super) schema_fingerprint: String,
    pub(super) state: ManagedTableState,
    pub(super) fencing_token: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum RelationState {
    Vacant,
    Unmanaged,
    ManagedObjectMissing(ManagedTableRecord),
    Managed(ManagedTableRecord),
}

pub(super) async fn load_relation_state(
    transaction: &Transaction<'_>,
    table: &QualifiedName,
) -> Result<RelationState, SnapshotTargetError> {
    let sql = format!(
        "SELECT pipeline_id, snapshot_group_id, relation_oid, source_relation_id, table_generation, schema_fingerprint, state, fencing_token FROM {}.managed_tables WHERE target_schema = $1 AND target_table = $2 FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let record = transaction
        .query_opt(&sql, &[&table.schema, &table.name])
        .await?
        .map(|row| managed_table_record_from_row(table, &row))
        .transpose()?;
    let exists = relation_exists(transaction, table).await?;
    Ok(match (exists, record) {
        (false, None) => RelationState::Vacant,
        (true, None) => RelationState::Unmanaged,
        (false, Some(record)) => RelationState::ManagedObjectMissing(record),
        (true, Some(record)) => RelationState::Managed(record),
    })
}

pub(super) fn managed_table_record_from_row(
    table: &QualifiedName,
    row: &tokio_postgres::Row,
) -> Result<ManagedTableRecord, SnapshotTargetError> {
    let relation_raw: i64 = row.try_get("source_relation_id")?;
    let source_relation_id =
        u32::try_from(relation_raw).map_err(|_| SnapshotTargetError::InvalidManagedTableValue {
            table: table.to_string(),
            field: "source_relation_id",
            value: relation_raw.to_string(),
        })?;
    let generation_raw: i64 = row.try_get("table_generation")?;
    let table_generation = u64::try_from(generation_raw).map_err(|_| {
        SnapshotTargetError::InvalidManagedTableValue {
            table: table.to_string(),
            field: "table_generation",
            value: generation_raw.to_string(),
        }
    })?;
    let state = ManagedTableState::parse(table, row.try_get("state")?)?;
    Ok(ManagedTableRecord {
        pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
        snapshot_group_id: row.try_get("snapshot_group_id")?,
        relation_oid: row.try_get("relation_oid")?,
        source_relation_id,
        table_generation,
        schema_fingerprint: row.try_get("schema_fingerprint")?,
        state,
        fencing_token: row.try_get("fencing_token")?,
    })
}

pub(super) fn validate_managed_identity(
    table: &QualifiedName,
    record: &ManagedTableRecord,
    pipeline_id: PipelineId,
    source_relation_id: u32,
) -> Result<(), SnapshotTargetError> {
    if record.pipeline_id != pipeline_id {
        return Err(SnapshotTargetError::ManagedByOtherPipeline {
            table: table.to_string(),
            expected: pipeline_id,
            actual: record.pipeline_id,
        });
    }
    if record.source_relation_id != source_relation_id {
        return Err(SnapshotTargetError::ManagedByOtherSource {
            table: table.to_string(),
            expected: source_relation_id,
            actual: record.source_relation_id,
        });
    }
    Ok(())
}

pub(super) fn validate_managed_fence(
    table: &QualifiedName,
    record: &ManagedTableRecord,
    current: i64,
) -> Result<(), SnapshotTargetError> {
    if record.fencing_token > current {
        Err(SnapshotTargetError::ManagedByNewerFence {
            object: table.to_string(),
            current,
            actual: record.fencing_token,
        })
    } else {
        Ok(())
    }
}

pub(super) fn matches_activation(
    record: &ManagedTableRecord,
    table: &SnapshotActivationTable,
    snapshot_group_id: Uuid,
) -> bool {
    record.snapshot_group_id == Some(snapshot_group_id)
        && record.source_relation_id == table.source_relation_id
        && record.table_generation == table.table_generation
        && record.schema_fingerprint == table.schema_fingerprint
}

fn validate_target_for_load(
    state: &RelationState,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<(), SnapshotTargetError> {
    let record = match state {
        RelationState::Vacant => return Ok(()),
        RelationState::Unmanaged => {
            return Err(SnapshotTargetError::ExistingUnmanagedTable(
                plan.target.to_string(),
            ));
        }
        RelationState::ManagedObjectMissing(_) => {
            return Err(SnapshotTargetError::ManagedRelationMissing(
                plan.target.to_string(),
            ));
        }
        RelationState::Managed(record) => record,
    };
    validate_managed_identity(
        &plan.target,
        record,
        ownership.fence.pipeline_id,
        plan.source_relation_id,
    )?;
    validate_managed_fence(&plan.target, record, ownership.fence.fencing_token)?;
    if record.state != ManagedTableState::Active {
        return Err(SnapshotTargetError::UnexpectedManagedTableState {
            table: plan.target.to_string(),
            expected: ManagedTableState::Active.as_str(),
            actual: record.state.as_str().to_owned(),
        });
    }
    if record.table_generation == plan.source_generation
        && record.schema_fingerprint == ownership.schema_fingerprint
    {
        return Err(SnapshotTargetError::TargetAlreadyActive(
            plan.target.to_string(),
        ));
    }
    Ok(())
}

/// Returns true when an exactly owned committed shadow may be safely replaced by this group.
fn validate_shadow_for_rebuild(
    state: &RelationState,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<bool, SnapshotTargetError> {
    let record = match state {
        RelationState::Vacant => return Ok(false),
        RelationState::Unmanaged => {
            return Err(SnapshotTargetError::ExistingUnmanagedTable(
                plan.shadow.target.to_string(),
            ));
        }
        RelationState::ManagedObjectMissing(_) => {
            return Err(SnapshotTargetError::ManagedRelationMissing(
                plan.shadow.target.to_string(),
            ));
        }
        RelationState::Managed(record) => record,
    };
    validate_managed_identity(
        &plan.shadow.target,
        record,
        ownership.fence.pipeline_id,
        plan.source_relation_id,
    )?;
    validate_managed_fence(&plan.shadow.target, record, ownership.fence.fencing_token)?;
    if record.state != ManagedTableState::Shadow {
        return Err(SnapshotTargetError::UnexpectedManagedTableState {
            table: plan.shadow.target.to_string(),
            expected: ManagedTableState::Shadow.as_str(),
            actual: record.state.as_str().to_owned(),
        });
    }
    if record.snapshot_group_id != Some(ownership.snapshot_group_id) {
        if record.fencing_token < ownership.fence.fencing_token {
            return Ok(true);
        }
        return Err(SnapshotTargetError::ShadowSnapshotGroupMismatch {
            table: plan.shadow.target.to_string(),
            expected: ownership.snapshot_group_id,
            actual: record.snapshot_group_id,
        });
    }
    if record.table_generation != plan.source_generation {
        return Err(SnapshotTargetError::ShadowGenerationMismatch {
            table: plan.shadow.target.to_string(),
            expected: plan.source_generation,
            actual: record.table_generation,
        });
    }
    if record.schema_fingerprint != ownership.schema_fingerprint {
        return Err(SnapshotTargetError::ShadowFingerprintMismatch(
            plan.shadow.target.to_string(),
        ));
    }
    Ok(true)
}

async fn remove_managed_shadow(
    transaction: &Transaction<'_>,
    table: &QualifiedName,
    record: &ManagedTableRecord,
) -> Result<(), SnapshotTargetError> {
    progress::delete_snapshot_progress_for_shadow(
        transaction,
        record.snapshot_group_id,
        record.relation_oid,
    )
    .await?;
    transaction
        .batch_execute(&format!("DROP TABLE {}", quote_qualified_name(table)?))
        .await?;
    let sql = format!(
        "DELETE FROM {}.managed_tables WHERE target_schema = $1 AND target_table = $2",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction
        .execute(&sql, &[&table.schema, &table.name])
        .await?;
    ensure_one_metadata_row(written)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManagedTypeRecord {
    pipeline_id: PipelineId,
    definition_checksum: Vec<u8>,
    fencing_token: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ManagedTypeAction {
    Create,
    Reuse,
    Reconcile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EnumAlteration {
    AddBefore { label: String, before: String },
    Append { label: String },
    Rename { from: String, to: String },
}

async fn managed_type_record(
    transaction: &Transaction<'_>,
    data_type: &QualifiedName,
) -> Result<Option<ManagedTypeRecord>, SnapshotTargetError> {
    let sql = format!(
        "SELECT pipeline_id, definition_checksum, fencing_token FROM {}.managed_types WHERE type_schema = $1 AND type_name = $2 FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    Ok(transaction
        .query_opt(&sql, &[&data_type.schema, &data_type.name])
        .await?
        .map(|row| -> Result<ManagedTypeRecord, tokio_postgres::Error> {
            Ok(ManagedTypeRecord {
                pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
                definition_checksum: row.try_get("definition_checksum")?,
                fencing_token: row.try_get("fencing_token")?,
            })
        })
        .transpose()?)
}

async fn type_exists(
    transaction: &Transaction<'_>,
    data_type: &QualifiedName,
) -> Result<bool, SnapshotTargetError> {
    Ok(transaction
        .query_one(TYPE_EXISTS_SQL, &[&data_type.schema, &data_type.name])
        .await?
        .try_get(0)?)
}

async fn ensure_prerequisite_type(
    transaction: &Transaction<'_>,
    prerequisite: &UserTypePlan,
    ownership: &SnapshotOwnership,
) -> Result<(), SnapshotTargetError> {
    let managed = managed_type_record(transaction, &prerequisite.name).await?;
    let exists = type_exists(transaction, &prerequisite.name).await?;
    let definition_checksum = Sha256::digest(prerequisite.create_sql.as_bytes()).to_vec();
    match classify_prerequisite_type(
        &prerequisite.name,
        exists,
        managed.as_ref(),
        ownership.fence.pipeline_id,
        ownership.fence.fencing_token,
        &definition_checksum,
    )? {
        ManagedTypeAction::Create => {
            transaction.batch_execute(&prerequisite.create_sql).await?;
            let sql = format!(
                "INSERT INTO {}.managed_types (type_schema, type_name, pipeline_id, definition_checksum, fencing_token) VALUES ($1, $2, $3, $4, $5)",
                quote_identifier(TARGET_METADATA_SCHEMA)?
            );
            let pipeline_id = ownership.fence.pipeline_id.as_uuid();
            let written = transaction
                .execute(
                    &sql,
                    &[
                        &prerequisite.name.schema,
                        &prerequisite.name.name,
                        &pipeline_id,
                        &definition_checksum,
                        &ownership.fence.fencing_token,
                    ],
                )
                .await?;
            ensure_one_metadata_row(written)
        }
        ManagedTypeAction::Reuse => {
            update_managed_type(transaction, prerequisite, ownership, &definition_checksum).await
        }
        ManagedTypeAction::Reconcile => {
            reconcile_prerequisite_type(transaction, prerequisite).await?;
            update_managed_type(transaction, prerequisite, ownership, &definition_checksum).await
        }
    }
}

async fn update_managed_type(
    transaction: &Transaction<'_>,
    prerequisite: &UserTypePlan,
    ownership: &SnapshotOwnership,
    definition_checksum: &[u8],
) -> Result<(), SnapshotTargetError> {
    let sql = format!(
        "UPDATE {}.managed_types SET definition_checksum = $3, fencing_token = $4, updated_at = clock_timestamp() WHERE type_schema = $1 AND type_name = $2",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction
        .execute(
            &sql,
            &[
                &prerequisite.name.schema,
                &prerequisite.name.name,
                &definition_checksum,
                &ownership.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_metadata_row(written)
}

async fn reconcile_prerequisite_type(
    transaction: &Transaction<'_>,
    prerequisite: &UserTypePlan,
) -> Result<(), SnapshotTargetError> {
    let UserTypeDefinition::Enum { labels: desired } = &prerequisite.definition else {
        return Err(SnapshotTargetError::UnsupportedPrerequisiteTypeEvolution {
            name: prerequisite.name.to_string(),
            reason: "constraint-free domain base-type changes require a versioned target type"
                .to_owned(),
        });
    };
    let existing = load_enum_labels(transaction, &prerequisite.name).await?;
    let alterations = plan_enum_evolution(&existing, desired).map_err(|reason| {
        SnapshotTargetError::UnsupportedPrerequisiteTypeEvolution {
            name: prerequisite.name.to_string(),
            reason,
        }
    })?;
    let type_name = quote_qualified_name(&prerequisite.name)?;
    for alteration in alterations {
        let sql = match alteration {
            EnumAlteration::AddBefore { label, before } => format!(
                "ALTER TYPE {type_name} ADD VALUE {} BEFORE {}",
                quote_literal(&label)?,
                quote_literal(&before)?
            ),
            EnumAlteration::Append { label } => {
                format!(
                    "ALTER TYPE {type_name} ADD VALUE {}",
                    quote_literal(&label)?
                )
            }
            EnumAlteration::Rename { from, to } => format!(
                "ALTER TYPE {type_name} RENAME VALUE {} TO {}",
                quote_literal(&from)?,
                quote_literal(&to)?
            ),
        };
        transaction.batch_execute(&sql).await?;
    }
    Ok(())
}

async fn load_enum_labels(
    transaction: &Transaction<'_>,
    name: &QualifiedName,
) -> Result<Vec<String>, SnapshotTargetError> {
    let kind: String = transaction
        .query_one(
            "SELECT t.typtype::text
               FROM pg_catalog.pg_type t
               JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
              WHERE n.nspname = $1 AND t.typname = $2",
            &[&name.schema, &name.name],
        )
        .await?
        .try_get(0)?;
    if kind != "e" {
        return Err(SnapshotTargetError::UnsupportedPrerequisiteTypeEvolution {
            name: name.to_string(),
            reason: format!("managed object is PostgreSQL type kind `{kind}`, expected enum"),
        });
    }
    transaction
        .query(
            "SELECT e.enumlabel
               FROM pg_catalog.pg_type t
               JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
               JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
              WHERE n.nspname = $1 AND t.typname = $2
              ORDER BY e.enumsortorder",
            &[&name.schema, &name.name],
        )
        .await?
        .into_iter()
        .map(|row| row.try_get(0).map_err(SnapshotTargetError::from))
        .collect()
}

fn plan_enum_evolution(
    existing: &[String],
    desired: &[String],
) -> Result<Vec<EnumAlteration>, String> {
    if existing == desired {
        return Ok(Vec::new());
    }

    let existing_is_subsequence = existing.iter().try_fold(0, |position, label| {
        desired[position..]
            .iter()
            .position(|candidate| candidate == label)
            .map(|offset| position + offset + 1)
    });
    if existing_is_subsequence.is_some() {
        let mut current = existing.to_vec();
        let mut alterations = Vec::with_capacity(desired.len().saturating_sub(existing.len()));
        for (position, label) in desired.iter().enumerate() {
            if current.get(position) == Some(label) {
                continue;
            }
            if current.iter().any(|candidate| candidate == label) {
                return Err("enum label reordering is not supported".to_owned());
            }
            if let Some(before) = current.get(position).cloned() {
                alterations.push(EnumAlteration::AddBefore {
                    label: label.clone(),
                    before,
                });
            } else {
                alterations.push(EnumAlteration::Append {
                    label: label.clone(),
                });
            }
            current.insert(position, label.clone());
        }
        return Ok(alterations);
    }

    if existing.len() == desired.len()
        && existing
            .iter()
            .zip(desired)
            .all(|(from, to)| from == to || !existing.iter().any(|label| label == to))
    {
        return Ok(existing
            .iter()
            .zip(desired)
            .filter(|(from, to)| from != to)
            .map(|(from, to)| EnumAlteration::Rename {
                from: from.clone(),
                to: to.clone(),
            })
            .collect());
    }

    Err("enum label removal, reordering, or combined rename/add is not supported".to_owned())
}

fn classify_prerequisite_type(
    name: &QualifiedName,
    exists: bool,
    managed: Option<&ManagedTypeRecord>,
    pipeline_id: PipelineId,
    fencing_token: i64,
    definition_checksum: &[u8],
) -> Result<ManagedTypeAction, SnapshotTargetError> {
    match (exists, managed) {
        (false, None) => Ok(ManagedTypeAction::Create),
        (true, None) => Err(SnapshotTargetError::ExistingPrerequisiteType(
            name.to_string(),
        )),
        (false, Some(_)) => Err(SnapshotTargetError::ManagedPrerequisiteTypeMissing(
            name.to_string(),
        )),
        (true, Some(record)) if record.pipeline_id != pipeline_id => Err(
            SnapshotTargetError::PrerequisiteTypeManagedByOtherPipeline {
                name: name.to_string(),
                expected: pipeline_id,
                actual: record.pipeline_id,
            },
        ),
        (true, Some(record)) if record.fencing_token > fencing_token => {
            Err(SnapshotTargetError::ManagedByNewerFence {
                object: name.to_string(),
                current: fencing_token,
                actual: record.fencing_token,
            })
        }
        (true, Some(record)) if record.definition_checksum != definition_checksum => {
            Ok(ManagedTypeAction::Reconcile)
        }
        (true, Some(_)) => Ok(ManagedTypeAction::Reuse),
    }
}

fn validate_ownership(ownership: &SnapshotOwnership) -> Result<(), SnapshotTargetError> {
    if ownership.fence.fencing_token <= 0 || ownership.schema_fingerprint.is_empty() {
        Err(SnapshotTargetError::InvalidOwnership)
    } else if ownership.snapshot_group_id.is_nil() {
        Err(SnapshotTargetError::InvalidSnapshotGroupId)
    } else {
        Ok(())
    }
}

async fn register_shadow(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<(), SnapshotTargetError> {
    let generation = database_generation(plan.source_generation)?;
    let relation_id = i64::from(plan.source_relation_id);
    let pipeline_id = ownership.fence.pipeline_id.as_uuid();
    let relation_oid = relation_oid(transaction, &plan.shadow.target)
        .await?
        .ok_or_else(|| {
            SnapshotTargetError::ManagedRelationMissing(plan.shadow.target.to_string())
        })?;
    let sql = format!(
        "INSERT INTO {}.managed_tables (target_schema, target_table, pipeline_id, snapshot_group_id, relation_oid, source_relation_id, table_generation, schema_fingerprint, state, fencing_token) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'shadow', $9)",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction
        .execute(
            &sql,
            &[
                &plan.shadow.target.schema,
                &plan.shadow.target.name,
                &pipeline_id,
                &ownership.snapshot_group_id,
                &relation_oid,
                &relation_id,
                &generation,
                &ownership.schema_fingerprint,
                &ownership.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_metadata_row(written)
}

pub(super) fn database_generation(generation: u64) -> Result<i64, SnapshotTargetError> {
    i64::try_from(generation).map_err(|_| SnapshotTargetError::GenerationOutOfRange(generation))
}

pub(super) fn ensure_one_metadata_row(written: u64) -> Result<(), SnapshotTargetError> {
    if written == 1 {
        Ok(())
    } else {
        Err(SnapshotTargetError::UnexpectedMetadataWriteCount(written))
    }
}

#[cfg(test)]
mod tests {
    use std::task::{Context, Poll};

    use cloudberry_etl_core::schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, ReplicaIdentity,
        TableKind,
    };

    use super::*;

    fn column(attnum: i16, name: &str, kind: PgTypeKind, pk: Option<u16>) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid: u32::try_from(attnum).unwrap(),
                name: QualifiedName::new("pg_catalog", name).unwrap(),
                kind,
            },
            nullable: pk.is_none(),
            primary_key_ordinal: pk,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn table() -> TableSchema {
        TableSchema {
            relation_id: 42,
            generation: 7,
            name: QualifiedName::new("source", "items").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                column(1, "id", PgTypeKind::Int8, Some(1)),
                column(2, "payload", PgTypeKind::Text, None),
                column(3, "raw", PgTypeKind::Bytea, None),
            ],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    #[test]
    fn plans_a_distinct_typed_shadow_and_explicit_text_copy() {
        let plan = plan_snapshot_target(
            &table(),
            QualifiedName::new("target", "items").unwrap(),
            QualifiedName::new("target", "items_shadow").unwrap(),
        )
        .unwrap();
        assert_eq!(plan.target.to_string(), "target.items");
        assert_eq!(plan.shadow.target.to_string(), "target.items_shadow");
        assert!(plan.shadow.create_sql.contains("\"id\" bigint"));
        assert!(plan.shadow.create_sql.contains("\"raw\" bytea"));
        assert!(
            plan.copy_sql
                .starts_with("COPY \"target\".\"items_shadow\" (\"id\", \"payload\", \"raw\")")
        );
        assert!(plan.copy_sql.contains("FORMAT text"));
        assert!(plan.copy_sql.contains("DELIMITER E'\\t'"));
        assert!(plan.copy_sql.contains("NULL E'\\\\N'"));
        assert!(!plan.copy_sql.contains('*'));
        assert_eq!(plan.source_relation_id, 42);
        assert_eq!(plan.source_generation, 7);
        assert_eq!(plan.shadow.storage, TargetStorage::AoColumn);
    }

    #[test]
    fn shadow_keeps_the_explicit_business_storage_profile() {
        let plan = plan_snapshot_target_with_storage(
            &table(),
            QualifiedName::new("target", "items").unwrap(),
            QualifiedName::new("target", "items_shadow").unwrap(),
            TargetStorage::PaxExperimental,
        )
        .unwrap();
        assert_eq!(plan.shadow.storage, TargetStorage::PaxExperimental);
        assert!(plan.shadow.create_sql.contains("USING pax"));
    }

    #[test]
    fn rejects_target_shadow_alias_cross_schema_and_invalid_ownership() {
        let name = QualifiedName::new("target", "items").unwrap();
        assert!(matches!(
            plan_snapshot_target(&table(), name.clone(), name),
            Err(SnapshotTargetError::TargetIsShadow)
        ));
        assert!(matches!(
            plan_snapshot_target(
                &table(),
                QualifiedName::new("target", "items").unwrap(),
                QualifiedName::new("temporary", "items_shadow").unwrap(),
            ),
            Err(SnapshotTargetError::CrossSchemaShadowUnsupported { .. })
        ));
        let mut ownership = SnapshotOwnership {
            fence: PipelineFence {
                pipeline_id: PipelineId::new(),
                topology_generation: 1,
                fencing_token: 1,
            },
            snapshot_group_id: Uuid::from_u128(1),
            schema_fingerprint: String::new(),
        };
        assert!(matches!(
            validate_ownership(&ownership),
            Err(SnapshotTargetError::InvalidOwnership)
        ));
        ownership.schema_fingerprint = "sha256:abc".to_owned();
        ownership.fence.fencing_token = 0;
        assert!(matches!(
            validate_ownership(&ownership),
            Err(SnapshotTargetError::InvalidOwnership)
        ));
        assert!(matches!(
            database_generation(u64::MAX),
            Err(SnapshotTargetError::GenerationOutOfRange(u64::MAX))
        ));
    }

    #[test]
    fn committed_shadow_is_rebuilt_only_with_exact_ownership() {
        let plan = plan_snapshot_target(
            &table(),
            QualifiedName::new("target", "items").unwrap(),
            QualifiedName::new("target", "items_shadow").unwrap(),
        )
        .unwrap();
        let ownership = SnapshotOwnership {
            fence: PipelineFence {
                pipeline_id: PipelineId::new(),
                topology_generation: 1,
                fencing_token: 2,
            },
            snapshot_group_id: Uuid::from_u128(1),
            schema_fingerprint: "sha256:abc".to_owned(),
        };
        let record = ManagedTableRecord {
            pipeline_id: ownership.fence.pipeline_id,
            snapshot_group_id: Some(ownership.snapshot_group_id),
            relation_oid: None,
            source_relation_id: 42,
            table_generation: 7,
            schema_fingerprint: ownership.schema_fingerprint.clone(),
            state: ManagedTableState::Shadow,
            fencing_token: 1,
        };
        assert!(
            validate_shadow_for_rebuild(&RelationState::Managed(record.clone()), &plan, &ownership)
                .unwrap()
        );

        let mut stale_group = record.clone();
        stale_group.snapshot_group_id = Some(Uuid::from_u128(2));
        assert!(
            validate_shadow_for_rebuild(&RelationState::Managed(stale_group), &plan, &ownership)
                .unwrap()
        );

        let mut concurrent_group = record.clone();
        concurrent_group.snapshot_group_id = Some(Uuid::from_u128(2));
        concurrent_group.fencing_token = ownership.fence.fencing_token;
        assert!(matches!(
            validate_shadow_for_rebuild(
                &RelationState::Managed(concurrent_group),
                &plan,
                &ownership
            ),
            Err(SnapshotTargetError::ShadowSnapshotGroupMismatch { .. })
        ));

        let mut newer_fence = record.clone();
        newer_fence.fencing_token = ownership.fence.fencing_token + 1;
        assert!(matches!(
            validate_shadow_for_rebuild(&RelationState::Managed(newer_fence), &plan, &ownership),
            Err(SnapshotTargetError::ManagedByNewerFence {
                current: 2,
                actual: 3,
                ..
            })
        ));

        let mut wrong = record;
        wrong.schema_fingerprint.push_str("-different");
        assert!(matches!(
            validate_shadow_for_rebuild(&RelationState::Managed(wrong), &plan, &ownership),
            Err(SnapshotTargetError::ShadowFingerprintMismatch(_))
        ));

        let active = ManagedTableRecord {
            pipeline_id: ownership.fence.pipeline_id,
            snapshot_group_id: Some(ownership.snapshot_group_id),
            relation_oid: None,
            source_relation_id: 42,
            table_generation: 7,
            schema_fingerprint: ownership.schema_fingerprint.clone(),
            state: ManagedTableState::Active,
            fencing_token: 2,
        };
        assert!(matches!(
            validate_target_for_load(&RelationState::Managed(active), &plan, &ownership),
            Err(SnapshotTargetError::TargetAlreadyActive(_))
        ));
    }

    #[test]
    fn managed_user_type_reuse_requires_pipeline_and_definition_match() {
        let name = QualifiedName::new("target", "status").unwrap();
        let pipeline_id = PipelineId::new();
        let checksum = vec![1, 2, 3];
        let record = ManagedTypeRecord {
            pipeline_id,
            definition_checksum: checksum.clone(),
            fencing_token: 1,
        };
        assert_eq!(
            classify_prerequisite_type(&name, false, None, pipeline_id, 2, &checksum).unwrap(),
            ManagedTypeAction::Create
        );
        assert_eq!(
            classify_prerequisite_type(&name, true, Some(&record), pipeline_id, 2, &checksum)
                .unwrap(),
            ManagedTypeAction::Reuse
        );
        assert!(matches!(
            classify_prerequisite_type(&name, true, Some(&record), PipelineId::new(), 2, &checksum),
            Err(SnapshotTargetError::PrerequisiteTypeManagedByOtherPipeline { .. })
        ));
        assert!(matches!(
            classify_prerequisite_type(&name, true, Some(&record), pipeline_id, 2, &[9]),
            Ok(ManagedTypeAction::Reconcile)
        ));
        assert!(matches!(
            classify_prerequisite_type(&name, true, None, pipeline_id, 2, &checksum),
            Err(SnapshotTargetError::ExistingPrerequisiteType(_))
        ));

        let mut newer_fence = record;
        newer_fence.fencing_token = 3;
        assert!(matches!(
            classify_prerequisite_type(&name, true, Some(&newer_fence), pipeline_id, 2, &checksum),
            Err(SnapshotTargetError::ManagedByNewerFence {
                current: 2,
                actual: 3,
                ..
            })
        ));
    }

    #[test]
    fn enum_evolution_adds_labels_without_reordering_existing_values() {
        let existing = strings(&["new", "done"]);
        let desired = strings(&["queued", "new", "running", "done", "archived"]);
        assert_eq!(
            plan_enum_evolution(&existing, &desired).unwrap(),
            vec![
                EnumAlteration::AddBefore {
                    label: "queued".into(),
                    before: "new".into(),
                },
                EnumAlteration::AddBefore {
                    label: "running".into(),
                    before: "done".into(),
                },
                EnumAlteration::Append {
                    label: "archived".into(),
                },
            ]
        );
    }

    #[test]
    fn enum_evolution_supports_unambiguous_positional_renames() {
        assert_eq!(
            plan_enum_evolution(&strings(&["new", "done"]), &strings(&["ready", "closed"]))
                .unwrap(),
            vec![
                EnumAlteration::Rename {
                    from: "new".into(),
                    to: "ready".into(),
                },
                EnumAlteration::Rename {
                    from: "done".into(),
                    to: "closed".into(),
                },
            ]
        );
    }

    #[test]
    fn enum_evolution_rejects_removal_reorder_and_mixed_changes() {
        assert!(
            plan_enum_evolution(&strings(&["new", "done"]), &strings(&["done", "new"])).is_err()
        );
        assert!(plan_enum_evolution(&strings(&["new", "done"]), &strings(&["new"])).is_err());
        assert!(
            plan_enum_evolution(
                &strings(&["new", "done"]),
                &strings(&["ready", "running", "done"])
            )
            .is_err()
        );
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[derive(Default)]
    struct BackpressureSink {
        chunks: Vec<Bytes>,
        block_ready_once: bool,
        ready_polls: usize,
    }

    impl Sink<Bytes> for BackpressureSink {
        type Error = &'static str;

        fn poll_ready(
            mut self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.ready_polls += 1;
            if self.block_ready_once {
                self.block_ready_once = false;
                context.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }

        fn start_send(mut self: Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
            self.chunks.push(item);
            self.block_ready_once = true;
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn forwards_chunks_in_order_and_waits_for_sink_readiness() {
        let stream = futures::stream::iter([
            Ok::<_, &'static str>(Bytes::from_static(b"first")),
            Ok(Bytes::from_static(b"second")),
        ]);
        let mut sink = BackpressureSink {
            block_ready_once: true,
            ..BackpressureSink::default()
        };
        let mut sink = Pin::new(&mut sink);
        forward_chunks(stream, &mut sink).await.unwrap();
        assert_eq!(
            sink.chunks,
            [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
        );
        assert!(sink.ready_polls >= 4);
    }

    #[tokio::test]
    async fn source_stream_error_stops_before_later_chunks() {
        let stream = futures::stream::iter([
            Ok(Bytes::from_static(b"first")),
            Err("source failed"),
            Ok(Bytes::from_static(b"must-not-send")),
        ]);
        let mut sink = BackpressureSink::default();
        let mut sink = Pin::new(&mut sink);
        assert!(matches!(
            forward_chunks(stream, &mut sink).await,
            Err(SnapshotTargetError::SourceStream(message)) if message == "source failed"
        ));
        assert_eq!(sink.chunks, [Bytes::from_static(b"first")]);
    }
}
