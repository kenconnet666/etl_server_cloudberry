//! Durable target-side state for whole-table reconciliation.
//!
//! One row is retained per `(pipeline, topology generation, source relation)`. The ledger records
//! run boundaries and final aggregate digests only; page-level progress intentionally remains
//! ephemeral so a restart repeats a read-only scan instead of trying to resume an invalid source
//! snapshot. Every mutation locks the pipeline fence before locking or writing this table.

use std::{str::FromStr as _, time::SystemTime};

use cloudberry_etl_core::{
    id::PipelineId,
    lsn::PgLsn,
    schema::{POSTGRES_IDENTIFIER_MAX_BYTES, QualifiedName},
};
use thiserror::Error;
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

use crate::{
    checkpoint::{CheckpointError, PipelineFence, lock_pipeline_fence},
    snapshot::{
        ManagedTableState, RelationState, SnapshotActivationDisposition, SnapshotActivationOutcome,
        SnapshotActivationRequest, SnapshotTargetError,
        activate_table_snapshot_group_in_transaction, load_relation_state, relation_oid,
        validate_managed_fence, validate_managed_identity,
    },
};

pub const RECONCILIATION_DIGEST_BYTES: usize = 64;
pub const RECONCILIATION_REASON_MAX_BYTES: usize = 4096;

const SELECT_COLUMNS: &str = r#"
pipeline_id, topology_generation, source_relation_id,
target_schema, target_table, target_relation_oid, table_generation, schema_fingerprint,
run_id, state, source_node_id, temporary_slot_name,
source_system_identifier::text AS source_system_identifier, source_timeline,
source_snapshot_lsn::text AS source_snapshot_lsn,
target_checkpoint_lsn::text AS target_checkpoint_lsn,
source_rows, source_bytes, source_digest, target_rows, target_bytes, target_digest,
started_at, completed_at, last_consistent_at, last_mismatch_at, next_due_at,
failure_reason, consecutive_failures, fencing_token
"#;

const INSERT_RECONCILIATION_SQL: &str = r#"
INSERT INTO pg2cb_meta.table_reconciliation_state (
    pipeline_id, topology_generation, source_relation_id,
    target_schema, target_table, target_relation_oid, table_generation, schema_fingerprint,
    run_id, state, source_node_id, temporary_slot_name,
    source_system_identifier, source_timeline, source_snapshot_lsn, fencing_token
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8,
    $9, 'aligning', $10, $11, $12::text::numeric, $13, $14::text::pg_lsn, $15
)
"#;

const REPLACE_RECONCILIATION_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET target_schema = $4, target_table = $5, target_relation_oid = $6,
    table_generation = $7, schema_fingerprint = $8, run_id = $9,
    state = 'aligning', source_node_id = $10, temporary_slot_name = $11,
    source_system_identifier = $12::text::numeric, source_timeline = $13,
    source_snapshot_lsn = $14::text::pg_lsn, target_checkpoint_lsn = NULL,
    source_rows = NULL, source_bytes = NULL, source_digest = NULL,
    target_rows = NULL, target_bytes = NULL, target_digest = NULL,
    started_at = clock_timestamp(), completed_at = NULL, next_due_at = NULL,
    failure_reason = NULL, fencing_token = $15, updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
"#;

const MARK_SCANNING_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET state = 'scanning', target_checkpoint_lsn = $6::text::pg_lsn,
    updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
  AND run_id = $4 AND fencing_token = $5 AND state = 'aligning'
"#;

const COMPLETE_MATCHED_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET state = 'matched', source_rows = $6, source_bytes = $7, source_digest = $8,
    target_rows = $9, target_bytes = $10, target_digest = $11,
    completed_at = clock_timestamp(), last_consistent_at = clock_timestamp(),
    next_due_at = $12, failure_reason = NULL, consecutive_failures = 0,
    updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
  AND run_id = $4 AND fencing_token = $5 AND state = 'scanning'
"#;

const COMPLETE_RELOAD_PENDING_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET state = 'reload_pending', source_rows = $6, source_bytes = $7, source_digest = $8,
    target_rows = $9, target_bytes = $10, target_digest = $11,
    last_mismatch_at = clock_timestamp(), updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
  AND run_id = $4 AND fencing_token = $5 AND state = 'scanning'
"#;

const COMPLETE_RELOADED_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET state = 'reloaded', completed_at = clock_timestamp(),
    last_consistent_at = clock_timestamp(), next_due_at = $6,
    failure_reason = NULL, consecutive_failures = 0, updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
  AND run_id = $4 AND fencing_token = $5 AND state = 'reload_pending'
"#;

const FAIL_RECONCILIATION_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET state = 'failed', completed_at = clock_timestamp(), next_due_at = $6,
    failure_reason = $7, consecutive_failures = $8, updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
  AND run_id = $4 AND fencing_token = $5
  AND state IN ('aligning', 'scanning', 'reload_pending')
"#;

const SUPERSEDE_RECONCILIATION_SQL: &str = r#"
UPDATE pg2cb_meta.table_reconciliation_state
SET state = 'superseded', completed_at = COALESCE(completed_at, clock_timestamp()),
    next_due_at = NULL, failure_reason = $5, fencing_token = $6,
    updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3
  AND run_id = $4
"#;

/// Durable lifecycle of the latest reconciliation run for a table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationState {
    Aligning,
    Scanning,
    Matched,
    ReloadPending,
    Reloaded,
    Failed,
    Superseded,
}

impl ReconciliationState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Aligning => "aligning",
            Self::Scanning => "scanning",
            Self::Matched => "matched",
            Self::ReloadPending => "reload_pending",
            Self::Reloaded => "reloaded",
            Self::Failed => "failed",
            Self::Superseded => "superseded",
        }
    }

    /// A source snapshot/temporary slot cannot survive a process restart.
    #[must_use]
    pub const fn is_interrupted(self) -> bool {
        matches!(self, Self::Aligning | Self::Scanning)
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Matched | Self::Reloaded | Self::Failed | Self::Superseded
        )
    }

    fn from_str(value: &str) -> Result<Self, ReconciliationStateError> {
        match value {
            "aligning" => Ok(Self::Aligning),
            "scanning" => Ok(Self::Scanning),
            "matched" => Ok(Self::Matched),
            "reload_pending" => Ok(Self::ReloadPending),
            "reloaded" => Ok(Self::Reloaded),
            "failed" => Ok(Self::Failed),
            "superseded" => Ok(Self::Superseded),
            other => Err(ReconciliationStateError::InvalidPersistedValue {
                field: "state",
                value: other.to_owned(),
            }),
        }
    }
}

/// Stable table and source-snapshot identity for one reconciliation run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationRunIdentity {
    pub fence: PipelineFence,
    pub source_relation_id: u32,
    pub target: QualifiedName,
    pub target_relation_oid: u32,
    pub table_generation: u64,
    pub schema_fingerprint: String,
    pub run_id: Uuid,
    pub source_node_id: i32,
    pub temporary_slot_name: String,
    pub source_system_identifier: u64,
    pub source_timeline: u32,
    pub source_snapshot_lsn: PgLsn,
}

/// Aggregate result of one full canonical table scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationStats {
    pub rows: u64,
    pub bytes: u64,
    /// Concatenated 256-bit accumulators from the order-independent v1 digest.
    pub digest: [u8; RECONCILIATION_DIGEST_BYTES],
}

/// A loaded latest-run row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredReconciliationState {
    pub run: ReconciliationRunIdentity,
    pub state: ReconciliationState,
    pub target_checkpoint_lsn: Option<PgLsn>,
    pub source: Option<ReconciliationStats>,
    pub target: Option<ReconciliationStats>,
    pub started_at: SystemTime,
    pub completed_at: Option<SystemTime>,
    pub last_consistent_at: Option<SystemTime>,
    pub last_mismatch_at: Option<SystemTime>,
    pub next_due_at: Option<SystemTime>,
    pub failure_reason: Option<String>,
    pub consecutive_failures: u64,
}

impl StoredReconciliationState {
    #[must_use]
    pub const fn was_interrupted(&self) -> bool {
        self.state.is_interrupted()
    }
}

/// Startup work required for a durable nonterminal reconciliation run.
///
/// The exported/imported source snapshot is process-scoped, including while a reload is pending.
/// Startup callers must supersede the old run, clean any orphan loading group, and start from a
/// fresh boundary; this enum distinguishes which interrupted phase needs that cleanup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationStartupRecovery {
    RestartInterruptedScan(StoredReconciliationState),
    RestartPendingReload(StoredReconciliationState),
}

impl ReconciliationStartupRecovery {
    #[must_use]
    pub const fn stored(&self) -> &StoredReconciliationState {
        match self {
            Self::RestartInterruptedScan(stored) | Self::RestartPendingReload(stored) => stored,
        }
    }
}

/// Final result of one canonical source/target scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationScanCompletion {
    Matched {
        source: ReconciliationStats,
        target: ReconciliationStats,
        next_due_at: SystemTime,
    },
    ReloadPending {
        source: ReconciliationStats,
        target: ReconciliationStats,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BeginReconciliationOutcome {
    Started,
    AlreadyExists(ReconciliationState),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationTransitionOutcome {
    Transitioned,
    AlreadyComplete,
}

/// Atomic outcome of promoting a reconciliation shadow and durably completing its run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationReloadOutcome {
    pub activation: SnapshotActivationOutcome,
    pub reconciliation: ReconciliationTransitionOutcome,
}

#[derive(Debug, Error)]
pub enum ReconciliationStateError {
    #[error("target reconciliation state database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Fence(#[from] CheckpointError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotTargetError),
    #[error("topology generation {0} exceeds the target bigint range")]
    GenerationOutOfRange(u64),
    #[error("table generation {0} exceeds the target bigint range")]
    TableGenerationOutOfRange(u64),
    #[error("table generation must be greater than zero")]
    InvalidTableGeneration,
    #[error("reconciliation {field} value {value} exceeds the target bigint range")]
    StatisticOutOfRange { field: &'static str, value: u64 },
    #[error("source relation ID and target relation OID must be non-zero")]
    InvalidRelationIdentity,
    #[error("target table identity is invalid: {0}")]
    InvalidTargetIdentity(String),
    #[error("schema fingerprint cannot be empty or contain NUL")]
    InvalidSchemaFingerprint,
    #[error("reconciliation run ID cannot be nil")]
    InvalidRunId,
    #[error("temporary replication slot name is invalid")]
    InvalidTemporarySlotName,
    #[error("source system identifier and timeline must be non-zero")]
    InvalidSourceIdentity,
    #[error("source snapshot LSN must be non-zero")]
    InvalidSourceSnapshotLsn,
    #[error("target checkpoint {checkpoint} passes source snapshot boundary {snapshot}")]
    TargetCheckpointPastSnapshotBoundary { checkpoint: PgLsn, snapshot: PgLsn },
    #[error("failure or supersede reason cannot be empty or contain NUL")]
    InvalidReason,
    #[error("failure or supersede reason is {actual_bytes} bytes; maximum is {max_bytes} bytes")]
    ReasonTooLong {
        actual_bytes: usize,
        max_bytes: usize,
    },
    #[error("reconciliation state was not found")]
    NotFound,
    #[error("reconciliation run differs in immutable field `{field}`")]
    RunIdentityMismatch { field: &'static str },
    #[error("reconciliation run ID {stored} does not match requested run {requested}")]
    RunMismatch { stored: Uuid, requested: Uuid },
    #[error("reconciliation row has fence {stored}, expected active fence {active}")]
    FenceMismatch { stored: i64, active: i64 },
    #[error("reconciliation row fence {stored} is newer than active fence {active}")]
    NewerStoredFence { stored: i64, active: i64 },
    #[error("cannot start a new run while run {run_id} is in state {state}")]
    ActiveRunConflict { run_id: Uuid, state: &'static str },
    #[error("illegal reconciliation transition from {from} to {to}")]
    IllegalTransition {
        from: &'static str,
        to: &'static str,
    },
    #[error("replayed reconciliation completion differs in `{field}`")]
    CompletionMismatch { field: &'static str },
    #[error("reconciliation reload activation request differs in `{field}`")]
    ReloadRequestMismatch { field: &'static str },
    #[error("reconciliation reload table generation overflowed after {0}")]
    ReloadGenerationOverflow(u64),
    #[error("active reconciliation target differs from the run in `{field}`")]
    ReloadTargetMismatch { field: &'static str },
    #[error(
        "reconciliation reload activation disposition {activation:?} is inconsistent with durable state {state}"
    )]
    ReloadActivationStateMismatch {
        state: &'static str,
        activation: SnapshotActivationDisposition,
    },
    #[error("persisted reconciliation state contains invalid {field}: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("reconciliation state write affected {0} rows instead of one")]
    UnexpectedWriteCount(u64),
}

/// Starts or idempotently observes a reconciliation run in its own transaction.
pub async fn begin_reconciliation(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
) -> Result<BeginReconciliationOutcome, ReconciliationStateError> {
    let transaction = client.transaction().await?;
    let outcome = begin_reconciliation_in_transaction(&transaction, run).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Starts a run inside a caller-owned transaction.
///
/// A terminal row may be replaced directly only while its stable table/source identity remains
/// exact. After activation changes the table generation, fingerprint, target name, or relation
/// OID, the caller must explicitly supersede the old terminal run before beginning the new one.
pub async fn begin_reconciliation_in_transaction(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
) -> Result<BeginReconciliationOutcome, ReconciliationStateError> {
    let values = validate_run(run)?;
    lock_pipeline_fence(transaction, run.fence).await?;
    let existing = load_locked(transaction, run.fence, run.source_relation_id).await?;

    if let Some(existing) = existing {
        ensure_not_newer_fence(&existing, run.fence)?;
        if existing.run.run_id == run.run_id {
            validate_same_run(&existing, run)?;
            require_current_fence(&existing, run.fence)?;
            return Ok(BeginReconciliationOutcome::AlreadyExists(existing.state));
        }
        if !existing.state.is_terminal() {
            return Err(ReconciliationStateError::ActiveRunConflict {
                run_id: existing.run.run_id,
                state: existing.state.as_str(),
            });
        }
        if existing.state != ReconciliationState::Superseded {
            validate_stable_identity(&existing, run)?;
        }
        let written = transaction
            .execute(REPLACE_RECONCILIATION_SQL, &values.parameters())
            .await?;
        ensure_one_row(written)?;
        return Ok(BeginReconciliationOutcome::Started);
    }

    let written = transaction
        .execute(INSERT_RECONCILIATION_SQL, &values.parameters())
        .await?;
    ensure_one_row(written)?;
    Ok(BeginReconciliationOutcome::Started)
}

/// Loads one row under the active pipeline fence in a short transaction.
pub async fn load_reconciliation_state(
    client: &mut Client,
    fence: PipelineFence,
    source_relation_id: u32,
) -> Result<Option<StoredReconciliationState>, ReconciliationStateError> {
    validate_key(fence, source_relation_id)?;
    let transaction = client.transaction().await?;
    let state =
        load_reconciliation_state_in_transaction(&transaction, fence, source_relation_id).await?;
    transaction.commit().await?;
    Ok(state)
}

/// Loads one row while holding the caller's pipeline fence lock.
pub async fn load_reconciliation_state_in_transaction(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    source_relation_id: u32,
) -> Result<Option<StoredReconciliationState>, ReconciliationStateError> {
    validate_key(fence, source_relation_id)?;
    lock_pipeline_fence(transaction, fence).await?;
    let state = load_locked(transaction, fence, source_relation_id).await?;
    if let Some(state) = &state {
        ensure_not_newer_fence(state, fence)?;
    }
    Ok(state)
}

/// Lists every nonterminal run that requires explicit startup recovery.
///
/// Interrupted scans and pending reloads are deliberately different variants so a caller cannot
/// overlook orphan shadow cleanup before superseding a process-scoped snapshot run.
pub async fn load_reconciliation_startup_recovery(
    client: &mut Client,
    fence: PipelineFence,
) -> Result<Vec<ReconciliationStartupRecovery>, ReconciliationStateError> {
    validate_key(fence, 1)?;
    let generation = database_generation(fence.topology_generation)?;
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM pg2cb_meta.table_reconciliation_state \
         WHERE pipeline_id = $1 AND topology_generation = $2 \
           AND state IN ('aligning', 'scanning', 'reload_pending') \
         ORDER BY source_relation_id FOR UPDATE"
    );
    let rows = transaction
        .query(&sql, &[&fence.pipeline_id.as_uuid(), &generation])
        .await?;
    let stored = rows
        .iter()
        .map(reconciliation_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    for state in &stored {
        ensure_not_newer_fence(state, fence)?;
    }
    let recovery = stored
        .into_iter()
        .map(startup_recovery_from_stored)
        .collect::<Result<Vec<_>, _>>()?;
    transaction.commit().await?;
    Ok(recovery)
}

/// Marks that apply is aligned with the source snapshot boundary.
///
/// The checkpoint is the last applied transaction's commit LSN, so it may be numerically below
/// the temporary slot's consistent point even after every transaction ending at or before that
/// point has been applied. A checkpoint above the boundary cannot describe the exported snapshot
/// and fails closed.
pub async fn mark_reconciliation_scanning(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    target_checkpoint_lsn: PgLsn,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    let transaction = client.transaction().await?;
    let outcome =
        mark_reconciliation_scanning_in_transaction(&transaction, run, target_checkpoint_lsn)
            .await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Marks scan readiness inside a caller-owned transaction.
pub async fn mark_reconciliation_scanning_in_transaction(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    target_checkpoint_lsn: PgLsn,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    validate_run(run)?;
    validate_checkpoint_alignment(run.source_snapshot_lsn, target_checkpoint_lsn)?;
    lock_pipeline_fence(transaction, run.fence).await?;
    let stored = require_run_locked(transaction, run, true).await?;
    if stored.state == ReconciliationState::Scanning {
        return if stored.target_checkpoint_lsn == Some(target_checkpoint_lsn) {
            Ok(ReconciliationTransitionOutcome::AlreadyComplete)
        } else {
            Err(ReconciliationStateError::CompletionMismatch {
                field: "target_checkpoint_lsn",
            })
        };
    }
    require_transition(stored.state, ReconciliationState::Scanning)?;
    let generation = database_generation(run.fence.topology_generation)?;
    let written = transaction
        .execute(
            MARK_SCANNING_SQL,
            &[
                &run.fence.pipeline_id.as_uuid(),
                &generation,
                &i64::from(run.source_relation_id),
                &run.run_id,
                &run.fence.fencing_token,
                &target_checkpoint_lsn.to_string(),
            ],
        )
        .await?;
    ensure_one_row(written)?;
    Ok(ReconciliationTransitionOutcome::Transitioned)
}

/// Completes a canonical scan in a short transaction.
pub async fn complete_reconciliation_scan(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    completion: &ReconciliationScanCompletion,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    let transaction = client.transaction().await?;
    let outcome =
        complete_reconciliation_scan_in_transaction(&transaction, run, completion).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Completes a canonical scan in the caller's transaction.
///
/// This API can persist only `matched` or `reload_pending`. A reload can become `reloaded` only
/// through [`activate_reconciliation_reload`], which owns the target cutover transaction.
pub async fn complete_reconciliation_scan_in_transaction(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    completion: &ReconciliationScanCompletion,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    validate_run(run)?;
    let prepared = PreparedScanCompletion::new(completion)?;
    lock_pipeline_fence(transaction, run.fence).await?;
    let stored = require_run_locked(transaction, run, true).await?;

    if stored.state == prepared.final_state() {
        prepared.validate_replay(&stored)?;
        return Ok(ReconciliationTransitionOutcome::AlreadyComplete);
    }
    require_transition(stored.state, prepared.final_state())?;

    let generation = database_generation(run.fence.topology_generation)?;
    let pipeline_id = run.fence.pipeline_id.as_uuid();
    let source_relation_id = i64::from(run.source_relation_id);
    let written = match prepared {
        PreparedScanCompletion::Matched {
            source,
            target,
            next_due_at,
        } => {
            transaction
                .execute(
                    COMPLETE_MATCHED_SQL,
                    &[
                        &pipeline_id,
                        &generation,
                        &source_relation_id,
                        &run.run_id,
                        &run.fence.fencing_token,
                        &source.rows,
                        &source.bytes,
                        &&source.digest[..],
                        &target.rows,
                        &target.bytes,
                        &&target.digest[..],
                        &next_due_at,
                    ],
                )
                .await?
        }
        PreparedScanCompletion::ReloadPending { source, target } => {
            transaction
                .execute(
                    COMPLETE_RELOAD_PENDING_SQL,
                    &[
                        &pipeline_id,
                        &generation,
                        &source_relation_id,
                        &run.run_id,
                        &run.fence.fencing_token,
                        &source.rows,
                        &source.bytes,
                        &&source.digest[..],
                        &target.rows,
                        &target.bytes,
                        &&target.digest[..],
                    ],
                )
                .await?
        }
    };
    ensure_one_row(written)?;
    Ok(ReconciliationTransitionOutcome::Transitioned)
}

/// Atomically promotes a one-table reconciliation shadow and marks the run `reloaded`.
///
/// The request must use the run's exact source snapshot provenance and the next table generation.
/// This function deliberately owns and commits the transaction: no caller can separate physical
/// activation from durable reconciliation completion.
pub async fn activate_reconciliation_reload(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    request: &SnapshotActivationRequest,
    next_due_at: SystemTime,
) -> Result<ReconciliationReloadOutcome, ReconciliationStateError> {
    validate_run(run)?;
    validate_reload_request(run, request)?;
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, run.fence).await?;
    let stored = require_run_locked(&transaction, run, true).await?;
    if !matches!(
        stored.state,
        ReconciliationState::ReloadPending | ReconciliationState::Reloaded
    ) {
        require_transition(stored.state, ReconciliationState::Reloaded)?;
        unreachable!("only reload_pending can transition to reloaded")
    }

    let target = validate_reload_target(&transaction, run, request).await?;
    let activation = activate_table_snapshot_group_in_transaction(&transaction, request).await?;
    let reconciliation = match (stored.state, target, activation.disposition) {
        (
            ReconciliationState::ReloadPending,
            ReloadTargetState::Original,
            SnapshotActivationDisposition::Activated,
        )
        | (
            ReconciliationState::ReloadPending,
            ReloadTargetState::RequestedSnapshotActive,
            SnapshotActivationDisposition::AlreadyActive,
        ) => {
            mark_reconciliation_reloaded_in_transaction(&transaction, run, next_due_at).await?;
            ReconciliationTransitionOutcome::Transitioned
        }
        (
            ReconciliationState::Reloaded,
            ReloadTargetState::RequestedSnapshotActive,
            SnapshotActivationDisposition::AlreadyActive,
        ) => ReconciliationTransitionOutcome::AlreadyComplete,
        (state, _, disposition) => {
            return Err(ReconciliationStateError::ReloadActivationStateMismatch {
                state: state.as_str(),
                activation: disposition,
            });
        }
    };
    transaction.commit().await?;
    Ok(ReconciliationReloadOutcome {
        activation,
        reconciliation,
    })
}

/// Fails an active run and schedules its retry.
pub async fn fail_reconciliation(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    reason: &str,
    next_due_at: SystemTime,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    let transaction = client.transaction().await?;
    let outcome =
        fail_reconciliation_in_transaction(&transaction, run, reason, next_due_at).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Fails an active run in a caller-owned transaction.
pub async fn fail_reconciliation_in_transaction(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    reason: &str,
    next_due_at: SystemTime,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    validate_run(run)?;
    validate_reason(reason)?;
    lock_pipeline_fence(transaction, run.fence).await?;
    let stored = require_run_locked(transaction, run, true).await?;
    if stored.state == ReconciliationState::Failed {
        return if stored.failure_reason.as_deref() == Some(reason) {
            Ok(ReconciliationTransitionOutcome::AlreadyComplete)
        } else {
            Err(ReconciliationStateError::CompletionMismatch {
                field: "failure_reason",
            })
        };
    }
    require_transition(stored.state, ReconciliationState::Failed)?;
    let failures = stored.consecutive_failures.checked_add(1).ok_or(
        ReconciliationStateError::StatisticOutOfRange {
            field: "consecutive_failures",
            value: stored.consecutive_failures,
        },
    )?;
    let failures = database_statistic("consecutive_failures", failures)?;
    let generation = database_generation(run.fence.topology_generation)?;
    let written = transaction
        .execute(
            FAIL_RECONCILIATION_SQL,
            &[
                &run.fence.pipeline_id.as_uuid(),
                &generation,
                &i64::from(run.source_relation_id),
                &run.run_id,
                &run.fence.fencing_token,
                &next_due_at,
                &reason,
                &failures,
            ],
        )
        .await?;
    ensure_one_row(written)?;
    Ok(ReconciliationTransitionOutcome::Transitioned)
}

/// Explicitly invalidates the latest run, including one owned by an older lease.
pub async fn supersede_reconciliation(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    reason: &str,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    let transaction = client.transaction().await?;
    let outcome = supersede_reconciliation_in_transaction(&transaction, run, reason).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Supersedes a run in a caller-owned transaction.
///
/// Unlike ordinary transitions, this operation may adopt a row written under an older fencing
/// token. It still validates the full run identity and rejects a row from a newer token.
pub async fn supersede_reconciliation_in_transaction(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    reason: &str,
) -> Result<ReconciliationTransitionOutcome, ReconciliationStateError> {
    validate_run(run)?;
    validate_reason(reason)?;
    lock_pipeline_fence(transaction, run.fence).await?;
    let stored = require_run_locked(transaction, run, false).await?;
    ensure_not_newer_fence(&stored, run.fence)?;
    if stored.state == ReconciliationState::Superseded
        && stored.run.fence.fencing_token == run.fence.fencing_token
    {
        return if stored.failure_reason.as_deref() == Some(reason) {
            Ok(ReconciliationTransitionOutcome::AlreadyComplete)
        } else {
            Err(ReconciliationStateError::CompletionMismatch {
                field: "failure_reason",
            })
        };
    }
    let generation = database_generation(run.fence.topology_generation)?;
    let written = transaction
        .execute(
            SUPERSEDE_RECONCILIATION_SQL,
            &[
                &run.fence.pipeline_id.as_uuid(),
                &generation,
                &i64::from(run.source_relation_id),
                &run.run_id,
                &reason,
                &run.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_row(written)?;
    Ok(ReconciliationTransitionOutcome::Transitioned)
}

struct ValidatedRun<'a> {
    run: &'a ReconciliationRunIdentity,
    pipeline_id: Uuid,
    generation: i64,
    source_relation_id: i64,
    target_relation_oid: i64,
    table_generation: i64,
    source_system_identifier: String,
    source_timeline: i64,
    source_snapshot_lsn: String,
}

impl ValidatedRun<'_> {
    fn parameters(&self) -> [&(dyn tokio_postgres::types::ToSql + Sync); 15] {
        [
            &self.pipeline_id,
            &self.generation,
            &self.source_relation_id,
            &self.run.target.schema,
            &self.run.target.name,
            &self.target_relation_oid,
            &self.table_generation,
            &self.run.schema_fingerprint,
            &self.run.run_id,
            &self.run.source_node_id,
            &self.run.temporary_slot_name,
            &self.source_system_identifier,
            &self.source_timeline,
            &self.source_snapshot_lsn,
            &self.run.fence.fencing_token,
        ]
    }
}

#[derive(Debug)]
struct DatabaseStats<'a> {
    rows: i64,
    bytes: i64,
    digest: &'a [u8; RECONCILIATION_DIGEST_BYTES],
}

enum PreparedScanCompletion<'a> {
    Matched {
        source: DatabaseStats<'a>,
        target: DatabaseStats<'a>,
        next_due_at: SystemTime,
    },
    ReloadPending {
        source: DatabaseStats<'a>,
        target: DatabaseStats<'a>,
    },
}

impl<'a> PreparedScanCompletion<'a> {
    fn new(completion: &'a ReconciliationScanCompletion) -> Result<Self, ReconciliationStateError> {
        match completion {
            ReconciliationScanCompletion::Matched {
                source,
                target,
                next_due_at,
            } => Ok(Self::Matched {
                source: prepare_stats("source", source)?,
                target: prepare_stats("target", target)?,
                next_due_at: *next_due_at,
            }),
            ReconciliationScanCompletion::ReloadPending { source, target } => {
                Ok(Self::ReloadPending {
                    source: prepare_stats("source", source)?,
                    target: prepare_stats("target", target)?,
                })
            }
        }
    }

    const fn final_state(&self) -> ReconciliationState {
        match self {
            Self::Matched { .. } => ReconciliationState::Matched,
            Self::ReloadPending { .. } => ReconciliationState::ReloadPending,
        }
    }

    fn validate_replay(
        &self,
        stored: &StoredReconciliationState,
    ) -> Result<(), ReconciliationStateError> {
        match self {
            Self::Matched { source, target, .. } | Self::ReloadPending { source, target } => {
                validate_replayed_stats("source_stats", stored.source.as_ref(), source)?;
                validate_replayed_stats("target_stats", stored.target.as_ref(), target)
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReloadTargetState {
    Original,
    RequestedSnapshotActive,
}

fn validate_reload_request(
    run: &ReconciliationRunIdentity,
    request: &SnapshotActivationRequest,
) -> Result<(), ReconciliationStateError> {
    let fence_difference = if request.fence.pipeline_id != run.fence.pipeline_id {
        Some("fence.pipeline_id")
    } else if request.fence.topology_generation != run.fence.topology_generation {
        Some("fence.topology_generation")
    } else if request.fence.fencing_token != run.fence.fencing_token {
        Some("fence.fencing_token")
    } else {
        None
    };
    if let Some(field) = fence_difference {
        return Err(ReconciliationStateError::ReloadRequestMismatch { field });
    }

    let [table] = request.tables.as_slice() else {
        return Err(ReconciliationStateError::ReloadRequestMismatch {
            field: "tables.len",
        });
    };
    if table.target != run.target {
        return Err(ReconciliationStateError::ReloadRequestMismatch {
            field: "tables[0].target",
        });
    }
    if table.source_relation_id != run.source_relation_id {
        return Err(ReconciliationStateError::ReloadRequestMismatch {
            field: "tables[0].source_relation_id",
        });
    }
    let next_generation = run.table_generation.checked_add(1).ok_or(
        ReconciliationStateError::ReloadGenerationOverflow(run.table_generation),
    )?;
    if table.table_generation != next_generation {
        return Err(ReconciliationStateError::ReloadRequestMismatch {
            field: "tables[0].table_generation",
        });
    }
    if table.schema_fingerprint != run.schema_fingerprint {
        return Err(ReconciliationStateError::ReloadRequestMismatch {
            field: "tables[0].schema_fingerprint",
        });
    }

    let [checkpoint] = request.initial_checkpoints.as_slice() else {
        return Err(ReconciliationStateError::ReloadRequestMismatch {
            field: "initial_checkpoints.len",
        });
    };
    let checkpoint_difference = if checkpoint.key.pipeline_id != run.fence.pipeline_id {
        Some("initial_checkpoints[0].key.pipeline_id")
    } else if checkpoint.key.topology_generation != run.fence.topology_generation {
        Some("initial_checkpoints[0].key.topology_generation")
    } else if checkpoint.key.node_id != run.source_node_id {
        Some("initial_checkpoints[0].key.node_id")
    } else if checkpoint.system_identifier != run.source_system_identifier {
        Some("initial_checkpoints[0].system_identifier")
    } else if checkpoint.timeline != run.source_timeline {
        Some("initial_checkpoints[0].timeline")
    } else if checkpoint.slot_name != run.temporary_slot_name {
        Some("initial_checkpoints[0].slot_name")
    } else if checkpoint.applied_lsn != run.source_snapshot_lsn {
        Some("initial_checkpoints[0].applied_lsn")
    } else {
        None
    };
    if let Some(field) = checkpoint_difference {
        return Err(ReconciliationStateError::ReloadRequestMismatch { field });
    }
    Ok(())
}

async fn validate_reload_target(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    request: &SnapshotActivationRequest,
) -> Result<ReloadTargetState, ReconciliationStateError> {
    let table = &request.tables[0];
    let RelationState::Managed(record) = load_relation_state(transaction, &run.target).await?
    else {
        return Err(ReconciliationStateError::ReloadTargetMismatch {
            field: "managed_state",
        });
    };
    validate_managed_identity(
        &run.target,
        &record,
        run.fence.pipeline_id,
        run.source_relation_id,
    )?;
    validate_managed_fence(&run.target, &record, run.fence.fencing_token)?;
    if record.state != ManagedTableState::Active {
        return Err(ReconciliationStateError::ReloadTargetMismatch { field: "state" });
    }
    let metadata_oid =
        record
            .relation_oid
            .ok_or(ReconciliationStateError::ReloadTargetMismatch {
                field: "target_relation_oid",
            })?;
    let physical_oid = relation_oid(transaction, &run.target).await?.ok_or(
        ReconciliationStateError::ReloadTargetMismatch {
            field: "physical_target_relation_oid",
        },
    )?;
    if metadata_oid != physical_oid {
        return Err(ReconciliationStateError::ReloadTargetMismatch {
            field: "physical_target_relation_oid",
        });
    }

    if record.table_generation == run.table_generation {
        if record.schema_fingerprint != run.schema_fingerprint {
            return Err(ReconciliationStateError::ReloadTargetMismatch {
                field: "schema_fingerprint",
            });
        }
        if metadata_oid != i64::from(run.target_relation_oid) {
            return Err(ReconciliationStateError::ReloadTargetMismatch {
                field: "target_relation_oid",
            });
        }
        return Ok(ReloadTargetState::Original);
    }

    if record.table_generation != table.table_generation {
        return Err(ReconciliationStateError::ReloadTargetMismatch {
            field: "table_generation",
        });
    }
    if record.schema_fingerprint != table.schema_fingerprint {
        return Err(ReconciliationStateError::ReloadTargetMismatch {
            field: "schema_fingerprint",
        });
    }
    if record.snapshot_group_id != Some(request.snapshot_group_id) {
        return Err(ReconciliationStateError::ReloadTargetMismatch {
            field: "snapshot_group_id",
        });
    }
    Ok(ReloadTargetState::RequestedSnapshotActive)
}

async fn mark_reconciliation_reloaded_in_transaction(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    next_due_at: SystemTime,
) -> Result<(), ReconciliationStateError> {
    let generation = database_generation(run.fence.topology_generation)?;
    let written = transaction
        .execute(
            COMPLETE_RELOADED_SQL,
            &[
                &run.fence.pipeline_id.as_uuid(),
                &generation,
                &i64::from(run.source_relation_id),
                &run.run_id,
                &run.fence.fencing_token,
                &next_due_at,
            ],
        )
        .await?;
    ensure_one_row(written)
}

fn startup_recovery_from_stored(
    stored: StoredReconciliationState,
) -> Result<ReconciliationStartupRecovery, ReconciliationStateError> {
    match stored.state {
        ReconciliationState::Aligning | ReconciliationState::Scanning => Ok(
            ReconciliationStartupRecovery::RestartInterruptedScan(stored),
        ),
        ReconciliationState::ReloadPending => {
            Ok(ReconciliationStartupRecovery::RestartPendingReload(stored))
        }
        state => Err(ReconciliationStateError::InvalidPersistedValue {
            field: "startup_recovery_state",
            value: state.as_str().to_owned(),
        }),
    }
}

fn validate_run(
    run: &ReconciliationRunIdentity,
) -> Result<ValidatedRun<'_>, ReconciliationStateError> {
    let generation = database_generation(run.fence.topology_generation)?;
    if run.fence.fencing_token <= 0 {
        return Err(CheckpointError::InvalidFencingToken.into());
    }
    if run.source_relation_id == 0 || run.target_relation_oid == 0 {
        return Err(ReconciliationStateError::InvalidRelationIdentity);
    }
    QualifiedName::new(run.target.schema.clone(), run.target.name.clone())
        .map_err(|error| ReconciliationStateError::InvalidTargetIdentity(error.to_string()))?;
    let table_generation = i64::try_from(run.table_generation)
        .map_err(|_| ReconciliationStateError::TableGenerationOutOfRange(run.table_generation))?;
    if table_generation == 0 {
        return Err(ReconciliationStateError::InvalidTableGeneration);
    }
    if run.schema_fingerprint.is_empty() || run.schema_fingerprint.contains('\0') {
        return Err(ReconciliationStateError::InvalidSchemaFingerprint);
    }
    if run.run_id.is_nil() {
        return Err(ReconciliationStateError::InvalidRunId);
    }
    if !is_generated_slot_name(&run.temporary_slot_name) {
        return Err(ReconciliationStateError::InvalidTemporarySlotName);
    }
    if run.source_system_identifier == 0 || run.source_timeline == 0 {
        return Err(ReconciliationStateError::InvalidSourceIdentity);
    }
    if run.source_snapshot_lsn == PgLsn::ZERO {
        return Err(ReconciliationStateError::InvalidSourceSnapshotLsn);
    }
    Ok(ValidatedRun {
        run,
        pipeline_id: run.fence.pipeline_id.as_uuid(),
        generation,
        source_relation_id: i64::from(run.source_relation_id),
        target_relation_oid: i64::from(run.target_relation_oid),
        table_generation,
        source_system_identifier: run.source_system_identifier.to_string(),
        source_timeline: i64::from(run.source_timeline),
        source_snapshot_lsn: run.source_snapshot_lsn.to_string(),
    })
}

fn validate_key(
    fence: PipelineFence,
    source_relation_id: u32,
) -> Result<(), ReconciliationStateError> {
    database_generation(fence.topology_generation)?;
    if fence.fencing_token <= 0 {
        return Err(CheckpointError::InvalidFencingToken.into());
    }
    if source_relation_id == 0 {
        return Err(ReconciliationStateError::InvalidRelationIdentity);
    }
    Ok(())
}

fn validate_reason(reason: &str) -> Result<(), ReconciliationStateError> {
    if reason.is_empty() || reason.contains('\0') {
        return Err(ReconciliationStateError::InvalidReason);
    }
    if reason.len() > RECONCILIATION_REASON_MAX_BYTES {
        return Err(ReconciliationStateError::ReasonTooLong {
            actual_bytes: reason.len(),
            max_bytes: RECONCILIATION_REASON_MAX_BYTES,
        });
    }
    Ok(())
}

fn is_generated_slot_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(first) if first.is_ascii_lowercase())
        && value.len() <= POSTGRES_IDENTIFIER_MAX_BYTES
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn validate_checkpoint_alignment(
    source_snapshot_lsn: PgLsn,
    target_checkpoint_lsn: PgLsn,
) -> Result<(), ReconciliationStateError> {
    if target_checkpoint_lsn > source_snapshot_lsn {
        Err(
            ReconciliationStateError::TargetCheckpointPastSnapshotBoundary {
                checkpoint: target_checkpoint_lsn,
                snapshot: source_snapshot_lsn,
            },
        )
    } else {
        Ok(())
    }
}

fn prepare_stats<'a>(
    side: &'static str,
    stats: &'a ReconciliationStats,
) -> Result<DatabaseStats<'a>, ReconciliationStateError> {
    Ok(DatabaseStats {
        rows: database_statistic(
            if side == "source" {
                "source_rows"
            } else {
                "target_rows"
            },
            stats.rows,
        )?,
        bytes: database_statistic(
            if side == "source" {
                "source_bytes"
            } else {
                "target_bytes"
            },
            stats.bytes,
        )?,
        digest: &stats.digest,
    })
}

fn database_generation(generation: u64) -> Result<i64, ReconciliationStateError> {
    i64::try_from(generation)
        .map_err(|_| ReconciliationStateError::GenerationOutOfRange(generation))
}

fn database_statistic(field: &'static str, value: u64) -> Result<i64, ReconciliationStateError> {
    i64::try_from(value).map_err(|_| ReconciliationStateError::StatisticOutOfRange { field, value })
}

async fn load_locked(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    source_relation_id: u32,
) -> Result<Option<StoredReconciliationState>, ReconciliationStateError> {
    let generation = database_generation(fence.topology_generation)?;
    let sql = format!(
        "SELECT {SELECT_COLUMNS} FROM pg2cb_meta.table_reconciliation_state \
         WHERE pipeline_id = $1 AND topology_generation = $2 AND source_relation_id = $3 \
         FOR UPDATE"
    );
    transaction
        .query_opt(
            &sql,
            &[
                &fence.pipeline_id.as_uuid(),
                &generation,
                &i64::from(source_relation_id),
            ],
        )
        .await?
        .map(|row| reconciliation_from_row(&row))
        .transpose()
}

async fn require_run_locked(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    require_same_fence: bool,
) -> Result<StoredReconciliationState, ReconciliationStateError> {
    let stored = load_locked(transaction, run.fence, run.source_relation_id)
        .await?
        .ok_or(ReconciliationStateError::NotFound)?;
    validate_same_run(&stored, run)?;
    if require_same_fence {
        require_current_fence(&stored, run.fence)?;
    }
    Ok(stored)
}

fn validate_same_run(
    stored: &StoredReconciliationState,
    run: &ReconciliationRunIdentity,
) -> Result<(), ReconciliationStateError> {
    if stored.run.run_id != run.run_id {
        return Err(ReconciliationStateError::RunMismatch {
            stored: stored.run.run_id,
            requested: run.run_id,
        });
    }
    let difference = if stored.run.fence.pipeline_id != run.fence.pipeline_id {
        Some("pipeline_id")
    } else if stored.run.fence.topology_generation != run.fence.topology_generation {
        Some("topology_generation")
    } else if stored.run.source_relation_id != run.source_relation_id {
        Some("source_relation_id")
    } else if stored.run.target != run.target {
        Some("target")
    } else if stored.run.target_relation_oid != run.target_relation_oid {
        Some("target_relation_oid")
    } else if stored.run.table_generation != run.table_generation {
        Some("table_generation")
    } else if stored.run.schema_fingerprint != run.schema_fingerprint {
        Some("schema_fingerprint")
    } else if stored.run.source_node_id != run.source_node_id {
        Some("source_node_id")
    } else if stored.run.temporary_slot_name != run.temporary_slot_name {
        Some("temporary_slot_name")
    } else if stored.run.source_system_identifier != run.source_system_identifier {
        Some("source_system_identifier")
    } else if stored.run.source_timeline != run.source_timeline {
        Some("source_timeline")
    } else if stored.run.source_snapshot_lsn != run.source_snapshot_lsn {
        Some("source_snapshot_lsn")
    } else {
        None
    };
    match difference {
        Some(field) => Err(ReconciliationStateError::RunIdentityMismatch { field }),
        None => Ok(()),
    }
}

fn validate_stable_identity(
    stored: &StoredReconciliationState,
    run: &ReconciliationRunIdentity,
) -> Result<(), ReconciliationStateError> {
    let difference = if stored.run.target != run.target {
        Some("target")
    } else if stored.run.target_relation_oid != run.target_relation_oid {
        Some("target_relation_oid")
    } else if stored.run.table_generation != run.table_generation {
        Some("table_generation")
    } else if stored.run.schema_fingerprint != run.schema_fingerprint {
        Some("schema_fingerprint")
    } else if stored.run.source_node_id != run.source_node_id {
        Some("source_node_id")
    } else if stored.run.source_system_identifier != run.source_system_identifier {
        Some("source_system_identifier")
    } else if stored.run.source_timeline != run.source_timeline {
        Some("source_timeline")
    } else {
        None
    };
    match difference {
        Some(field) => Err(ReconciliationStateError::RunIdentityMismatch { field }),
        None => Ok(()),
    }
}

fn require_current_fence(
    stored: &StoredReconciliationState,
    fence: PipelineFence,
) -> Result<(), ReconciliationStateError> {
    let stored_token = stored.run.fence.fencing_token;
    if stored_token == fence.fencing_token {
        Ok(())
    } else {
        Err(ReconciliationStateError::FenceMismatch {
            stored: stored_token,
            active: fence.fencing_token,
        })
    }
}

fn ensure_not_newer_fence(
    stored: &StoredReconciliationState,
    fence: PipelineFence,
) -> Result<(), ReconciliationStateError> {
    let stored_token = stored.run.fence.fencing_token;
    if stored_token <= fence.fencing_token {
        Ok(())
    } else {
        Err(ReconciliationStateError::NewerStoredFence {
            stored: stored_token,
            active: fence.fencing_token,
        })
    }
}

fn require_transition(
    from: ReconciliationState,
    to: ReconciliationState,
) -> Result<(), ReconciliationStateError> {
    let allowed = matches!(
        (from, to),
        (ReconciliationState::Aligning, ReconciliationState::Scanning)
            | (ReconciliationState::Scanning, ReconciliationState::Matched)
            | (
                ReconciliationState::Scanning,
                ReconciliationState::ReloadPending
            )
            | (
                ReconciliationState::ReloadPending,
                ReconciliationState::Reloaded
            )
            | (ReconciliationState::Aligning, ReconciliationState::Failed)
            | (ReconciliationState::Scanning, ReconciliationState::Failed)
            | (
                ReconciliationState::ReloadPending,
                ReconciliationState::Failed
            )
    );
    if allowed {
        Ok(())
    } else {
        Err(ReconciliationStateError::IllegalTransition {
            from: from.as_str(),
            to: to.as_str(),
        })
    }
}

fn validate_replayed_stats(
    field: &'static str,
    stored: Option<&ReconciliationStats>,
    proposed: &DatabaseStats<'_>,
) -> Result<(), ReconciliationStateError> {
    let Some(stored) = stored else {
        return Err(ReconciliationStateError::CompletionMismatch { field });
    };
    if stored.rows == proposed.rows as u64
        && stored.bytes == proposed.bytes as u64
        && stored.digest == *proposed.digest
    {
        Ok(())
    } else {
        Err(ReconciliationStateError::CompletionMismatch { field })
    }
}

fn reconciliation_from_row(
    row: &Row,
) -> Result<StoredReconciliationState, ReconciliationStateError> {
    let pipeline_id = PipelineId::from_uuid(row.try_get("pipeline_id")?);
    let topology_generation = persisted_u64(row, "topology_generation")?;
    let source_relation_id = persisted_u32(row, "source_relation_id", false)?;
    let target_relation_oid = persisted_u32(row, "target_relation_oid", false)?;
    let table_generation = persisted_u64(row, "table_generation")?;
    let target_schema: String = row.try_get("target_schema")?;
    let target_table: String = row.try_get("target_table")?;
    let target = QualifiedName::new(target_schema, target_table).map_err(|error| {
        ReconciliationStateError::InvalidPersistedValue {
            field: "target",
            value: error.to_string(),
        }
    })?;
    let run_id: Uuid = row.try_get("run_id")?;
    if run_id.is_nil() {
        return Err(ReconciliationStateError::InvalidPersistedValue {
            field: "run_id",
            value: run_id.to_string(),
        });
    }
    let source_system_identifier = persisted_parse(row, "source_system_identifier")?;
    let source_timeline = persisted_u32(row, "source_timeline", false)?;
    let source_snapshot_lsn = persisted_lsn(row, "source_snapshot_lsn")?;
    let target_checkpoint_lsn = persisted_optional_lsn(row, "target_checkpoint_lsn")?;
    let source = persisted_stats(row, "source_rows", "source_bytes", "source_digest")?;
    let target_stats = persisted_stats(row, "target_rows", "target_bytes", "target_digest")?;
    if source.is_some() != target_stats.is_some() {
        return Err(ReconciliationStateError::InvalidPersistedValue {
            field: "source_target_statistics",
            value: "only one side is present".to_owned(),
        });
    }
    let fencing_token: i64 = row.try_get("fencing_token")?;
    if fencing_token <= 0 {
        return Err(ReconciliationStateError::InvalidPersistedValue {
            field: "fencing_token",
            value: fencing_token.to_string(),
        });
    }
    Ok(StoredReconciliationState {
        run: ReconciliationRunIdentity {
            fence: PipelineFence {
                pipeline_id,
                topology_generation,
                fencing_token,
            },
            source_relation_id,
            target,
            target_relation_oid,
            table_generation,
            schema_fingerprint: row.try_get("schema_fingerprint")?,
            run_id,
            source_node_id: row.try_get("source_node_id")?,
            temporary_slot_name: row.try_get("temporary_slot_name")?,
            source_system_identifier,
            source_timeline,
            source_snapshot_lsn,
        },
        state: ReconciliationState::from_str(row.try_get("state")?)?,
        target_checkpoint_lsn,
        source,
        target: target_stats,
        started_at: row.try_get("started_at")?,
        completed_at: row.try_get("completed_at")?,
        last_consistent_at: row.try_get("last_consistent_at")?,
        last_mismatch_at: row.try_get("last_mismatch_at")?,
        next_due_at: row.try_get("next_due_at")?,
        failure_reason: row.try_get("failure_reason")?,
        consecutive_failures: persisted_u64(row, "consecutive_failures")?,
    })
}

fn persisted_stats(
    row: &Row,
    rows_field: &'static str,
    bytes_field: &'static str,
    digest_field: &'static str,
) -> Result<Option<ReconciliationStats>, ReconciliationStateError> {
    let rows: Option<i64> = row.try_get(rows_field)?;
    let bytes: Option<i64> = row.try_get(bytes_field)?;
    let digest: Option<Vec<u8>> = row.try_get(digest_field)?;
    match (rows, bytes, digest) {
        (None, None, None) => Ok(None),
        (Some(rows), Some(bytes), Some(digest)) => {
            let rows = u64::try_from(rows).map_err(|_| invalid_persisted(rows_field, rows))?;
            let bytes = u64::try_from(bytes).map_err(|_| invalid_persisted(bytes_field, bytes))?;
            let digest = digest.try_into().map_err(|value: Vec<u8>| {
                ReconciliationStateError::InvalidPersistedValue {
                    field: digest_field,
                    value: format!("{} bytes", value.len()),
                }
            })?;
            Ok(Some(ReconciliationStats {
                rows,
                bytes,
                digest,
            }))
        }
        _ => Err(ReconciliationStateError::InvalidPersistedValue {
            field: rows_field,
            value: "partially populated statistics".to_owned(),
        }),
    }
}

fn persisted_u64(row: &Row, field: &'static str) -> Result<u64, ReconciliationStateError> {
    let value: i64 = row.try_get(field)?;
    u64::try_from(value).map_err(|_| invalid_persisted(field, value))
}

fn persisted_u32(
    row: &Row,
    field: &'static str,
    allow_zero: bool,
) -> Result<u32, ReconciliationStateError> {
    let value: i64 = row.try_get(field)?;
    let converted = u32::try_from(value).map_err(|_| invalid_persisted(field, value))?;
    if !allow_zero && converted == 0 {
        return Err(invalid_persisted(field, value));
    }
    Ok(converted)
}

fn persisted_parse<T>(row: &Row, field: &'static str) -> Result<T, ReconciliationStateError>
where
    T: std::str::FromStr,
{
    let value: String = row.try_get(field)?;
    value
        .parse()
        .map_err(|_| ReconciliationStateError::InvalidPersistedValue { field, value })
}

fn persisted_lsn(row: &Row, field: &'static str) -> Result<PgLsn, ReconciliationStateError> {
    let value: String = row.try_get(field)?;
    PgLsn::from_str(&value)
        .map_err(|_| ReconciliationStateError::InvalidPersistedValue { field, value })
}

fn persisted_optional_lsn(
    row: &Row,
    field: &'static str,
) -> Result<Option<PgLsn>, ReconciliationStateError> {
    let value: Option<String> = row.try_get(field)?;
    value
        .map(|value| {
            PgLsn::from_str(&value)
                .map_err(|_| ReconciliationStateError::InvalidPersistedValue { field, value })
        })
        .transpose()
}

fn invalid_persisted(field: &'static str, value: impl ToString) -> ReconciliationStateError {
    ReconciliationStateError::InvalidPersistedValue {
        field,
        value: value.to_string(),
    }
}

fn ensure_one_row(written: u64) -> Result<(), ReconciliationStateError> {
    if written == 1 {
        Ok(())
    } else {
        Err(ReconciliationStateError::UnexpectedWriteCount(written))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, UNIX_EPOCH};

    use crate::{
        checkpoint::{CheckpointKey, NodeCheckpoint},
        snapshot::SnapshotActivationTable,
    };

    use super::*;

    fn run() -> ReconciliationRunIdentity {
        ReconciliationRunIdentity {
            fence: PipelineFence {
                pipeline_id: PipelineId::from_uuid(Uuid::from_u128(1)),
                topology_generation: 3,
                fencing_token: 7,
            },
            source_relation_id: 42,
            target: QualifiedName::new("analytics", "orders").unwrap(),
            target_relation_oid: 17_001,
            table_generation: 9,
            schema_fingerprint: "sha256:orders-v9".to_owned(),
            run_id: Uuid::from_u128(2),
            source_node_id: 0,
            temporary_slot_name: "pg2cb_reconcile_orders_2".to_owned(),
            source_system_identifier: 7_123_456_789,
            source_timeline: 1,
            source_snapshot_lsn: PgLsn::new(0x100),
        }
    }

    fn stats(byte: u8) -> ReconciliationStats {
        ReconciliationStats {
            rows: 12,
            bytes: 240,
            digest: [byte; RECONCILIATION_DIGEST_BYTES],
        }
    }

    fn stored(state: ReconciliationState) -> StoredReconciliationState {
        StoredReconciliationState {
            run: run(),
            state,
            target_checkpoint_lsn: None,
            source: None,
            target: None,
            started_at: UNIX_EPOCH,
            completed_at: None,
            last_consistent_at: None,
            last_mismatch_at: None,
            next_due_at: None,
            failure_reason: None,
            consecutive_failures: 0,
        }
    }

    fn reload_request() -> SnapshotActivationRequest {
        let run = run();
        SnapshotActivationRequest {
            fence: run.fence,
            snapshot_group_id: Uuid::from_u128(3),
            tables: vec![SnapshotActivationTable {
                target: run.target.clone(),
                shadow: QualifiedName::new("analytics", "orders_reload").unwrap(),
                source_relation_id: run.source_relation_id,
                table_generation: run.table_generation + 1,
                schema_fingerprint: run.schema_fingerprint.clone(),
            }],
            initial_checkpoints: vec![NodeCheckpoint {
                key: CheckpointKey {
                    pipeline_id: run.fence.pipeline_id,
                    topology_generation: run.fence.topology_generation,
                    node_id: run.source_node_id,
                },
                system_identifier: run.source_system_identifier,
                timeline: run.source_timeline,
                slot_name: run.temporary_slot_name.clone(),
                applied_lsn: run.source_snapshot_lsn,
            }],
        }
    }

    fn reload_mismatch_field(request: &SnapshotActivationRequest) -> &'static str {
        match validate_reload_request(&run(), request) {
            Err(ReconciliationStateError::ReloadRequestMismatch { field }) => field,
            result => panic!("expected reload request mismatch, got {result:?}"),
        }
    }

    #[test]
    fn restart_only_interrupts_source_session_states() {
        assert!(ReconciliationState::Aligning.is_interrupted());
        assert!(ReconciliationState::Scanning.is_interrupted());
        for state in [
            ReconciliationState::Matched,
            ReconciliationState::ReloadPending,
            ReconciliationState::Reloaded,
            ReconciliationState::Failed,
            ReconciliationState::Superseded,
        ] {
            assert!(!state.is_interrupted(), "{}", state.as_str());
        }
    }

    #[test]
    fn startup_recovery_distinguishes_interrupted_scans_from_pending_reload() {
        for state in [ReconciliationState::Aligning, ReconciliationState::Scanning] {
            let recovery = startup_recovery_from_stored(stored(state)).unwrap();
            assert!(matches!(
                recovery,
                ReconciliationStartupRecovery::RestartInterruptedScan(ref value)
                    if value.state == state
            ));
        }
        let recovery =
            startup_recovery_from_stored(stored(ReconciliationState::ReloadPending)).unwrap();
        assert!(matches!(
            recovery,
            ReconciliationStartupRecovery::RestartPendingReload(ref value)
                if value.state == ReconciliationState::ReloadPending
        ));
        assert!(matches!(
            startup_recovery_from_stored(stored(ReconciliationState::Matched)),
            Err(ReconciliationStateError::InvalidPersistedValue {
                field: "startup_recovery_state",
                ..
            })
        ));
    }

    #[test]
    fn state_machine_rejects_skipping_alignment_or_reload() {
        assert!(
            require_transition(ReconciliationState::Aligning, ReconciliationState::Scanning)
                .is_ok()
        );
        assert!(
            require_transition(ReconciliationState::Scanning, ReconciliationState::Matched).is_ok()
        );
        assert!(
            require_transition(
                ReconciliationState::Scanning,
                ReconciliationState::ReloadPending
            )
            .is_ok()
        );
        assert!(
            require_transition(
                ReconciliationState::ReloadPending,
                ReconciliationState::Reloaded
            )
            .is_ok()
        );
        assert!(matches!(
            require_transition(ReconciliationState::Aligning, ReconciliationState::Matched),
            Err(ReconciliationStateError::IllegalTransition { .. })
        ));
        assert!(matches!(
            require_transition(ReconciliationState::Scanning, ReconciliationState::Reloaded),
            Err(ReconciliationStateError::IllegalTransition { .. })
        ));
    }

    #[test]
    fn validates_run_identity_before_database_io() {
        let mut invalid = run();
        invalid.run_id = Uuid::nil();
        assert!(matches!(
            validate_run(&invalid),
            Err(ReconciliationStateError::InvalidRunId)
        ));

        invalid = run();
        invalid.source_snapshot_lsn = PgLsn::ZERO;
        assert!(matches!(
            validate_run(&invalid),
            Err(ReconciliationStateError::InvalidSourceSnapshotLsn)
        ));

        invalid = run();
        invalid.temporary_slot_name = "x".repeat(POSTGRES_IDENTIFIER_MAX_BYTES + 1);
        assert!(matches!(
            validate_run(&invalid),
            Err(ReconciliationStateError::InvalidTemporarySlotName)
        ));

        invalid = run();
        invalid.table_generation = 0;
        assert!(matches!(
            validate_run(&invalid),
            Err(ReconciliationStateError::InvalidTableGeneration)
        ));

        for invalid_slot in ["_slot", "1slot", "Upper", "has-dash", "with.dot"] {
            invalid = run();
            invalid.temporary_slot_name = invalid_slot.to_owned();
            assert!(matches!(
                validate_run(&invalid),
                Err(ReconciliationStateError::InvalidTemporarySlotName)
            ));
        }
        invalid = run();
        invalid.temporary_slot_name = format!(
            "slot{}",
            char::from_u32(0x754c).expect("valid Unicode scalar")
        );
        assert!(matches!(
            validate_run(&invalid),
            Err(ReconciliationStateError::InvalidTemporarySlotName)
        ));
    }

    #[test]
    fn completion_prepares_fixed_width_aggregate_digests() {
        let completion = ReconciliationScanCompletion::Matched {
            source: stats(1),
            target: stats(2),
            next_due_at: UNIX_EPOCH + Duration::from_secs(100),
        };
        let prepared = PreparedScanCompletion::new(&completion).unwrap();
        assert_eq!(prepared.final_state(), ReconciliationState::Matched);

        let oversized = ReconciliationScanCompletion::ReloadPending {
            source: ReconciliationStats {
                rows: u64::MAX,
                bytes: 0,
                digest: [0; RECONCILIATION_DIGEST_BYTES],
            },
            target: stats(2),
        };
        assert!(matches!(
            PreparedScanCompletion::new(&oversized),
            Err(ReconciliationStateError::StatisticOutOfRange {
                field: "source_rows",
                ..
            })
        ));
    }

    #[test]
    fn reload_request_is_bound_to_the_exact_run_and_snapshot_provenance() {
        assert!(validate_reload_request(&run(), &reload_request()).is_ok());

        let mut request = reload_request();
        request.fence.pipeline_id = PipelineId::new();
        assert_eq!(reload_mismatch_field(&request), "fence.pipeline_id");
        request = reload_request();
        request.fence.topology_generation += 1;
        assert_eq!(reload_mismatch_field(&request), "fence.topology_generation");
        request = reload_request();
        request.fence.fencing_token += 1;
        assert_eq!(reload_mismatch_field(&request), "fence.fencing_token");

        request = reload_request();
        request.tables.clear();
        assert_eq!(reload_mismatch_field(&request), "tables.len");
        request = reload_request();
        request.tables.push(request.tables[0].clone());
        assert_eq!(reload_mismatch_field(&request), "tables.len");
        request = reload_request();
        request.tables[0].target = QualifiedName::new("analytics", "other").unwrap();
        assert_eq!(reload_mismatch_field(&request), "tables[0].target");
        request = reload_request();
        request.tables[0].source_relation_id += 1;
        assert_eq!(
            reload_mismatch_field(&request),
            "tables[0].source_relation_id"
        );
        request = reload_request();
        request.tables[0].table_generation += 1;
        assert_eq!(
            reload_mismatch_field(&request),
            "tables[0].table_generation"
        );
        request = reload_request();
        request.tables[0].schema_fingerprint.push_str("-drift");
        assert_eq!(
            reload_mismatch_field(&request),
            "tables[0].schema_fingerprint"
        );

        request = reload_request();
        request.initial_checkpoints.clear();
        assert_eq!(reload_mismatch_field(&request), "initial_checkpoints.len");
        request = reload_request();
        request
            .initial_checkpoints
            .push(request.initial_checkpoints[0].clone());
        assert_eq!(reload_mismatch_field(&request), "initial_checkpoints.len");
        request = reload_request();
        request.initial_checkpoints[0].key.pipeline_id = PipelineId::new();
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].key.pipeline_id"
        );
        request = reload_request();
        request.initial_checkpoints[0].key.topology_generation += 1;
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].key.topology_generation"
        );
        request = reload_request();
        request.initial_checkpoints[0].key.node_id += 1;
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].key.node_id"
        );
        request = reload_request();
        request.initial_checkpoints[0].system_identifier += 1;
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].system_identifier"
        );
        request = reload_request();
        request.initial_checkpoints[0].timeline += 1;
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].timeline"
        );
        request = reload_request();
        request.initial_checkpoints[0].slot_name.push_str("_other");
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].slot_name"
        );
        request = reload_request();
        request.initial_checkpoints[0].applied_lsn = PgLsn::new(0x101);
        assert_eq!(
            reload_mismatch_field(&request),
            "initial_checkpoints[0].applied_lsn"
        );

        let mut overflow = run();
        overflow.table_generation = u64::MAX;
        assert!(matches!(
            validate_reload_request(&overflow, &reload_request()),
            Err(ReconciliationStateError::ReloadGenerationOverflow(u64::MAX))
        ));
    }

    #[test]
    fn target_checkpoint_must_not_pass_the_exported_snapshot_boundary() {
        let boundary = PgLsn::new(0x100);
        assert!(validate_checkpoint_alignment(boundary, PgLsn::new(0xff)).is_ok());
        assert!(validate_checkpoint_alignment(boundary, boundary).is_ok());
        assert!(matches!(
            validate_checkpoint_alignment(boundary, PgLsn::new(0x101)),
            Err(
                ReconciliationStateError::TargetCheckpointPastSnapshotBoundary {
                    checkpoint,
                    snapshot
                }
            ) if checkpoint == PgLsn::new(0x101) && snapshot == boundary
        ));
    }

    #[test]
    fn failure_reason_has_a_fixed_utf8_byte_budget() {
        assert!(validate_reason(&"x".repeat(RECONCILIATION_REASON_MAX_BYTES)).is_ok());
        assert!(matches!(
            validate_reason(&"x".repeat(RECONCILIATION_REASON_MAX_BYTES + 1)),
            Err(ReconciliationStateError::ReasonTooLong {
                actual_bytes: 4097,
                max_bytes: 4096
            })
        ));
        let multibyte = char::from_u32(0x754c)
            .expect("valid Unicode scalar")
            .to_string()
            .repeat(1366);
        assert_eq!(multibyte.len(), 4098);
        assert!(matches!(
            validate_reason(&multibyte),
            Err(ReconciliationStateError::ReasonTooLong {
                actual_bytes: 4098,
                max_bytes: 4096
            })
        ));
    }

    #[test]
    fn sql_writes_are_run_and_fence_guarded() {
        for sql in [
            MARK_SCANNING_SQL,
            COMPLETE_MATCHED_SQL,
            COMPLETE_RELOAD_PENDING_SQL,
            COMPLETE_RELOADED_SQL,
            FAIL_RECONCILIATION_SQL,
        ] {
            assert!(sql.contains("run_id = $4"));
            assert!(sql.contains("fencing_token = $5"));
        }
        assert!(SUPERSEDE_RECONCILIATION_SQL.contains("run_id = $4"));
        assert!(!INSERT_RECONCILIATION_SQL.contains("source_rows"));
        assert!(!MARK_SCANNING_SQL.contains("source_rows"));
    }
}
