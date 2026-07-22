//! Durable per-table execution state for schema events.

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

use crate::checkpoint::{CheckpointError, PipelineFence, lock_pipeline_fence};

const LOAD_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, source_relation_id, action, plan, barrier_lsn::text AS barrier_lsn,
       active_table_generation, pending_table_generation, snapshot_group_id, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.table_schema_transitions
 WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn
   AND source_xid = $3 AND source_relation_id = $4
"#;

const LOCK_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, source_relation_id, action, plan, barrier_lsn::text AS barrier_lsn,
       active_table_generation, pending_table_generation, snapshot_group_id, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.table_schema_transitions
 WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn
   AND source_xid = $3 AND source_relation_id = $4
 FOR UPDATE
"#;

const LIST_UNFINISHED_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, source_relation_id, action, plan, barrier_lsn::text AS barrier_lsn,
       active_table_generation, pending_table_generation, snapshot_group_id, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.table_schema_transitions
 WHERE pipeline_id = $1 AND topology_generation = $2 AND state <> 'completed'
 ORDER BY source_lsn, source_xid, source_relation_id
 FOR UPDATE
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableTransitionAction {
    Noop,
    Online,
    Reload,
    Drop,
    Add,
}

impl TableTransitionAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Noop => "noop",
            Self::Online => "online",
            Self::Reload => "reload",
            Self::Drop => "drop",
            Self::Add => "add",
        }
    }

    fn parse(value: &str) -> Result<Self, TableTransitionError> {
        match value {
            "noop" => Ok(Self::Noop),
            "online" => Ok(Self::Online),
            "reload" => Ok(Self::Reload),
            "drop" => Ok(Self::Drop),
            "add" => Ok(Self::Add),
            other => Err(TableTransitionError::InvalidPersistedValue {
                field: "action",
                value: other.to_owned(),
            }),
        }
    }

    #[must_use]
    pub fn can_transition(self, from: TableTransitionState, to: TableTransitionState) -> bool {
        if to == TableTransitionState::Blocked
            && !matches!(
                from,
                TableTransitionState::Blocked | TableTransitionState::Completed
            )
        {
            return true;
        }
        match self {
            Self::Noop => matches!(
                (from, to),
                (
                    TableTransitionState::Pending,
                    TableTransitionState::Completed
                ) | (
                    TableTransitionState::Blocked,
                    TableTransitionState::Completed
                )
            ),
            Self::Online => matches!(
                (from, to),
                (
                    TableTransitionState::Pending,
                    TableTransitionState::Applying
                ) | (
                    TableTransitionState::Blocked,
                    TableTransitionState::Applying
                ) | (
                    TableTransitionState::Applying,
                    TableTransitionState::Completed
                )
            ),
            Self::Reload | Self::Add => matches!(
                (from, to),
                (
                    TableTransitionState::Pending,
                    TableTransitionState::Snapshotting
                ) | (
                    TableTransitionState::Blocked,
                    TableTransitionState::Snapshotting
                ) | (
                    TableTransitionState::Snapshotting,
                    TableTransitionState::CatchingUp
                ) | (
                    TableTransitionState::CatchingUp,
                    TableTransitionState::CutoverPending
                ) | (
                    TableTransitionState::CutoverPending,
                    TableTransitionState::Completed
                )
            ),
            Self::Drop => matches!(
                (from, to),
                (
                    TableTransitionState::Pending | TableTransitionState::Blocked,
                    TableTransitionState::CutoverPending
                ) | (
                    TableTransitionState::CutoverPending,
                    TableTransitionState::Completed
                )
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableTransitionState {
    Pending,
    Applying,
    Snapshotting,
    CatchingUp,
    CutoverPending,
    Blocked,
    Completed,
}

impl TableTransitionState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Applying => "applying",
            Self::Snapshotting => "snapshotting",
            Self::CatchingUp => "catching_up",
            Self::CutoverPending => "cutover_pending",
            Self::Blocked => "blocked",
            Self::Completed => "completed",
        }
    }

    fn parse(value: &str) -> Result<Self, TableTransitionError> {
        match value {
            "pending" => Ok(Self::Pending),
            "applying" => Ok(Self::Applying),
            "snapshotting" => Ok(Self::Snapshotting),
            "catching_up" => Ok(Self::CatchingUp),
            "cutover_pending" => Ok(Self::CutoverPending),
            "blocked" => Ok(Self::Blocked),
            "completed" => Ok(Self::Completed),
            other => Err(TableTransitionError::InvalidPersistedValue {
                field: "state",
                value: other.to_owned(),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableTransitionKey {
    pub pipeline_id: PipelineId,
    pub source_lsn: PgLsn,
    pub source_xid: u64,
    pub source_relation_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransitionRecord {
    pub event_id: Uuid,
    pub fence: PipelineFence,
    pub source_lsn: PgLsn,
    pub source_xid: u64,
    pub source_relation_id: u32,
    pub action: TableTransitionAction,
    pub plan: JsonValue,
    pub barrier_lsn: PgLsn,
    pub active_table_generation: Option<u64>,
    pub pending_table_generation: Option<u64>,
    pub snapshot_group_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTransition {
    pub event_id: Uuid,
    pub key: TableTransitionKey,
    pub topology_generation: u64,
    pub action: TableTransitionAction,
    pub plan: JsonValue,
    pub barrier_lsn: PgLsn,
    pub active_table_generation: Option<u64>,
    pub pending_table_generation: Option<u64>,
    pub snapshot_group_id: Option<Uuid>,
    pub state: TableTransitionState,
    pub failure_reason: Option<String>,
    pub fencing_token: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableTransitionRecordOutcome {
    Inserted,
    Adopted,
    AlreadyRecorded,
}

#[derive(Debug, Error)]
pub enum TableTransitionError {
    #[error("table transition database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Fence(#[from] CheckpointError),
    #[error("table transition event id cannot be nil")]
    InvalidEventId,
    #[error("table transition source relation id must be positive")]
    InvalidSourceRelationId,
    #[error("table transition key belongs to pipeline {key}, active fence belongs to {fence}")]
    PipelineMismatch { key: PipelineId, fence: PipelineId },
    #[error(
        "table transition topology generation {stored} does not match active generation {active}"
    )]
    TopologyMismatch { stored: u64, active: u64 },
    #[error("table transition plan cannot be null")]
    InvalidPlan,
    #[error("table transition source xid {0} exceeds the target bigint range")]
    XidOutOfRange(u64),
    #[error("table transition {field} generation {value} exceeds the target bigint range")]
    GenerationOutOfRange { field: &'static str, value: u64 },
    #[error("table transition snapshot group id cannot be nil")]
    InvalidSnapshotGroupId,
    #[error("new table transitions cannot preassign a snapshot group")]
    UnexpectedSnapshotGroupAssignment,
    #[error("action {0} does not use a table snapshot")]
    SnapshotUnsupportedAction(&'static str),
    #[error("table snapshot transition has no pending generation")]
    MissingPendingGeneration,
    #[error("persisted table transition contains invalid {field}: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("replayed table transition differs in immutable field `{0}`")]
    ReplayMismatch(&'static str),
    #[error("table transition fencing token {stored} is newer than active token {active}")]
    NewerStoredFence { stored: i64, active: i64 },
    #[error("table transition fencing token {stored} does not match active token {active}")]
    FenceMismatch { stored: i64, active: i64 },
    #[error("table transition was not found")]
    NotFound,
    #[error("table transition expected state {expected}, found {actual}")]
    StateMismatch {
        expected: &'static str,
        actual: &'static str,
    },
    #[error("action {action} cannot transition from {from} to {to}")]
    IllegalStateTransition {
        action: &'static str,
        from: &'static str,
        to: &'static str,
    },
    #[error("blocked table transition requires a non-empty failure reason")]
    MissingFailureReason,
    #[error("only a blocked table transition may carry a failure reason")]
    UnexpectedFailureReason,
    #[error("table transition write affected {0} rows instead of one")]
    UnexpectedWriteCount(u64),
}

pub async fn record_table_transition(
    client: &mut Client,
    record: &TableTransitionRecord,
) -> Result<TableTransitionRecordOutcome, TableTransitionError> {
    let transaction = client.transaction().await?;
    let outcome = record_table_transition_in_transaction(&transaction, record).await?;
    transaction.commit().await?;
    Ok(outcome)
}

pub async fn record_table_transition_in_transaction(
    transaction: &Transaction<'_>,
    record: &TableTransitionRecord,
) -> Result<TableTransitionRecordOutcome, TableTransitionError> {
    validate_record(record)?;
    lock_pipeline_fence(transaction, record.fence).await?;
    let key = record_key(record);
    if let Some(existing) = load_locked(transaction, key).await? {
        validate_replay(&existing, record)?;
        if existing.fencing_token > record.fence.fencing_token {
            return Err(TableTransitionError::NewerStoredFence {
                stored: existing.fencing_token,
                active: record.fence.fencing_token,
            });
        }
        if existing.state != TableTransitionState::Completed
            && existing.fencing_token < record.fence.fencing_token
        {
            let written = transaction
                .execute(
                    "UPDATE pg2cb_meta.table_schema_transitions SET fencing_token = $5, updated_at = clock_timestamp() WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3 AND source_relation_id = $4 AND fencing_token < $5 AND state <> 'completed'",
                    &[&key.pipeline_id.as_uuid(), &key.source_lsn.to_string(), &database_xid(key.source_xid)?, &i64::from(key.source_relation_id), &record.fence.fencing_token],
                )
                .await?;
            ensure_one(written)?;
            return Ok(TableTransitionRecordOutcome::Adopted);
        }
        return Ok(TableTransitionRecordOutcome::AlreadyRecorded);
    }

    let topology_generation = database_generation("topology", record.fence.topology_generation)?;
    let source_xid = database_xid(record.source_xid)?;
    let source_relation_id = i64::from(record.source_relation_id);
    let active_generation = optional_generation("active", record.active_table_generation)?;
    let pending_generation = optional_generation("pending", record.pending_table_generation)?;
    let written = transaction
        .execute(
            "INSERT INTO pg2cb_meta.table_schema_transitions (event_id, pipeline_id, topology_generation, source_lsn, source_xid, source_relation_id, action, plan, barrier_lsn, active_table_generation, pending_table_generation, snapshot_group_id, fencing_token) VALUES ($1, $2, $3, $4::text::pg_lsn, $5, $6, $7, $8, $9::text::pg_lsn, $10, $11, $12, $13)",
            &[&record.event_id, &record.fence.pipeline_id.as_uuid(), &topology_generation, &record.source_lsn.to_string(), &source_xid, &source_relation_id, &record.action.as_str(), &record.plan, &record.barrier_lsn.to_string(), &active_generation, &pending_generation, &record.snapshot_group_id, &record.fence.fencing_token],
        )
        .await?;
    ensure_one(written)?;
    Ok(TableTransitionRecordOutcome::Inserted)
}

pub async fn load_table_transition(
    client: &Client,
    key: TableTransitionKey,
) -> Result<Option<TableTransition>, TableTransitionError> {
    validate_key(key)?;
    let pipeline_id = key.pipeline_id.as_uuid();
    let source_lsn = key.source_lsn.to_string();
    let source_xid = database_xid(key.source_xid)?;
    let source_relation_id = i64::from(key.source_relation_id);
    let row = client
        .query_opt(
            LOAD_SQL,
            &[&pipeline_id, &source_lsn, &source_xid, &source_relation_id],
        )
        .await?;
    row.map(|row| transition_from_row(&row)).transpose()
}

pub async fn list_unfinished_table_transitions(
    client: &mut Client,
    fence: PipelineFence,
) -> Result<Vec<TableTransition>, TableTransitionError> {
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let generation = database_generation("topology", fence.topology_generation)?;
    transaction
        .execute(
            "UPDATE pg2cb_meta.table_schema_transitions SET fencing_token = $3, updated_at = clock_timestamp() WHERE pipeline_id = $1 AND topology_generation = $2 AND fencing_token < $3 AND state <> 'completed'",
            &[&fence.pipeline_id.as_uuid(), &generation, &fence.fencing_token],
        )
        .await?;
    let rows = transaction
        .query(
            LIST_UNFINISHED_SQL,
            &[&fence.pipeline_id.as_uuid(), &generation],
        )
        .await?;
    let transitions = rows
        .iter()
        .map(transition_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(newer) = transitions
        .iter()
        .find(|transition| transition.fencing_token > fence.fencing_token)
    {
        return Err(TableTransitionError::NewerStoredFence {
            stored: newer.fencing_token,
            active: fence.fencing_token,
        });
    }
    transaction.commit().await?;
    Ok(transitions)
}

pub async fn advance_table_transition_state(
    client: &mut Client,
    fence: PipelineFence,
    key: TableTransitionKey,
    expected: TableTransitionState,
    next: TableTransitionState,
    failure_reason: Option<&str>,
) -> Result<(), TableTransitionError> {
    let transaction = client.transaction().await?;
    advance_table_transition_state_in_transaction(
        &transaction,
        fence,
        key,
        expected,
        next,
        failure_reason,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

pub async fn advance_table_transition_state_in_transaction(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    key: TableTransitionKey,
    expected: TableTransitionState,
    next: TableTransitionState,
    failure_reason: Option<&str>,
) -> Result<(), TableTransitionError> {
    validate_failure_reason(next, failure_reason)?;
    validate_key(key)?;
    if key.pipeline_id != fence.pipeline_id {
        return Err(TableTransitionError::PipelineMismatch {
            key: key.pipeline_id,
            fence: fence.pipeline_id,
        });
    }
    lock_pipeline_fence(transaction, fence).await?;
    let current = load_locked(transaction, key)
        .await?
        .ok_or(TableTransitionError::NotFound)?;
    if current.fencing_token != fence.fencing_token {
        return Err(TableTransitionError::FenceMismatch {
            stored: current.fencing_token,
            active: fence.fencing_token,
        });
    }
    if current.topology_generation != fence.topology_generation {
        return Err(TableTransitionError::TopologyMismatch {
            stored: current.topology_generation,
            active: fence.topology_generation,
        });
    }
    if current.state != expected {
        return Err(TableTransitionError::StateMismatch {
            expected: expected.as_str(),
            actual: current.state.as_str(),
        });
    }
    if !current.action.can_transition(expected, next) {
        return Err(TableTransitionError::IllegalStateTransition {
            action: current.action.as_str(),
            from: expected.as_str(),
            to: next.as_str(),
        });
    }
    let written = transaction
        .execute(
            "UPDATE pg2cb_meta.table_schema_transitions SET state = $7, failure_reason = $8, updated_at = clock_timestamp(), completed_at = CASE WHEN $7 = 'completed' THEN clock_timestamp() ELSE NULL END WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3 AND source_relation_id = $4 AND fencing_token = $5 AND state = $6",
            &[&key.pipeline_id.as_uuid(), &key.source_lsn.to_string(), &database_xid(key.source_xid)?, &i64::from(key.source_relation_id), &fence.fencing_token, &expected.as_str(), &next.as_str(), &failure_reason],
        )
        .await?;
    ensure_one(written)?;
    Ok(())
}

/// Durably assigns a snapshot group and starts a reload/add shadow load.
///
/// Assignment and state movement share the pipeline fence transaction. An exact retry after an
/// ambiguous commit returns the already-assigned transition; a different group fails closed.
pub async fn begin_table_snapshot_transition(
    client: &mut Client,
    fence: PipelineFence,
    key: TableTransitionKey,
    expected: TableTransitionState,
    snapshot_group_id: Uuid,
) -> Result<TableTransition, TableTransitionError> {
    if snapshot_group_id.is_nil() {
        return Err(TableTransitionError::InvalidSnapshotGroupId);
    }
    validate_key(key)?;
    if key.pipeline_id != fence.pipeline_id {
        return Err(TableTransitionError::PipelineMismatch {
            key: key.pipeline_id,
            fence: fence.pipeline_id,
        });
    }
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let mut current = load_locked(&transaction, key)
        .await?
        .ok_or(TableTransitionError::NotFound)?;
    if current.fencing_token != fence.fencing_token {
        return Err(TableTransitionError::FenceMismatch {
            stored: current.fencing_token,
            active: fence.fencing_token,
        });
    }
    if current.topology_generation != fence.topology_generation {
        return Err(TableTransitionError::TopologyMismatch {
            stored: current.topology_generation,
            active: fence.topology_generation,
        });
    }
    if !matches!(
        current.action,
        TableTransitionAction::Reload | TableTransitionAction::Add
    ) {
        return Err(TableTransitionError::SnapshotUnsupportedAction(
            current.action.as_str(),
        ));
    }
    if current.pending_table_generation.is_none() {
        return Err(TableTransitionError::MissingPendingGeneration);
    }
    if current.state == TableTransitionState::Snapshotting
        && current.snapshot_group_id == Some(snapshot_group_id)
    {
        transaction.commit().await?;
        return Ok(current);
    }
    if current.state != expected {
        return Err(TableTransitionError::StateMismatch {
            expected: expected.as_str(),
            actual: current.state.as_str(),
        });
    }
    if current.snapshot_group_id.is_some() {
        return Err(TableTransitionError::ReplayMismatch("snapshot_group_id"));
    }
    if !current
        .action
        .can_transition(expected, TableTransitionState::Snapshotting)
    {
        return Err(TableTransitionError::IllegalStateTransition {
            action: current.action.as_str(),
            from: expected.as_str(),
            to: TableTransitionState::Snapshotting.as_str(),
        });
    }
    let written = transaction
        .execute(
            "UPDATE pg2cb_meta.table_schema_transitions SET snapshot_group_id = $7, state = 'snapshotting', failure_reason = NULL, updated_at = clock_timestamp() WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3 AND source_relation_id = $4 AND fencing_token = $5 AND state = $6 AND snapshot_group_id IS NULL",
            &[&key.pipeline_id.as_uuid(), &key.source_lsn.to_string(), &database_xid(key.source_xid)?, &i64::from(key.source_relation_id), &fence.fencing_token, &expected.as_str(), &snapshot_group_id],
        )
        .await?;
    ensure_one(written)?;
    current.snapshot_group_id = Some(snapshot_group_id);
    current.state = TableTransitionState::Snapshotting;
    current.failure_reason = None;
    transaction.commit().await?;
    Ok(current)
}

fn record_key(record: &TableTransitionRecord) -> TableTransitionKey {
    TableTransitionKey {
        pipeline_id: record.fence.pipeline_id,
        source_lsn: record.source_lsn,
        source_xid: record.source_xid,
        source_relation_id: record.source_relation_id,
    }
}

async fn load_locked(
    transaction: &Transaction<'_>,
    key: TableTransitionKey,
) -> Result<Option<TableTransition>, TableTransitionError> {
    validate_key(key)?;
    let pipeline_id = key.pipeline_id.as_uuid();
    let source_lsn = key.source_lsn.to_string();
    let source_xid = database_xid(key.source_xid)?;
    let source_relation_id = i64::from(key.source_relation_id);
    transaction
        .query_opt(
            LOCK_SQL,
            &[&pipeline_id, &source_lsn, &source_xid, &source_relation_id],
        )
        .await?
        .map(|row| transition_from_row(&row))
        .transpose()
}

fn transition_from_row(row: &Row) -> Result<TableTransition, TableTransitionError> {
    let pipeline_id = PipelineId::from_uuid(row.try_get("pipeline_id")?);
    let source_lsn = parse_lsn(row.try_get("source_lsn")?, "source_lsn")?;
    let source_xid = persisted_u64(row, "source_xid")?;
    let source_relation_id =
        u32::try_from(persisted_u64(row, "source_relation_id")?).map_err(|_| {
            TableTransitionError::InvalidPersistedValue {
                field: "source_relation_id",
                value: "out of range".to_owned(),
            }
        })?;
    let snapshot_group_id: Option<Uuid> = row.try_get("snapshot_group_id")?;
    if snapshot_group_id.is_some_and(|id| id.is_nil()) {
        return Err(TableTransitionError::InvalidPersistedValue {
            field: "snapshot_group_id",
            value: Uuid::nil().to_string(),
        });
    }
    Ok(TableTransition {
        event_id: row.try_get("event_id")?,
        key: TableTransitionKey {
            pipeline_id,
            source_lsn,
            source_xid,
            source_relation_id,
        },
        topology_generation: persisted_u64(row, "topology_generation")?,
        action: TableTransitionAction::parse(row.try_get("action")?)?,
        plan: row.try_get("plan")?,
        barrier_lsn: parse_lsn(row.try_get("barrier_lsn")?, "barrier_lsn")?,
        active_table_generation: persisted_optional_u64(row, "active_table_generation")?,
        pending_table_generation: persisted_optional_u64(row, "pending_table_generation")?,
        snapshot_group_id,
        state: TableTransitionState::parse(row.try_get("state")?)?,
        failure_reason: row.try_get("failure_reason")?,
        fencing_token: row.try_get("fencing_token")?,
    })
}

fn validate_record(record: &TableTransitionRecord) -> Result<(), TableTransitionError> {
    if record.event_id.is_nil() {
        return Err(TableTransitionError::InvalidEventId);
    }
    if record.source_relation_id == 0 {
        return Err(TableTransitionError::InvalidSourceRelationId);
    }
    if record.plan.is_null() {
        return Err(TableTransitionError::InvalidPlan);
    }
    if record.snapshot_group_id.is_some() {
        return Err(TableTransitionError::UnexpectedSnapshotGroupAssignment);
    }
    let valid_generations = match record.action {
        TableTransitionAction::Noop => {
            record.active_table_generation.is_none() && record.pending_table_generation.is_none()
        }
        TableTransitionAction::Online | TableTransitionAction::Reload => {
            matches!(
                (record.active_table_generation, record.pending_table_generation),
                (Some(active), Some(pending)) if pending == active.saturating_add(1) && active != u64::MAX
            )
        }
        TableTransitionAction::Drop => {
            record.active_table_generation.is_some() && record.pending_table_generation.is_none()
        }
        TableTransitionAction::Add => {
            record.active_table_generation.is_none() && record.pending_table_generation == Some(1)
        }
    };
    if !valid_generations {
        return Err(TableTransitionError::ReplayMismatch("table_generations"));
    }
    database_generation("topology", record.fence.topology_generation)?;
    database_xid(record.source_xid)?;
    optional_generation("active", record.active_table_generation)?;
    optional_generation("pending", record.pending_table_generation)?;
    Ok(())
}

fn validate_key(key: TableTransitionKey) -> Result<(), TableTransitionError> {
    if key.source_relation_id == 0 {
        return Err(TableTransitionError::InvalidSourceRelationId);
    }
    database_xid(key.source_xid)?;
    Ok(())
}

fn validate_replay(
    stored: &TableTransition,
    record: &TableTransitionRecord,
) -> Result<(), TableTransitionError> {
    let fields = [
        (stored.event_id == record.event_id, "event_id"),
        (
            stored.topology_generation == record.fence.topology_generation,
            "topology_generation",
        ),
        (stored.action == record.action, "action"),
        (stored.plan == record.plan, "plan"),
        (stored.barrier_lsn == record.barrier_lsn, "barrier_lsn"),
        (
            stored.active_table_generation == record.active_table_generation,
            "active_table_generation",
        ),
        (
            stored.pending_table_generation == record.pending_table_generation,
            "pending_table_generation",
        ),
    ];
    if let Some((_, field)) = fields.into_iter().find(|(matches, _)| !matches) {
        return Err(TableTransitionError::ReplayMismatch(field));
    }
    Ok(())
}

fn validate_failure_reason(
    next: TableTransitionState,
    failure_reason: Option<&str>,
) -> Result<(), TableTransitionError> {
    match (next, failure_reason) {
        (TableTransitionState::Blocked, Some(reason)) if !reason.is_empty() => Ok(()),
        (TableTransitionState::Blocked, _) => Err(TableTransitionError::MissingFailureReason),
        (_, None) => Ok(()),
        (_, Some(_)) => Err(TableTransitionError::UnexpectedFailureReason),
    }
}

fn database_xid(value: u64) -> Result<i64, TableTransitionError> {
    i64::try_from(value).map_err(|_| TableTransitionError::XidOutOfRange(value))
}

fn database_generation(field: &'static str, value: u64) -> Result<i64, TableTransitionError> {
    i64::try_from(value).map_err(|_| TableTransitionError::GenerationOutOfRange { field, value })
}

fn optional_generation(
    field: &'static str,
    value: Option<u64>,
) -> Result<Option<i64>, TableTransitionError> {
    value
        .map(|value| database_generation(field, value))
        .transpose()
}

fn persisted_u64(row: &Row, field: &'static str) -> Result<u64, TableTransitionError> {
    let value: i64 = row.try_get(field)?;
    u64::try_from(value).map_err(|_| TableTransitionError::InvalidPersistedValue {
        field,
        value: value.to_string(),
    })
}

fn persisted_optional_u64(
    row: &Row,
    field: &'static str,
) -> Result<Option<u64>, TableTransitionError> {
    let value: Option<i64> = row.try_get(field)?;
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| TableTransitionError::InvalidPersistedValue {
                field,
                value: value.to_string(),
            })
        })
        .transpose()
}

fn parse_lsn(value: &str, field: &'static str) -> Result<PgLsn, TableTransitionError> {
    value
        .parse()
        .map_err(|_| TableTransitionError::InvalidPersistedValue {
            field,
            value: value.to_owned(),
        })
}

fn ensure_one(written: u64) -> Result<(), TableTransitionError> {
    if written == 1 {
        Ok(())
    } else {
        Err(TableTransitionError::UnexpectedWriteCount(written))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_specific_state_machines_are_fail_closed() {
        use TableTransitionAction::{Add, Drop, Noop, Online, Reload};
        use TableTransitionState::{
            Applying, Blocked, CatchingUp, Completed, CutoverPending, Pending, Snapshotting,
        };

        assert!(Noop.can_transition(Pending, Completed));
        assert!(!Noop.can_transition(Pending, Applying));
        assert!(Online.can_transition(Pending, Applying));
        assert!(Online.can_transition(Applying, Completed));
        assert!(!Online.can_transition(Pending, Completed));
        for action in [Reload, Add] {
            assert!(action.can_transition(Pending, Snapshotting));
            assert!(action.can_transition(Snapshotting, CatchingUp));
            assert!(action.can_transition(CatchingUp, CutoverPending));
            assert!(action.can_transition(CutoverPending, Completed));
            assert!(!action.can_transition(Snapshotting, Completed));
        }
        assert!(Drop.can_transition(Pending, CutoverPending));
        assert!(Drop.can_transition(CutoverPending, Completed));
        assert!(Reload.can_transition(Snapshotting, Blocked));
        assert!(Reload.can_transition(Blocked, Snapshotting));
        assert!(!Reload.can_transition(Blocked, Blocked));
        assert!(!Reload.can_transition(Completed, Blocked));
    }

    #[test]
    fn failure_reason_matches_only_the_retry_state() {
        assert!(validate_failure_reason(TableTransitionState::Blocked, Some("retry")).is_ok());
        assert!(validate_failure_reason(TableTransitionState::Blocked, None).is_err());
        assert!(validate_failure_reason(TableTransitionState::Completed, Some("stale")).is_err());
        assert!(validate_failure_reason(TableTransitionState::Completed, None).is_ok());
    }

    #[test]
    fn transition_keys_reject_zero_relations() {
        let key = TableTransitionKey {
            pipeline_id: PipelineId::new(),
            source_lsn: PgLsn::new(1),
            source_xid: 1,
            source_relation_id: 0,
        };
        assert!(matches!(
            validate_key(key),
            Err(TableTransitionError::InvalidSourceRelationId)
        ));
    }

    #[test]
    fn record_generations_match_action_lifecycle() {
        let fence = PipelineFence {
            pipeline_id: PipelineId::new(),
            topology_generation: 3,
            fencing_token: 7,
        };
        let mut record = TableTransitionRecord {
            event_id: Uuid::now_v7(),
            fence,
            source_lsn: PgLsn::new(10),
            source_xid: 11,
            source_relation_id: 42,
            action: TableTransitionAction::Reload,
            plan: serde_json::json!({"action": "reload"}),
            barrier_lsn: PgLsn::new(10),
            active_table_generation: Some(4),
            pending_table_generation: Some(5),
            snapshot_group_id: None,
        };
        assert!(validate_record(&record).is_ok());
        record.pending_table_generation = Some(6);
        assert!(matches!(
            validate_record(&record),
            Err(TableTransitionError::ReplayMismatch("table_generations"))
        ));
        record.action = TableTransitionAction::Add;
        record.active_table_generation = None;
        record.pending_table_generation = Some(1);
        assert!(validate_record(&record).is_ok());
    }
}
