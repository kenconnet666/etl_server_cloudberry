//! Durable schema-event ledger access for Phase 2 tight DDL follow.
//!
//! Each committed source transaction that contains managed DDL is recorded in
//! `pg2cb_meta.schema_events` (migration V8) keyed by `(pipeline_id, source_lsn,
//! source_xid)` so replayed WAL is idempotent. Ordered DDL messages share one event payload,
//! matching PostgreSQL's commit boundary. The engine records an event as
//! `Pending`, drives it through `InTransition`, then `Completed`/`Failed`; a
//! restart lists unfinished events in source-LSN order and resumes them. This
//! module owns the typed CRUD and the state-transition guards; it never advances
//! a WAL checkpoint.

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

use crate::checkpoint::{CheckpointError, PipelineFence, lock_pipeline_fence};

const INSERT_SCHEMA_EVENT_SQL: &str = r#"
INSERT INTO pg2cb_meta.schema_events (
    event_id, pipeline_id, topology_generation, source_lsn, source_xid,
    command_tag, schema_fingerprint, transitions, state, fencing_token
)
VALUES ($1, $2, $3, $4::text::pg_lsn, $5, $6, $7, $8, 'pending', $9)
"#;

const LOAD_SCHEMA_EVENT_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, command_tag, schema_fingerprint, transitions, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.schema_events
 WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3
"#;

const LOCK_SCHEMA_EVENT_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, command_tag, schema_fingerprint, transitions, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.schema_events
 WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3
 FOR UPDATE
"#;

const LIST_UNFINISHED_SCHEMA_EVENTS_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, command_tag, schema_fingerprint, transitions, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.schema_events
 WHERE pipeline_id = $1 AND topology_generation = $2
   AND state IN ('pending', 'in_transition')
 ORDER BY source_lsn, source_xid
 FOR UPDATE
"#;

const ADOPT_UNFINISHED_SCHEMA_EVENTS_SQL: &str = r#"
UPDATE pg2cb_meta.schema_events
   SET fencing_token = $3
 WHERE pipeline_id = $1 AND topology_generation = $2
   AND state IN ('pending', 'in_transition')
   AND fencing_token < $3
"#;

const ADVANCE_STATE_SQL: &str = r#"
UPDATE pg2cb_meta.schema_events
SET state = $7,
    failure_reason = $8,
    processed_at = CASE WHEN $7 IN ('completed', 'failed') THEN clock_timestamp() ELSE NULL END
WHERE pipeline_id = $1 AND topology_generation = $2
  AND source_lsn = $3::text::pg_lsn AND source_xid = $4
  AND fencing_token = $5 AND state = $6
"#;

const LOCK_UNFINISHED_TABLE_TRANSITIONS_SQL: &str = r#"
SELECT topology_generation, fencing_token
  FROM pg2cb_meta.table_schema_transitions
 WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3
   AND state <> 'completed'
 FOR UPDATE
"#;

const BLOCK_UNFINISHED_TABLE_TRANSITIONS_SQL: &str = r#"
UPDATE pg2cb_meta.table_schema_transitions
   SET state = 'blocked', failure_reason = $6, updated_at = clock_timestamp(),
       completed_at = NULL
 WHERE pipeline_id = $1 AND topology_generation = $2
   AND source_lsn = $3::text::pg_lsn AND source_xid = $4
   AND fencing_token = $5 AND state <> 'completed'
"#;

/// Lifecycle state of a persisted schema event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaEventState {
    /// Recorded but no table transition has started yet.
    Pending,
    /// A table transition is in progress for this event.
    InTransition,
    /// The transition finished and the target caught up.
    Completed,
    /// The transition could not be applied online; requires operator or rebuild.
    Failed,
}

impl SchemaEventState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InTransition => "in_transition",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    fn from_str(value: &str) -> Result<Self, SchemaEventError> {
        match value {
            "pending" => Ok(Self::Pending),
            "in_transition" => Ok(Self::InTransition),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(SchemaEventError::InvalidPersistedValue {
                field: "state",
                value: other.to_owned(),
            }),
        }
    }

    /// Legal forward transitions. The ledger only moves forward: a completed or
    /// failed event is terminal, and pending may go straight to completed for a
    /// no-op transition (e.g. a whitelisted change with nothing to reload).
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Pending, Self::InTransition)
                | (Self::Pending, Self::Completed)
                | (Self::Pending, Self::Failed)
                | (Self::InTransition, Self::Completed)
                | (Self::InTransition, Self::Failed)
        )
    }
}

/// A durable schema event row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaEvent {
    pub event_id: Uuid,
    pub pipeline_id: PipelineId,
    pub topology_generation: u64,
    pub source_lsn: PgLsn,
    pub source_xid: u64,
    pub command_tag: String,
    pub schema_fingerprint: String,
    pub transitions: JsonValue,
    pub state: SchemaEventState,
    pub failure_reason: Option<String>,
    pub fencing_token: i64,
}

/// The immutable identity plus payload used to record a new event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaEventRecord {
    pub event_id: Uuid,
    pub fence: PipelineFence,
    pub source_lsn: PgLsn,
    pub source_xid: u64,
    pub command_tag: String,
    pub schema_fingerprint: String,
    pub transitions: JsonValue,
}

/// Outcome of recording a schema event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordOutcome {
    /// A new pending event row was written.
    Inserted,
    /// An unfinished event was adopted by a newer active fencing token.
    Adopted,
    /// An event with the same source identity already existed (idempotent WAL replay).
    AlreadyRecorded,
}

#[derive(Debug, Error)]
pub enum SchemaEventError {
    #[error("schema event database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Fence(#[from] CheckpointError),
    #[error("topology generation {0} exceeds the target bigint range")]
    GenerationOutOfRange(u64),
    #[error("source xid {0} exceeds the target bigint range")]
    XidOutOfRange(u64),
    #[error("fencing token must be positive")]
    InvalidFencingToken,
    #[error("command tag cannot be empty")]
    InvalidCommandTag,
    #[error("schema fingerprint cannot be empty")]
    InvalidSchemaFingerprint,
    #[error("schema event id cannot be nil")]
    InvalidEventId,
    #[error("schema transition payload cannot be null")]
    InvalidTransitions,
    #[error("replayed schema event differs in immutable field `{field}`")]
    ReplayMismatch { field: &'static str },
    #[error("schema event fencing token {stored_token} is newer than active token {active_token}")]
    NewerStoredFence {
        stored_token: i64,
        active_token: i64,
    },
    #[error("persisted schema event contains invalid {field}: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("illegal schema event transition from {from} to {to}")]
    IllegalTransition {
        from: &'static str,
        to: &'static str,
    },
    #[error("schema event state update affected {0} rows instead of one")]
    UnexpectedWriteCount(u64),
    #[error("schema event was not found")]
    NotFound,
    #[error("schema event topology generation {stored} does not match active generation {active}")]
    TopologyMismatch { stored: u64, active: u64 },
    #[error("schema event fencing token {stored} does not match active token {active}")]
    FenceMismatch { stored: i64, active: i64 },
    #[error("schema-event fallback requires a non-empty failure reason")]
    InvalidFailureReason,
    #[error(
        "unfinished table transition uses topology generation {stored} and fence {stored_fence}, expected generation {active} and fence {active_fence}"
    )]
    TableTransitionFenceMismatch {
        stored: u64,
        stored_fence: i64,
        active: u64,
        active_fence: i64,
    },
}

/// Record a new pending schema event in its own target transaction.
pub async fn record_schema_event(
    client: &mut Client,
    record: &SchemaEventRecord,
) -> Result<RecordOutcome, SchemaEventError> {
    let transaction = client.transaction().await?;
    let outcome = record_schema_event_in_transaction(&transaction, record).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Record or adopt a pending schema event inside the caller's target transaction.
///
/// A duplicate source identity is idempotent only when every immutable field is exact. An
/// unfinished row from an older lease is adopted after the active pipeline fence is locked;
/// terminal rows remain immutable historical evidence.
pub async fn record_schema_event_in_transaction(
    transaction: &Transaction<'_>,
    record: &SchemaEventRecord,
) -> Result<RecordOutcome, SchemaEventError> {
    let generation = database_generation(record.fence.topology_generation)?;
    let xid = database_xid(record.source_xid)?;
    validate_record(record)?;
    lock_pipeline_fence(transaction, record.fence).await?;
    let pipeline_id = record.fence.pipeline_id.as_uuid();
    let lsn = record.source_lsn.to_string();
    let existing = transaction
        .query_opt(LOCK_SCHEMA_EVENT_SQL, &[&pipeline_id, &lsn, &xid])
        .await?
        .map(|row| schema_event_from_row(&row))
        .transpose()?;
    if let Some(mut existing) = existing {
        validate_replay(&existing, record)?;
        if existing.fencing_token > record.fence.fencing_token {
            return Err(SchemaEventError::NewerStoredFence {
                stored_token: existing.fencing_token,
                active_token: record.fence.fencing_token,
            });
        }
        if matches!(
            existing.state,
            SchemaEventState::Pending | SchemaEventState::InTransition
        ) && existing.fencing_token < record.fence.fencing_token
        {
            let updated = transaction
                .execute(
                    "UPDATE pg2cb_meta.schema_events SET fencing_token = $4
                      WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn
                        AND source_xid = $3 AND fencing_token < $4
                        AND state IN ('pending', 'in_transition')",
                    &[&pipeline_id, &lsn, &xid, &record.fence.fencing_token],
                )
                .await?;
            ensure_one_row(updated)?;
            existing.fencing_token = record.fence.fencing_token;
            return Ok(RecordOutcome::Adopted);
        }
        return Ok(RecordOutcome::AlreadyRecorded);
    }
    let written = transaction
        .execute(
            INSERT_SCHEMA_EVENT_SQL,
            &[
                &record.event_id,
                &pipeline_id,
                &generation,
                &lsn,
                &xid,
                &record.command_tag,
                &record.schema_fingerprint,
                &record.transitions,
                &record.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_row(written)?;
    Ok(RecordOutcome::Inserted)
}

/// Load a single event by its source identity, if present.
pub async fn load_schema_event(
    client: &Client,
    pipeline_id: PipelineId,
    source_lsn: PgLsn,
    source_xid: u64,
) -> Result<Option<SchemaEvent>, SchemaEventError> {
    let xid = database_xid(source_xid)?;
    let row = client
        .query_opt(
            LOAD_SCHEMA_EVENT_SQL,
            &[&pipeline_id.as_uuid(), &source_lsn.to_string(), &xid],
        )
        .await?;
    row.map(|row| schema_event_from_row(&row)).transpose()
}

/// List unfinished (`pending`/`in_transition`) events for a generation in
/// source-LSN order, so a restart can resume transitions deterministically.
pub async fn list_unfinished_schema_events(
    client: &mut Client,
    fence: PipelineFence,
) -> Result<Vec<SchemaEvent>, SchemaEventError> {
    if fence.fencing_token <= 0 {
        return Err(SchemaEventError::InvalidFencingToken);
    }
    let generation = database_generation(fence.topology_generation)?;
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    transaction
        .execute(
            ADOPT_UNFINISHED_SCHEMA_EVENTS_SQL,
            &[
                &fence.pipeline_id.as_uuid(),
                &generation,
                &fence.fencing_token,
            ],
        )
        .await?;
    let rows = transaction
        .query(
            LIST_UNFINISHED_SCHEMA_EVENTS_SQL,
            &[&fence.pipeline_id.as_uuid(), &generation],
        )
        .await?;
    let events = rows
        .iter()
        .map(schema_event_from_row)
        .collect::<Result<Vec<_>, _>>()?;
    if let Some(event) = events
        .iter()
        .find(|event| event.fencing_token > fence.fencing_token)
    {
        return Err(SchemaEventError::NewerStoredFence {
            stored_token: event.fencing_token,
            active_token: fence.fencing_token,
        });
    }
    transaction.commit().await?;
    Ok(events)
}

/// Advance an event's state, guarding the legal transition and requiring the row
/// to still be in `expected_from` (optimistic concurrency). A `Failed` target
/// must carry a reason; other targets must not.
pub async fn advance_schema_event_state(
    client: &mut Client,
    fence: PipelineFence,
    source_lsn: PgLsn,
    source_xid: u64,
    expected_from: SchemaEventState,
    next: SchemaEventState,
    failure_reason: Option<&str>,
) -> Result<(), SchemaEventError> {
    let transaction = client.transaction().await?;
    advance_schema_event_state_in_transaction(
        &transaction,
        fence,
        source_lsn,
        source_xid,
        expected_from,
        next,
        failure_reason,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// Advance an event in the same target transaction as the corresponding schema/metadata change.
pub async fn advance_schema_event_state_in_transaction(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    source_lsn: PgLsn,
    source_xid: u64,
    expected_from: SchemaEventState,
    next: SchemaEventState,
    failure_reason: Option<&str>,
) -> Result<(), SchemaEventError> {
    if !expected_from.can_transition_to(next) {
        return Err(SchemaEventError::IllegalTransition {
            from: expected_from.as_str(),
            to: next.as_str(),
        });
    }
    let reason: Option<&str> = if next == SchemaEventState::Failed {
        failure_reason.or(Some("unspecified transition failure"))
    } else {
        None
    };
    let generation = database_generation(fence.topology_generation)?;
    if fence.fencing_token <= 0 {
        return Err(SchemaEventError::InvalidFencingToken);
    }
    let xid = database_xid(source_xid)?;
    lock_pipeline_fence(transaction, fence).await?;
    let updated = transaction
        .execute(
            ADVANCE_STATE_SQL,
            &[
                &fence.pipeline_id.as_uuid(),
                &generation,
                &source_lsn.to_string(),
                &xid,
                &fence.fencing_token,
                &expected_from.as_str(),
                &next.as_str(),
                &reason,
            ],
        )
        .await?;
    ensure_one_row(updated)
}

/// Atomically closes a schema event that is falling back to a pipeline rebuild.
///
/// Every unfinished child transition is blocked with the event failure reason before the parent
/// becomes failed. Calling this for an already-failed event repairs ledgers written before table
/// transitions were closed atomically.
pub async fn fail_schema_event_and_block_transitions(
    client: &mut Client,
    fence: PipelineFence,
    source_lsn: PgLsn,
    source_xid: u64,
    failure_reason: &str,
) -> Result<(), SchemaEventError> {
    if failure_reason.is_empty() || failure_reason.contains('\0') {
        return Err(SchemaEventError::InvalidFailureReason);
    }
    if fence.fencing_token <= 0 {
        return Err(SchemaEventError::InvalidFencingToken);
    }
    let generation = database_generation(fence.topology_generation)?;
    let xid = database_xid(source_xid)?;
    let pipeline_id = fence.pipeline_id.as_uuid();
    let lsn = source_lsn.to_string();
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let event = transaction
        .query_opt(LOCK_SCHEMA_EVENT_SQL, &[&pipeline_id, &lsn, &xid])
        .await?
        .map(|row| schema_event_from_row(&row))
        .transpose()?
        .ok_or(SchemaEventError::NotFound)?;
    if event.topology_generation != fence.topology_generation {
        return Err(SchemaEventError::TopologyMismatch {
            stored: event.topology_generation,
            active: fence.topology_generation,
        });
    }
    if event.fencing_token != fence.fencing_token {
        return Err(SchemaEventError::FenceMismatch {
            stored: event.fencing_token,
            active: fence.fencing_token,
        });
    }
    if event.state == SchemaEventState::Completed {
        return Err(SchemaEventError::IllegalTransition {
            from: event.state.as_str(),
            to: SchemaEventState::Failed.as_str(),
        });
    }
    let effective_reason = event
        .failure_reason
        .as_deref()
        .filter(|reason| !reason.is_empty())
        .unwrap_or(failure_reason);
    let transition_rows = transaction
        .query(
            LOCK_UNFINISHED_TABLE_TRANSITIONS_SQL,
            &[&pipeline_id, &lsn, &xid],
        )
        .await?;
    for row in transition_rows {
        let stored_generation: i64 = row.try_get("topology_generation")?;
        let stored_generation = u64::try_from(stored_generation).map_err(|_| {
            SchemaEventError::InvalidPersistedValue {
                field: "table_transition.topology_generation",
                value: stored_generation.to_string(),
            }
        })?;
        let stored_fence: i64 = row.try_get("fencing_token")?;
        if stored_generation != fence.topology_generation || stored_fence != fence.fencing_token {
            return Err(SchemaEventError::TableTransitionFenceMismatch {
                stored: stored_generation,
                stored_fence,
                active: fence.topology_generation,
                active_fence: fence.fencing_token,
            });
        }
    }
    transaction
        .execute(
            BLOCK_UNFINISHED_TABLE_TRANSITIONS_SQL,
            &[
                &pipeline_id,
                &generation,
                &lsn,
                &xid,
                &fence.fencing_token,
                &effective_reason,
            ],
        )
        .await?;
    if event.state != SchemaEventState::Failed {
        advance_schema_event_state_in_transaction(
            &transaction,
            fence,
            source_lsn,
            source_xid,
            event.state,
            SchemaEventState::Failed,
            Some(effective_reason),
        )
        .await?;
    }
    transaction.commit().await?;
    Ok(())
}

fn validate_record(record: &SchemaEventRecord) -> Result<(), SchemaEventError> {
    if record.fence.fencing_token <= 0 {
        return Err(SchemaEventError::InvalidFencingToken);
    }
    if record.event_id.is_nil() {
        return Err(SchemaEventError::InvalidEventId);
    }
    if record.command_tag.is_empty() || record.command_tag.contains('\0') {
        return Err(SchemaEventError::InvalidCommandTag);
    }
    if record.schema_fingerprint.is_empty() || record.schema_fingerprint.contains('\0') {
        return Err(SchemaEventError::InvalidSchemaFingerprint);
    }
    if record.transitions.is_null() {
        return Err(SchemaEventError::InvalidTransitions);
    }
    Ok(())
}

fn validate_replay(
    stored: &SchemaEvent,
    proposed: &SchemaEventRecord,
) -> Result<(), SchemaEventError> {
    let comparisons = [
        (stored.event_id == proposed.event_id, "event_id"),
        (
            stored.topology_generation == proposed.fence.topology_generation,
            "topology_generation",
        ),
        (stored.command_tag == proposed.command_tag, "command_tag"),
        (
            stored.schema_fingerprint == proposed.schema_fingerprint,
            "schema_fingerprint",
        ),
        (stored.transitions == proposed.transitions, "transitions"),
    ];
    for (matches, field) in comparisons {
        if !matches {
            return Err(SchemaEventError::ReplayMismatch { field });
        }
    }
    Ok(())
}

fn ensure_one_row(written: u64) -> Result<(), SchemaEventError> {
    if written == 1 {
        Ok(())
    } else {
        Err(SchemaEventError::UnexpectedWriteCount(written))
    }
}

fn schema_event_from_row(row: &Row) -> Result<SchemaEvent, SchemaEventError> {
    let generation: i64 = row.try_get("topology_generation")?;
    let source_xid: i64 = row.try_get("source_xid")?;
    let lsn_text: String = row.try_get("source_lsn")?;
    let state_text: String = row.try_get("state")?;
    let source_lsn =
        lsn_text
            .parse::<PgLsn>()
            .map_err(|_| SchemaEventError::InvalidPersistedValue {
                field: "source_lsn",
                value: lsn_text.clone(),
            })?;
    Ok(SchemaEvent {
        event_id: row.try_get("event_id")?,
        pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
        topology_generation: u64::try_from(generation).map_err(|_| {
            SchemaEventError::InvalidPersistedValue {
                field: "topology_generation",
                value: generation.to_string(),
            }
        })?,
        source_lsn,
        source_xid: u64::try_from(source_xid).map_err(|_| {
            SchemaEventError::InvalidPersistedValue {
                field: "source_xid",
                value: source_xid.to_string(),
            }
        })?,
        command_tag: row.try_get("command_tag")?,
        schema_fingerprint: row.try_get("schema_fingerprint")?,
        transitions: row.try_get("transitions")?,
        state: SchemaEventState::from_str(&state_text)?,
        failure_reason: row.try_get("failure_reason")?,
        fencing_token: row.try_get("fencing_token")?,
    })
}

fn database_generation(value: u64) -> Result<i64, SchemaEventError> {
    i64::try_from(value).map_err(|_| SchemaEventError::GenerationOutOfRange(value))
}

fn database_xid(value: u64) -> Result<i64, SchemaEventError> {
    i64::try_from(value).map_err(|_| SchemaEventError::XidOutOfRange(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_string_round_trips() {
        for state in [
            SchemaEventState::Pending,
            SchemaEventState::InTransition,
            SchemaEventState::Completed,
            SchemaEventState::Failed,
        ] {
            assert_eq!(SchemaEventState::from_str(state.as_str()).unwrap(), state);
        }
        assert!(matches!(
            SchemaEventState::from_str("bogus"),
            Err(SchemaEventError::InvalidPersistedValue { field: "state", .. })
        ));
    }

    #[test]
    fn only_forward_transitions_are_legal() {
        use SchemaEventState::{Completed, Failed, InTransition, Pending};
        // Legal forward moves.
        assert!(Pending.can_transition_to(InTransition));
        assert!(Pending.can_transition_to(Completed));
        assert!(Pending.can_transition_to(Failed));
        assert!(InTransition.can_transition_to(Completed));
        assert!(InTransition.can_transition_to(Failed));
        // Terminal states never move.
        assert!(!Completed.can_transition_to(InTransition));
        assert!(!Completed.can_transition_to(Failed));
        assert!(!Failed.can_transition_to(Completed));
        // No backward move and no self-loop.
        assert!(!InTransition.can_transition_to(Pending));
        assert!(!Pending.can_transition_to(Pending));
    }

    #[test]
    fn out_of_range_generation_and_xid_are_rejected() {
        assert!(matches!(
            database_generation(u64::MAX),
            Err(SchemaEventError::GenerationOutOfRange(_))
        ));
        assert!(matches!(
            database_xid(u64::MAX),
            Err(SchemaEventError::XidOutOfRange(_))
        ));
        assert_eq!(database_generation(7).unwrap(), 7);
        assert_eq!(database_xid(42).unwrap(), 42);
    }

    #[test]
    fn insert_sql_is_idempotent_on_source_identity() {
        assert!(LOCK_SCHEMA_EVENT_SQL.ends_with("FOR UPDATE\n"));
        // The list query must be ordered so restart replay is deterministic.
        assert!(LIST_UNFINISHED_SCHEMA_EVENTS_SQL.contains("ORDER BY source_lsn, source_xid"));
        assert!(
            LIST_UNFINISHED_SCHEMA_EVENTS_SQL.contains("state IN ('pending', 'in_transition')")
        );
        // The advance guard requires the expected prior state.
        assert!(ADVANCE_STATE_SQL.contains("AND fencing_token = $5 AND state = $6"));
    }

    #[test]
    fn fallback_blocks_only_the_current_events_unfinished_tables() {
        assert!(LOCK_UNFINISHED_TABLE_TRANSITIONS_SQL.ends_with("FOR UPDATE\n"));
        assert!(BLOCK_UNFINISHED_TABLE_TRANSITIONS_SQL.contains("state = 'blocked'"));
        assert!(BLOCK_UNFINISHED_TABLE_TRANSITIONS_SQL.contains("state <> 'completed'"));
        assert!(BLOCK_UNFINISHED_TABLE_TRANSITIONS_SQL.contains("fencing_token = $5"));
    }
}
