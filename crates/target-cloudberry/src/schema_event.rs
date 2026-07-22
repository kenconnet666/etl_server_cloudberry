//! Durable schema-event ledger access for Phase 2 tight DDL follow.
//!
//! Each source DDL that touches a managed relation is recorded in
//! `pg2cb_meta.schema_events` (migration V8) keyed by `(pipeline_id, source_lsn,
//! source_xid)` so replayed WAL is idempotent. The engine records an event as
//! `Pending`, drives it through `InTransition`, then `Completed`/`Failed`; a
//! restart lists unfinished events in source-LSN order and resumes them. This
//! module owns the typed CRUD and the state-transition guards; it never advances
//! a WAL checkpoint.

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::checkpoint::PipelineFence;

const INSERT_SCHEMA_EVENT_SQL: &str = r#"
INSERT INTO pg2cb_meta.schema_events (
    event_id, pipeline_id, topology_generation, source_lsn, source_xid,
    command_tag, schema_fingerprint, transitions, state, fencing_token
)
VALUES ($1, $2, $3, $4::text::pg_lsn, $5, $6, $7, $8, 'pending', $9)
ON CONFLICT (pipeline_id, source_lsn, source_xid) DO NOTHING
"#;

const LOAD_SCHEMA_EVENT_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, command_tag, schema_fingerprint, transitions, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.schema_events
 WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3
"#;

const LIST_UNFINISHED_SCHEMA_EVENTS_SQL: &str = r#"
SELECT event_id, pipeline_id, topology_generation, source_lsn::text AS source_lsn,
       source_xid, command_tag, schema_fingerprint, transitions, state,
       failure_reason, fencing_token
  FROM pg2cb_meta.schema_events
 WHERE pipeline_id = $1 AND topology_generation = $2
   AND state IN ('pending', 'in_transition')
 ORDER BY source_lsn, source_xid
"#;

const ADVANCE_STATE_SQL: &str = r#"
UPDATE pg2cb_meta.schema_events
SET state = $4,
    failure_reason = $5,
    processed_at = CASE WHEN $4 IN ('completed', 'failed') THEN clock_timestamp() ELSE NULL END
WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn AND source_xid = $3
  AND state = $6
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
    /// An event with the same source identity already existed (idempotent WAL replay).
    AlreadyRecorded,
}

#[derive(Debug, Error)]
pub enum SchemaEventError {
    #[error("schema event database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("topology generation {0} exceeds the target bigint range")]
    GenerationOutOfRange(u64),
    #[error("source xid {0} exceeds the target bigint range")]
    XidOutOfRange(u64),
    #[error("fencing token must be positive")]
    InvalidFencingToken,
    #[error("command tag cannot be empty")]
    InvalidCommandTag,
    #[error("persisted schema event contains invalid {field}: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("illegal schema event transition from {from} to {to}")]
    IllegalTransition { from: &'static str, to: &'static str },
    #[error("schema event state update affected {0} rows instead of one")]
    UnexpectedWriteCount(u64),
}

/// Record a new pending schema event. Idempotent on `(pipeline_id, source_lsn,
/// source_xid)`: a replayed WAL event returns [`RecordOutcome::AlreadyRecorded`]
/// without overwriting the existing row.
pub async fn record_schema_event(
    client: &Client,
    record: &SchemaEventRecord,
) -> Result<RecordOutcome, SchemaEventError> {
    let generation = database_generation(record.fence.topology_generation)?;
    let xid = database_xid(record.source_xid)?;
    if record.fence.fencing_token <= 0 {
        return Err(SchemaEventError::InvalidFencingToken);
    }
    if record.command_tag.is_empty() {
        return Err(SchemaEventError::InvalidCommandTag);
    }
    let pipeline_id = record.fence.pipeline_id.as_uuid();
    let lsn = record.source_lsn.to_string();
    let written = client
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
    if written == 1 {
        Ok(RecordOutcome::Inserted)
    } else {
        Ok(RecordOutcome::AlreadyRecorded)
    }
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
    client: &Client,
    fence: PipelineFence,
) -> Result<Vec<SchemaEvent>, SchemaEventError> {
    let generation = database_generation(fence.topology_generation)?;
    let rows = client
        .query(
            LIST_UNFINISHED_SCHEMA_EVENTS_SQL,
            &[&fence.pipeline_id.as_uuid(), &generation],
        )
        .await?;
    rows.iter().map(schema_event_from_row).collect()
}

/// Advance an event's state, guarding the legal transition and requiring the row
/// to still be in `expected_from` (optimistic concurrency). A `Failed` target
/// must carry a reason; other targets must not.
pub async fn advance_schema_event_state(
    client: &Client,
    pipeline_id: PipelineId,
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
    let xid = database_xid(source_xid)?;
    let updated = client
        .execute(
            ADVANCE_STATE_SQL,
            &[
                &pipeline_id.as_uuid(),
                &source_lsn.to_string(),
                &xid,
                &next.as_str(),
                &reason,
                &expected_from.as_str(),
            ],
        )
        .await?;
    if updated == 1 {
        Ok(())
    } else {
        Err(SchemaEventError::UnexpectedWriteCount(updated))
    }
}

fn schema_event_from_row(row: &Row) -> Result<SchemaEvent, SchemaEventError> {
    let generation: i64 = row.try_get("topology_generation")?;
    let source_xid: i64 = row.try_get("source_xid")?;
    let lsn_text: String = row.try_get("source_lsn")?;
    let state_text: String = row.try_get("state")?;
    let source_lsn = lsn_text.parse::<PgLsn>().map_err(|_| {
        SchemaEventError::InvalidPersistedValue {
            field: "source_lsn",
            value: lsn_text.clone(),
        }
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
        assert!(INSERT_SCHEMA_EVENT_SQL
            .contains("ON CONFLICT (pipeline_id, source_lsn, source_xid) DO NOTHING"));
        // The list query must be ordered so restart replay is deterministic.
        assert!(LIST_UNFINISHED_SCHEMA_EVENTS_SQL.contains("ORDER BY source_lsn, source_xid"));
        assert!(LIST_UNFINISHED_SCHEMA_EVENTS_SQL.contains("state IN ('pending', 'in_transition')"));
        // The advance guard requires the expected prior state.
        assert!(ADVANCE_STATE_SQL.contains("AND state = $6"));
    }
}
