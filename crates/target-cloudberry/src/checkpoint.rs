//! Cloudberry-side checkpoint persistence.

use std::str::FromStr as _;

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use thiserror::Error;
use tokio_postgres::{Client, Row, Transaction};

pub const LOAD_PIPELINE_FENCE_SQL: &str = "SELECT topology_generation, fencing_token FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1";

pub const LOCK_PIPELINE_FENCE_SQL: &str = "SELECT topology_generation, fencing_token FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1 FOR UPDATE";

pub const ACTIVATE_PIPELINE_FENCE_SQL: &str = r#"
INSERT INTO pg2cb_meta.pipeline_state (pipeline_id, topology_generation, fencing_token)
VALUES ($1, $2, $3)
ON CONFLICT (pipeline_id) DO UPDATE SET
    topology_generation = EXCLUDED.topology_generation,
    fencing_token = EXCLUDED.fencing_token,
    updated_at = clock_timestamp()
WHERE EXCLUDED.topology_generation >= pg2cb_meta.pipeline_state.topology_generation
  AND EXCLUDED.fencing_token >= pg2cb_meta.pipeline_state.fencing_token
RETURNING topology_generation, fencing_token
"#;

pub const LOAD_NODE_CHECKPOINT_SQL: &str = r#"
SELECT pipeline_id, topology_generation, node_id,
       system_identifier::text AS system_identifier,
       timeline, slot_name, applied_lsn::text AS applied_lsn, fencing_token
FROM pg2cb_meta.node_checkpoints
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
"#;

pub const LOCK_NODE_CHECKPOINT_SQL: &str = r#"
SELECT pipeline_id, topology_generation, node_id,
       system_identifier::text AS system_identifier,
       timeline, slot_name, applied_lsn::text AS applied_lsn, fencing_token
FROM pg2cb_meta.node_checkpoints
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
FOR UPDATE
"#;

pub const INSERT_NODE_CHECKPOINT_SQL: &str = r#"
INSERT INTO pg2cb_meta.node_checkpoints (
    pipeline_id, topology_generation, node_id, system_identifier,
    timeline, slot_name, applied_lsn, fencing_token
)
VALUES ($1, $2, $3, $4::numeric, $5, $6, $7::pg_lsn, $8)
"#;

pub const UPDATE_NODE_CHECKPOINT_SQL: &str = r#"
UPDATE pg2cb_meta.node_checkpoints
SET applied_lsn = $4::pg_lsn, fencing_token = $5, updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineFence {
    pub pipeline_id: PipelineId,
    pub topology_generation: u64,
    pub fencing_token: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointKey {
    pub pipeline_id: PipelineId,
    pub topology_generation: u64,
    pub node_id: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeCheckpoint {
    pub key: CheckpointKey,
    pub system_identifier: u64,
    pub timeline: u32,
    pub slot_name: String,
    pub applied_lsn: PgLsn,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredNodeCheckpoint {
    pub checkpoint: NodeCheckpoint,
    pub fencing_token: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdvanceOutcome {
    Inserted,
    Unchanged,
    Advanced { previous_lsn: PgLsn },
}

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("target checkpoint database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("topology generation {0} exceeds the target bigint range")]
    GenerationOutOfRange(u64),
    #[error("fencing token must be positive")]
    InvalidFencingToken,
    #[error("timeline must be non-zero")]
    InvalidTimeline,
    #[error("slot name cannot be empty or contain NUL")]
    InvalidSlotName,
    #[error("pipeline fence has not been initialized")]
    MissingFence,
    #[error(
        "stale pipeline fence: expected generation {expected_generation} token {expected_token}, found generation {actual_generation} token {actual_token}"
    )]
    StaleFence {
        expected_generation: u64,
        expected_token: i64,
        actual_generation: u64,
        actual_token: i64,
    },
    #[error("the proposed checkpoint key does not match the active pipeline fence")]
    FenceKeyMismatch,
    #[error("persisted checkpoint contains invalid {field}: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("source node identity changed within one topology generation")]
    SourceIdentityChanged,
    #[error("checkpoint LSN would regress from {current} to {proposed}")]
    LsnRegression { current: PgLsn, proposed: PgLsn },
    #[error("checkpoint write affected {0} rows instead of one")]
    UnexpectedWriteCount(u64),
}

/// Activates a monotonically newer fencing token/topology generation.
pub async fn activate_pipeline_fence(
    client: &Client,
    fence: PipelineFence,
) -> Result<(), CheckpointError> {
    let generation = database_generation(fence.topology_generation)?;
    validate_fence(fence)?;
    let pipeline_id = fence.pipeline_id.as_uuid();
    let activated = client
        .query_opt(
            ACTIVATE_PIPELINE_FENCE_SQL,
            &[&pipeline_id, &generation, &fence.fencing_token],
        )
        .await?;
    if activated.is_some() {
        return Ok(());
    }

    let current = client
        .query_one(LOAD_PIPELINE_FENCE_SQL, &[&pipeline_id])
        .await?;
    let actual_generation = generation_from_row(&current, "topology_generation")?;
    let actual_token = current.try_get("fencing_token")?;
    Err(CheckpointError::StaleFence {
        expected_generation: fence.topology_generation,
        expected_token: fence.fencing_token,
        actual_generation,
        actual_token,
    })
}

/// Loads the target-authoritative applied checkpoint for one source node.
pub async fn load_node_checkpoint(
    client: &Client,
    key: CheckpointKey,
) -> Result<Option<StoredNodeCheckpoint>, CheckpointError> {
    let generation = database_generation(key.topology_generation)?;
    let pipeline_id = key.pipeline_id.as_uuid();
    client
        .query_opt(
            LOAD_NODE_CHECKPOINT_SQL,
            &[&pipeline_id, &generation, &key.node_id],
        )
        .await?
        .map(checkpoint_from_row)
        .transpose()
}

/// Locks and verifies the pipeline fence inside the caller's apply transaction.
pub async fn lock_pipeline_fence(
    transaction: &Transaction<'_>,
    expected: PipelineFence,
) -> Result<(), CheckpointError> {
    let expected_generation = database_generation(expected.topology_generation)?;
    validate_fence(expected)?;
    let pipeline_id = expected.pipeline_id.as_uuid();
    let Some(row) = transaction
        .query_opt(LOCK_PIPELINE_FENCE_SQL, &[&pipeline_id])
        .await?
    else {
        return Err(CheckpointError::MissingFence);
    };
    let actual_generation = generation_from_row(&row, "topology_generation")?;
    let actual_token = row.try_get("fencing_token")?;
    if expected_generation == row.try_get::<_, i64>("topology_generation")?
        && expected.fencing_token == actual_token
    {
        Ok(())
    } else {
        Err(CheckpointError::StaleFence {
            expected_generation: expected.topology_generation,
            expected_token: expected.fencing_token,
            actual_generation,
            actual_token,
        })
    }
}

/// Advances a per-node checkpoint inside the transaction that applied its data.
///
/// This function locks the pipeline fence and existing node row. Its success is
/// not durable until the caller commits the transaction.
pub async fn advance_node_checkpoint(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    proposed: &NodeCheckpoint,
) -> Result<AdvanceOutcome, CheckpointError> {
    validate_checkpoint(proposed)?;
    if proposed.key.pipeline_id != fence.pipeline_id
        || proposed.key.topology_generation != fence.topology_generation
    {
        return Err(CheckpointError::FenceKeyMismatch);
    }
    lock_pipeline_fence(transaction, fence).await?;

    let generation = database_generation(proposed.key.topology_generation)?;
    let pipeline_id = proposed.key.pipeline_id.as_uuid();
    let existing = transaction
        .query_opt(
            LOCK_NODE_CHECKPOINT_SQL,
            &[&pipeline_id, &generation, &proposed.key.node_id],
        )
        .await?
        .map(checkpoint_from_row)
        .transpose()?;

    let outcome = check_advance(existing.as_ref(), proposed)?;
    match outcome {
        AdvanceOutcome::Inserted => {
            let system_identifier = proposed.system_identifier.to_string();
            let timeline = i64::from(proposed.timeline);
            let applied_lsn = proposed.applied_lsn.to_string();
            let written = transaction
                .execute(
                    INSERT_NODE_CHECKPOINT_SQL,
                    &[
                        &pipeline_id,
                        &generation,
                        &proposed.key.node_id,
                        &system_identifier,
                        &timeline,
                        &proposed.slot_name,
                        &applied_lsn,
                        &fence.fencing_token,
                    ],
                )
                .await?;
            ensure_one_row(written)?;
        }
        AdvanceOutcome::Unchanged | AdvanceOutcome::Advanced { .. } => {
            let applied_lsn = proposed.applied_lsn.to_string();
            let written = transaction
                .execute(
                    UPDATE_NODE_CHECKPOINT_SQL,
                    &[
                        &pipeline_id,
                        &generation,
                        &proposed.key.node_id,
                        &applied_lsn,
                        &fence.fencing_token,
                    ],
                )
                .await?;
            ensure_one_row(written)?;
        }
    }
    Ok(outcome)
}

/// Pure monotonicity and source-identity check used before a checkpoint write.
pub fn check_advance(
    current: Option<&StoredNodeCheckpoint>,
    proposed: &NodeCheckpoint,
) -> Result<AdvanceOutcome, CheckpointError> {
    validate_checkpoint(proposed)?;
    let Some(current) = current else {
        return Ok(AdvanceOutcome::Inserted);
    };
    if current.checkpoint.key != proposed.key
        || current.checkpoint.system_identifier != proposed.system_identifier
        || current.checkpoint.timeline != proposed.timeline
        || current.checkpoint.slot_name != proposed.slot_name
    {
        return Err(CheckpointError::SourceIdentityChanged);
    }
    match proposed.applied_lsn.cmp(&current.checkpoint.applied_lsn) {
        std::cmp::Ordering::Less => Err(CheckpointError::LsnRegression {
            current: current.checkpoint.applied_lsn,
            proposed: proposed.applied_lsn,
        }),
        std::cmp::Ordering::Equal => Ok(AdvanceOutcome::Unchanged),
        std::cmp::Ordering::Greater => Ok(AdvanceOutcome::Advanced {
            previous_lsn: current.checkpoint.applied_lsn,
        }),
    }
}

fn validate_fence(fence: PipelineFence) -> Result<(), CheckpointError> {
    database_generation(fence.topology_generation)?;
    if fence.fencing_token <= 0 {
        return Err(CheckpointError::InvalidFencingToken);
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: &NodeCheckpoint) -> Result<(), CheckpointError> {
    database_generation(checkpoint.key.topology_generation)?;
    if checkpoint.timeline == 0 {
        return Err(CheckpointError::InvalidTimeline);
    }
    if checkpoint.slot_name.is_empty() || checkpoint.slot_name.contains('\0') {
        return Err(CheckpointError::InvalidSlotName);
    }
    Ok(())
}

fn database_generation(generation: u64) -> Result<i64, CheckpointError> {
    i64::try_from(generation).map_err(|_| CheckpointError::GenerationOutOfRange(generation))
}

fn generation_from_row(row: &Row, column: &'static str) -> Result<u64, CheckpointError> {
    let value: i64 = row.try_get(column)?;
    u64::try_from(value).map_err(|_| CheckpointError::InvalidPersistedValue {
        field: column,
        value: value.to_string(),
    })
}

fn checkpoint_from_row(row: Row) -> Result<StoredNodeCheckpoint, CheckpointError> {
    let generation = generation_from_row(&row, "topology_generation")?;
    let system_identifier = parse_persisted::<u64>(&row, "system_identifier")?;
    let timeline_raw: i64 = row.try_get("timeline")?;
    let timeline =
        u32::try_from(timeline_raw).map_err(|_| CheckpointError::InvalidPersistedValue {
            field: "timeline",
            value: timeline_raw.to_string(),
        })?;
    let lsn_text: String = row.try_get("applied_lsn")?;
    let applied_lsn =
        PgLsn::from_str(&lsn_text).map_err(|_| CheckpointError::InvalidPersistedValue {
            field: "applied_lsn",
            value: lsn_text,
        })?;
    let pipeline_id = PipelineId::from_uuid(row.try_get("pipeline_id")?);
    Ok(StoredNodeCheckpoint {
        checkpoint: NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id,
                topology_generation: generation,
                node_id: row.try_get("node_id")?,
            },
            system_identifier,
            timeline,
            slot_name: row.try_get("slot_name")?,
            applied_lsn,
        },
        fencing_token: row.try_get("fencing_token")?,
    })
}

fn parse_persisted<T>(row: &Row, column: &'static str) -> Result<T, CheckpointError>
where
    T: std::str::FromStr,
{
    let value: String = row.try_get(column)?;
    value
        .parse()
        .map_err(|_| CheckpointError::InvalidPersistedValue {
            field: column,
            value,
        })
}

fn ensure_one_row(written: u64) -> Result<(), CheckpointError> {
    if written == 1 {
        Ok(())
    } else {
        Err(CheckpointError::UnexpectedWriteCount(written))
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn checkpoint(lsn: u64) -> NodeCheckpoint {
        NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: PipelineId::from_uuid(Uuid::nil()),
                topology_generation: 7,
                node_id: 2,
            },
            system_identifier: u64::MAX,
            timeline: 3,
            slot_name: "pg2cb_worker_2".into(),
            applied_lsn: PgLsn::new(lsn),
        }
    }

    fn stored(lsn: u64) -> StoredNodeCheckpoint {
        StoredNodeCheckpoint {
            checkpoint: checkpoint(lsn),
            fencing_token: 11,
        }
    }

    #[test]
    fn checkpoint_is_monotonic_per_node_and_generation() {
        assert_eq!(
            check_advance(None, &checkpoint(10)).unwrap(),
            AdvanceOutcome::Inserted
        );
        assert_eq!(
            check_advance(Some(&stored(10)), &checkpoint(10)).unwrap(),
            AdvanceOutcome::Unchanged
        );
        assert_eq!(
            check_advance(Some(&stored(10)), &checkpoint(12)).unwrap(),
            AdvanceOutcome::Advanced {
                previous_lsn: PgLsn::new(10)
            }
        );
        assert!(matches!(
            check_advance(Some(&stored(10)), &checkpoint(9)),
            Err(CheckpointError::LsnRegression { .. })
        ));
    }

    #[test]
    fn identity_change_requires_a_new_topology_generation() {
        let current = stored(10);
        let mut proposed = checkpoint(11);
        proposed.timeline += 1;
        assert!(matches!(
            check_advance(Some(&current), &proposed),
            Err(CheckpointError::SourceIdentityChanged)
        ));
    }

    #[test]
    fn validates_values_before_database_io() {
        let mut invalid = checkpoint(1);
        invalid.timeline = 0;
        assert!(matches!(
            check_advance(None, &invalid),
            Err(CheckpointError::InvalidTimeline)
        ));
        invalid.timeline = 1;
        invalid.slot_name = "bad\0slot".into();
        assert!(matches!(
            check_advance(None, &invalid),
            Err(CheckpointError::InvalidSlotName)
        ));
    }

    #[test]
    fn sql_uses_native_lsn_and_lossless_system_identifier_types() {
        assert!(INSERT_NODE_CHECKPOINT_SQL.contains("$4::numeric"));
        assert!(INSERT_NODE_CHECKPOINT_SQL.contains("$7::pg_lsn"));
        assert!(LOCK_PIPELINE_FENCE_SQL.ends_with("FOR UPDATE"));
    }
}
