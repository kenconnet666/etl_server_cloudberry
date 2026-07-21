//! Durable per-table progress for bounded snapshot COPY pages.
//!
//! A cursor is the source primary key in ordinal order, encoded with PostgreSQL's canonical text
//! output functions.  [`copy_snapshot_page`] owns both the target COPY sink and the progress
//! update inside a caller-owned transaction, so committing one without the other is impossible.
//! Persisted progress may be reused only after the caller has proved that its source snapshot and
//! slot boundary are valid for the registered group; this target module does not make that source
//! recovery decision.

use std::fmt::Display;

use bytes::Bytes;
use cloudberry_etl_core::{id::PipelineId, schema::QualifiedName};
use futures::Stream;
use sha2::{Digest, Sha256};
use tokio_postgres::{Row, Transaction};
use uuid::Uuid;

use crate::checkpoint::{PipelineFence, lock_pipeline_fence};

use super::{
    ManagedTableState, RelationState, SnapshotActivationRequest, SnapshotActivationTable,
    SnapshotOwnership, SnapshotTargetError, SnapshotTargetPlan, database_generation,
    ensure_one_metadata_row, load_relation_state, manifest, relation_oid, validate_managed_fence,
    validate_managed_identity,
};

pub const SNAPSHOT_CURSOR_FORMAT_VERSION: u16 = 1;
pub(super) const SNAPSHOT_PROGRESS_VERSION: u16 = 1;

pub const LOCK_SNAPSHOT_TABLE_PROGRESS_SQL: &str = r#"
SELECT snapshot_group_id, target_schema, target_table, shadow_schema, shadow_table,
       pipeline_id, topology_generation, shadow_relation_oid, source_relation_id,
       table_generation, schema_fingerprint, cursor_format_version, primary_key_arity,
       cursor_values, cursor_digest, completed, pages_copied, rows_copied, fencing_token
FROM pg2cb_meta.snapshot_table_progress
WHERE snapshot_group_id = $1 AND target_schema = $2 AND target_table = $3
FOR UPDATE
"#;

pub const LOCK_SNAPSHOT_GROUP_PROGRESS_SQL: &str = r#"
SELECT snapshot_group_id, target_schema, target_table, shadow_schema, shadow_table,
       pipeline_id, topology_generation, shadow_relation_oid, source_relation_id,
       table_generation, schema_fingerprint, cursor_format_version, primary_key_arity,
       cursor_values, cursor_digest, completed, pages_copied, rows_copied, fencing_token
FROM pg2cb_meta.snapshot_table_progress
WHERE snapshot_group_id = $1
ORDER BY target_schema, target_table
FOR UPDATE
"#;

pub const INSERT_SNAPSHOT_TABLE_PROGRESS_SQL: &str = r#"
INSERT INTO pg2cb_meta.snapshot_table_progress (
    snapshot_group_id, target_schema, target_table, shadow_schema, shadow_table,
    pipeline_id, topology_generation, shadow_relation_oid, source_relation_id,
    table_generation, schema_fingerprint, cursor_format_version, primary_key_arity,
    cursor_values, cursor_digest, completed, pages_copied, rows_copied, fencing_token
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
        $14, $15, false, 0, 0, $16)
"#;

pub const ADVANCE_SNAPSHOT_TABLE_PROGRESS_SQL: &str = r#"
UPDATE pg2cb_meta.snapshot_table_progress
SET cursor_values = $4,
    cursor_digest = $5,
    completed = $6,
    pages_copied = $7,
    rows_copied = $8,
    updated_at = clock_timestamp(),
    completed_at = CASE WHEN $6 THEN clock_timestamp() ELSE NULL END
WHERE snapshot_group_id = $1 AND target_schema = $2 AND target_table = $3
  AND shadow_relation_oid = $9
  AND cursor_values = $10 AND cursor_digest = $11
  AND completed = false AND pages_copied = $12 AND rows_copied = $13
  AND fencing_token = $14
"#;

const DELETE_SNAPSHOT_PROGRESS_FOR_SHADOW_SQL: &str = r#"
DELETE FROM pg2cb_meta.snapshot_table_progress
WHERE snapshot_group_id = $1 AND shadow_relation_oid = $2
"#;

const DELETE_SNAPSHOT_GROUP_PROGRESS_SQL: &str = r#"
DELETE FROM pg2cb_meta.snapshot_table_progress
WHERE snapshot_group_id = $1
"#;

/// Durable state for one table in a registered snapshot group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotTableProgress {
    pub fence: PipelineFence,
    pub snapshot_group_id: Uuid,
    pub target: QualifiedName,
    pub shadow: QualifiedName,
    pub shadow_relation_oid: i64,
    pub source_relation_id: u32,
    pub table_generation: u64,
    pub schema_fingerprint: String,
    pub cursor_format_version: u16,
    pub primary_key_arity: usize,
    pub cursor: Vec<String>,
    pub cursor_digest: [u8; 32],
    pub completed: bool,
    pub pages_copied: u64,
    pub rows_copied: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SnapshotPageApplyOutcome {
    Applied(SnapshotTableProgress),
    ResumeAt(SnapshotTableProgress),
    AlreadyCompleted(SnapshotTableProgress),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotProgressIdentity {
    fence: PipelineFence,
    snapshot_group_id: Uuid,
    target: QualifiedName,
    shadow: QualifiedName,
    shadow_relation_oid: i64,
    source_relation_id: u32,
    table_generation: u64,
    schema_fingerprint: String,
    primary_key_arity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CompletedSnapshotTableIdentity {
    pub(super) table: SnapshotActivationTable,
    pub(super) shadow_relation_oid: i64,
}

/// Registers an empty progress row after the exact managed shadow has been created.
///
/// Re-registration accepts only byte-for-byte equivalent identity under the same active fence.
/// Fence adoption is a separate group-wide operation so a caller cannot accidentally combine old
/// progress with only a subset of newly-owned shadows.
pub async fn register_snapshot_table_progress(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    lock_or_register_progress(transaction, plan, ownership).await
}

/// Copies one bounded page and advances its durable cursor in the same caller-owned transaction.
///
/// `expected_cursor` is the cursor used to derive the source range.  A lost commit response is
/// resolved before COPY: an already-advanced row returns [`SnapshotPageApplyOutcome::ResumeAt`]
/// without sending any bytes.  `completed` must mean the source session proved that this range is
/// the fixed tail of its snapshot.
pub async fn copy_snapshot_page<S, E>(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
    expected_cursor: &[String],
    next_cursor: Vec<String>,
    completed: bool,
    stream: S,
) -> Result<SnapshotPageApplyOutcome, SnapshotTargetError>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Display,
{
    validate_cursor_values(expected_cursor)?;
    validate_cursor_values(&next_cursor)?;

    let current = lock_or_register_progress(transaction, plan, ownership).await?;
    validate_progress_contract(&current)?;
    if current.completed {
        return Ok(SnapshotPageApplyOutcome::AlreadyCompleted(current));
    }
    if current.cursor != expected_cursor {
        return Ok(SnapshotPageApplyOutcome::ResumeAt(current));
    }
    validate_page_intent(&current, &next_cursor, completed)?;

    let sink = transaction.copy_in(&plan.copy_sql).await?;
    let mut sink = std::pin::pin!(sink);
    super::forward_chunks(stream, &mut sink).await?;
    let copied_rows = sink
        .finish()
        .await
        .map_err(|error| SnapshotTargetError::CopySink(error.to_string()))?;
    let advanced = advance_progress(&current, next_cursor, copied_rows, completed)?;
    persist_advanced_progress(transaction, &current, &advanced).await?;
    Ok(SnapshotPageApplyOutcome::Applied(advanced))
}

pub(super) async fn complete_full_snapshot_copy(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
    copied_rows: u64,
) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    let current = lock_or_register_progress(transaction, plan, ownership).await?;
    validate_progress_contract(&current)?;
    if current.completed
        || current.pages_copied != 0
        || current.rows_copied != 0
        || !current.cursor.is_empty()
    {
        return Err(SnapshotTargetError::SnapshotProgressNotFresh(
            current.target.to_string(),
        ));
    }
    let advanced = advance_progress(&current, Vec::new(), copied_rows, true)?;
    persist_advanced_progress(transaction, &current, &advanced).await?;
    Ok(advanced)
}

pub(super) async fn lock_completed_snapshot_tables(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
    expected: &[CompletedSnapshotTableIdentity],
) -> Result<(), SnapshotTargetError> {
    let progress = lock_validate_snapshot_group_progress(transaction, request, expected).await?;
    for table in progress {
        if !table.completed {
            return Err(SnapshotTargetError::SnapshotProgressIncomplete(
                table.target.to_string(),
            ));
        }
    }
    Ok(())
}

pub(super) async fn delete_loading_snapshot_group_progress(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
    expected: &[CompletedSnapshotTableIdentity],
) -> Result<(), SnapshotTargetError> {
    lock_validate_snapshot_group_progress(transaction, request, expected).await?;
    let deleted = transaction
        .execute(
            DELETE_SNAPSHOT_GROUP_PROGRESS_SQL,
            &[&request.snapshot_group_id],
        )
        .await?;
    if deleted == expected.len() as u64 {
        Ok(())
    } else {
        Err(SnapshotTargetError::UnexpectedMetadataWriteCount(deleted))
    }
}

async fn lock_validate_snapshot_group_progress(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
    expected: &[CompletedSnapshotTableIdentity],
) -> Result<Vec<SnapshotTableProgress>, SnapshotTargetError> {
    if request.tables.len() != expected.len() {
        // Cleanup intentionally supplies only manifest tables whose shadows exist. Activation
        // supplies the complete manifest. Both lists must still be exact and internally ordered;
        // the database row count below rejects missing or extra progress in either mode.
        if expected.len() > request.tables.len() {
            return Err(SnapshotTargetError::IncompleteSnapshotProgress(
                request.snapshot_group_id,
            ));
        }
    }
    let rows = transaction
        .query(
            LOCK_SNAPSHOT_GROUP_PROGRESS_SQL,
            &[&request.snapshot_group_id],
        )
        .await?;
    if rows.len() != expected.len() {
        return Err(SnapshotTargetError::IncompleteSnapshotProgress(
            request.snapshot_group_id,
        ));
    }
    let mut validated = Vec::with_capacity(rows.len());
    for (expected, row) in expected.iter().zip(&rows) {
        let table = &expected.table;
        if table != &expected.table {
            return Err(SnapshotTargetError::SnapshotProgressIdentityMismatch {
                table: table.target.to_string(),
                field: "activation_table_order",
            });
        }
        let identity = SnapshotProgressIdentity {
            fence: request.fence,
            snapshot_group_id: request.snapshot_group_id,
            target: table.target.clone(),
            shadow: table.shadow.clone(),
            shadow_relation_oid: expected.shadow_relation_oid,
            source_relation_id: table.source_relation_id,
            table_generation: table.table_generation,
            schema_fingerprint: table.schema_fingerprint.clone(),
            primary_key_arity: 0,
        };
        let progress = progress_from_row(row)?;
        validate_stored_identity(&progress, &identity, false)?;
        validate_progress_contract(&progress)?;
        let physical_primary_key_arity =
            physical_primary_key_arity(transaction, expected.shadow_relation_oid).await?;
        if progress.primary_key_arity != physical_primary_key_arity {
            return Err(SnapshotTargetError::SnapshotProgressIdentityMismatch {
                table: table.target.to_string(),
                field: "physical_primary_key_arity",
            });
        }
        validated.push(progress);
    }
    Ok(validated)
}

async fn physical_primary_key_arity(
    transaction: &Transaction<'_>,
    shadow_relation_oid: i64,
) -> Result<usize, SnapshotTargetError> {
    let row = transaction
        .query_opt(
            "SELECT i.indnkeyatts::integer AS primary_key_arity
               FROM pg_catalog.pg_index AS i
              WHERE i.indrelid = $1::bigint::oid
                AND i.indisprimary",
            &[&shadow_relation_oid],
        )
        .await?;
    let Some(row) = row else {
        return Ok(0);
    };
    let arity: i32 = row.try_get("primary_key_arity")?;
    usize::try_from(arity).map_err(|_| SnapshotTargetError::InvalidSnapshotProgressValue {
        table: shadow_relation_oid.to_string(),
        field: "physical_primary_key_arity",
        value: arity.to_string(),
    })
}

pub(super) async fn delete_snapshot_progress_for_shadow(
    transaction: &Transaction<'_>,
    snapshot_group_id: Option<Uuid>,
    shadow_relation_oid: Option<i64>,
) -> Result<(), SnapshotTargetError> {
    let (Some(snapshot_group_id), Some(shadow_relation_oid)) =
        (snapshot_group_id, shadow_relation_oid)
    else {
        return Ok(());
    };
    if shadow_relation_oid <= 0 {
        return Err(SnapshotTargetError::MissingRelationIdentity(
            shadow_relation_oid.to_string(),
        ));
    }
    let deleted = transaction
        .execute(
            DELETE_SNAPSHOT_PROGRESS_FOR_SHADOW_SQL,
            &[&snapshot_group_id, &shadow_relation_oid],
        )
        .await?;
    if deleted <= 1 {
        Ok(())
    } else {
        Err(SnapshotTargetError::UnexpectedMetadataWriteCount(deleted))
    }
}

async fn lock_or_register_progress(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    let identity = lock_progress_identity(transaction, plan, ownership).await?;
    if let Some(row) = load_progress_locked(transaction, &identity).await? {
        let progress = progress_from_row(&row)?;
        validate_stored_identity(&progress, &identity, true)?;
        validate_progress_contract(&progress)?;
        return Ok(progress);
    }

    let progress = initial_progress(identity)?;
    let generation = database_generation(progress.fence.topology_generation)?;
    let source_relation_id = i64::from(progress.source_relation_id);
    let table_generation = database_generation(progress.table_generation)?;
    let cursor_format_version = i32::from(progress.cursor_format_version);
    let primary_key_arity = database_arity(progress.primary_key_arity)?;
    let pipeline_id = progress.fence.pipeline_id.as_uuid();
    let inserted = transaction
        .execute(
            INSERT_SNAPSHOT_TABLE_PROGRESS_SQL,
            &[
                &progress.snapshot_group_id,
                &progress.target.schema,
                &progress.target.name,
                &progress.shadow.schema,
                &progress.shadow.name,
                &pipeline_id,
                &generation,
                &progress.shadow_relation_oid,
                &source_relation_id,
                &table_generation,
                &progress.schema_fingerprint,
                &cursor_format_version,
                &primary_key_arity,
                &progress.cursor,
                &&progress.cursor_digest[..],
                &progress.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_metadata_row(inserted)?;
    Ok(progress)
}

async fn lock_progress_identity(
    transaction: &Transaction<'_>,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<SnapshotProgressIdentity, SnapshotTargetError> {
    super::validate_ownership(ownership)?;
    lock_pipeline_fence(transaction, ownership.fence).await?;
    let stored = manifest::load_snapshot_group(transaction, ownership.snapshot_group_id).await?;
    if stored.snapshot_progress_version != SNAPSHOT_PROGRESS_VERSION {
        return Err(SnapshotTargetError::UnsupportedSnapshotProgressVersion {
            group: ownership.snapshot_group_id,
            version: stored.snapshot_progress_version,
        });
    }
    manifest::validate_apply_membership(&stored, plan, ownership)?;
    if stored.state != manifest::SnapshotGroupState::Loading {
        return Err(SnapshotTargetError::SnapshotGroupAlreadyActive(
            ownership.snapshot_group_id,
        ));
    }

    // Match activation's target-before-shadow managed-row lock order.
    let target = load_relation_state(transaction, &plan.target).await?;
    super::validate_target_for_load(&target, plan, ownership)?;
    let shadow = load_relation_state(transaction, &plan.shadow.target).await?;
    let RelationState::Managed(record) = shadow else {
        return Err(SnapshotTargetError::IncompleteShadow(
            plan.shadow.target.to_string(),
        ));
    };
    validate_managed_identity(
        &plan.shadow.target,
        &record,
        ownership.fence.pipeline_id,
        plan.source_relation_id,
    )?;
    validate_managed_fence(&plan.shadow.target, &record, ownership.fence.fencing_token)?;
    if record.state != ManagedTableState::Shadow
        || record.snapshot_group_id != Some(ownership.snapshot_group_id)
        || record.table_generation != plan.source_generation
        || record.schema_fingerprint != ownership.schema_fingerprint
        || record.fencing_token != ownership.fence.fencing_token
    {
        return Err(SnapshotTargetError::SnapshotProgressIdentityMismatch {
            table: plan.target.to_string(),
            field: "managed_shadow",
        });
    }
    let shadow_relation_oid = record.relation_oid.ok_or_else(|| {
        SnapshotTargetError::MissingRelationIdentity(plan.shadow.target.to_string())
    })?;
    let actual_relation_oid = relation_oid(transaction, &plan.shadow.target)
        .await?
        .unwrap_or_default();
    if shadow_relation_oid <= 0 || actual_relation_oid != shadow_relation_oid {
        return Err(SnapshotTargetError::RelationIdentityMismatch {
            table: plan.shadow.target.to_string(),
            expected: shadow_relation_oid,
            actual: actual_relation_oid,
        });
    }
    Ok(SnapshotProgressIdentity {
        fence: ownership.fence,
        snapshot_group_id: ownership.snapshot_group_id,
        target: plan.target.clone(),
        shadow: plan.shadow.target.clone(),
        shadow_relation_oid,
        source_relation_id: plan.source_relation_id,
        table_generation: plan.source_generation,
        schema_fingerprint: ownership.schema_fingerprint.clone(),
        primary_key_arity: plan.shadow.primary_key.len(),
    })
}

async fn load_progress_locked(
    transaction: &Transaction<'_>,
    identity: &SnapshotProgressIdentity,
) -> Result<Option<Row>, SnapshotTargetError> {
    Ok(transaction
        .query_opt(
            LOCK_SNAPSHOT_TABLE_PROGRESS_SQL,
            &[
                &identity.snapshot_group_id,
                &identity.target.schema,
                &identity.target.name,
            ],
        )
        .await?)
}

fn initial_progress(
    identity: SnapshotProgressIdentity,
) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    let cursor = Vec::new();
    let cursor_digest = cursor_digest(
        identity.primary_key_arity,
        &identity.schema_fingerprint,
        &cursor,
    )?;
    Ok(SnapshotTableProgress {
        fence: identity.fence,
        snapshot_group_id: identity.snapshot_group_id,
        target: identity.target,
        shadow: identity.shadow,
        shadow_relation_oid: identity.shadow_relation_oid,
        source_relation_id: identity.source_relation_id,
        table_generation: identity.table_generation,
        schema_fingerprint: identity.schema_fingerprint,
        cursor_format_version: SNAPSHOT_CURSOR_FORMAT_VERSION,
        primary_key_arity: identity.primary_key_arity,
        cursor,
        cursor_digest,
        completed: false,
        pages_copied: 0,
        rows_copied: 0,
    })
}

fn progress_from_row(row: &Row) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    let target = QualifiedName::new(
        row.try_get::<_, String>("target_schema")?,
        row.try_get::<_, String>("target_table")?,
    )
    .map_err(crate::schema::SchemaError::from)?;
    let shadow = QualifiedName::new(
        row.try_get::<_, String>("shadow_schema")?,
        row.try_get::<_, String>("shadow_table")?,
    )
    .map_err(crate::schema::SchemaError::from)?;
    let topology_generation = persisted_u64(row, &target, "topology_generation")?;
    let source_relation_id_raw: i64 = row.try_get("source_relation_id")?;
    let source_relation_id = u32::try_from(source_relation_id_raw).map_err(|_| {
        invalid_progress_value(&target, "source_relation_id", source_relation_id_raw)
    })?;
    let table_generation = persisted_u64(row, &target, "table_generation")?;
    let cursor_format_raw: i32 = row.try_get("cursor_format_version")?;
    let cursor_format_version = u16::try_from(cursor_format_raw)
        .map_err(|_| invalid_progress_value(&target, "cursor_format_version", cursor_format_raw))?;
    let primary_key_arity = persisted_usize(row, &target, "primary_key_arity")?;
    let cursor: Vec<String> = row.try_get("cursor_values")?;
    let digest: Vec<u8> = row.try_get("cursor_digest")?;
    let cursor_digest = digest.try_into().map_err(|value: Vec<u8>| {
        invalid_progress_value(&target, "cursor_digest_length", value.len())
    })?;
    let pages_copied = persisted_u64(row, &target, "pages_copied")?;
    let rows_copied = persisted_u64(row, &target, "rows_copied")?;
    let fencing_token: i64 = row.try_get("fencing_token")?;
    if fencing_token <= 0 {
        return Err(invalid_progress_value(
            &target,
            "fencing_token",
            fencing_token,
        ));
    }
    let shadow_relation_oid: i64 = row.try_get("shadow_relation_oid")?;
    if shadow_relation_oid <= 0 {
        return Err(invalid_progress_value(
            &target,
            "shadow_relation_oid",
            shadow_relation_oid,
        ));
    }
    Ok(SnapshotTableProgress {
        fence: PipelineFence {
            pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
            topology_generation,
            fencing_token,
        },
        snapshot_group_id: row.try_get("snapshot_group_id")?,
        target,
        shadow,
        shadow_relation_oid,
        source_relation_id,
        table_generation,
        schema_fingerprint: row.try_get("schema_fingerprint")?,
        cursor_format_version,
        primary_key_arity,
        cursor,
        cursor_digest,
        completed: row.try_get("completed")?,
        pages_copied,
        rows_copied,
    })
}

fn validate_stored_identity(
    stored: &SnapshotTableProgress,
    expected: &SnapshotProgressIdentity,
    validate_arity: bool,
) -> Result<(), SnapshotTargetError> {
    let mismatch = if stored.snapshot_group_id != expected.snapshot_group_id {
        Some("snapshot_group_id")
    } else if stored.target != expected.target {
        Some("target")
    } else if stored.shadow != expected.shadow {
        Some("shadow")
    } else if stored.fence.pipeline_id != expected.fence.pipeline_id {
        Some("pipeline_id")
    } else if stored.fence.topology_generation != expected.fence.topology_generation {
        Some("topology_generation")
    } else if stored.shadow_relation_oid != expected.shadow_relation_oid {
        Some("shadow_relation_oid")
    } else if stored.source_relation_id != expected.source_relation_id {
        Some("source_relation_id")
    } else if stored.table_generation != expected.table_generation {
        Some("table_generation")
    } else if stored.schema_fingerprint != expected.schema_fingerprint {
        Some("schema_fingerprint")
    } else if stored.fence.fencing_token != expected.fence.fencing_token {
        Some("fencing_token")
    } else if validate_arity && stored.primary_key_arity != expected.primary_key_arity {
        Some("primary_key_arity")
    } else {
        None
    };
    if let Some(field) = mismatch {
        Err(SnapshotTargetError::SnapshotProgressIdentityMismatch {
            table: expected.target.to_string(),
            field,
        })
    } else {
        Ok(())
    }
}

fn validate_progress_contract(progress: &SnapshotTableProgress) -> Result<(), SnapshotTargetError> {
    if progress.cursor_format_version != SNAPSHOT_CURSOR_FORMAT_VERSION {
        return Err(invalid_progress_value(
            &progress.target,
            "cursor_format_version",
            progress.cursor_format_version,
        ));
    }
    validate_cursor_arity(progress.primary_key_arity, &progress.cursor)?;
    let expected = cursor_digest(
        progress.primary_key_arity,
        &progress.schema_fingerprint,
        &progress.cursor,
    )?;
    if progress.cursor_digest != expected {
        return Err(SnapshotTargetError::SnapshotCursorDigestMismatch(
            progress.target.to_string(),
        ));
    }
    if progress.completed && progress.pages_copied == 0 {
        return Err(invalid_progress_value(
            &progress.target,
            "completed_pages_copied",
            progress.pages_copied,
        ));
    }
    Ok(())
}

fn validate_page_intent(
    current: &SnapshotTableProgress,
    next_cursor: &[String],
    completed: bool,
) -> Result<(), SnapshotTargetError> {
    validate_cursor_arity(current.primary_key_arity, next_cursor)?;
    if !completed && current.primary_key_arity == 0 {
        return Err(SnapshotTargetError::SnapshotPaginationRequiresPrimaryKey(
            current.target.to_string(),
        ));
    }
    if !completed && current.cursor == next_cursor {
        return Err(SnapshotTargetError::SnapshotCursorDidNotAdvance(
            current.target.to_string(),
        ));
    }
    Ok(())
}

fn advance_progress(
    current: &SnapshotTableProgress,
    next_cursor: Vec<String>,
    copied_rows: u64,
    completed: bool,
) -> Result<SnapshotTableProgress, SnapshotTargetError> {
    validate_page_intent(current, &next_cursor, completed)?;
    if copied_rows == 0 && !completed {
        return Err(SnapshotTargetError::EmptyIncompleteSnapshotPage(
            current.target.to_string(),
        ));
    }
    if copied_rows == 0 && current.cursor != next_cursor {
        return Err(SnapshotTargetError::SnapshotCursorAdvancedWithoutRows(
            current.target.to_string(),
        ));
    }
    let pages_copied = current.pages_copied.checked_add(1).ok_or_else(|| {
        SnapshotTargetError::SnapshotProgressCounterOverflow(current.target.to_string())
    })?;
    let rows_copied = current
        .rows_copied
        .checked_add(copied_rows)
        .ok_or_else(|| {
            SnapshotTargetError::SnapshotProgressCounterOverflow(current.target.to_string())
        })?;
    database_count("pages_copied", pages_copied)?;
    database_count("rows_copied", rows_copied)?;
    let mut advanced = current.clone();
    advanced.cursor_digest = cursor_digest(
        advanced.primary_key_arity,
        &advanced.schema_fingerprint,
        &next_cursor,
    )?;
    advanced.cursor = next_cursor;
    advanced.completed = completed;
    advanced.pages_copied = pages_copied;
    advanced.rows_copied = rows_copied;
    Ok(advanced)
}

async fn persist_advanced_progress(
    transaction: &Transaction<'_>,
    previous: &SnapshotTableProgress,
    advanced: &SnapshotTableProgress,
) -> Result<(), SnapshotTargetError> {
    let pages_copied = database_count("pages_copied", advanced.pages_copied)?;
    let rows_copied = database_count("rows_copied", advanced.rows_copied)?;
    let previous_pages = database_count("pages_copied", previous.pages_copied)?;
    let previous_rows = database_count("rows_copied", previous.rows_copied)?;
    let written = transaction
        .execute(
            ADVANCE_SNAPSHOT_TABLE_PROGRESS_SQL,
            &[
                &advanced.snapshot_group_id,
                &advanced.target.schema,
                &advanced.target.name,
                &advanced.cursor,
                &&advanced.cursor_digest[..],
                &advanced.completed,
                &pages_copied,
                &rows_copied,
                &advanced.shadow_relation_oid,
                &previous.cursor,
                &&previous.cursor_digest[..],
                &previous_pages,
                &previous_rows,
                &advanced.fence.fencing_token,
            ],
        )
        .await?;
    if written == 1 {
        Ok(())
    } else {
        Err(SnapshotTargetError::SnapshotProgressChanged(
            advanced.target.to_string(),
        ))
    }
}

fn cursor_digest(
    primary_key_arity: usize,
    schema_fingerprint: &str,
    cursor: &[String],
) -> Result<[u8; 32], SnapshotTargetError> {
    validate_cursor_arity(primary_key_arity, cursor)?;
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"pg2cb-snapshot-cursor-v1");
    hash_field(&mut hasher, &SNAPSHOT_CURSOR_FORMAT_VERSION.to_be_bytes());
    let arity = u64::try_from(primary_key_arity)
        .map_err(|_| SnapshotTargetError::SnapshotCursorArityOutOfRange(primary_key_arity))?;
    hash_field(&mut hasher, &arity.to_be_bytes());
    hash_field(&mut hasher, schema_fingerprint.as_bytes());
    for value in cursor {
        hash_field(&mut hasher, value.as_bytes());
    }
    Ok(hasher.finalize().into())
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn validate_cursor_values(cursor: &[String]) -> Result<(), SnapshotTargetError> {
    if cursor.iter().any(|value| value.contains('\0')) {
        Err(SnapshotTargetError::InvalidSnapshotCursorValue)
    } else {
        Ok(())
    }
}

fn validate_cursor_arity(
    primary_key_arity: usize,
    cursor: &[String],
) -> Result<(), SnapshotTargetError> {
    validate_cursor_values(cursor)?;
    if cursor.is_empty() || cursor.len() == primary_key_arity {
        Ok(())
    } else {
        Err(SnapshotTargetError::SnapshotCursorArityMismatch {
            expected: primary_key_arity,
            actual: cursor.len(),
        })
    }
}

fn persisted_u64(
    row: &Row,
    target: &QualifiedName,
    field: &'static str,
) -> Result<u64, SnapshotTargetError> {
    let raw: i64 = row.try_get(field)?;
    u64::try_from(raw).map_err(|_| invalid_progress_value(target, field, raw))
}

fn persisted_usize(
    row: &Row,
    target: &QualifiedName,
    field: &'static str,
) -> Result<usize, SnapshotTargetError> {
    let raw: i32 = row.try_get(field)?;
    usize::try_from(raw).map_err(|_| invalid_progress_value(target, field, raw))
}

fn invalid_progress_value(
    target: &QualifiedName,
    field: &'static str,
    value: impl ToString,
) -> SnapshotTargetError {
    SnapshotTargetError::InvalidSnapshotProgressValue {
        table: target.to_string(),
        field,
        value: value.to_string(),
    }
}

fn database_count(field: &'static str, value: u64) -> Result<i64, SnapshotTargetError> {
    i64::try_from(value)
        .map_err(|_| SnapshotTargetError::SnapshotProgressValueOutOfRange { field, value })
}

fn database_arity(value: usize) -> Result<i32, SnapshotTargetError> {
    i32::try_from(value).map_err(|_| SnapshotTargetError::SnapshotCursorArityOutOfRange(value))
}

#[cfg(test)]
mod tests {
    use cloudberry_etl_core::id::PipelineId;

    use super::*;

    fn progress() -> SnapshotTableProgress {
        let cursor = vec!["tenant-a".to_owned(), "10".to_owned()];
        SnapshotTableProgress {
            fence: PipelineFence {
                pipeline_id: PipelineId::new(),
                topology_generation: 3,
                fencing_token: 7,
            },
            snapshot_group_id: Uuid::from_u128(1),
            target: QualifiedName::new("target", "items").unwrap(),
            shadow: QualifiedName::new("target", "items_shadow").unwrap(),
            shadow_relation_oid: 16_384,
            source_relation_id: 42,
            table_generation: 9,
            schema_fingerprint: "sha256:items-v9".to_owned(),
            cursor_format_version: SNAPSHOT_CURSOR_FORMAT_VERSION,
            primary_key_arity: 2,
            cursor_digest: cursor_digest(2, "sha256:items-v9", &cursor).unwrap(),
            cursor,
            completed: false,
            pages_copied: 1,
            rows_copied: 100,
        }
    }

    #[test]
    fn cursor_digest_binds_format_arity_schema_and_each_value() {
        let original = cursor_digest(2, "sha256:v1", &["a".into(), "10".into()]).unwrap();
        assert_ne!(
            original,
            cursor_digest(2, "sha256:v2", &["a".into(), "10".into()]).unwrap()
        );
        assert_ne!(
            original,
            cursor_digest(2, "sha256:v1", &["a".into(), "11".into()]).unwrap()
        );
        assert!(matches!(
            cursor_digest(2, "sha256:v1", &["a".into()]),
            Err(SnapshotTargetError::SnapshotCursorArityMismatch {
                expected: 2,
                actual: 1
            })
        ));
    }

    #[test]
    fn bounded_pages_require_progress_and_real_rows() {
        let current = progress();
        assert!(matches!(
            advance_progress(&current, current.cursor.clone(), 10, false),
            Err(SnapshotTargetError::SnapshotCursorDidNotAdvance(_))
        ));
        assert!(matches!(
            advance_progress(&current, vec!["tenant-a".into(), "20".into()], 0, false,),
            Err(SnapshotTargetError::EmptyIncompleteSnapshotPage(_))
        ));
        assert!(matches!(
            advance_progress(&current, vec!["tenant-a".into(), "20".into()], 0, true,),
            Err(SnapshotTargetError::SnapshotCursorAdvancedWithoutRows(_))
        ));
    }

    #[test]
    fn final_page_advances_exact_counters_and_marks_completion() {
        let current = progress();
        let advanced =
            advance_progress(&current, vec!["tenant-b".into(), "4".into()], 25, true).unwrap();
        assert!(advanced.completed);
        assert_eq!(advanced.pages_copied, 2);
        assert_eq!(advanced.rows_copied, 125);
        assert!(validate_progress_contract(&advanced).is_ok());
    }

    #[test]
    fn empty_tail_can_complete_without_inventing_a_cursor() {
        let current = progress();
        let cursor = current.cursor.clone();
        let advanced = advance_progress(&current, cursor, 0, true).unwrap();
        assert!(advanced.completed);
        assert_eq!(advanced.pages_copied, 2);
        assert_eq!(advanced.rows_copied, 100);
    }

    #[test]
    fn completion_contract_rejects_zero_page_and_changed_digest() {
        let mut value = progress();
        value.completed = true;
        value.pages_copied = 0;
        assert!(matches!(
            validate_progress_contract(&value),
            Err(SnapshotTargetError::InvalidSnapshotProgressValue {
                field: "completed_pages_copied",
                ..
            })
        ));
        value.pages_copied = 1;
        value.cursor_digest[0] ^= 0xff;
        assert!(matches!(
            validate_progress_contract(&value),
            Err(SnapshotTargetError::SnapshotCursorDigestMismatch(_))
        ));
    }
}
