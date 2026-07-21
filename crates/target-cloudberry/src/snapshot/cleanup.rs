//! Fail-closed cleanup for interrupted snapshot loads and retained quarantines.
//!
//! Cleanup is deliberately separate from activation.  Both paths use the same target transaction
//! and pipeline fence, but cleanup never infers ownership from a table name alone.  A loading
//! group must have an exact persisted manifest, and a quarantine must still have the exact catalog
//! object identity captured when it was registered.

use std::{collections::HashSet, time::Duration};

use cloudberry_etl_core::{id::PipelineId, schema::QualifiedName};
use tokio_postgres::{Client, Transaction};
use uuid::Uuid;

use crate::{
    checkpoint::{PipelineFence, lock_pipeline_fence},
    migration::TARGET_METADATA_SCHEMA,
    sql::{quote_identifier, quote_qualified_name},
};

use super::{
    ManagedTableRecord, ManagedTableState, RelationState, SnapshotTargetError,
    activation::quarantine_name,
    database_generation, ensure_one_metadata_row, load_relation_state,
    manifest::{self, SnapshotGroupState},
    progress, relation_oid,
};

/// Authority and immutable ownership information required to remove one interrupted load.
///
/// `current_fence` is the lease currently held by the caller. `group_fence` is copied from the
/// persisted manifest and must match it exactly. The current lease may be newer than the group,
/// which is what permits restart recovery after a process crash without allowing an old holder to
/// delete a newer group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotGroupCleanupRequest {
    pub current_fence: PipelineFence,
    pub group_fence: PipelineFence,
    pub snapshot_group_id: Uuid,
}

/// Result of removing a loading group and all of its owned shadows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCleanupOutcome {
    pub snapshot_group_id: Uuid,
    pub dropped_shadows: Vec<QualifiedName>,
}

/// Retention and work-budget controls for quarantine garbage collection.
///
/// `retention = None` is the safe default and makes the operation a no-op. A positive retention
/// is required before any destructive statement can be issued. `max_tables` bounds one
/// transaction so a large historical quarantine cannot monopolize the target database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuarantineGcPolicy {
    pub retention: Option<Duration>,
    pub max_tables: u32,
}

impl Default for QuarantineGcPolicy {
    fn default() -> Self {
        Self {
            retention: None,
            max_tables: 100,
        }
    }
}

impl QuarantineGcPolicy {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            retention: None,
            max_tables: 100,
        }
    }

    pub fn enabled(retention: Duration, max_tables: u32) -> Result<Self, SnapshotTargetError> {
        let policy = Self {
            retention: Some(retention),
            max_tables,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(self) -> Result<(), SnapshotTargetError> {
        if self.max_tables == 0 {
            return Err(SnapshotTargetError::InvalidQuarantineBatchSize);
        }
        if self.retention.is_some_and(|retention| retention.is_zero()) {
            return Err(SnapshotTargetError::InvalidQuarantineRetention);
        }
        Ok(())
    }

    fn retention_micros(self) -> Result<Option<i64>, SnapshotTargetError> {
        self.validate()?;
        let Some(retention) = self.retention else {
            return Ok(None);
        };
        let micros = retention.as_micros();
        let micros =
            i64::try_from(micros).map_err(|_| SnapshotTargetError::InvalidQuarantineRetention)?;
        Ok(Some(micros.max(1)))
    }
}

/// Result of one quarantine GC pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantineGcOutcome {
    pub disabled: bool,
    pub dropped: Vec<QualifiedName>,
}

/// Removes every shadow registered by a loading snapshot group.
///
/// All ownership checks happen before the first `DROP TABLE`. Any mismatch, missing metadata,
/// changed relation OID, or unexpected extra shadow aborts the transaction and leaves every object
/// untouched. The manifest is removed only after all physical drops and metadata deletes succeed.
pub async fn cleanup_loading_snapshot_group(
    client: &mut Client,
    request: SnapshotGroupCleanupRequest,
) -> Result<SnapshotCleanupOutcome, SnapshotTargetError> {
    validate_cleanup_request(request)?;

    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, request.current_fence).await?;
    let outcome = cleanup_loading_snapshot_group_locked(&transaction, request).await?;
    transaction.commit().await?;
    Ok(outcome)
}

/// Discovers and removes every loading group made stale by the current fence.
///
/// Discovery, validation, all physical drops, and manifest deletion run in one transaction under
/// the exact current pipeline fence. A loading group owned by the current generation and token is
/// never selected because it may still be an in-flight snapshot owned by this process.
pub async fn cleanup_stale_snapshot_groups(
    client: &mut Client,
    current_fence: PipelineFence,
) -> Result<Vec<SnapshotCleanupOutcome>, SnapshotTargetError> {
    if current_fence.fencing_token <= 0 {
        return Err(crate::checkpoint::CheckpointError::InvalidFencingToken.into());
    }
    let generation = database_generation(current_fence.topology_generation)?;
    let pipeline_id = current_fence.pipeline_id.as_uuid();
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, current_fence).await?;

    let stale_sql = format!(
        "SELECT snapshot_group_id, topology_generation, fencing_token FROM {}.snapshot_groups WHERE pipeline_id = $1 AND state = 'loading' AND (topology_generation < $2 OR (topology_generation = $2 AND fencing_token < $3)) ORDER BY topology_generation, fencing_token, snapshot_group_id FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let rows = transaction
        .query(
            &stale_sql,
            &[&pipeline_id, &generation, &current_fence.fencing_token],
        )
        .await?;
    let mut outcomes = Vec::with_capacity(rows.len());
    for row in rows {
        let snapshot_group_id: Uuid = row.try_get("snapshot_group_id")?;
        let topology_raw: i64 = row.try_get("topology_generation")?;
        let topology_generation = u64::try_from(topology_raw)
            .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))?;
        let group_fence = PipelineFence {
            pipeline_id: current_fence.pipeline_id,
            topology_generation,
            fencing_token: row.try_get("fencing_token")?,
        };
        let request = SnapshotGroupCleanupRequest {
            current_fence,
            group_fence,
            snapshot_group_id,
        };
        validate_cleanup_request(request)?;
        outcomes.push(cleanup_loading_snapshot_group_locked(&transaction, request).await?);
    }
    transaction.commit().await?;
    Ok(outcomes)
}

async fn cleanup_loading_snapshot_group_locked(
    transaction: &Transaction<'_>,
    request: SnapshotGroupCleanupRequest,
) -> Result<SnapshotCleanupOutcome, SnapshotTargetError> {
    let stored = manifest::load_snapshot_group(transaction, request.snapshot_group_id).await?;

    validate_group_fence(&stored.request.fence, request)?;
    if stored.state != SnapshotGroupState::Loading {
        return Err(SnapshotTargetError::SnapshotGroupNotLoading(
            request.snapshot_group_id,
        ));
    }

    let expected_shadows = stored
        .request
        .tables
        .iter()
        .map(|table| table.shadow.clone())
        .collect::<HashSet<_>>();
    let mut shadows = Vec::with_capacity(expected_shadows.len());
    let mut progress_identities = Vec::with_capacity(expected_shadows.len());

    for table in &stored.request.tables {
        let state = load_relation_state(transaction, &table.shadow).await?;
        let record = match state {
            RelationState::Managed(record) => record,
            // Registration precedes per-table COPY, so an interrupted group may legitimately
            // have no committed shadow yet for some manifest entries.
            RelationState::Vacant => continue,
            RelationState::ManagedObjectMissing(_) => {
                return Err(SnapshotTargetError::ManagedRelationMissing(
                    table.shadow.to_string(),
                ));
            }
            RelationState::Unmanaged => {
                return Err(SnapshotTargetError::ExistingUnmanagedTable(
                    table.shadow.to_string(),
                ));
            }
        };
        validate_shadow_ownership(
            &table.shadow,
            &record,
            request.snapshot_group_id,
            &stored.request.fence,
            table.source_relation_id,
            table.table_generation,
            &table.schema_fingerprint,
        )?;
        validate_relation_identity(transaction, &table.shadow, &record).await?;
        let shadow_relation_oid = record.relation_oid.ok_or_else(|| {
            SnapshotTargetError::MissingRelationIdentity(table.shadow.to_string())
        })?;
        progress_identities.push(progress::CompletedSnapshotTableIdentity {
            table: table.clone(),
            shadow_relation_oid,
        });
        shadows.push(table.shadow.clone());
    }

    // A manifest with an unlisted shadow is corrupt. Refuse to guess whether it is safe to drop.
    let pipeline_id = request.current_fence.pipeline_id.as_uuid();
    let extra_sql = format!(
        "SELECT target_schema, target_table FROM {}.managed_tables WHERE pipeline_id = $1 AND snapshot_group_id = $2 AND state = 'shadow' FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let extra_rows = transaction
        .query(&extra_sql, &[&pipeline_id, &request.snapshot_group_id])
        .await?;
    for row in extra_rows {
        let name = QualifiedName::new(
            row.try_get::<_, String>("target_schema")?,
            row.try_get::<_, String>("target_table")?,
        )
        .map_err(crate::schema::SchemaError::from)?;
        if !expected_shadows.contains(&name) {
            return Err(SnapshotTargetError::UnexpectedSnapshotShadow(
                name.to_string(),
            ));
        }
    }

    if stored.snapshot_progress_version == progress::SNAPSHOT_PROGRESS_VERSION {
        progress::delete_loading_snapshot_group_progress(
            transaction,
            &stored.request,
            &progress_identities,
        )
        .await?;
    } else {
        progress::delete_loading_snapshot_group_progress(transaction, &stored.request, &[]).await?;
    }

    for shadow in &shadows {
        transaction
            .batch_execute(&format!("DROP TABLE {}", quote_qualified_name(shadow)?))
            .await?;
        let delete_sql = format!(
            "DELETE FROM {}.managed_tables WHERE target_schema = $1 AND target_table = $2 AND pipeline_id = $3 AND snapshot_group_id = $4 AND state = 'shadow'",
            quote_identifier(TARGET_METADATA_SCHEMA)?
        );
        let deleted = transaction
            .execute(
                &delete_sql,
                &[
                    &shadow.schema,
                    &shadow.name,
                    &pipeline_id,
                    &request.snapshot_group_id,
                ],
            )
            .await?;
        ensure_one_metadata_row(deleted)?;
    }

    let group_id = request.snapshot_group_id;
    let delete_tables = format!(
        "DELETE FROM {}.snapshot_group_tables WHERE snapshot_group_id = $1",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let deleted_tables = transaction.execute(&delete_tables, &[&group_id]).await?;
    if deleted_tables != stored.request.tables.len() as u64 {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(group_id));
    }

    let delete_nodes = format!(
        "DELETE FROM {}.snapshot_group_nodes WHERE snapshot_group_id = $1",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let deleted_nodes = transaction.execute(&delete_nodes, &[&group_id]).await?;
    if deleted_nodes != stored.request.initial_checkpoints.len() as u64 {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(group_id));
    }

    let delete_group = format!(
        "DELETE FROM {}.snapshot_groups WHERE snapshot_group_id = $1 AND pipeline_id = $2 AND topology_generation = $3 AND fencing_token = $4 AND state = 'loading'",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let generation = database_generation(stored.request.fence.topology_generation)?;
    let deleted_group = transaction
        .execute(
            &delete_group,
            &[
                &group_id,
                &pipeline_id,
                &generation,
                &stored.request.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_metadata_row(deleted_group)?;

    Ok(SnapshotCleanupOutcome {
        snapshot_group_id: group_id,
        dropped_shadows: shadows,
    })
}

/// Garbage-collects eligible quarantines for one pipeline.
///
/// The default policy is disabled. When enabled, each candidate is checked against its immutable
/// reconciliation record, exact managed-table ownership, and catalog OID before the table is
/// dropped. The audit record is retained and marked with the purging fence.
pub async fn garbage_collect_quarantined_tables(
    client: &mut Client,
    fence: PipelineFence,
    policy: QuarantineGcPolicy,
) -> Result<QuarantineGcOutcome, SnapshotTargetError> {
    let Some(retention_micros) = policy.retention_micros()? else {
        return Ok(QuarantineGcOutcome {
            disabled: true,
            dropped: Vec::new(),
        });
    };
    if fence.fencing_token <= 0 {
        return Err(crate::checkpoint::CheckpointError::InvalidFencingToken.into());
    }

    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let pipeline_id = fence.pipeline_id.as_uuid();
    let limit = i64::from(policy.max_tables);
    let candidate_sql = format!(
        "SELECT snapshot_group_id, original_schema, original_table, quarantine_schema, quarantine_table, quarantine_relation_oid, pipeline_id, topology_generation, source_relation_id, previous_snapshot_group_id, table_generation, schema_fingerprint, previous_fencing_token, fencing_token FROM {}.snapshot_reconciliation_log WHERE pipeline_id = $1 AND purged_at IS NULL AND quarantine_relation_oid IS NOT NULL AND previous_fencing_token IS NOT NULL AND recorded_at <= clock_timestamp() - ($2::bigint * interval '1 microsecond') ORDER BY recorded_at, snapshot_group_id, original_schema, original_table LIMIT $3 FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let rows = transaction
        .query(&candidate_sql, &[&pipeline_id, &retention_micros, &limit])
        .await?;
    let mut dropped = Vec::with_capacity(rows.len());

    for row in rows {
        let record = quarantine_record_from_row(&row)?;
        validate_quarantine_record(&record, fence)?;

        let old_record = ManagedTableRecord {
            pipeline_id: record.pipeline_id,
            snapshot_group_id: record.previous_snapshot_group_id,
            relation_oid: Some(record.relation_oid),
            source_relation_id: record.source_relation_id,
            table_generation: record.table_generation,
            schema_fingerprint: record.schema_fingerprint.clone(),
            state: ManagedTableState::Quarantined,
            fencing_token: record.previous_fencing_token,
        };
        let expected_name = quarantine_name(&record.original, &old_record)?;
        if expected_name != record.quarantine {
            return Err(SnapshotTargetError::QuarantineRecordMismatch(
                record.quarantine.to_string(),
            ));
        }

        let state = load_relation_state(&transaction, &record.quarantine).await?;
        let managed = match state {
            RelationState::Managed(record) if record.state == ManagedTableState::Quarantined => {
                record
            }
            RelationState::ManagedObjectMissing(_)
            | RelationState::Vacant
            | RelationState::Unmanaged => {
                return Err(SnapshotTargetError::MissingQuarantineMetadata(
                    record.quarantine.to_string(),
                ));
            }
            RelationState::Managed(_) => {
                return Err(SnapshotTargetError::QuarantineMetadataMismatch(
                    record.quarantine.to_string(),
                ));
            }
        };
        if managed.pipeline_id != record.pipeline_id
            || managed.snapshot_group_id != record.previous_snapshot_group_id
            || managed.source_relation_id != record.source_relation_id
            || managed.table_generation != record.table_generation
            || managed.schema_fingerprint != record.schema_fingerprint
            || managed.fencing_token != record.fencing_token
            || managed.relation_oid != Some(record.relation_oid)
        {
            return Err(SnapshotTargetError::QuarantineMetadataMismatch(
                record.quarantine.to_string(),
            ));
        }
        validate_relation_identity(&transaction, &record.quarantine, &managed).await?;

        let duplicate_sql = format!(
            "SELECT count(*)::bigint FROM {}.snapshot_reconciliation_log WHERE quarantine_schema = $1 AND quarantine_table = $2 AND purged_at IS NULL",
            quote_identifier(TARGET_METADATA_SCHEMA)?
        );
        let duplicate_count: i64 = transaction
            .query_one(
                &duplicate_sql,
                &[&record.quarantine.schema, &record.quarantine.name],
            )
            .await?
            .try_get(0)?;
        if duplicate_count != 1 {
            return Err(SnapshotTargetError::QuarantineRecordMismatch(
                record.quarantine.to_string(),
            ));
        }

        transaction
            .batch_execute(&format!(
                "DROP TABLE {}",
                quote_qualified_name(&record.quarantine)?
            ))
            .await?;
        let delete_metadata = format!(
            "DELETE FROM {}.managed_tables WHERE target_schema = $1 AND target_table = $2 AND pipeline_id = $3 AND state = 'quarantined' AND relation_oid = $4",
            quote_identifier(TARGET_METADATA_SCHEMA)?
        );
        let deleted = transaction
            .execute(
                &delete_metadata,
                &[
                    &record.quarantine.schema,
                    &record.quarantine.name,
                    &pipeline_id,
                    &managed.relation_oid,
                ],
            )
            .await?;
        ensure_one_metadata_row(deleted)?;

        let mark_purged = format!(
            "UPDATE {}.snapshot_reconciliation_log SET purged_at = clock_timestamp(), purged_by_fencing_token = $6 WHERE snapshot_group_id = $1 AND original_schema = $2 AND original_table = $3 AND quarantine_schema = $4 AND quarantine_table = $5 AND purged_at IS NULL",
            quote_identifier(TARGET_METADATA_SCHEMA)?
        );
        let marked = transaction
            .execute(
                &mark_purged,
                &[
                    &record.snapshot_group_id,
                    &record.original.schema,
                    &record.original.name,
                    &record.quarantine.schema,
                    &record.quarantine.name,
                    &fence.fencing_token,
                ],
            )
            .await?;
        ensure_one_metadata_row(marked)?;
        dropped.push(record.quarantine);
    }

    transaction.commit().await?;
    Ok(QuarantineGcOutcome {
        disabled: false,
        dropped,
    })
}

fn validate_cleanup_request(
    request: SnapshotGroupCleanupRequest,
) -> Result<(), SnapshotTargetError> {
    if request.snapshot_group_id.is_nil()
        || request.current_fence.fencing_token <= 0
        || request.group_fence.fencing_token <= 0
        || request.current_fence.pipeline_id != request.group_fence.pipeline_id
        || request.current_fence.topology_generation < request.group_fence.topology_generation
        || request.current_fence.fencing_token < request.group_fence.fencing_token
    {
        return Err(SnapshotTargetError::InvalidSnapshotCleanupFence);
    }
    Ok(())
}

fn validate_group_fence(
    actual: &PipelineFence,
    request: SnapshotGroupCleanupRequest,
) -> Result<(), SnapshotTargetError> {
    if actual == &request.group_fence {
        return Ok(());
    }
    Err(SnapshotTargetError::SnapshotGroupFenceMismatch {
        group: request.snapshot_group_id,
        expected_generation: request.group_fence.topology_generation,
        expected_token: request.group_fence.fencing_token,
        actual_generation: actual.topology_generation,
        actual_token: actual.fencing_token,
    })
}

fn validate_shadow_ownership(
    table: &QualifiedName,
    record: &ManagedTableRecord,
    group_id: Uuid,
    group_fence: &PipelineFence,
    source_relation_id: u32,
    table_generation: u64,
    schema_fingerprint: &str,
) -> Result<(), SnapshotTargetError> {
    if record.pipeline_id != group_fence.pipeline_id {
        return Err(SnapshotTargetError::ManagedByOtherPipeline {
            table: table.to_string(),
            expected: group_fence.pipeline_id,
            actual: record.pipeline_id,
        });
    }
    if record.snapshot_group_id != Some(group_id)
        || record.source_relation_id != source_relation_id
        || record.table_generation != table_generation
        || record.schema_fingerprint != schema_fingerprint
        || record.state != ManagedTableState::Shadow
        || record.fencing_token != group_fence.fencing_token
    {
        return Err(SnapshotTargetError::SnapshotTableManifestMismatch {
            group: group_id,
            table: table.to_string(),
        });
    }
    if record.relation_oid.is_none() {
        return Err(SnapshotTargetError::MissingRelationIdentity(
            table.to_string(),
        ));
    }
    Ok(())
}

async fn validate_relation_identity(
    transaction: &Transaction<'_>,
    table: &QualifiedName,
    record: &ManagedTableRecord,
) -> Result<(), SnapshotTargetError> {
    let expected = record
        .relation_oid
        .ok_or_else(|| SnapshotTargetError::MissingRelationIdentity(table.to_string()))?;
    let actual = relation_oid(transaction, table)
        .await?
        .ok_or_else(|| SnapshotTargetError::ManagedRelationMissing(table.to_string()))?;
    if actual != expected {
        return Err(SnapshotTargetError::RelationIdentityMismatch {
            table: table.to_string(),
            expected,
            actual,
        });
    }
    Ok(())
}

#[derive(Debug)]
struct QuarantineRecord {
    snapshot_group_id: Uuid,
    original: QualifiedName,
    quarantine: QualifiedName,
    relation_oid: i64,
    pipeline_id: PipelineId,
    topology_generation: u64,
    source_relation_id: u32,
    previous_snapshot_group_id: Option<Uuid>,
    table_generation: u64,
    schema_fingerprint: String,
    previous_fencing_token: i64,
    fencing_token: i64,
}

fn quarantine_record_from_row(
    row: &tokio_postgres::Row,
) -> Result<QuarantineRecord, SnapshotTargetError> {
    let snapshot_group_id: Uuid = row.try_get("snapshot_group_id")?;
    let original = QualifiedName::new(
        row.try_get::<_, String>("original_schema")?,
        row.try_get::<_, String>("original_table")?,
    )
    .map_err(crate::schema::SchemaError::from)?;
    let quarantine = QualifiedName::new(
        row.try_get::<_, String>("quarantine_schema")?,
        row.try_get::<_, String>("quarantine_table")?,
    )
    .map_err(crate::schema::SchemaError::from)?;
    let source_raw: i64 = row.try_get("source_relation_id")?;
    let generation_raw: i64 = row.try_get("table_generation")?;
    let topology_raw: i64 = row.try_get("topology_generation")?;
    let topology_generation = u64::try_from(topology_raw)
        .map_err(|_| SnapshotTargetError::QuarantineRecordMismatch(quarantine.to_string()))?;
    let source_relation_id = u32::try_from(source_raw)
        .map_err(|_| SnapshotTargetError::QuarantineRecordMismatch(quarantine.to_string()))?;
    let table_generation = u64::try_from(generation_raw)
        .map_err(|_| SnapshotTargetError::QuarantineRecordMismatch(quarantine.to_string()))?;
    let schema_fingerprint: String = row.try_get("schema_fingerprint")?;
    let previous_fencing_token: Option<i64> = row.try_get("previous_fencing_token")?;
    let fencing_token: i64 = row.try_get("fencing_token")?;
    let relation_oid: Option<i64> = row.try_get("quarantine_relation_oid")?;
    let Some(relation_oid) = relation_oid.filter(|oid| *oid > 0) else {
        return Err(SnapshotTargetError::MissingRelationIdentity(
            quarantine.to_string(),
        ));
    };
    let Some(previous_fencing_token) = previous_fencing_token.filter(|token| *token > 0) else {
        return Err(SnapshotTargetError::QuarantineRecordMismatch(
            quarantine.to_string(),
        ));
    };
    if snapshot_group_id.is_nil() || schema_fingerprint.is_empty() || fencing_token <= 0 {
        return Err(SnapshotTargetError::QuarantineRecordMismatch(
            quarantine.to_string(),
        ));
    }
    Ok(QuarantineRecord {
        snapshot_group_id,
        original,
        quarantine,
        relation_oid,
        pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
        topology_generation,
        source_relation_id,
        previous_snapshot_group_id: row.try_get("previous_snapshot_group_id")?,
        table_generation,
        schema_fingerprint,
        previous_fencing_token,
        fencing_token,
    })
}

fn validate_quarantine_record(
    record: &QuarantineRecord,
    fence: PipelineFence,
) -> Result<(), SnapshotTargetError> {
    if record.pipeline_id != fence.pipeline_id
        || record.topology_generation > fence.topology_generation
        || record.fencing_token > fence.fencing_token
    {
        return Err(SnapshotTargetError::InvalidSnapshotCleanupFence);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence(generation: u64, token: i64) -> PipelineFence {
        PipelineFence {
            pipeline_id: PipelineId::from_uuid(Uuid::from_u128(1)),
            topology_generation: generation,
            fencing_token: token,
        }
    }

    #[test]
    fn quarantine_gc_is_disabled_by_default_and_rejects_unsafe_limits() {
        assert_eq!(
            QuarantineGcPolicy::default().retention_micros().unwrap(),
            None
        );
        assert!(matches!(
            QuarantineGcPolicy::enabled(Duration::ZERO, 1),
            Err(SnapshotTargetError::InvalidQuarantineRetention)
        ));
        assert!(matches!(
            QuarantineGcPolicy::enabled(Duration::from_secs(1), 0),
            Err(SnapshotTargetError::InvalidQuarantineBatchSize)
        ));
        assert_eq!(
            QuarantineGcPolicy::enabled(Duration::from_secs(30), 25)
                .unwrap()
                .retention_micros()
                .unwrap(),
            Some(30_000_000)
        );
    }

    #[test]
    fn cleanup_authority_must_dominate_the_exact_group_fence() {
        let valid = SnapshotGroupCleanupRequest {
            current_fence: fence(3, 9),
            group_fence: fence(2, 8),
            snapshot_group_id: Uuid::from_u128(2),
        };
        assert!(validate_cleanup_request(valid).is_ok());

        let stale_authority = SnapshotGroupCleanupRequest {
            current_fence: fence(2, 7),
            ..valid
        };
        assert!(matches!(
            validate_cleanup_request(stale_authority),
            Err(SnapshotTargetError::InvalidSnapshotCleanupFence)
        ));
    }

    #[test]
    fn cleanup_group_fence_comparison_is_field_exact() {
        let request = SnapshotGroupCleanupRequest {
            current_fence: fence(3, 9),
            group_fence: fence(2, 8),
            snapshot_group_id: Uuid::from_u128(2),
        };
        assert!(validate_group_fence(&request.group_fence, request).is_ok());
        assert!(matches!(
            validate_group_fence(&fence(2, 7), request),
            Err(SnapshotTargetError::SnapshotGroupFenceMismatch { .. })
        ));
    }
}
