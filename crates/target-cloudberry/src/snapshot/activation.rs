//! Atomic activation of a complete snapshot group.

use std::{collections::HashSet, fmt::Write as _};

use cloudberry_etl_core::schema::QualifiedName;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Transaction};
use uuid::Uuid;

use crate::{
    checkpoint::{
        advance_node_checkpoint, check_advance, checkpoint_reached, load_node_checkpoint_locked,
        lock_pipeline_fence,
    },
    migration::TARGET_METADATA_SCHEMA,
    sql::{quote_identifier, quote_qualified_name},
};

use super::{
    ManagedTableRecord, ManagedTableState, RelationState, SnapshotActivationDisposition,
    SnapshotActivationOutcome, SnapshotActivationRequest, SnapshotActivationTable,
    SnapshotTargetError, database_generation, ensure_one_metadata_row, load_relation_state,
    manifest, matches_activation, progress, validate_managed_fence, validate_managed_identity,
};

#[derive(Debug)]
enum TableActivationState {
    Pending {
        shadow: ManagedTableRecord,
        previous_active: Option<ManagedTableRecord>,
    },
    Active {
        target: ManagedTableRecord,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconciliationReason {
    Replaced,
    Stale,
}

impl ReconciliationReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Replaced => "replaced",
            Self::Stale => "stale",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaleActiveTable {
    target: QualifiedName,
    record: ManagedTableRecord,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuarantinePlan {
    original: QualifiedName,
    quarantine: QualifiedName,
    record: ManagedTableRecord,
    reason: ReconciliationReason,
}

fn group_is_active(states: &[TableActivationState]) -> Result<bool, SnapshotTargetError> {
    let active_count = states
        .iter()
        .filter(|state| matches!(state, TableActivationState::Active { .. }))
        .count();
    if active_count == states.len() {
        Ok(true)
    } else if active_count == 0 {
        Ok(false)
    } else {
        Err(SnapshotTargetError::MixedActivationState)
    }
}

/// Atomically promotes a complete group of snapshot shadows and initializes every node checkpoint.
///
/// The same request is safe to retry after an unknown commit result. A retry succeeds only when
/// every table is already active with exact ownership and every checkpoint has reached the
/// supplied slot consistent point.
pub async fn activate_snapshot_group(
    client: &mut Client,
    request: &SnapshotActivationRequest,
) -> Result<SnapshotActivationOutcome, SnapshotTargetError> {
    let canonical = manifest::canonical_request(request)?;
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, canonical.fence).await?;
    let stored = manifest::load_snapshot_group(&transaction, canonical.snapshot_group_id).await?;
    let request = manifest::validate_exact_request(&stored, &canonical)?;
    let table_order = (0..request.tables.len()).collect::<Vec<_>>();
    let checkpoint_order = (0..request.initial_checkpoints.len()).collect::<Vec<_>>();

    let mut states = Vec::with_capacity(table_order.len());
    for &index in &table_order {
        let table = &request.tables[index];
        let target = load_relation_state(&transaction, &table.target).await?;
        let shadow = load_relation_state(&transaction, &table.shadow).await?;
        states.push(classify_table(&request, table, target, shadow)?);
    }

    let stale = load_stale_active_tables(&transaction, &request).await?;
    if stored.snapshot_progress_version == progress::SNAPSHOT_PROGRESS_VERSION {
        let progress_identities = activation_progress_identities(&request.tables, &states)?;
        progress::lock_completed_snapshot_tables(&transaction, &request, &progress_identities)
            .await?;
    }
    let mut reserved_names = request
        .tables
        .iter()
        .flat_map(|table| [table.target.clone(), table.shadow.clone()])
        .collect::<HashSet<_>>();
    // Keep a stale original name reserved as well as its future quarantine name. This prevents
    // a pathological deterministic quarantine name from colliding with another source table.
    reserved_names.extend(stale.iter().map(|table| table.target.clone()));

    if group_is_active(&states)? {
        if stored.state != manifest::SnapshotGroupState::Active {
            return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
                request.snapshot_group_id,
            ));
        }
        verify_retry_checkpoints(&transaction, &request, &checkpoint_order).await?;
        let stale_plans = preflight_stale_quarantines(
            &transaction,
            stale,
            &mut reserved_names,
            request.fence.fencing_token,
        )
        .await?;
        let quarantined = apply_stale_quarantines(&transaction, &request, &stale_plans).await?;
        transaction.commit().await?;
        return Ok(SnapshotActivationOutcome {
            disposition: SnapshotActivationDisposition::AlreadyActive,
            quarantined,
        });
    }
    if stored.state != manifest::SnapshotGroupState::Loading {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
            request.snapshot_group_id,
        ));
    }
    // Lock and validate checkpoint rows before the first DDL mutation. This keeps all expected
    // failures on the read-only side of the transaction.
    for &index in &checkpoint_order {
        let checkpoint = &request.initial_checkpoints[index];
        let current = load_node_checkpoint_locked(&transaction, checkpoint.key).await?;
        check_advance(current.as_ref(), checkpoint)?;
    }

    let mut quarantines = Vec::with_capacity(states.len());

    // Preflight deterministic quarantine names for the whole group.
    for (position, &index) in table_order.iter().enumerate() {
        let table = &request.tables[index];
        let TableActivationState::Pending {
            previous_active, ..
        } = &states[position]
        else {
            unreachable!("mixed activation states were rejected")
        };
        if let Some(previous_active) = previous_active {
            let quarantine = quarantine_name(&table.target, previous_active)?;
            if !reserved_names.insert(quarantine.clone())
                || !matches!(
                    load_relation_state(&transaction, &quarantine).await?,
                    RelationState::Vacant
                )
            {
                return Err(SnapshotTargetError::QuarantineNameConflict(
                    quarantine.to_string(),
                ));
            }
            quarantines.push(Some(quarantine));
        } else {
            quarantines.push(None);
        }
    }

    let stale_plans = preflight_stale_quarantines(
        &transaction,
        stale,
        &mut reserved_names,
        request.fence.fencing_token,
    )
    .await?;

    let mut quarantined = Vec::new();
    for (position, &index) in table_order.iter().enumerate() {
        let table = &request.tables[index];
        let TableActivationState::Pending {
            shadow,
            previous_active,
        } = &states[position]
        else {
            unreachable!("mixed activation states were rejected")
        };

        if let (Some(previous_active), Some(quarantine)) =
            (previous_active, quarantines[position].as_ref())
        {
            transaction
                .batch_execute(&rename_table_sql(&table.target, &quarantine.name)?)
                .await?;
            relocate_metadata(
                &transaction,
                &table.target,
                quarantine,
                previous_active,
                ManagedTableState::Quarantined,
                request.fence.fencing_token,
            )
            .await?;
            record_reconciliation(
                &transaction,
                &request,
                &table.target,
                quarantine,
                previous_active,
                ReconciliationReason::Replaced,
            )
            .await?;
            quarantined.push(quarantine.clone());
        }

        promote_shadow(&transaction, table).await?;
        relocate_metadata(
            &transaction,
            &table.shadow,
            &table.target,
            shadow,
            ManagedTableState::Active,
            request.fence.fencing_token,
        )
        .await?;
    }

    quarantined.extend(apply_stale_quarantines(&transaction, &request, &stale_plans).await?);

    for &index in &checkpoint_order {
        advance_node_checkpoint(
            &transaction,
            request.fence,
            &request.initial_checkpoints[index],
        )
        .await?;
    }

    manifest::mark_snapshot_group_active(&transaction, request.snapshot_group_id).await?;

    transaction.commit().await?;
    Ok(SnapshotActivationOutcome {
        disposition: SnapshotActivationDisposition::Activated,
        quarantined,
    })
}

async fn load_stale_active_tables(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
) -> Result<Vec<StaleActiveTable>, SnapshotTargetError> {
    let sql = format!(
        "SELECT target_schema, target_table, pipeline_id, snapshot_group_id, relation_oid, source_relation_id, table_generation, schema_fingerprint, state, fencing_token FROM {}.managed_tables WHERE pipeline_id = $1 AND state = 'active' ORDER BY target_schema, target_table FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let pipeline_id = request.fence.pipeline_id.as_uuid();
    let rows = transaction.query(&sql, &[&pipeline_id]).await?;
    let mut stale = Vec::new();
    for row in rows {
        let schema: String = row.try_get("target_schema")?;
        let name: String = row.try_get("target_table")?;
        let target = QualifiedName::new(schema, name).map_err(crate::schema::SchemaError::from)?;
        if request.tables.iter().any(|table| table.target == target) {
            continue;
        }
        let record = super::managed_table_record_from_row(&target, &row)?;
        validate_managed_fence(&target, &record, request.fence.fencing_token)?;
        if !super::relation_exists(transaction, &target).await? {
            return Err(SnapshotTargetError::ManagedRelationMissing(
                target.to_string(),
            ));
        }
        stale.push(StaleActiveTable { target, record });
    }
    Ok(stale)
}

async fn preflight_stale_quarantines(
    transaction: &Transaction<'_>,
    stale: Vec<StaleActiveTable>,
    reserved_names: &mut HashSet<QualifiedName>,
    fencing_token: i64,
) -> Result<Vec<QuarantinePlan>, SnapshotTargetError> {
    let mut plans = Vec::with_capacity(stale.len());
    for stale in stale {
        validate_managed_fence(&stale.target, &stale.record, fencing_token)?;
        let quarantine = quarantine_name(&stale.target, &stale.record)?;
        if !reserved_names.insert(quarantine.clone())
            || !matches!(
                load_relation_state(transaction, &quarantine).await?,
                RelationState::Vacant
            )
        {
            return Err(SnapshotTargetError::QuarantineNameConflict(
                quarantine.to_string(),
            ));
        }
        plans.push(QuarantinePlan {
            original: stale.target,
            quarantine,
            record: stale.record,
            reason: ReconciliationReason::Stale,
        });
    }
    Ok(plans)
}

async fn apply_stale_quarantines(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
    plans: &[QuarantinePlan],
) -> Result<Vec<QualifiedName>, SnapshotTargetError> {
    let mut quarantined = Vec::with_capacity(plans.len());
    for plan in plans {
        transaction
            .batch_execute(&rename_table_sql(&plan.original, &plan.quarantine.name)?)
            .await?;
        relocate_metadata(
            transaction,
            &plan.original,
            &plan.quarantine,
            &plan.record,
            ManagedTableState::Quarantined,
            request.fence.fencing_token,
        )
        .await?;
        record_reconciliation(
            transaction,
            request,
            &plan.original,
            &plan.quarantine,
            &plan.record,
            plan.reason,
        )
        .await?;
        quarantined.push(plan.quarantine.clone());
    }
    Ok(quarantined)
}

async fn record_reconciliation(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
    original: &QualifiedName,
    quarantine: &QualifiedName,
    record: &ManagedTableRecord,
    reason: ReconciliationReason,
) -> Result<(), SnapshotTargetError> {
    let sql = format!(
        "INSERT INTO {}.snapshot_reconciliation_log (snapshot_group_id, original_schema, original_table, quarantine_schema, quarantine_table, quarantine_relation_oid, pipeline_id, topology_generation, source_relation_id, previous_snapshot_group_id, table_generation, schema_fingerprint, reason, previous_fencing_token, fencing_token) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let pipeline_id = request.fence.pipeline_id.as_uuid();
    let topology_generation = database_generation(request.fence.topology_generation)?;
    let source_relation_id = i64::from(record.source_relation_id);
    let table_generation = database_generation(record.table_generation)?;
    let reason = reason.as_str();
    let written = transaction
        .execute(
            &sql,
            &[
                &request.snapshot_group_id,
                &original.schema,
                &original.name,
                &quarantine.schema,
                &quarantine.name,
                &record.relation_oid,
                &pipeline_id,
                &topology_generation,
                &source_relation_id,
                &record.snapshot_group_id,
                &table_generation,
                &record.schema_fingerprint,
                &reason,
                &record.fencing_token,
                &request.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_metadata_row(written)
}

/// Validates the unique active group when a later lease resumes WAL consumption.
///
/// The group's original fencing token is immutable provenance. The caller must hold the current
/// (equal or newer) pipeline fence, while the table manifest and initial source boundaries must
/// remain exact.
pub async fn validate_active_snapshot_group(
    client: &mut Client,
    fence: crate::checkpoint::PipelineFence,
    tables: &[SnapshotActivationTable],
) -> Result<Uuid, SnapshotTargetError> {
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let generation = database_generation(fence.topology_generation)?;
    let pipeline_id = fence.pipeline_id.as_uuid();
    let group_sql = format!(
        "SELECT snapshot_group_id FROM {}.snapshot_groups WHERE pipeline_id = $1 AND topology_generation = $2 AND state = 'active' ORDER BY snapshot_group_id FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let groups = transaction
        .query(&group_sql, &[&pipeline_id, &generation])
        .await?;
    let snapshot_group_id = match groups.as_slice() {
        [] => {
            return Err(SnapshotTargetError::MissingActiveSnapshotGroup {
                pipeline: fence.pipeline_id,
                generation: fence.topology_generation,
            });
        }
        [group] => group.try_get("snapshot_group_id")?,
        _ => {
            return Err(SnapshotTargetError::MultipleActiveSnapshotGroups {
                pipeline: fence.pipeline_id,
                generation: fence.topology_generation,
            });
        }
    };
    let stored = manifest::load_snapshot_group(&transaction, snapshot_group_id).await?;
    if stored.state != manifest::SnapshotGroupState::Active
        || stored.request.fence.pipeline_id != fence.pipeline_id
        || stored.request.fence.topology_generation != fence.topology_generation
        || stored.request.fence.fencing_token > fence.fencing_token
    {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
            snapshot_group_id,
        ));
    }

    let expected = manifest::canonical_request(&SnapshotActivationRequest {
        fence,
        snapshot_group_id,
        tables: tables.to_vec(),
        initial_checkpoints: stored.request.initial_checkpoints.clone(),
    })?;
    if expected.tables != stored.request.tables {
        return Err(SnapshotTargetError::SnapshotGroupManifestMismatch(
            snapshot_group_id,
        ));
    }

    let mut states = Vec::with_capacity(expected.tables.len());
    for table in &expected.tables {
        let target = load_relation_state(&transaction, &table.target).await?;
        let shadow = load_relation_state(&transaction, &table.shadow).await?;
        let state = classify_table(&expected, table, target, shadow)?;
        if !matches!(state, TableActivationState::Active { .. }) {
            return Err(SnapshotTargetError::MixedActivationState);
        }
        states.push(state);
    }
    if stored.snapshot_progress_version == progress::SNAPSHOT_PROGRESS_VERSION {
        let progress_identities = activation_progress_identities(&expected.tables, &states)?;
        progress::lock_completed_snapshot_tables(
            &transaction,
            &stored.request,
            &progress_identities,
        )
        .await?;
    }
    for boundary in &stored.request.initial_checkpoints {
        let current = load_node_checkpoint_locked(&transaction, boundary.key).await?;
        if !checkpoint_reached(current.as_ref(), boundary)? {
            return Err(SnapshotTargetError::ActiveCheckpointIncomplete(
                boundary.key.node_id,
            ));
        }
    }
    transaction.commit().await?;
    Ok(snapshot_group_id)
}

fn classify_table(
    request: &SnapshotActivationRequest,
    table: &SnapshotActivationTable,
    target: RelationState,
    shadow: RelationState,
) -> Result<TableActivationState, SnapshotTargetError> {
    let target = match target {
        RelationState::Vacant => None,
        RelationState::Unmanaged => {
            return Err(SnapshotTargetError::ExistingUnmanagedTable(
                table.target.to_string(),
            ));
        }
        RelationState::ManagedObjectMissing(_) => {
            return Err(SnapshotTargetError::ManagedRelationMissing(
                table.target.to_string(),
            ));
        }
        RelationState::Managed(record) => {
            validate_managed_identity(
                &table.target,
                &record,
                request.fence.pipeline_id,
                table.source_relation_id,
            )?;
            validate_managed_fence(&table.target, &record, request.fence.fencing_token)?;
            require_state(&table.target, &record, ManagedTableState::Active)?;
            Some(record)
        }
    };

    let shadow = match shadow {
        RelationState::Vacant => None,
        RelationState::Unmanaged => {
            return Err(SnapshotTargetError::ExistingUnmanagedTable(
                table.shadow.to_string(),
            ));
        }
        RelationState::ManagedObjectMissing(_) => {
            return Err(SnapshotTargetError::ManagedRelationMissing(
                table.shadow.to_string(),
            ));
        }
        RelationState::Managed(record) => {
            validate_managed_identity(
                &table.shadow,
                &record,
                request.fence.pipeline_id,
                table.source_relation_id,
            )?;
            validate_managed_fence(&table.shadow, &record, request.fence.fencing_token)?;
            require_state(&table.shadow, &record, ManagedTableState::Shadow)?;
            if record.snapshot_group_id != Some(request.snapshot_group_id) {
                return Err(SnapshotTargetError::ShadowSnapshotGroupMismatch {
                    table: table.shadow.to_string(),
                    expected: request.snapshot_group_id,
                    actual: record.snapshot_group_id,
                });
            }
            if record.table_generation != table.table_generation {
                return Err(SnapshotTargetError::ShadowGenerationMismatch {
                    table: table.shadow.to_string(),
                    expected: table.table_generation,
                    actual: record.table_generation,
                });
            }
            if record.schema_fingerprint != table.schema_fingerprint {
                return Err(SnapshotTargetError::ShadowFingerprintMismatch(
                    table.shadow.to_string(),
                ));
            }
            Some(record)
        }
    };

    match (target, shadow) {
        (Some(active), None) if matches_activation(&active, table, request.snapshot_group_id) => {
            Ok(TableActivationState::Active { target: active })
        }
        (Some(active), Some(_))
            if matches_activation(&active, table, request.snapshot_group_id) =>
        {
            Err(SnapshotTargetError::IncompleteShadow(
                table.shadow.to_string(),
            ))
        }
        (Some(active), Some(shadow)) => {
            if active.table_generation >= table.table_generation {
                return Err(SnapshotTargetError::ActivationGenerationNotNewer {
                    table: table.target.to_string(),
                    active: active.table_generation,
                    proposed: table.table_generation,
                });
            }
            Ok(TableActivationState::Pending {
                shadow,
                previous_active: Some(active),
            })
        }
        (None, Some(shadow)) => Ok(TableActivationState::Pending {
            shadow,
            previous_active: None,
        }),
        (_, None) => Err(SnapshotTargetError::IncompleteShadow(
            table.shadow.to_string(),
        )),
    }
}

fn activation_progress_identities(
    tables: &[SnapshotActivationTable],
    states: &[TableActivationState],
) -> Result<Vec<progress::CompletedSnapshotTableIdentity>, SnapshotTargetError> {
    if tables.len() != states.len() {
        return Err(SnapshotTargetError::DuplicateActivationIdentity);
    }
    tables
        .iter()
        .zip(states)
        .map(|(table, state)| {
            let relation_oid = match state {
                TableActivationState::Pending { shadow, .. } => shadow.relation_oid,
                TableActivationState::Active { target } => target.relation_oid,
            }
            .ok_or_else(|| SnapshotTargetError::MissingRelationIdentity(table.target.to_string()))?;
            if relation_oid <= 0 {
                return Err(SnapshotTargetError::MissingRelationIdentity(
                    table.target.to_string(),
                ));
            }
            Ok(progress::CompletedSnapshotTableIdentity {
                table: table.clone(),
                shadow_relation_oid: relation_oid,
            })
        })
        .collect()
}

fn require_state(
    table: &QualifiedName,
    record: &ManagedTableRecord,
    expected: ManagedTableState,
) -> Result<(), SnapshotTargetError> {
    if record.state == expected {
        Ok(())
    } else {
        Err(SnapshotTargetError::UnexpectedManagedTableState {
            table: table.to_string(),
            expected: expected.as_str(),
            actual: record.state.as_str().to_owned(),
        })
    }
}

async fn verify_retry_checkpoints(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
    checkpoint_order: &[usize],
) -> Result<(), SnapshotTargetError> {
    for &index in checkpoint_order {
        let checkpoint = &request.initial_checkpoints[index];
        let current = load_node_checkpoint_locked(transaction, checkpoint.key).await?;
        if !checkpoint_reached(current.as_ref(), checkpoint)? {
            return Err(SnapshotTargetError::ActiveCheckpointIncomplete(
                checkpoint.key.node_id,
            ));
        }
    }
    Ok(())
}

pub(super) fn quarantine_name(
    target: &QualifiedName,
    record: &ManagedTableRecord,
) -> Result<QualifiedName, SnapshotTargetError> {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, target.schema.as_bytes());
    hash_field(&mut hasher, target.name.as_bytes());
    hash_field(&mut hasher, record.pipeline_id.as_uuid().as_bytes());
    hash_field(&mut hasher, &record.source_relation_id.to_be_bytes());
    hash_field(&mut hasher, &record.table_generation.to_be_bytes());
    hash_field(&mut hasher, record.schema_fingerprint.as_bytes());
    hash_field(&mut hasher, &record.fencing_token.to_be_bytes());
    let digest = hasher.finalize();
    let mut name = String::with_capacity(40);
    name.push_str("pg2cb_q_");
    for byte in &digest[..16] {
        write!(name, "{byte:02x}").expect("writing to a String cannot fail");
    }
    Ok(QualifiedName::new(&target.schema, name).map_err(crate::schema::SchemaError::from)?)
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

async fn promote_shadow(
    transaction: &Transaction<'_>,
    table: &SnapshotActivationTable,
) -> Result<(), SnapshotTargetError> {
    if table.shadow.name != table.target.name {
        transaction
            .batch_execute(&rename_table_sql(&table.shadow, &table.target.name)?)
            .await?;
    }
    Ok(())
}

fn rename_table_sql(table: &QualifiedName, new_name: &str) -> Result<String, SnapshotTargetError> {
    Ok(format!(
        "ALTER TABLE {} RENAME TO {}",
        quote_qualified_name(table)?,
        quote_identifier(new_name)?
    ))
}

async fn relocate_metadata(
    transaction: &Transaction<'_>,
    from: &QualifiedName,
    to: &QualifiedName,
    record: &ManagedTableRecord,
    state: ManagedTableState,
    fencing_token: i64,
) -> Result<(), SnapshotTargetError> {
    let delete_sql = format!(
        "DELETE FROM {}.managed_tables WHERE target_schema = $1 AND target_table = $2",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let deleted = transaction
        .execute(&delete_sql, &[&from.schema, &from.name])
        .await?;
    ensure_one_metadata_row(deleted)?;

    let insert_sql = format!(
        "INSERT INTO {}.managed_tables (target_schema, target_table, pipeline_id, snapshot_group_id, relation_oid, source_relation_id, table_generation, schema_fingerprint, state, fencing_token) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let pipeline_id = record.pipeline_id.as_uuid();
    let source_relation_id = i64::from(record.source_relation_id);
    let table_generation = database_generation(record.table_generation)?;
    let state = state.as_str();
    let inserted = transaction
        .execute(
            &insert_sql,
            &[
                &to.schema,
                &to.name,
                &pipeline_id,
                &record.snapshot_group_id,
                &record.relation_oid,
                &source_relation_id,
                &table_generation,
                &record.schema_fingerprint,
                &state,
                &fencing_token,
            ],
        )
        .await?;
    ensure_one_metadata_row(inserted)
}

#[cfg(test)]
mod tests {
    use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
    use uuid::Uuid;

    use crate::checkpoint::{CheckpointKey, NodeCheckpoint, PipelineFence};

    use super::*;

    fn pipeline_id() -> PipelineId {
        PipelineId::from_uuid(Uuid::nil())
    }

    fn record(state: ManagedTableState, generation: u64, fingerprint: &str) -> ManagedTableRecord {
        ManagedTableRecord {
            pipeline_id: pipeline_id(),
            snapshot_group_id: Some(Uuid::from_u128(1)),
            relation_oid: None,
            source_relation_id: 42,
            table_generation: generation,
            schema_fingerprint: fingerprint.to_owned(),
            state,
            fencing_token: 11,
        }
    }

    fn table() -> SnapshotActivationTable {
        SnapshotActivationTable {
            target: QualifiedName::new("target", "items").unwrap(),
            shadow: QualifiedName::new("target", "items_shadow").unwrap(),
            source_relation_id: 42,
            table_generation: 7,
            schema_fingerprint: "sha256:new".to_owned(),
        }
    }

    fn request() -> SnapshotActivationRequest {
        let fence = PipelineFence {
            pipeline_id: pipeline_id(),
            topology_generation: 3,
            fencing_token: 12,
        };
        SnapshotActivationRequest {
            fence,
            snapshot_group_id: Uuid::from_u128(1),
            tables: vec![table()],
            initial_checkpoints: vec![NodeCheckpoint {
                key: CheckpointKey {
                    pipeline_id: fence.pipeline_id,
                    topology_generation: fence.topology_generation,
                    node_id: 1,
                },
                system_identifier: 99,
                timeline: 1,
                slot_name: "pg2cb_slot".to_owned(),
                applied_lsn: PgLsn::new(100),
            }],
        }
    }

    #[test]
    fn validates_complete_unique_group_and_orders_locks() {
        let mut valid = request();
        valid.tables.push(SnapshotActivationTable {
            target: QualifiedName::new("a", "second").unwrap(),
            shadow: QualifiedName::new("a", "second_shadow").unwrap(),
            source_relation_id: 43,
            table_generation: 7,
            schema_fingerprint: "sha256:second".to_owned(),
        });
        let canonical = manifest::canonical_request(&valid).unwrap();
        assert_eq!(canonical.tables[0].target.schema, "a");
        assert_eq!(canonical.initial_checkpoints[0].key.node_id, 1);

        valid.tables[1].target = valid.tables[0].target.clone();
        valid.tables[1].shadow = QualifiedName::new("target", "other_shadow").unwrap();
        assert!(matches!(
            manifest::canonical_request(&valid),
            Err(SnapshotTargetError::DuplicateActivationIdentity)
        ));

        let mut zero = request();
        zero.initial_checkpoints[0].applied_lsn = PgLsn::ZERO;
        assert!(matches!(
            manifest::canonical_request(&zero),
            Err(SnapshotTargetError::ZeroInitialCheckpoint(1))
        ));

        let mut cross_schema = request();
        cross_schema.tables[0].shadow.schema = "temporary".to_owned();
        assert!(matches!(
            manifest::canonical_request(&cross_schema),
            Err(SnapshotTargetError::CrossSchemaShadowUnsupported { .. })
        ));
    }

    #[test]
    fn classifies_pending_and_idempotently_active_tables() {
        let request = request();
        let table = &request.tables[0];
        let pending = classify_table(
            &request,
            table,
            RelationState::Managed(record(ManagedTableState::Active, 6, "sha256:old")),
            RelationState::Managed(record(ManagedTableState::Shadow, 7, "sha256:new")),
        )
        .unwrap();
        assert!(matches!(
            pending,
            TableActivationState::Pending {
                previous_active: Some(_),
                ..
            }
        ));

        let active = classify_table(
            &request,
            table,
            RelationState::Managed(record(ManagedTableState::Active, 7, "sha256:new")),
            RelationState::Vacant,
        )
        .unwrap();
        assert!(matches!(active, TableActivationState::Active { .. }));
    }

    #[test]
    fn rejects_stale_or_incomplete_activation() {
        let request = request();
        let table = &request.tables[0];
        assert!(matches!(
            classify_table(
                &request,
                table,
                RelationState::Managed(record(ManagedTableState::Active, 7, "sha256:other",)),
                RelationState::Managed(record(ManagedTableState::Shadow, 7, "sha256:new",)),
            ),
            Err(SnapshotTargetError::ActivationGenerationNotNewer { .. })
        ));
        assert!(matches!(
            classify_table(
                &request,
                table,
                RelationState::Vacant,
                RelationState::Vacant,
            ),
            Err(SnapshotTargetError::IncompleteShadow(_))
        ));

        let pending = TableActivationState::Pending {
            shadow: record(ManagedTableState::Shadow, 7, "sha256:new"),
            previous_active: None,
        };
        assert!(matches!(
            group_is_active(&[
                TableActivationState::Active {
                    target: record(ManagedTableState::Active, 7, "sha256:new")
                },
                pending
            ]),
            Err(SnapshotTargetError::MixedActivationState)
        ));
    }

    #[test]
    fn activation_progress_identity_uses_the_promoted_physical_relation() {
        let request = request();
        let table = request.tables[0].clone();
        let mut pending_shadow = record(ManagedTableState::Shadow, 7, "sha256:new");
        pending_shadow.relation_oid = Some(16_384);
        let pending = TableActivationState::Pending {
            shadow: pending_shadow,
            previous_active: None,
        };
        let identities = activation_progress_identities(&[table.clone()], &[pending]).unwrap();
        assert_eq!(identities[0].table, table);
        assert_eq!(identities[0].shadow_relation_oid, 16_384);

        let mut active_target = record(ManagedTableState::Active, 7, "sha256:new");
        active_target.relation_oid = Some(16_384);
        let active = TableActivationState::Active {
            target: active_target,
        };
        assert_eq!(
            activation_progress_identities(&[table], &[active]).unwrap()[0]
                .shadow_relation_oid,
            16_384
        );
    }

    #[test]
    fn activation_rejects_progress_without_a_physical_relation_identity() {
        let request = request();
        let pending = TableActivationState::Pending {
            shadow: record(ManagedTableState::Shadow, 7, "sha256:new"),
            previous_active: None,
        };
        assert!(matches!(
            activation_progress_identities(&request.tables, &[pending]),
            Err(SnapshotTargetError::MissingRelationIdentity(_))
        ));
    }

    #[test]
    fn rejects_table_metadata_from_a_newer_fence() {
        let request = request();
        let table = &request.tables[0];
        let mut shadow = record(ManagedTableState::Shadow, 7, "sha256:new");
        shadow.fencing_token = request.fence.fencing_token + 1;

        assert!(matches!(
            classify_table(
                &request,
                table,
                RelationState::Vacant,
                RelationState::Managed(shadow),
            ),
            Err(SnapshotTargetError::ManagedByNewerFence {
                current: 12,
                actual: 13,
                ..
            })
        ));
    }

    #[test]
    fn quarantine_names_are_short_deterministic_and_owner_specific() {
        let target = QualifiedName::new("target", "items").unwrap();
        let first = record(ManagedTableState::Active, 6, "sha256:old");
        let name = quarantine_name(&target, &first).unwrap();
        assert_eq!(name.schema, "target");
        assert!(name.name.starts_with("pg2cb_q_"));
        assert_eq!(name.name.len(), 40);
        assert_eq!(name, quarantine_name(&target, &first).unwrap());

        let mut other = first;
        other.fencing_token += 1;
        assert_ne!(name, quarantine_name(&target, &other).unwrap());
    }

    #[test]
    fn renders_quoted_rename_sql() {
        let source = QualifiedName::new("old schema", "select").unwrap();
        assert_eq!(
            rename_table_sql(&source, "new name").unwrap(),
            "ALTER TABLE \"old schema\".\"select\" RENAME TO \"new name\""
        );
    }
}
