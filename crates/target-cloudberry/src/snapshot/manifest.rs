//! Durable registration and validation of complete snapshot-group manifests.

use std::str::FromStr;

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn, schema::QualifiedName};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Row, Transaction};
use uuid::Uuid;

use crate::{
    checkpoint::{
        CheckpointKey, NodeCheckpoint, PipelineFence, checkpoint_reached, lock_pipeline_fence,
    },
    migration::TARGET_METADATA_SCHEMA,
    sql::quote_identifier,
};

use super::{
    SnapshotActivationRequest, SnapshotActivationTable, SnapshotOwnership, SnapshotTargetError,
    SnapshotTargetPlan, database_generation, ensure_one_metadata_row,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotGroupRegistrationDisposition {
    Registered,
    AlreadyRegistered,
    AlreadyActive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotGroupStatus {
    Loading,
    Active,
}

impl SnapshotGroupStatus {
    fn parse(snapshot_group_id: Uuid, value: &str) -> Result<Self, SnapshotTargetError> {
        match value {
            "loading" => Ok(Self::Loading),
            "active" => Ok(Self::Active),
            _ => Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
                snapshot_group_id,
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredSnapshotGroup {
    pub(super) state: SnapshotGroupStatus,
    pub(super) snapshot_progress_version: u16,
    pub(super) request: SnapshotActivationRequest,
}

/// Fenced, checksum-validated snapshot manifest used by table-reload recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotGroupManifest {
    pub status: SnapshotGroupStatus,
    pub request: SnapshotActivationRequest,
}

/// Registers the complete table and source-node boundary manifest before any shadow COPY starts.
///
/// Registration and retries serialize on the pipeline fence. Reusing an existing UUID succeeds
/// only when every header, table, and node-boundary field is exactly equivalent.
pub async fn begin_snapshot_group(
    client: &mut Client,
    request: &SnapshotActivationRequest,
) -> Result<SnapshotGroupRegistrationDisposition, SnapshotTargetError> {
    let transaction = client.transaction().await?;
    let disposition = begin_snapshot_group_in_transaction(&transaction, request).await?;
    transaction.commit().await?;
    Ok(disposition)
}

/// Caller-owned transaction form used to register a reload group with its table transitions.
pub async fn begin_snapshot_group_in_transaction(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
) -> Result<SnapshotGroupRegistrationDisposition, SnapshotTargetError> {
    let canonical = canonical_request(request)?;
    lock_pipeline_fence(transaction, canonical.fence).await?;

    if let Some(stored) =
        load_snapshot_group_optional(transaction, canonical.snapshot_group_id).await?
    {
        if stored.request != canonical {
            return Err(manifest_mismatch(&stored.request, &canonical));
        }
        let disposition = match stored.state {
            SnapshotGroupStatus::Loading => SnapshotGroupRegistrationDisposition::AlreadyRegistered,
            SnapshotGroupStatus::Active => SnapshotGroupRegistrationDisposition::AlreadyActive,
        };
        return Ok(disposition);
    }

    insert_snapshot_group(transaction, &canonical).await?;
    Ok(SnapshotGroupRegistrationDisposition::Registered)
}

/// Loads a complete manifest under the current pipeline fence without changing its ownership.
///
/// A newer lease may inspect an older same-topology group in order to decide whether it must be
/// reset or adopted. A stale lease and a different topology are rejected before any manifest is
/// returned.
pub async fn load_snapshot_group_manifest(
    client: &mut Client,
    current_fence: PipelineFence,
    snapshot_group_id: Uuid,
) -> Result<SnapshotGroupManifest, SnapshotTargetError> {
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, current_fence).await?;
    let stored = load_snapshot_group(&transaction, snapshot_group_id).await?;
    let group_fence = stored.request.fence;
    if group_fence.pipeline_id != current_fence.pipeline_id
        || group_fence.topology_generation != current_fence.topology_generation
        || group_fence.fencing_token > current_fence.fencing_token
    {
        return Err(SnapshotTargetError::SnapshotGroupFenceMismatch {
            group: snapshot_group_id,
            expected_generation: current_fence.topology_generation,
            expected_token: current_fence.fencing_token,
            actual_generation: group_fence.topology_generation,
            actual_token: group_fence.fencing_token,
        });
    }
    transaction.commit().await?;
    Ok(SnapshotGroupManifest {
        status: stored.state,
        request: stored.request,
    })
}

pub(super) async fn load_snapshot_group(
    transaction: &Transaction<'_>,
    snapshot_group_id: Uuid,
) -> Result<StoredSnapshotGroup, SnapshotTargetError> {
    load_snapshot_group_optional(transaction, snapshot_group_id)
        .await?
        .ok_or(SnapshotTargetError::SnapshotGroupNotRegistered(
            snapshot_group_id,
        ))
}

pub(super) async fn adopt_snapshot_group_fence(
    transaction: &Transaction<'_>,
    mut stored: StoredSnapshotGroup,
    current_fence: PipelineFence,
) -> Result<StoredSnapshotGroup, SnapshotTargetError> {
    let group_id = stored.request.snapshot_group_id;
    let previous_fence = stored.request.fence;
    if previous_fence == current_fence {
        return Ok(stored);
    }
    if previous_fence.pipeline_id != current_fence.pipeline_id
        || previous_fence.topology_generation != current_fence.topology_generation
        || previous_fence.fencing_token > current_fence.fencing_token
    {
        return Err(SnapshotTargetError::SnapshotGroupFenceMismatch {
            group: group_id,
            expected_generation: current_fence.topology_generation,
            expected_token: current_fence.fencing_token,
            actual_generation: previous_fence.topology_generation,
            actual_token: previous_fence.fencing_token,
        });
    }
    stored.request.fence = current_fence;
    let checksum = manifest_checksum(&stored.request);
    let written = transaction
        .execute(
            "UPDATE pg2cb_meta.snapshot_groups
                SET fencing_token = $3, manifest_checksum = $4, updated_at = clock_timestamp()
              WHERE snapshot_group_id = $1 AND fencing_token = $2",
            &[
                &group_id,
                &previous_fence.fencing_token,
                &current_fence.fencing_token,
                &checksum,
            ],
        )
        .await?;
    ensure_one_metadata_row(written)?;
    Ok(stored)
}

pub(super) fn validate_exact_request(
    stored: &StoredSnapshotGroup,
    request: &SnapshotActivationRequest,
) -> Result<SnapshotActivationRequest, SnapshotTargetError> {
    let canonical = canonical_request(request)?;
    if stored.request == canonical {
        Ok(canonical)
    } else {
        Err(manifest_mismatch(&stored.request, &canonical))
    }
}

pub(super) fn validate_apply_membership(
    stored: &StoredSnapshotGroup,
    plan: &SnapshotTargetPlan,
    ownership: &SnapshotOwnership,
) -> Result<(), SnapshotTargetError> {
    if stored.request.snapshot_group_id != ownership.snapshot_group_id
        || stored.request.fence.pipeline_id != ownership.fence.pipeline_id
        || stored.request.fence.topology_generation != ownership.fence.topology_generation
        || stored.request.fence.fencing_token != ownership.fence.fencing_token
    {
        let difference = if stored.request.snapshot_group_id != ownership.snapshot_group_id {
            format!(
                "snapshot_group_id: stored={}, caller={}",
                stored.request.snapshot_group_id, ownership.snapshot_group_id
            )
        } else if stored.request.fence.pipeline_id != ownership.fence.pipeline_id {
            format!(
                "fence.pipeline_id: stored={}, caller={}",
                stored.request.fence.pipeline_id, ownership.fence.pipeline_id
            )
        } else if stored.request.fence.topology_generation != ownership.fence.topology_generation {
            format!(
                "fence.topology_generation: stored={}, caller={}",
                stored.request.fence.topology_generation, ownership.fence.topology_generation
            )
        } else {
            format!(
                "fence.fencing_token: stored={}, caller={}",
                stored.request.fence.fencing_token, ownership.fence.fencing_token
            )
        };
        return Err(SnapshotTargetError::SnapshotGroupManifestMismatch {
            group: ownership.snapshot_group_id,
            difference,
        });
    }

    let expected = plan.activation_table(ownership.schema_fingerprint.clone());
    let Some(manifest_table) = stored
        .request
        .tables
        .iter()
        .find(|table| table.target == plan.target)
    else {
        return Err(SnapshotTargetError::SnapshotTableNotInManifest {
            group: ownership.snapshot_group_id,
            table: plan.target.to_string(),
        });
    };
    if manifest_table != &expected {
        return Err(SnapshotTargetError::SnapshotTableManifestMismatch {
            group: ownership.snapshot_group_id,
            table: plan.target.to_string(),
        });
    }
    Ok(())
}

fn manifest_mismatch(
    stored: &SnapshotActivationRequest,
    caller: &SnapshotActivationRequest,
) -> SnapshotTargetError {
    let difference = first_request_difference(stored, caller)
        .unwrap_or_else(|| "requests differ in an unrecognized field".to_owned());
    SnapshotTargetError::SnapshotGroupManifestMismatch {
        group: caller.snapshot_group_id,
        difference,
    }
}

fn first_request_difference(
    stored: &SnapshotActivationRequest,
    caller: &SnapshotActivationRequest,
) -> Option<String> {
    if stored.snapshot_group_id != caller.snapshot_group_id {
        return Some(format!(
            "snapshot_group_id: stored={}, caller={}",
            stored.snapshot_group_id, caller.snapshot_group_id
        ));
    }
    if stored.fence.pipeline_id != caller.fence.pipeline_id {
        return Some(format!(
            "fence.pipeline_id: stored={}, caller={}",
            stored.fence.pipeline_id, caller.fence.pipeline_id
        ));
    }
    if stored.fence.topology_generation != caller.fence.topology_generation {
        return Some(format!(
            "fence.topology_generation: stored={}, caller={}",
            stored.fence.topology_generation, caller.fence.topology_generation
        ));
    }
    if stored.fence.fencing_token != caller.fence.fencing_token {
        return Some(format!(
            "fence.fencing_token: stored={}, caller={}",
            stored.fence.fencing_token, caller.fence.fencing_token
        ));
    }
    first_table_difference(&stored.tables, &caller.tables).or_else(|| {
        first_checkpoint_difference(&stored.initial_checkpoints, &caller.initial_checkpoints)
    })
}

pub(super) fn first_table_difference(
    stored: &[SnapshotActivationTable],
    caller: &[SnapshotActivationTable],
) -> Option<String> {
    if stored.len() != caller.len() {
        return Some(format!(
            "tables.len: stored={}, caller={}",
            stored.len(),
            caller.len()
        ));
    }
    for (index, (stored, caller)) in stored.iter().zip(caller).enumerate() {
        if stored.target != caller.target {
            return Some(format!(
                "tables[{index}].target: stored={}, caller={}",
                stored.target, caller.target
            ));
        }
        if stored.shadow != caller.shadow {
            return Some(format!(
                "tables[{index}].shadow: stored={}, caller={}",
                stored.shadow, caller.shadow
            ));
        }
        if stored.source_relation_id != caller.source_relation_id {
            return Some(format!(
                "tables[{index}].source_relation_id: stored={}, caller={}",
                stored.source_relation_id, caller.source_relation_id
            ));
        }
        if stored.table_generation != caller.table_generation {
            return Some(format!(
                "tables[{index}].table_generation: stored={}, caller={}",
                stored.table_generation, caller.table_generation
            ));
        }
        if stored.schema_fingerprint != caller.schema_fingerprint {
            return Some(format!(
                "tables[{index}].schema_fingerprint: stored={}, caller={}",
                stored.schema_fingerprint, caller.schema_fingerprint
            ));
        }
    }
    None
}

fn first_checkpoint_difference(
    stored: &[NodeCheckpoint],
    caller: &[NodeCheckpoint],
) -> Option<String> {
    if stored.len() != caller.len() {
        return Some(format!(
            "initial_checkpoints.len: stored={}, caller={}",
            stored.len(),
            caller.len()
        ));
    }
    for (index, (stored, caller)) in stored.iter().zip(caller).enumerate() {
        if stored.key != caller.key {
            return Some(format!(
                "initial_checkpoints[{index}].key: stored={:?}, caller={:?}",
                stored.key, caller.key
            ));
        }
        if stored.system_identifier != caller.system_identifier {
            return Some(format!(
                "initial_checkpoints[{index}].system_identifier: stored={}, caller={}",
                stored.system_identifier, caller.system_identifier
            ));
        }
        if stored.timeline != caller.timeline {
            return Some(format!(
                "initial_checkpoints[{index}].timeline: stored={}, caller={}",
                stored.timeline, caller.timeline
            ));
        }
        if stored.slot_name != caller.slot_name {
            return Some(format!(
                "initial_checkpoints[{index}].slot_name: stored={}, caller={}",
                stored.slot_name, caller.slot_name
            ));
        }
        if stored.applied_lsn != caller.applied_lsn {
            return Some(format!(
                "initial_checkpoints[{index}].applied_lsn: stored={}, caller={}",
                stored.applied_lsn, caller.applied_lsn
            ));
        }
    }
    None
}

pub(super) async fn mark_snapshot_group_active(
    transaction: &Transaction<'_>,
    snapshot_group_id: Uuid,
) -> Result<(), SnapshotTargetError> {
    let sql = format!(
        "UPDATE {}.snapshot_groups SET state = 'active', activated_at = clock_timestamp(), updated_at = clock_timestamp() WHERE snapshot_group_id = $1 AND state = 'loading'",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction.execute(&sql, &[&snapshot_group_id]).await?;
    ensure_one_metadata_row(written)
}

pub(super) fn canonical_request(
    request: &SnapshotActivationRequest,
) -> Result<SnapshotActivationRequest, SnapshotTargetError> {
    if request.snapshot_group_id.is_nil() {
        return Err(SnapshotTargetError::InvalidSnapshotGroupId);
    }
    if request.tables.is_empty() || request.initial_checkpoints.is_empty() {
        return Err(SnapshotTargetError::EmptyActivationGroup);
    }
    if request.fence.fencing_token <= 0 {
        return Err(crate::checkpoint::CheckpointError::InvalidFencingToken.into());
    }
    database_generation(request.fence.topology_generation)?;

    let mut tables = request.tables.clone();
    for table in &tables {
        validate_table(table)?;
    }
    tables.sort_by(|left, right| {
        (&left.target.schema, &left.target.name).cmp(&(&right.target.schema, &right.target.name))
    });
    for pair in tables.windows(2) {
        if pair[0].target == pair[1].target {
            return Err(SnapshotTargetError::DuplicateActivationIdentity);
        }
    }

    let mut relation_names = std::collections::HashSet::with_capacity(tables.len() * 2);
    let mut source_relations = std::collections::HashSet::with_capacity(tables.len());
    for table in &tables {
        if !relation_names.insert(table.target.clone())
            || !relation_names.insert(table.shadow.clone())
            || !source_relations.insert(table.source_relation_id)
        {
            return Err(SnapshotTargetError::DuplicateActivationIdentity);
        }
    }

    let mut initial_checkpoints = request.initial_checkpoints.clone();
    for checkpoint in &initial_checkpoints {
        validate_checkpoint(request.fence, checkpoint)?;
    }
    initial_checkpoints.sort_by_key(|checkpoint| checkpoint.key.node_id);
    for pair in initial_checkpoints.windows(2) {
        if pair[0].key.node_id == pair[1].key.node_id {
            return Err(SnapshotTargetError::DuplicateActivationIdentity);
        }
    }

    Ok(SnapshotActivationRequest {
        fence: request.fence,
        snapshot_group_id: request.snapshot_group_id,
        tables,
        initial_checkpoints,
    })
}

fn validate_table(table: &SnapshotActivationTable) -> Result<(), SnapshotTargetError> {
    if table.target == table.shadow {
        return Err(SnapshotTargetError::TargetIsShadow);
    }
    if table.target.schema != table.shadow.schema {
        return Err(SnapshotTargetError::CrossSchemaShadowUnsupported {
            target: table.target.to_string(),
            shadow: table.shadow.to_string(),
        });
    }
    if table.source_relation_id == 0 {
        return Err(SnapshotTargetError::InvalidActivationSourceRelation);
    }
    database_generation(table.table_generation)?;
    if table.schema_fingerprint.is_empty() {
        return Err(SnapshotTargetError::EmptyActivationFingerprint(
            table.target.to_string(),
        ));
    }
    Ok(())
}

fn validate_checkpoint(
    fence: PipelineFence,
    checkpoint: &NodeCheckpoint,
) -> Result<(), SnapshotTargetError> {
    if checkpoint.key.pipeline_id != fence.pipeline_id
        || checkpoint.key.topology_generation != fence.topology_generation
    {
        return Err(crate::checkpoint::CheckpointError::FenceKeyMismatch.into());
    }
    if checkpoint.applied_lsn == PgLsn::ZERO {
        return Err(SnapshotTargetError::ZeroInitialCheckpoint(
            checkpoint.key.node_id,
        ));
    }
    checkpoint_reached(None, checkpoint)?;
    Ok(())
}

async fn insert_snapshot_group(
    transaction: &Transaction<'_>,
    request: &SnapshotActivationRequest,
) -> Result<(), SnapshotTargetError> {
    let pipeline_id = request.fence.pipeline_id.as_uuid();
    let topology_generation = database_generation(request.fence.topology_generation)?;
    let table_count = i64::try_from(request.tables.len()).map_err(|_| {
        SnapshotTargetError::CorruptSnapshotGroupManifest(request.snapshot_group_id)
    })?;
    let node_count = i64::try_from(request.initial_checkpoints.len()).map_err(|_| {
        SnapshotTargetError::CorruptSnapshotGroupManifest(request.snapshot_group_id)
    })?;
    let checksum = manifest_checksum(request);
    let group_sql = format!(
        "INSERT INTO {}.snapshot_groups (snapshot_group_id, pipeline_id, topology_generation, fencing_token, state, table_count, node_count, manifest_checksum, snapshot_progress_version) VALUES ($1, $2, $3, $4, 'loading', $5, $6, $7, 1)",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction
        .execute(
            &group_sql,
            &[
                &request.snapshot_group_id,
                &pipeline_id,
                &topology_generation,
                &request.fence.fencing_token,
                &table_count,
                &node_count,
                &checksum,
            ],
        )
        .await?;
    ensure_one_metadata_row(written)?;

    let target_schemas = request
        .tables
        .iter()
        .map(|table| table.target.schema.clone())
        .collect::<Vec<_>>();
    let target_tables = request
        .tables
        .iter()
        .map(|table| table.target.name.clone())
        .collect::<Vec<_>>();
    let shadow_schemas = request
        .tables
        .iter()
        .map(|table| table.shadow.schema.clone())
        .collect::<Vec<_>>();
    let shadow_tables = request
        .tables
        .iter()
        .map(|table| table.shadow.name.clone())
        .collect::<Vec<_>>();
    let source_relation_ids = request
        .tables
        .iter()
        .map(|table| i64::from(table.source_relation_id))
        .collect::<Vec<_>>();
    let table_generations = request
        .tables
        .iter()
        .map(|table| database_generation(table.table_generation))
        .collect::<Result<Vec<_>, _>>()?;
    let schema_fingerprints = request
        .tables
        .iter()
        .map(|table| table.schema_fingerprint.clone())
        .collect::<Vec<_>>();
    let table_sql = format!(
        "INSERT INTO {}.snapshot_group_tables (snapshot_group_id, target_schema, target_table, shadow_schema, shadow_table, source_relation_id, table_generation, schema_fingerprint) SELECT $1, manifest.target_schema, manifest.target_table, manifest.shadow_schema, manifest.shadow_table, manifest.source_relation_id, manifest.table_generation, manifest.schema_fingerprint FROM unnest($2::text[], $3::text[], $4::text[], $5::text[], $6::bigint[], $7::bigint[], $8::text[]) AS manifest(target_schema, target_table, shadow_schema, shadow_table, source_relation_id, table_generation, schema_fingerprint)",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction
        .execute(
            &table_sql,
            &[
                &request.snapshot_group_id,
                &target_schemas,
                &target_tables,
                &shadow_schemas,
                &shadow_tables,
                &source_relation_ids,
                &table_generations,
                &schema_fingerprints,
            ],
        )
        .await?;
    if written != request.tables.len() as u64 {
        return Err(SnapshotTargetError::UnexpectedMetadataWriteCount(written));
    }

    let node_ids = request
        .initial_checkpoints
        .iter()
        .map(|checkpoint| checkpoint.key.node_id)
        .collect::<Vec<_>>();
    let system_identifiers = request
        .initial_checkpoints
        .iter()
        .map(|checkpoint| checkpoint.system_identifier.to_string())
        .collect::<Vec<_>>();
    let timelines = request
        .initial_checkpoints
        .iter()
        .map(|checkpoint| i64::from(checkpoint.timeline))
        .collect::<Vec<_>>();
    let slot_names = request
        .initial_checkpoints
        .iter()
        .map(|checkpoint| checkpoint.slot_name.clone())
        .collect::<Vec<_>>();
    let consistent_lsns = request
        .initial_checkpoints
        .iter()
        .map(|checkpoint| checkpoint.applied_lsn.to_string())
        .collect::<Vec<_>>();
    let node_sql = format!(
        "INSERT INTO {}.snapshot_group_nodes (snapshot_group_id, node_id, system_identifier, timeline, slot_name, consistent_lsn) SELECT $1, boundary.node_id, boundary.system_identifier::numeric, boundary.timeline, boundary.slot_name, boundary.consistent_lsn::pg_lsn FROM unnest($2::integer[], $3::text[], $4::bigint[], $5::text[], $6::text[]) AS boundary(node_id, system_identifier, timeline, slot_name, consistent_lsn)",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let written = transaction
        .execute(
            &node_sql,
            &[
                &request.snapshot_group_id,
                &node_ids,
                &system_identifiers,
                &timelines,
                &slot_names,
                &consistent_lsns,
            ],
        )
        .await?;
    if written != request.initial_checkpoints.len() as u64 {
        return Err(SnapshotTargetError::UnexpectedMetadataWriteCount(written));
    }
    Ok(())
}

async fn load_snapshot_group_optional(
    transaction: &Transaction<'_>,
    snapshot_group_id: Uuid,
) -> Result<Option<StoredSnapshotGroup>, SnapshotTargetError> {
    if snapshot_group_id.is_nil() {
        return Err(SnapshotTargetError::InvalidSnapshotGroupId);
    }
    let group_sql = format!(
        "SELECT pipeline_id, topology_generation, fencing_token, state, table_count, node_count, manifest_checksum, snapshot_progress_version FROM {}.snapshot_groups WHERE snapshot_group_id = $1 FOR UPDATE",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let Some(group) = transaction
        .query_opt(&group_sql, &[&snapshot_group_id])
        .await?
    else {
        return Ok(None);
    };

    let pipeline_id = PipelineId::from_uuid(group.try_get("pipeline_id")?);
    let topology_generation = persisted_u64(&group, "topology_generation", snapshot_group_id)?;
    let fencing_token: i64 = group.try_get("fencing_token")?;
    let state = SnapshotGroupStatus::parse(snapshot_group_id, group.try_get("state")?)?;
    let table_count = persisted_count(&group, "table_count", snapshot_group_id)?;
    let node_count = persisted_count(&group, "node_count", snapshot_group_id)?;
    let stored_checksum: Vec<u8> = group.try_get("manifest_checksum")?;
    let progress_version_raw: i32 = group.try_get("snapshot_progress_version")?;
    let snapshot_progress_version = u16::try_from(progress_version_raw)
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))?;
    if fencing_token <= 0 || stored_checksum.len() != 32 || snapshot_progress_version > 1 {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
            snapshot_group_id,
        ));
    }

    let table_sql = format!(
        "SELECT target_schema, target_table, shadow_schema, shadow_table, source_relation_id, table_generation, schema_fingerprint FROM {}.snapshot_group_tables WHERE snapshot_group_id = $1 ORDER BY target_schema, target_table",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let table_rows = transaction.query(&table_sql, &[&snapshot_group_id]).await?;
    if table_rows.len() != table_count {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
            snapshot_group_id,
        ));
    }
    let tables = table_rows
        .iter()
        .map(|row| table_from_row(row, snapshot_group_id))
        .collect::<Result<Vec<_>, _>>()?;

    let node_sql = format!(
        "SELECT node_id, system_identifier::text AS system_identifier, timeline, slot_name, consistent_lsn::text AS consistent_lsn FROM {}.snapshot_group_nodes WHERE snapshot_group_id = $1 ORDER BY node_id",
        quote_identifier(TARGET_METADATA_SCHEMA)?
    );
    let node_rows = transaction.query(&node_sql, &[&snapshot_group_id]).await?;
    if node_rows.len() != node_count {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
            snapshot_group_id,
        ));
    }
    let initial_checkpoints = node_rows
        .iter()
        .map(|row| checkpoint_from_row(row, snapshot_group_id, pipeline_id, topology_generation))
        .collect::<Result<Vec<_>, _>>()?;

    let reconstructed = SnapshotActivationRequest {
        fence: PipelineFence {
            pipeline_id,
            topology_generation,
            fencing_token,
        },
        snapshot_group_id,
        tables,
        initial_checkpoints,
    };
    let canonical = canonical_request(&reconstructed)
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))?;
    if canonical != reconstructed || manifest_checksum(&canonical) != stored_checksum {
        return Err(SnapshotTargetError::CorruptSnapshotGroupManifest(
            snapshot_group_id,
        ));
    }
    Ok(Some(StoredSnapshotGroup {
        state,
        snapshot_progress_version,
        request: canonical,
    }))
}

/// Returns whether a snapshot group header and its manifest still exist.
///
/// Reconciliation reload failure uses this only after a durable `failed` state has been observed
/// to classify an unknown commit result.  The full manifest is still parsed and checksum-checked,
/// so a corrupt leftover is never mistaken for a successfully cleaned group.
pub(crate) async fn snapshot_group_exists_in_transaction(
    transaction: &Transaction<'_>,
    snapshot_group_id: Uuid,
) -> Result<bool, SnapshotTargetError> {
    Ok(load_snapshot_group_optional(transaction, snapshot_group_id)
        .await?
        .is_some())
}

fn table_from_row(
    row: &Row,
    snapshot_group_id: Uuid,
) -> Result<SnapshotActivationTable, SnapshotTargetError> {
    let target_schema: String = row.try_get("target_schema")?;
    let target_table: String = row.try_get("target_table")?;
    let shadow_schema: String = row.try_get("shadow_schema")?;
    let shadow_table: String = row.try_get("shadow_table")?;
    let relation_raw: i64 = row.try_get("source_relation_id")?;
    let source_relation_id = u32::try_from(relation_raw)
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))?;
    let table_generation = persisted_u64(row, "table_generation", snapshot_group_id)?;
    Ok(SnapshotActivationTable {
        target: QualifiedName::new(target_schema, target_table)
            .map_err(crate::schema::SchemaError::from)?,
        shadow: QualifiedName::new(shadow_schema, shadow_table)
            .map_err(crate::schema::SchemaError::from)?,
        source_relation_id,
        table_generation,
        schema_fingerprint: row.try_get("schema_fingerprint")?,
    })
}

fn checkpoint_from_row(
    row: &Row,
    snapshot_group_id: Uuid,
    pipeline_id: PipelineId,
    topology_generation: u64,
) -> Result<NodeCheckpoint, SnapshotTargetError> {
    let system_identifier = persisted_parse(row, "system_identifier", snapshot_group_id)?;
    let timeline_raw: i64 = row.try_get("timeline")?;
    let timeline = u32::try_from(timeline_raw)
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))?;
    let consistent_lsn: String = row.try_get("consistent_lsn")?;
    let applied_lsn = PgLsn::from_str(&consistent_lsn)
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))?;
    Ok(NodeCheckpoint {
        key: CheckpointKey {
            pipeline_id,
            topology_generation,
            node_id: row.try_get("node_id")?,
        },
        system_identifier,
        timeline,
        slot_name: row.try_get("slot_name")?,
        applied_lsn,
    })
}

fn persisted_count(
    row: &Row,
    field: &'static str,
    snapshot_group_id: Uuid,
) -> Result<usize, SnapshotTargetError> {
    let raw: i64 = row.try_get(field)?;
    usize::try_from(raw).ok().filter(|count| *count > 0).ok_or(
        SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id),
    )
}

fn persisted_u64(
    row: &Row,
    field: &'static str,
    snapshot_group_id: Uuid,
) -> Result<u64, SnapshotTargetError> {
    let raw: i64 = row.try_get(field)?;
    u64::try_from(raw)
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))
}

fn persisted_parse<T>(
    row: &Row,
    field: &'static str,
    snapshot_group_id: Uuid,
) -> Result<T, SnapshotTargetError>
where
    T: FromStr,
{
    let raw: String = row.try_get(field)?;
    raw.parse()
        .map_err(|_| SnapshotTargetError::CorruptSnapshotGroupManifest(snapshot_group_id))
}

fn manifest_checksum(request: &SnapshotActivationRequest) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hash_field(&mut hasher, b"pg2cb-snapshot-manifest-v1");
    hash_field(&mut hasher, request.snapshot_group_id.as_bytes());
    hash_field(&mut hasher, request.fence.pipeline_id.as_uuid().as_bytes());
    hash_field(
        &mut hasher,
        &request.fence.topology_generation.to_be_bytes(),
    );
    for table in &request.tables {
        hash_field(&mut hasher, table.target.schema.as_bytes());
        hash_field(&mut hasher, table.target.name.as_bytes());
        hash_field(&mut hasher, table.shadow.schema.as_bytes());
        hash_field(&mut hasher, table.shadow.name.as_bytes());
        hash_field(&mut hasher, &table.source_relation_id.to_be_bytes());
        hash_field(&mut hasher, &table.table_generation.to_be_bytes());
        hash_field(&mut hasher, table.schema_fingerprint.as_bytes());
    }
    for checkpoint in &request.initial_checkpoints {
        hash_field(&mut hasher, &checkpoint.key.node_id.to_be_bytes());
        hash_field(&mut hasher, &checkpoint.system_identifier.to_be_bytes());
        hash_field(&mut hasher, &checkpoint.timeline.to_be_bytes());
        hash_field(&mut hasher, checkpoint.slot_name.as_bytes());
        hash_field(&mut hasher, &checkpoint.applied_lsn.as_u64().to_be_bytes());
    }
    hasher.finalize().to_vec()
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> SnapshotActivationRequest {
        let fence = PipelineFence {
            pipeline_id: PipelineId::new(),
            topology_generation: 3,
            fencing_token: 7,
        };
        SnapshotActivationRequest {
            fence,
            snapshot_group_id: Uuid::now_v7(),
            tables: vec![SnapshotActivationTable {
                target: QualifiedName::new("target", "items").unwrap(),
                shadow: QualifiedName::new("target", "items_shadow").unwrap(),
                source_relation_id: 42,
                table_generation: 5,
                schema_fingerprint: "sha256:items".to_owned(),
            }],
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
    fn canonical_manifest_is_order_independent_but_field_exact() {
        let mut left = request();
        let mut second = left.tables[0].clone();
        second.target = QualifiedName::new("target", "another").unwrap();
        second.shadow = QualifiedName::new("target", "another_shadow").unwrap();
        second.source_relation_id = 43;
        left.tables.push(second);
        left.tables.reverse();
        let canonical = canonical_request(&left).unwrap();
        assert_eq!(canonical.tables[0].target.name, "another");

        let checksum = manifest_checksum(&canonical);
        let mut changed = canonical.clone();
        changed.initial_checkpoints[0].slot_name.push_str("_other");
        assert_ne!(checksum, manifest_checksum(&changed));
        assert_eq!(
            first_request_difference(&canonical, &changed).unwrap(),
            "initial_checkpoints[0].slot_name: stored=pg2cb_slot, caller=pg2cb_slot_other"
        );
    }

    #[test]
    fn rejects_cross_schema_shadows_and_incomplete_boundaries() {
        let mut invalid = request();
        invalid.tables[0].shadow = QualifiedName::new("other", "items_shadow").unwrap();
        assert!(matches!(
            canonical_request(&invalid),
            Err(SnapshotTargetError::CrossSchemaShadowUnsupported { .. })
        ));

        let mut zero = request();
        zero.initial_checkpoints[0].applied_lsn = PgLsn::ZERO;
        assert!(matches!(
            canonical_request(&zero),
            Err(SnapshotTargetError::ZeroInitialCheckpoint(1))
        ));
    }

    #[test]
    fn apply_membership_requires_every_table_field() {
        let request = canonical_request(&request()).unwrap();
        let stored = StoredSnapshotGroup {
            state: SnapshotGroupStatus::Loading,
            snapshot_progress_version: 1,
            request: request.clone(),
        };
        let ownership = SnapshotOwnership {
            fence: request.fence,
            snapshot_group_id: request.snapshot_group_id,
            schema_fingerprint: request.tables[0].schema_fingerprint.clone(),
        };
        let source = cloudberry_etl_core::schema::TableSchema {
            relation_id: request.tables[0].source_relation_id,
            generation: request.tables[0].table_generation,
            name: QualifiedName::new("source", "items").unwrap(),
            kind: cloudberry_etl_core::schema::TableKind::Ordinary,
            replica_identity: cloudberry_etl_core::schema::ReplicaIdentity::Default,
            columns: vec![cloudberry_etl_core::schema::ColumnSchema {
                attnum: 1,
                name: "id".to_owned(),
                data_type: cloudberry_etl_core::schema::PgType {
                    oid: 20,
                    name: QualifiedName::new("pg_catalog", "int8").unwrap(),
                    kind: cloudberry_etl_core::schema::PgTypeKind::Int8,
                },
                nullable: false,
                primary_key_ordinal: Some(1),
                generated: cloudberry_etl_core::schema::GeneratedColumn::None,
                identity: cloudberry_etl_core::schema::IdentityColumn::None,
                collation: None,
            }],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        };
        let plan = super::super::plan_snapshot_target(
            &source,
            request.tables[0].target.clone(),
            request.tables[0].shadow.clone(),
        )
        .unwrap();
        validate_apply_membership(&stored, &plan, &ownership).unwrap();

        let mut wrong = ownership;
        wrong.schema_fingerprint.push_str("-wrong");
        assert!(matches!(
            validate_apply_membership(&stored, &plan, &wrong),
            Err(SnapshotTargetError::SnapshotTableManifestMismatch { .. })
        ));
    }
}
