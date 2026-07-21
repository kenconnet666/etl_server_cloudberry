//! Shared target managed-table identity checks for data-changing transactions.

use cloudberry_etl_core::{id::PipelineId, schema::QualifiedName};
use thiserror::Error;
use tokio_postgres::{Row, Transaction};

use crate::{
    checkpoint::{CheckpointError, PipelineFence, lock_pipeline_fence},
    sql::{SqlRenderError, quote_qualified_name},
};

const LOCK_MANAGED_TABLE_SQL: &str = r#"
SELECT pipeline_id, relation_oid, source_relation_id, table_generation,
       schema_fingerprint, state, fencing_token
FROM pg2cb_meta.managed_tables
WHERE target_schema = $1 AND target_table = $2
FOR UPDATE
"#;

const LOAD_RELATION_OID_SQL: &str = r#"
SELECT c.oid::bigint AS relation_oid
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
WHERE n.nspname = $1 AND c.relname = $2
  AND c.relkind IN ('r', 'p')
"#;

/// Immutable table identity expected by one target apply plan.
///
/// `table_generation` is the persisted relation incarnation. It is deliberately independent from
/// the pgoutput connection's relation-cache generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableApplyIdentity {
    pub target: QualifiedName,
    pub source_relation_id: u32,
    pub table_generation: u64,
    pub schema_fingerprint: String,
}

#[derive(Debug, Error)]
pub enum ManagedTableError {
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    Sql(#[from] SqlRenderError),
    #[error("managed-table apply identity for `{0}` has a zero source relation ID")]
    InvalidSourceRelationId(String),
    #[error("managed-table apply identity for `{0}` has an empty or invalid schema fingerprint")]
    InvalidSchemaFingerprint(String),
    #[error("table generation {generation} for `{table}` exceeds the target bigint range")]
    GenerationOutOfRange { table: String, generation: u64 },
    #[error("target table `{0}` occurs more than once in one apply transaction")]
    DuplicateTarget(String),
    #[error("target table `{0}` has no managed-table metadata")]
    MissingMetadata(String),
    #[error("target table `{table}` is managed by pipeline {actual}, expected {expected}")]
    PipelineMismatch {
        table: String,
        expected: PipelineId,
        actual: PipelineId,
    },
    #[error("target table `{table}` is managed for source relation {actual}, expected {expected}")]
    SourceRelationMismatch {
        table: String,
        expected: u32,
        actual: u32,
    },
    #[error("target table `{table}` has generation {actual}, expected {expected}")]
    GenerationMismatch {
        table: String,
        expected: u64,
        actual: u64,
    },
    #[error("target table `{0}` has a different schema fingerprint")]
    SchemaFingerprintMismatch(String),
    #[error("target table `{table}` is in state `{actual}`, expected `active`")]
    StateMismatch { table: String, actual: String },
    #[error(
        "target table `{table}` has fencing token {actual}, newer than current token {current}"
    )]
    NewerFence {
        table: String,
        current: i64,
        actual: i64,
    },
    #[error("target table `{0}` has no persisted physical relation identity")]
    MissingRelationIdentity(String),
    #[error("physical target table `{0}` is missing")]
    PhysicalRelationMissing(String),
    #[error("physical target table `{table}` has OID {actual}, expected {expected}")]
    RelationIdentityMismatch {
        table: String,
        expected: i64,
        actual: i64,
    },
    #[error("persisted managed-table field `{field}` has invalid value `{value}` for `{table}`")]
    InvalidPersistedValue {
        table: String,
        field: &'static str,
        value: String,
    },
}

/// Locks and validates all active target tables before any staging or user-table DML starts.
///
/// The pipeline fence is locked first. Table metadata and physical relations are then locked in
/// stable target-name order, preventing multi-table apply transactions from acquiring these locks
/// in conflicting orders. A successful return holds every lock until the caller ends `transaction`.
pub async fn lock_active_apply_tables(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    identities: &[&TableApplyIdentity],
) -> Result<(), ManagedTableError> {
    let ordered = validate_and_order_identities(identities)?;
    lock_pipeline_fence(transaction, fence).await?;
    for identity in ordered {
        lock_active_apply_table_after_fence(transaction, fence, identity).await?;
    }
    Ok(())
}

/// Locks and validates one active target table under the current pipeline fence.
pub async fn lock_active_apply_table(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    identity: &TableApplyIdentity,
) -> Result<(), ManagedTableError> {
    lock_active_apply_tables(transaction, fence, &[identity]).await
}

async fn lock_active_apply_table_after_fence(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    identity: &TableApplyIdentity,
) -> Result<(), ManagedTableError> {
    let row = transaction
        .query_opt(
            LOCK_MANAGED_TABLE_SQL,
            &[&identity.target.schema, &identity.target.name],
        )
        .await?
        .ok_or_else(|| ManagedTableError::MissingMetadata(identity.target.to_string()))?;
    let stored = stored_identity_from_row(&identity.target, &row)?;
    let expected_oid = validate_stored_identity(fence, identity, &stored)?;

    let target = quote_qualified_name(&identity.target)?;
    transaction
        .batch_execute(&format!("LOCK TABLE {target} IN ROW EXCLUSIVE MODE"))
        .await?;
    let actual_oid = transaction
        .query_opt(
            LOAD_RELATION_OID_SQL,
            &[&identity.target.schema, &identity.target.name],
        )
        .await?
        .map(|row| row.try_get::<_, i64>("relation_oid"))
        .transpose()?
        .ok_or_else(|| ManagedTableError::PhysicalRelationMissing(identity.target.to_string()))?;
    validate_relation_identity(&identity.target, expected_oid, actual_oid)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredManagedTableIdentity {
    pipeline_id: PipelineId,
    relation_oid: Option<i64>,
    source_relation_id: u32,
    table_generation: u64,
    schema_fingerprint: String,
    state: String,
    fencing_token: i64,
}

fn validate_and_order_identities<'a>(
    identities: &[&'a TableApplyIdentity],
) -> Result<Vec<&'a TableApplyIdentity>, ManagedTableError> {
    for identity in identities {
        validate_input_identity(identity)?;
    }
    let mut ordered = identities.to_vec();
    ordered.sort_by(|left, right| {
        (&left.target.schema, &left.target.name).cmp(&(&right.target.schema, &right.target.name))
    });
    for pair in ordered.windows(2) {
        if pair[0].target == pair[1].target {
            return Err(ManagedTableError::DuplicateTarget(
                pair[0].target.to_string(),
            ));
        }
    }
    Ok(ordered)
}

fn validate_input_identity(identity: &TableApplyIdentity) -> Result<(), ManagedTableError> {
    quote_qualified_name(&identity.target)?;
    if identity.source_relation_id == 0 {
        return Err(ManagedTableError::InvalidSourceRelationId(
            identity.target.to_string(),
        ));
    }
    if identity.schema_fingerprint.is_empty() || identity.schema_fingerprint.contains('\0') {
        return Err(ManagedTableError::InvalidSchemaFingerprint(
            identity.target.to_string(),
        ));
    }
    i64::try_from(identity.table_generation).map_err(|_| {
        ManagedTableError::GenerationOutOfRange {
            table: identity.target.to_string(),
            generation: identity.table_generation,
        }
    })?;
    Ok(())
}

fn stored_identity_from_row(
    table: &QualifiedName,
    row: &Row,
) -> Result<StoredManagedTableIdentity, ManagedTableError> {
    let source_relation_raw: i64 = row.try_get("source_relation_id")?;
    let source_relation_id = u32::try_from(source_relation_raw)
        .map_err(|_| invalid_persisted(table, "source_relation_id", source_relation_raw))?;
    if source_relation_id == 0 {
        return Err(invalid_persisted(
            table,
            "source_relation_id",
            source_relation_raw,
        ));
    }
    let generation_raw: i64 = row.try_get("table_generation")?;
    let table_generation = u64::try_from(generation_raw)
        .map_err(|_| invalid_persisted(table, "table_generation", generation_raw))?;
    let fencing_token: i64 = row.try_get("fencing_token")?;
    if fencing_token <= 0 {
        return Err(invalid_persisted(table, "fencing_token", fencing_token));
    }
    let relation_oid: Option<i64> = row.try_get("relation_oid")?;
    if relation_oid.is_some_and(|oid| oid <= 0) {
        return Err(invalid_persisted(
            table,
            "relation_oid",
            relation_oid.unwrap_or_default(),
        ));
    }
    let schema_fingerprint: String = row.try_get("schema_fingerprint")?;
    if schema_fingerprint.is_empty() {
        return Err(invalid_persisted(table, "schema_fingerprint", ""));
    }
    Ok(StoredManagedTableIdentity {
        pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
        relation_oid,
        source_relation_id,
        table_generation,
        schema_fingerprint,
        state: row.try_get("state")?,
        fencing_token,
    })
}

fn validate_stored_identity(
    fence: PipelineFence,
    expected: &TableApplyIdentity,
    actual: &StoredManagedTableIdentity,
) -> Result<i64, ManagedTableError> {
    let table = expected.target.to_string();
    if actual.pipeline_id != fence.pipeline_id {
        return Err(ManagedTableError::PipelineMismatch {
            table,
            expected: fence.pipeline_id,
            actual: actual.pipeline_id,
        });
    }
    if actual.source_relation_id != expected.source_relation_id {
        return Err(ManagedTableError::SourceRelationMismatch {
            table,
            expected: expected.source_relation_id,
            actual: actual.source_relation_id,
        });
    }
    if actual.table_generation != expected.table_generation {
        return Err(ManagedTableError::GenerationMismatch {
            table,
            expected: expected.table_generation,
            actual: actual.table_generation,
        });
    }
    if actual.schema_fingerprint != expected.schema_fingerprint {
        return Err(ManagedTableError::SchemaFingerprintMismatch(table));
    }
    if actual.state != "active" {
        return Err(ManagedTableError::StateMismatch {
            table,
            actual: actual.state.clone(),
        });
    }
    if actual.fencing_token > fence.fencing_token {
        return Err(ManagedTableError::NewerFence {
            table,
            current: fence.fencing_token,
            actual: actual.fencing_token,
        });
    }
    actual
        .relation_oid
        .ok_or(ManagedTableError::MissingRelationIdentity(table))
}

fn validate_relation_identity(
    table: &QualifiedName,
    expected: i64,
    actual: i64,
) -> Result<(), ManagedTableError> {
    if actual == expected {
        Ok(())
    } else {
        Err(ManagedTableError::RelationIdentityMismatch {
            table: table.to_string(),
            expected,
            actual,
        })
    }
}

fn invalid_persisted(
    table: &QualifiedName,
    field: &'static str,
    value: impl ToString,
) -> ManagedTableError {
    ManagedTableError::InvalidPersistedValue {
        table: table.to_string(),
        field,
        value: value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fence() -> PipelineFence {
        PipelineFence {
            pipeline_id: PipelineId::new(),
            topology_generation: 7,
            fencing_token: 11,
        }
    }

    fn identity(schema: &str, name: &str) -> TableApplyIdentity {
        TableApplyIdentity {
            target: QualifiedName::new(schema, name).unwrap(),
            source_relation_id: 42,
            table_generation: 7,
            schema_fingerprint: "sha256:abc".to_owned(),
        }
    }

    fn stored(fence: PipelineFence) -> StoredManagedTableIdentity {
        StoredManagedTableIdentity {
            pipeline_id: fence.pipeline_id,
            relation_oid: Some(1234),
            source_relation_id: 42,
            table_generation: 7,
            schema_fingerprint: "sha256:abc".to_owned(),
            state: "active".to_owned(),
            fencing_token: fence.fencing_token,
        }
    }

    #[test]
    fn identities_are_sorted_and_duplicate_targets_fail_closed() {
        let identities = [identity("z", "b"), identity("a", "z"), identity("a", "a")];
        let identities = identities.iter().collect::<Vec<_>>();
        let ordered = validate_and_order_identities(&identities).unwrap();
        let names = ordered
            .iter()
            .map(|identity| identity.target.to_string())
            .collect::<Vec<_>>();
        assert_eq!(names, ["a.a", "a.z", "z.b"]);

        let duplicate = [identity("a", "same"), identity("a", "same")];
        let duplicate = duplicate.iter().collect::<Vec<_>>();
        assert!(matches!(
            validate_and_order_identities(&duplicate),
            Err(ManagedTableError::DuplicateTarget(table)) if table == "a.same"
        ));
    }

    #[test]
    fn input_identity_rejects_values_target_metadata_cannot_store() {
        let mut value = identity("public", "items");
        value.source_relation_id = 0;
        assert!(matches!(
            validate_input_identity(&value),
            Err(ManagedTableError::InvalidSourceRelationId(_))
        ));
        value = identity("public", "items");
        value.schema_fingerprint.clear();
        assert!(matches!(
            validate_input_identity(&value),
            Err(ManagedTableError::InvalidSchemaFingerprint(_))
        ));
        value = identity("public", "items");
        value.table_generation = u64::MAX;
        assert!(matches!(
            validate_input_identity(&value),
            Err(ManagedTableError::GenerationOutOfRange { .. })
        ));
    }

    #[test]
    fn active_identity_requires_every_persisted_field() {
        let fence = fence();
        let expected = identity("public", "items");
        let current = stored(fence);
        assert_eq!(
            validate_stored_identity(fence, &expected, &current).unwrap(),
            1234
        );

        let mut changed = current.clone();
        changed.pipeline_id = PipelineId::new();
        assert!(matches!(
            validate_stored_identity(fence, &expected, &changed),
            Err(ManagedTableError::PipelineMismatch { .. })
        ));
        changed = current.clone();
        changed.source_relation_id += 1;
        assert!(matches!(
            validate_stored_identity(fence, &expected, &changed),
            Err(ManagedTableError::SourceRelationMismatch { .. })
        ));
        changed = current.clone();
        changed.table_generation += 1;
        assert!(matches!(
            validate_stored_identity(fence, &expected, &changed),
            Err(ManagedTableError::GenerationMismatch { .. })
        ));
        changed = current.clone();
        changed.schema_fingerprint.push_str("-changed");
        assert!(matches!(
            validate_stored_identity(fence, &expected, &changed),
            Err(ManagedTableError::SchemaFingerprintMismatch(_))
        ));
        changed = current.clone();
        changed.state = "blocked".to_owned();
        assert!(matches!(
            validate_stored_identity(fence, &expected, &changed),
            Err(ManagedTableError::StateMismatch { .. })
        ));
        changed = current.clone();
        changed.relation_oid = None;
        assert!(matches!(
            validate_stored_identity(fence, &expected, &changed),
            Err(ManagedTableError::MissingRelationIdentity(_))
        ));
    }

    #[test]
    fn older_table_fence_is_adoptable_but_newer_fence_is_rejected() {
        let fence = fence();
        let expected = identity("public", "items");
        let mut current = stored(fence);
        current.fencing_token -= 1;
        assert!(validate_stored_identity(fence, &expected, &current).is_ok());

        current.fencing_token = fence.fencing_token + 1;
        assert!(matches!(
            validate_stored_identity(fence, &expected, &current),
            Err(ManagedTableError::NewerFence {
                current: 11,
                actual: 12,
                ..
            })
        ));
    }

    #[test]
    fn physical_relation_oid_must_match_after_the_table_lock() {
        let table = QualifiedName::new("public", "items").unwrap();
        assert!(validate_relation_identity(&table, 42, 42).is_ok());
        assert!(matches!(
            validate_relation_identity(&table, 42, 43),
            Err(ManagedTableError::RelationIdentityMismatch {
                expected: 42,
                actual: 43,
                ..
            })
        ));
    }

    #[test]
    fn sql_locks_metadata_and_filters_catalog_identity() {
        assert!(LOCK_MANAGED_TABLE_SQL.contains("target_schema = $1"));
        assert!(LOCK_MANAGED_TABLE_SQL.contains("target_table = $2"));
        assert!(LOCK_MANAGED_TABLE_SQL.trim_end().ends_with("FOR UPDATE"));
        assert!(LOAD_RELATION_OID_SQL.contains("c.oid::bigint"));
        assert!(LOAD_RELATION_OID_SQL.contains("c.relkind IN ('r', 'p')"));
    }
}
