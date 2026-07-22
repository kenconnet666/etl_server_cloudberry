//! Immutable source-schema to target-name planning.

use std::collections::{HashMap, HashSet};

use cloudberry_etl_core::{
    id::PipelineId,
    mapping::{DefaultNameMapper, SourcePrefix, shorten_identifier},
    schema::{QualifiedName, TableSchema},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use super::{PipelineSettings, TargetStorage};

#[derive(Debug, Error)]
pub enum TablePlanningError {
    #[error("invalid default table mapping: {0}")]
    DefaultMapping(#[from] cloudberry_etl_core::CoreError),
    #[error("explicit mapping references source table `{0}` outside the supported inventory")]
    UnknownExplicitSource(String),
    #[error("target table `{0}` is selected by more than one source table")]
    DuplicateTarget(String),
    #[error("shadow relation `{0}` is not unique")]
    DuplicateShadow(String),
    #[error("staging relation `{0}` is not unique")]
    DuplicateStaging(String),
    #[error("failed to serialize the source schema: {0}")]
    Fingerprint(#[from] serde_json::Error),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedTable {
    pub source: TableSchema,
    pub target: QualifiedName,
    pub shadow: QualifiedName,
    pub staging_name: String,
    pub storage: TargetStorage,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, Copy)]
pub struct TargetPlanningContext<'a> {
    pub database: &'a str,
    pub default_storage: TargetStorage,
}

pub fn plan_tables(
    pipeline_id: PipelineId,
    topology_generation: u64,
    source_prefix: &SourcePrefix,
    source_database: &str,
    target: TargetPlanningContext<'_>,
    settings: &PipelineSettings,
    tables: Vec<TableSchema>,
) -> Result<Vec<PlannedTable>, TablePlanningError> {
    let mapper = DefaultNameMapper {
        target_database: target.database.to_owned(),
        source_prefix: source_prefix.clone(),
        source_database: source_database.to_owned(),
    };
    let inventory_names: HashSet<_> = tables.iter().map(|table| table.name.clone()).collect();
    if let Some(mapping) = settings
        .table_mappings
        .iter()
        .find(|mapping| !inventory_names.contains(&mapping.source))
    {
        return Err(TablePlanningError::UnknownExplicitSource(
            mapping.source.to_string(),
        ));
    }

    let explicit: HashMap<_, _> = settings
        .table_mappings
        .iter()
        .map(|mapping| (mapping.source.clone(), mapping))
        .collect();
    let mut targets = HashSet::with_capacity(tables.len());
    let mut shadows = HashSet::with_capacity(tables.len());
    let mut staging_names = HashSet::with_capacity(tables.len());
    let mut planned = Vec::with_capacity(tables.len());

    for source in tables {
        let (target, storage) = match explicit.get(&source.name) {
            Some(mapping) => (
                mapping.target.clone(),
                mapping.storage.unwrap_or(target.default_storage),
            ),
            None => (mapper.map(&source.name)?.relation, target.default_storage),
        };
        if !targets.insert(target.clone()) {
            return Err(TablePlanningError::DuplicateTarget(target.to_string()));
        }

        let suffix = format!(
            "{}_{}_{}",
            pipeline_id.as_uuid().simple(),
            source.relation_id,
            topology_generation
        );
        let shadow = QualifiedName::new(
            target.schema.clone(),
            shorten_identifier(&format!("pg2cb_shadow_{suffix}")),
        )?;
        if !shadows.insert(shadow.clone()) {
            return Err(TablePlanningError::DuplicateShadow(shadow.to_string()));
        }
        let staging_name = shorten_identifier(&format!("pg2cb_stage_{suffix}"));
        if !staging_names.insert(staging_name.clone()) {
            return Err(TablePlanningError::DuplicateStaging(staging_name));
        }
        let schema_fingerprint = schema_fingerprint(&source, storage)?;
        planned.push(PlannedTable {
            source,
            target,
            shadow,
            staging_name,
            storage,
            schema_fingerprint,
        });
    }
    Ok(planned)
}

/// Hash portable schema identity and the selected physical storage profile.
///
/// Relation/type OIDs and runtime generations are excluded. Storage is included so changing a
/// profile cannot silently reuse a physically incompatible managed table.
pub fn schema_fingerprint(
    table: &TableSchema,
    storage: TargetStorage,
) -> Result<String, TablePlanningError> {
    let mut value = serde_json::to_value(table)?;
    if let Value::Object(object) = &mut value {
        object.remove("relation_id");
        object.remove("generation");
    }
    remove_object_key(&mut value, "oid");
    let digest = Sha256::digest(serde_json::to_vec(&(value, storage.fingerprint_tag()))?);
    let hexadecimal = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("sha256:{hexadecimal}"))
}

fn remove_object_key(value: &mut Value, key: &str) {
    match value {
        Value::Array(values) => {
            for value in values {
                remove_object_key(value, key);
            }
        }
        Value::Object(object) => {
            object.remove(key);
            for value in object.values_mut() {
                remove_object_key(value, key);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use cloudberry_etl_core::schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, ReplicaIdentity,
        TableKind,
    };
    use serde_json::json;

    use super::*;

    fn table(relation_id: u32, schema: &str, name: &str) -> TableSchema {
        TableSchema {
            relation_id,
            generation: 1,
            name: QualifiedName::new(schema, name).unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnSchema {
                attnum: 1,
                name: "id".into(),
                data_type: PgType {
                    oid: 23,
                    name: QualifiedName::new("pg_catalog", "int4").unwrap(),
                    kind: PgTypeKind::Int4,
                },
                nullable: false,
                primary_key_ordinal: Some(1),
                generated: GeneratedColumn::None,
                identity: IdentityColumn::None,
                collation: None,
            }],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn target(default_storage: TargetStorage) -> TargetPlanningContext<'static> {
        TargetPlanningContext {
            database: "analytics",
            default_storage,
        }
    }

    #[test]
    fn fingerprint_ignores_local_oids_and_runtime_generation() {
        let first = table(10, "public", "items");
        let mut restored = first.clone();
        restored.relation_id = 99;
        restored.generation = 8;
        restored.columns[0].data_type.oid = 9_999;
        assert_eq!(
            schema_fingerprint(&first, TargetStorage::AoColumn).unwrap(),
            schema_fingerprint(&restored, TargetStorage::AoColumn).unwrap()
        );
        restored.columns[0].data_type.kind = PgTypeKind::Int8;
        assert_ne!(
            schema_fingerprint(&first, TargetStorage::AoColumn).unwrap(),
            schema_fingerprint(&restored, TargetStorage::AoColumn).unwrap()
        );
        assert_ne!(
            schema_fingerprint(&first, TargetStorage::AoColumn).unwrap(),
            schema_fingerprint(&first, TargetStorage::PaxExperimental).unwrap()
        );
    }

    #[test]
    fn explicit_and_default_mappings_share_one_collision_domain() {
        let settings = PipelineSettings::parse(&json!({
            "table_mappings": [{
                "source": {"schema": "other", "name": "items"},
                "target": {"schema": "erp__source__public", "name": "items"}
            }]
        }))
        .unwrap();
        let result = plan_tables(
            PipelineId::new(),
            1,
            &SourcePrefix::new("erp").unwrap(),
            "source",
            target(TargetStorage::AoColumn),
            &settings,
            vec![table(1, "public", "items"), table(2, "other", "items")],
        );
        assert!(matches!(
            result,
            Err(TablePlanningError::DuplicateTarget(_))
        ));
    }

    #[test]
    fn rejects_typoed_explicit_source_and_derives_stable_names() {
        let typo = PipelineSettings::parse(&json!({
            "table_mappings": [{
                "source": {"schema": "public", "name": "missing"},
                "target": {"schema": "mapped", "name": "items"}
            }]
        }))
        .unwrap();
        let pipeline = PipelineId::new();
        let prefix = SourcePrefix::new("erp").unwrap();
        assert!(matches!(
            plan_tables(
                pipeline,
                1,
                &prefix,
                "source",
                target(TargetStorage::AoColumn),
                &typo,
                vec![table(1, "public", "items")]
            ),
            Err(TablePlanningError::UnknownExplicitSource(_))
        ));

        let settings = PipelineSettings::default();
        let first = plan_tables(
            pipeline,
            7,
            &prefix,
            "source",
            target(TargetStorage::AoColumn),
            &settings,
            vec![table(1, "public", "items")],
        )
        .unwrap();
        let second = plan_tables(
            pipeline,
            7,
            &prefix,
            "source",
            target(TargetStorage::AoColumn),
            &settings,
            vec![table(1, "public", "items")],
        )
        .unwrap();
        assert_eq!(first, second);
        assert_eq!(first[0].target.to_string(), "erp__source__public.items");
        assert!(first[0].shadow.name.len() <= 63);
        assert!(first[0].staging_name.len() <= 63);
        assert_eq!(first[0].storage, TargetStorage::AoColumn);
    }

    #[test]
    fn explicit_mapping_can_override_the_target_storage_default() {
        let settings = PipelineSettings::parse(&json!({
            "table_mappings": [{
                "source": {"schema": "public", "name": "items"},
                "target": {"schema": "analytics", "name": "items"},
                "storage": "pax_experimental"
            }]
        }))
        .unwrap();
        let planned = plan_tables(
            PipelineId::new(),
            1,
            &SourcePrefix::new("erp").unwrap(),
            "source",
            target(TargetStorage::AoColumn),
            &settings,
            vec![table(1, "public", "items")],
        )
        .unwrap();
        assert_eq!(planned[0].storage, TargetStorage::PaxExperimental);
        assert_ne!(
            planned[0].schema_fingerprint,
            schema_fingerprint(&planned[0].source, TargetStorage::AoColumn).unwrap()
        );
    }

    #[test]
    fn rejects_explicit_utf8_names_that_postgres_would_truncate() {
        let result = PipelineSettings::parse(&json!({
            "table_mappings": [{
                "source": {"schema": "public", "name": "items"},
                "target": {"schema": "mapped", "name": "界".repeat(22)}
            }]
        }));

        assert!(result.is_err());
    }

    #[test]
    fn default_mapping_shortens_long_composite_schema_deterministically() {
        let pipeline = PipelineId::new();
        let prefix = SourcePrefix::new("p".repeat(24)).unwrap();
        let source_database = "d".repeat(63);
        let source = table(1, &"s".repeat(63), "items");

        let first = plan_tables(
            pipeline,
            1,
            &prefix,
            &source_database,
            target(TargetStorage::AoColumn),
            &PipelineSettings::default(),
            vec![source.clone()],
        )
        .unwrap();
        let second = plan_tables(
            pipeline,
            1,
            &prefix,
            &source_database,
            target(TargetStorage::AoColumn),
            &PipelineSettings::default(),
            vec![source],
        )
        .unwrap();

        assert_eq!(first[0].target, second[0].target);
        assert!(first[0].target.schema.len() <= 63);

        let invalid_target_database = "界".repeat(22);
        let error = plan_tables(
            pipeline,
            1,
            &prefix,
            &source_database,
            TargetPlanningContext {
                database: &invalid_target_database,
                default_storage: TargetStorage::AoColumn,
            },
            &PipelineSettings::default(),
            vec![table(1, "public", "items")],
        )
        .unwrap_err();
        assert!(matches!(error, TablePlanningError::DefaultMapping(_)));
    }
}
