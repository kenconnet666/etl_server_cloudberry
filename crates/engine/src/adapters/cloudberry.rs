//! Cloudberry apply adapter for normalized transaction batches.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use cloudberry_etl_core::{
    change::{TableChange, TransactionChange},
    schema::{QualifiedName, TableSchema},
};
use cloudberry_etl_target_cloudberry::{
    apply::{ApplyPlan, ApplyPlanError, ApplyRequest, TableApplyBatch, execute_apply, plan_apply},
    checkpoint::{CheckpointKey, NodeCheckpoint, PipelineFence},
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::{
    batch::TransactionBatch,
    normalize::normalize_table_changes,
    pipeline::{PipelineError, TransactionSink},
};

#[derive(Debug, Error)]
pub enum AdapterConfigError {
    #[error(transparent)]
    ApplyPlan(#[from] ApplyPlanError),
    #[error("duplicate table binding for relation {relation_id} generation {generation}")]
    DuplicateBinding { relation_id: u32, generation: u64 },
    #[error("target table `{0}` is bound more than once")]
    DuplicateTarget(String),
    #[error("staging table name `{0}` is bound more than once")]
    DuplicateStagingName(String),
    #[error("replication slot name cannot be empty or contain NUL")]
    InvalidSlotName,
}

/// Source schema selection used to decide whether a transactional DDL event requires a rebuild.
/// Empty DDL scope is always treated as unknown and therefore fail-closed.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DdlScope {
    include_schemas: Option<HashSet<String>>,
    exclude_schemas: HashSet<String>,
}

impl DdlScope {
    #[must_use]
    pub fn new(include_schemas: Option<HashSet<String>>, exclude_schemas: HashSet<String>) -> Self {
        Self {
            include_schemas,
            exclude_schemas,
        }
    }

    #[must_use]
    pub fn from_lists(include_schemas: Option<&[String]>, exclude_schemas: &[String]) -> Self {
        Self::new(
            include_schemas.map(|schemas| schemas.iter().cloned().collect()),
            exclude_schemas.iter().cloned().collect(),
        )
    }

    pub fn exclude(&mut self, schema: impl Into<String>) {
        self.exclude_schemas.insert(schema.into());
    }

    fn requires_barrier(&self, message: &cloudberry_etl_core::change::DdlMessage) -> bool {
        if message.affected_schemas.is_empty() {
            return true;
        }
        match &self.include_schemas {
            Some(included) => message
                .affected_schemas
                .iter()
                .any(|schema| included.contains(schema) && !self.exclude_schemas.contains(schema)),
            None => message
                .affected_schemas
                .iter()
                .any(|schema| !self.exclude_schemas.contains(schema)),
        }
    }
}

/// Immutable source-schema to target-table binding with a precompiled apply plan.
#[derive(Debug, Clone)]
pub struct TableBinding {
    schema: TableSchema,
    plan: Arc<ApplyPlan>,
}

impl TableBinding {
    pub fn new(
        schema: TableSchema,
        target: QualifiedName,
        staging_name: impl Into<String>,
    ) -> Result<Self, AdapterConfigError> {
        let staging_name = staging_name.into();
        let plan = plan_apply(&schema, target, &staging_name)?;
        Ok(Self {
            schema,
            plan: Arc::new(plan),
        })
    }

    #[must_use]
    pub const fn schema(&self) -> &TableSchema {
        &self.schema
    }

    #[must_use]
    pub fn plan(&self) -> &Arc<ApplyPlan> {
        &self.plan
    }

    const fn key(&self) -> (u32, u64) {
        (self.schema.relation_id, self.schema.generation)
    }
}

/// Read-only registry used for every batch handled by one source-node sink.
#[derive(Debug, Clone)]
pub struct TableBindingRegistry {
    bindings: HashMap<(u32, u64), TableBinding>,
}

impl TableBindingRegistry {
    pub fn new(
        bindings: impl IntoIterator<Item = TableBinding>,
    ) -> Result<Self, AdapterConfigError> {
        let mut by_key = HashMap::new();
        let mut targets = HashSet::new();
        let mut staging_names = HashSet::new();
        for binding in bindings {
            let key = binding.key();
            if by_key.contains_key(&key) {
                return Err(AdapterConfigError::DuplicateBinding {
                    relation_id: key.0,
                    generation: key.1,
                });
            }
            let target = binding.plan.table.target.clone();
            if !targets.insert(target.clone()) {
                return Err(AdapterConfigError::DuplicateTarget(target.to_string()));
            }
            if !staging_names.insert(binding.plan.staging_name.clone()) {
                return Err(AdapterConfigError::DuplicateStagingName(
                    binding.plan.staging_name.clone(),
                ));
            }
            by_key.insert(key, binding);
        }
        Ok(Self { bindings: by_key })
    }

    #[must_use]
    pub fn get(&self, relation_id: u32, generation: u64) -> Option<&TableBinding> {
        self.bindings.get(&(relation_id, generation))
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }
}

/// Convert a committed node batch into one atomic target apply request using conservative DDL
/// handling. Kept as a compatibility wrapper for callers that do not configure a source scope.
pub fn build_apply_request(
    fence: PipelineFence,
    slot_name: &str,
    registry: &TableBindingRegistry,
    batch: &TransactionBatch,
) -> Result<ApplyRequest, PipelineError> {
    build_apply_request_scoped(fence, slot_name, registry, &DdlScope::default(), batch)
}

/// Convert a committed node batch using the configured source schema scope.
pub fn build_apply_request_scoped(
    fence: PipelineFence,
    slot_name: &str,
    registry: &TableBindingRegistry,
    ddl_scope: &DdlScope,
    batch: &TransactionBatch,
) -> Result<ApplyRequest, PipelineError> {
    reject_schema_barriers(batch, ddl_scope)?;

    let mut grouped: BTreeMap<(u32, u64), Vec<&TableChange>> = BTreeMap::new();
    for transaction in batch.transactions() {
        for change in &transaction.changes {
            if let TransactionChange::Row(change) = change {
                grouped
                    .entry((change.relation_id, change.generation))
                    .or_default()
                    .push(change);
            }
        }
    }

    let mut tables = Vec::with_capacity(grouped.len());
    for ((relation_id, generation), changes) in grouped {
        let binding = registry.get(relation_id, generation).ok_or_else(|| {
            PipelineError::Target(format!(
                "no immutable table binding for relation {relation_id} generation {generation}"
            ))
        })?;
        let rows = normalize_table_changes(binding.schema(), changes)?;
        if !rows.is_empty() {
            tables.push(TableApplyBatch {
                plan: Arc::clone(binding.plan()),
                rows,
            });
        }
    }

    let final_position = &batch.final_transaction().final_position;
    Ok(ApplyRequest {
        fence,
        checkpoint: NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: final_position.node_id,
            },
            system_identifier: final_position.system_identifier,
            timeline: final_position.timeline,
            slot_name: slot_name.to_owned(),
            applied_lsn: final_position.lsn,
        },
        tables,
    })
}

fn reject_schema_barriers(
    batch: &TransactionBatch,
    ddl_scope: &DdlScope,
) -> Result<(), PipelineError> {
    for transaction in batch.transactions() {
        for change in &transaction.changes {
            match change {
                TransactionChange::Ddl(message) => {
                    if ddl_scope.requires_barrier(message) {
                        return Err(PipelineError::SchemaBarrier(format!(
                            "DDL `{}` in transaction {} for relations {:?} and schemas {:?}",
                            message.command_tag,
                            transaction.xid,
                            message.relation_ids,
                            message.affected_schemas
                        )));
                    }
                }
                TransactionChange::Truncate { relation_ids, .. } => {
                    return Err(PipelineError::SchemaBarrier(format!(
                        "TRUNCATE in transaction {} for relations {relation_ids:?}",
                        transaction.xid
                    )));
                }
                TransactionChange::Row(_) => {}
            }
        }
    }
    Ok(())
}

/// Applies normalized rows and the target-authoritative checkpoint atomically.
pub struct CloudberryTransactionSink {
    client: Mutex<Client>,
    fence: PipelineFence,
    slot_name: String,
    registry: TableBindingRegistry,
    ddl_scope: DdlScope,
}

impl CloudberryTransactionSink {
    pub fn new(
        client: Client,
        fence: PipelineFence,
        slot_name: impl Into<String>,
        registry: TableBindingRegistry,
        ddl_scope: DdlScope,
    ) -> Result<Self, AdapterConfigError> {
        let slot_name = slot_name.into();
        if slot_name.is_empty() || slot_name.contains('\0') {
            return Err(AdapterConfigError::InvalidSlotName);
        }
        Ok(Self {
            client: Mutex::new(client),
            fence,
            slot_name,
            registry,
            ddl_scope,
        })
    }
}

#[async_trait]
impl TransactionSink for CloudberryTransactionSink {
    async fn apply(&self, batch: &TransactionBatch) -> Result<(), PipelineError> {
        let request = build_apply_request_scoped(
            self.fence,
            &self.slot_name,
            &self.registry,
            &self.ddl_scope,
            batch,
        )?;
        let mut client = self.client.lock().await;
        execute_apply(&mut client, &request)
            .await
            .map_err(|error| PipelineError::Target(error.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use bytes::Bytes;
    use chrono::Utc;
    use cloudberry_etl_core::{
        change::{
            Cell, DdlMessage, RowChange, SourcePosition, SourceTransaction, TableChange, Tuple,
        },
        id::PipelineId,
        lsn::PgLsn,
        schema::{
            ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, ReplicaIdentity,
            TableKind,
        },
    };

    use super::*;
    use crate::batch::{BatchLimits, Batcher};

    fn text(value: &str) -> Cell {
        Cell::Text(Bytes::copy_from_slice(value.as_bytes()))
    }

    fn column(attnum: i16, name: &str, primary_key_ordinal: Option<u16>) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid: 23,
                name: QualifiedName::new("pg_catalog", "int4").unwrap(),
                kind: PgTypeKind::Int4,
            },
            nullable: primary_key_ordinal.is_none(),
            primary_key_ordinal,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn schema(relation_id: u32, generation: u64, name: &str) -> TableSchema {
        TableSchema {
            relation_id,
            generation,
            name: QualifiedName::new("public", name).unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![column(1, "id", Some(1)), column(2, "payload", None)],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn insert(relation_id: u32, generation: u64, id: &str, payload: &str) -> TransactionChange {
        TransactionChange::Row(TableChange {
            relation_id,
            generation,
            change: RowChange::Insert {
                new: Tuple {
                    cells: vec![text(id), text(payload)],
                },
            },
        })
    }

    fn transaction(lsn: u64, changes: Vec<TransactionChange>) -> SourceTransaction {
        SourceTransaction {
            xid: lsn as u32,
            commit_time: Utc::now(),
            final_position: SourcePosition {
                node_id: 3,
                system_identifier: 99,
                timeline: 2,
                lsn: PgLsn::new(lsn),
            },
            changes,
        }
    }

    fn batch(changes: Vec<TransactionChange>) -> TransactionBatch {
        let mut batcher = Batcher::new(BatchLimits {
            max_rows: 100,
            max_bytes: 1_024 * 1_024,
            max_delay: Duration::from_secs(1),
        })
        .unwrap();
        batcher.push(transaction(20, changes)).unwrap();
        batcher.flush().unwrap()
    }

    fn fence() -> PipelineFence {
        PipelineFence {
            pipeline_id: PipelineId::new(),
            topology_generation: 4,
            fencing_token: 8,
        }
    }

    fn binding(
        relation_id: u32,
        generation: u64,
        source_name: &str,
        target_name: &str,
        staging_name: &str,
    ) -> TableBinding {
        TableBinding::new(
            schema(relation_id, generation, source_name),
            QualifiedName::new("target", target_name).unwrap(),
            staging_name,
        )
        .unwrap()
    }

    #[test]
    fn builds_an_ordered_normalized_request_and_node_checkpoint() {
        let registry = TableBindingRegistry::new([
            binding(9, 2, "later", "later", "stage_later"),
            binding(7, 3, "first", "first", "stage_first"),
        ])
        .unwrap();
        let batch = batch(vec![insert(9, 2, "2", "b"), insert(7, 3, "1", "a")]);
        let fence = fence();
        let request = build_apply_request(fence, "slot_node_3", &registry, &batch).unwrap();

        assert_eq!(request.tables.len(), 2);
        assert_eq!(request.tables[0].plan.table.target.name, "first");
        assert_eq!(request.tables[1].plan.table.target.name, "later");
        assert!(Arc::ptr_eq(
            &request.tables[0].plan,
            registry.get(7, 3).unwrap().plan()
        ));
        assert_eq!(request.tables[0].rows[0].cells, [text("1"), text("a")]);
        assert_eq!(request.checkpoint.key.pipeline_id, fence.pipeline_id);
        assert_eq!(request.checkpoint.key.topology_generation, 4);
        assert_eq!(request.checkpoint.key.node_id, 3);
        assert_eq!(request.checkpoint.system_identifier, 99);
        assert_eq!(request.checkpoint.timeline, 2);
        assert_eq!(request.checkpoint.slot_name, "slot_node_3");
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));
    }

    #[test]
    fn empty_row_batch_still_advances_the_checkpoint() {
        let registry = TableBindingRegistry::new([]).unwrap();
        let batch = batch(Vec::new());
        let request = build_apply_request(fence(), "slot", &registry, &batch).unwrap();
        assert!(request.tables.is_empty());
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));
    }

    #[test]
    fn rejects_ddl_and_truncate_before_target_apply_planning() {
        let registry = TableBindingRegistry::new([]).unwrap();
        let ddl = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![7],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "abc".to_owned(),
        })]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &ddl),
            Err(PipelineError::SchemaBarrier(message)) if message.contains("ALTER TABLE")
        ));
        let external_publication_ddl = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER PUBLICATION".to_owned(),
            relation_ids: vec![],
            affected_schemas: vec![],
            schema_fingerprint: "abc".to_owned(),
        })]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &external_publication_ddl),
            Err(PipelineError::SchemaBarrier(message)) if message.contains("ALTER PUBLICATION")
        ));

        let truncate = batch(vec![TransactionChange::Truncate {
            relation_ids: vec![7],
            cascade: false,
            restart_identity: false,
        }]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &truncate),
            Err(PipelineError::SchemaBarrier(message)) if message.contains("TRUNCATE")
        ));
    }

    #[test]
    fn ddl_scope_ignores_out_of_scope_events_but_keeps_checkpoint() {
        let registry = TableBindingRegistry::new([]).unwrap();
        let ddl = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![],
            affected_schemas: vec!["other".to_owned()],
            schema_fingerprint: "abc".to_owned(),
        })]);
        let scope = DdlScope::new(Some(HashSet::from(["included".to_owned()])), HashSet::new());
        let request = build_apply_request_scoped(fence(), "slot", &registry, &scope, &ddl).unwrap();
        assert!(request.tables.is_empty());
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));
    }

    #[test]
    fn ddl_scope_is_conservative_for_unknown_and_unincluded_modes() {
        let registry = TableBindingRegistry::new([]).unwrap();
        let unknown = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER PUBLICATION".to_owned(),
            relation_ids: vec![],
            affected_schemas: vec![],
            schema_fingerprint: "abc".to_owned(),
        })]);
        assert!(matches!(
            build_apply_request_scoped(
                fence(),
                "slot",
                &registry,
                &DdlScope::new(Some(HashSet::from(["included".to_owned()])), HashSet::new()),
                &unknown,
            ),
            Err(PipelineError::SchemaBarrier(_))
        ));

        let known = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![],
            affected_schemas: vec!["excluded".to_owned()],
            schema_fingerprint: "abc".to_owned(),
        })]);
        let mut excluded = HashSet::new();
        excluded.insert("excluded".to_owned());
        let request = build_apply_request_scoped(
            fence(),
            "slot",
            &registry,
            &DdlScope::new(None, excluded),
            &known,
        )
        .unwrap();
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));
    }

    #[test]
    fn rejects_missing_generation_bindings_and_normalization_errors() {
        let registry =
            TableBindingRegistry::new([binding(7, 3, "items", "items", "stage_items")]).unwrap();
        let wrong_generation = batch(vec![insert(7, 4, "1", "a")]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &wrong_generation),
            Err(PipelineError::Target(message)) if message.contains("generation 4")
        ));

        let null_key = batch(vec![TransactionChange::Row(TableChange {
            relation_id: 7,
            generation: 3,
            change: RowChange::Insert {
                new: Tuple {
                    cells: vec![Cell::Null, text("a")],
                },
            },
        })]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &null_key),
            Err(PipelineError::Normalize(_))
        ));
    }

    #[test]
    fn registry_rejects_ambiguous_runtime_ownership() {
        let duplicate_binding = TableBindingRegistry::new([
            binding(7, 3, "items", "items", "stage_items"),
            binding(7, 3, "items", "other", "stage_other"),
        ]);
        assert!(matches!(
            duplicate_binding,
            Err(AdapterConfigError::DuplicateBinding { .. })
        ));

        let duplicate_target = TableBindingRegistry::new([
            binding(7, 3, "items", "shared", "stage_items"),
            binding(8, 1, "other", "shared", "stage_other"),
        ]);
        assert!(matches!(
            duplicate_target,
            Err(AdapterConfigError::DuplicateTarget(_))
        ));

        let duplicate_staging = TableBindingRegistry::new([
            binding(7, 3, "items", "items", "stage_shared"),
            binding(8, 1, "other", "other", "stage_shared"),
        ]);
        assert!(matches!(
            duplicate_staging,
            Err(AdapterConfigError::DuplicateStagingName(_))
        ));
    }
}
