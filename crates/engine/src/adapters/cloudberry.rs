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
use cloudberry_etl_source_postgres::{
    spool::{ChangeChunk, ChangeStats, ChunkLimits},
    wal::CommittedTransaction,
};
use cloudberry_etl_target_cloudberry::{
    apply::{
        ApplyPlan, ApplyPlanError, ApplyRequest, DataChunkDisposition, LedgeredDataChunkOutcome,
        LedgeredDataChunkRequest, LedgeredEmptyTransactionOutcome, TableApplyBatch,
        execute_ledgered_data_chunk, execute_ledgered_empty_transaction, plan_apply,
    },
    checkpoint::{CheckpointKey, NodeCheckpoint, PipelineFence},
    chunk::{DataChunkIdentity, TransactionChunkKey, TransactionChunkManifest},
    managed::TableApplyIdentity,
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::{
    batch::TransactionBatch,
    normalize::normalize_table_changes,
    pipeline::{PipelineError, TransactionSink},
};

// This versions the logical record digest consumed by the target ledger, not its memory or spool
// storage representation. The same source transaction must retain one identity if spill policy
// changes between attempts.
const TRANSACTION_MANIFEST_VERSION: u16 = 1;
const DEFAULT_CHUNK_MAX_RECORDS: usize = 10_000;
const DEFAULT_CHUNK_MAX_BYTES: usize = 16 * 1024 * 1024;

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
    #[error("invalid target chunk limits: {0}")]
    InvalidChunkLimits(String),
    #[error("table generation {0} exceeds the target bigint range")]
    InvalidTableGeneration(u64),
    #[error("table schema fingerprint cannot be empty or contain NUL")]
    InvalidSchemaFingerprint,
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
        use cloudberry_etl_core::change::DdlReplicationImpact;

        // Commands proven harmless to the mirrored row stream (index maintenance,
        // comments, privileges, statistics) never need a barrier, even when they
        // touch a managed schema. Everything else stays fail-closed below.
        if message.replication_impact() == DdlReplicationImpact::Irrelevant {
            return false;
        }
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
    identity: Arc<TableApplyIdentity>,
    plan: Arc<ApplyPlan>,
}

impl TableBinding {
    pub fn new(
        schema: TableSchema,
        target: QualifiedName,
        staging_name: impl Into<String>,
        table_generation: u64,
        schema_fingerprint: impl Into<String>,
    ) -> Result<Self, AdapterConfigError> {
        if i64::try_from(table_generation).is_err() {
            return Err(AdapterConfigError::InvalidTableGeneration(table_generation));
        }
        let schema_fingerprint = schema_fingerprint.into();
        if schema_fingerprint.is_empty() || schema_fingerprint.contains('\0') {
            return Err(AdapterConfigError::InvalidSchemaFingerprint);
        }
        let staging_name = staging_name.into();
        let identity = Arc::new(TableApplyIdentity {
            target: target.clone(),
            source_relation_id: schema.relation_id,
            table_generation,
            schema_fingerprint,
        });
        let plan = plan_apply(&schema, target, &staging_name)?;
        Ok(Self {
            schema,
            identity,
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

    #[must_use]
    pub fn identity(&self) -> &Arc<TableApplyIdentity> {
        &self.identity
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

    let mut changes = Vec::new();
    for transaction in batch.transactions() {
        let reader = transaction
            .change_source
            .reader()
            .map_err(|error| PipelineError::Source(error.to_string()))?;
        for change in reader {
            let change = change.map_err(|error| PipelineError::Source(error.to_string()))?;
            if let TransactionChange::Row(change) = change {
                changes.push(change);
            }
        }
    }
    let tables = build_table_apply_batches(registry, &changes)?;

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

fn build_table_apply_batches<'a>(
    registry: &TableBindingRegistry,
    changes: impl IntoIterator<Item = &'a TableChange>,
) -> Result<Vec<TableApplyBatch>, PipelineError> {
    let mut grouped: BTreeMap<(u32, u64), Vec<&TableChange>> = BTreeMap::new();
    for change in changes {
        grouped
            .entry((change.relation_id, change.generation))
            .or_default()
            .push(change);
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
                identity: Arc::clone(binding.identity()),
                plan: Arc::clone(binding.plan()),
                rows,
            });
        }
    }
    Ok(tables)
}

fn build_chunk_tables(
    registry: &TableBindingRegistry,
    chunk: &ChangeChunk,
) -> Result<Vec<TableApplyBatch>, PipelineError> {
    build_table_apply_batches(
        registry,
        chunk.changes.iter().filter_map(|change| match change {
            TransactionChange::Row(change) => Some(change),
            TransactionChange::Ddl(_) | TransactionChange::Truncate { .. } => None,
        }),
    )
}

fn transaction_manifest(
    fence: PipelineFence,
    slot_name: &str,
    transaction: &CommittedTransaction,
    stats: ChangeStats,
) -> TransactionChunkManifest {
    let position = &transaction.final_position;
    TransactionChunkManifest {
        key: TransactionChunkKey {
            pipeline_id: fence.pipeline_id,
            topology_generation: fence.topology_generation,
            node_id: position.node_id,
            end_lsn: position.lsn,
        },
        system_identifier: position.system_identifier,
        timeline: position.timeline,
        slot_name: slot_name.to_owned(),
        xid: transaction.xid,
        manifest_version: TRANSACTION_MANIFEST_VERSION,
        record_count: stats.change_count,
        manifest_digest: stats.digest,
    }
}

fn reject_schema_barriers(
    batch: &TransactionBatch,
    ddl_scope: &DdlScope,
) -> Result<(), PipelineError> {
    // The immutable change-source summary is produced while assembling the batch. Keep the normal
    // row-only path single-pass; exceptional barrier batches are still fully inspected before the
    // first target transaction starts.
    if !batch.has_generation_barrier() {
        return Ok(());
    }
    for transaction in batch.transactions() {
        let stats = transaction
            .change_source
            .stats()
            .map_err(|error| PipelineError::Source(error.to_string()))?;
        if !stats.has_generation_barrier {
            continue;
        }
        let reader = transaction
            .change_source
            .reader()
            .map_err(|error| PipelineError::Source(error.to_string()))?;
        for change in reader {
            let change = change.map_err(|error| PipelineError::Source(error.to_string()))?;
            match change {
                TransactionChange::Ddl(message) => {
                    if ddl_scope.requires_barrier(&message) {
                        return Err(PipelineError::SchemaBarrier {
                            reason: format!(
                                "DDL `{}` in transaction {} for relations {:?} and schemas {:?}",
                                message.command_tag,
                                transaction.xid,
                                message.relation_ids,
                                message.affected_schemas
                            ),
                            command_tag: Some(message.command_tag.clone()),
                        });
                    }
                }
                TransactionChange::Truncate { relation_ids, .. } => {
                    return Err(PipelineError::SchemaBarrier {
                        reason: format!(
                            "TRUNCATE in transaction {} for relations {relation_ids:?}",
                            transaction.xid
                        ),
                        command_tag: None,
                    });
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
    chunk_limits: ChunkLimits,
}

impl CloudberryTransactionSink {
    pub fn new(
        client: Client,
        fence: PipelineFence,
        slot_name: impl Into<String>,
        registry: TableBindingRegistry,
        ddl_scope: DdlScope,
    ) -> Result<Self, AdapterConfigError> {
        Self::new_with_chunk_limits(
            client,
            fence,
            slot_name,
            registry,
            ddl_scope,
            ChunkLimits {
                max_records: DEFAULT_CHUNK_MAX_RECORDS,
                max_bytes: DEFAULT_CHUNK_MAX_BYTES,
            },
        )
    }

    pub fn new_with_chunk_limits(
        client: Client,
        fence: PipelineFence,
        slot_name: impl Into<String>,
        registry: TableBindingRegistry,
        ddl_scope: DdlScope,
        chunk_limits: ChunkLimits,
    ) -> Result<Self, AdapterConfigError> {
        let slot_name = slot_name.into();
        if slot_name.is_empty() || slot_name.contains('\0') {
            return Err(AdapterConfigError::InvalidSlotName);
        }
        chunk_limits
            .validate()
            .map_err(|error| AdapterConfigError::InvalidChunkLimits(error.to_string()))?;
        Ok(Self {
            client: Mutex::new(client),
            fence,
            slot_name,
            registry,
            ddl_scope,
            chunk_limits,
        })
    }

    async fn apply_transaction(
        &self,
        client: &mut Client,
        transaction: &CommittedTransaction,
    ) -> Result<(), PipelineError> {
        let stats = transaction
            .change_source
            .stats()
            .map_err(|error| PipelineError::Source(error.to_string()))?;
        let manifest = transaction_manifest(self.fence, &self.slot_name, transaction, stats);
        if manifest.record_count == 0 {
            return match execute_ledgered_empty_transaction(client, self.fence, &manifest)
                .await
                .map_err(|error| PipelineError::Target(error.to_string()))?
            {
                LedgeredEmptyTransactionOutcome::Completed { .. }
                | LedgeredEmptyTransactionOutcome::AlreadyCheckpointed { .. } => Ok(()),
            };
        }

        let mut next_seq = 0;

        let mut chunks = transaction
            .change_source
            .chunks_from(next_seq, self.chunk_limits)
            .map_err(|error| PipelineError::Source(error.to_string()))?;
        loop {
            let chunk = chunks
                .next()
                .transpose()
                .map_err(|error| PipelineError::Source(error.to_string()))?
                .ok_or_else(|| {
                    PipelineError::Source(format!(
                        "change source ended at sequence {next_seq}, before manifest record count {}",
                        manifest.record_count
                    ))
                })?;
            if chunk.start_seq != next_seq || chunk.end_seq > manifest.record_count {
                return Err(PipelineError::Source(format!(
                    "change chunk range {}..{} does not match resume sequence {next_seq} and manifest count {}",
                    chunk.start_seq, chunk.end_seq, manifest.record_count
                )));
            }
            let chunk_identity = DataChunkIdentity {
                start_seq: chunk.start_seq,
                end_seq: chunk.end_seq,
                digest: chunk.digest,
            };
            let tables = build_chunk_tables(&self.registry, &chunk)?;
            let request = LedgeredDataChunkRequest {
                fence: self.fence,
                manifest: manifest.clone(),
                chunk: chunk_identity,
                tables,
            };
            let outcome = execute_ledgered_data_chunk(client, &request)
                .await
                .map_err(|error| PipelineError::Target(error.to_string()))?;
            let (durable_next, disposition, completed) = match outcome {
                LedgeredDataChunkOutcome::InProgress {
                    next_seq,
                    disposition,
                } => (next_seq, disposition, false),
                LedgeredDataChunkOutcome::Completed {
                    next_seq,
                    disposition,
                    ..
                } => (next_seq, disposition, true),
                LedgeredDataChunkOutcome::AlreadyCheckpointed { .. } => return Ok(()),
            };
            validate_chunk_step(
                next_seq,
                manifest.record_count,
                chunk.end_seq,
                durable_next,
                disposition,
                completed,
            )?;
            if completed {
                return Ok(());
            }
            if durable_next != chunk.end_seq {
                chunks = transaction
                    .change_source
                    .chunks_from(durable_next, self.chunk_limits)
                    .map_err(|error| PipelineError::Source(error.to_string()))?;
            }
            next_seq = durable_next;
        }
    }
}

fn validate_chunk_step(
    previous: u64,
    record_count: u64,
    requested_end: u64,
    next_seq: u64,
    disposition: DataChunkDisposition,
    completed: bool,
) -> Result<(), PipelineError> {
    validate_resume_sequence(previous, record_count, next_seq)?;
    if matches!(disposition, DataChunkDisposition::Applied { .. }) && next_seq != requested_end {
        return Err(PipelineError::Target(format!(
            "target recorded chunk ending at {requested_end} at unexpected sequence {next_seq}"
        )));
    }
    if completed != (next_seq == record_count) {
        return Err(PipelineError::Target(format!(
            "target completion state does not match sequence {next_seq} of {record_count} records"
        )));
    }
    Ok(())
}

fn validate_resume_sequence(
    previous: u64,
    record_count: u64,
    next_seq: u64,
) -> Result<(), PipelineError> {
    if next_seq > record_count {
        return Err(PipelineError::Target(format!(
            "target resume sequence {next_seq} exceeds manifest record count {record_count}"
        )));
    }
    if next_seq < previous || (next_seq == previous && previous < record_count) {
        return Err(PipelineError::Target(format!(
            "target resume sequence {next_seq} did not advance beyond {previous}"
        )));
    }
    Ok(())
}

#[async_trait]
impl TransactionSink for CloudberryTransactionSink {
    async fn apply(&self, batch: &TransactionBatch) -> Result<(), PipelineError> {
        // Scan the complete batch before the first target chunk commits. A barrier later in a
        // batch must not be crossed by earlier data transactions.
        reject_schema_barriers(batch, &self.ddl_scope)?;
        let mut client = self.client.lock().await;
        for transaction in batch.transactions() {
            self.apply_transaction(&mut client, transaction).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, fs, path::PathBuf, time::Duration};

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
    use cloudberry_etl_source_postgres::spool::{
        ChangeSource, SpoolIdentity, SpoolJournal, SpoolLimits, SpoolResult,
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

    fn move_key(
        relation_id: u32,
        generation: u64,
        old_id: &str,
        new_id: &str,
        payload: &str,
    ) -> TransactionChange {
        TransactionChange::Row(TableChange {
            relation_id,
            generation,
            change: RowChange::Update {
                old_key: Some(Tuple {
                    cells: vec![text(old_id)],
                }),
                new: Tuple {
                    cells: vec![text(new_id), text(payload)],
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

    fn spooled_transaction(changes: &[TransactionChange]) -> (PathBuf, CommittedTransaction) {
        let root =
            std::env::temp_dir().join(format!("pg2cb-engine-spool-{}", uuid::Uuid::new_v4()));
        let journal = SpoolJournal::open(
            &root,
            SpoolIdentity {
                pipeline_id: PipelineId::new(),
                topology_generation: 4,
                node_id: 3,
                system_identifier: 99,
                timeline: 2,
            },
            SpoolLimits {
                memory_high_water_bytes: 64,
                segment_target_bytes: 64,
                disk_high_water_bytes: 1024 * 1024 * 1024,
                minimum_free_disk_bytes: 0,
            },
        )
        .unwrap();
        let mut writer = journal.begin(20, PgLsn::new(10)).unwrap();
        for change in changes {
            writer.append(change).unwrap();
        }
        let handle = writer.finish(PgLsn::new(19), PgLsn::new(20)).unwrap();
        (
            root,
            CommittedTransaction {
                transaction: transaction(20, Vec::new()),
                commit_lsn: PgLsn::new(19),
                change_source: ChangeSource::Spool(handle),
            },
        )
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
            4,
            format!("sha256:test-{relation_id}"),
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
        assert!(Arc::ptr_eq(
            &request.tables[0].identity,
            registry.get(7, 3).unwrap().identity()
        ));
        assert_eq!(request.tables[0].identity.source_relation_id, 7);
        assert_eq!(request.tables[0].identity.table_generation, 4);
        assert_eq!(
            request.tables[0].identity.schema_fingerprint,
            "sha256:test-7"
        );
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
    fn builds_stable_manifest_before_bounded_chunk_planning() {
        let changes = vec![
            insert(7, 3, "1", "a"),
            insert(7, 3, "2", "b"),
            insert(7, 3, "3", "c"),
        ];
        let committed = CommittedTransaction::from(transaction(20, changes));
        let stats = committed.change_source.stats().unwrap();
        let fence = fence();
        let manifest = transaction_manifest(fence, "slot", &committed, stats);
        let checkpoint = manifest.node_checkpoint();
        let whole = committed
            .change_source
            .chunks_from(
                0,
                ChunkLimits {
                    max_records: 100,
                    max_bytes: 1024 * 1024,
                },
            )
            .unwrap()
            .next()
            .unwrap()
            .unwrap();

        assert_eq!(manifest.record_count, 3);
        assert_eq!(manifest.manifest_digest, whole.digest);
        assert_eq!(manifest.key.end_lsn, PgLsn::new(20));
        assert_eq!(manifest.xid, 20);
        assert_eq!(checkpoint.applied_lsn, manifest.key.end_lsn);
    }

    #[test]
    fn memory_and_spool_build_identical_resumed_chunk_plans() {
        let changes = vec![
            insert(7, 3, "1", "a"),
            move_key(7, 3, "1", "2", "b"),
            insert(7, 3, "3", "c"),
        ];
        let memory = CommittedTransaction::from(transaction(20, changes.clone()));
        let (root, spool) = spooled_transaction(&changes);
        let limits = ChunkLimits {
            max_records: 1,
            max_bytes: 1024,
        };
        let memory_chunks = memory
            .change_source
            .chunks_from(0, limits)
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        let spool_chunks = spool
            .change_source
            .chunks_from(0, limits)
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        assert_eq!(memory_chunks, spool_chunks);
        let memory_chunk = &memory_chunks[1];
        let spool_chunk = &spool_chunks[1];
        let registry =
            TableBindingRegistry::new([binding(7, 3, "items", "items", "stage_items")]).unwrap();
        let memory_tables = build_chunk_tables(&registry, memory_chunk).unwrap();
        let spool_tables = build_chunk_tables(&registry, spool_chunk).unwrap();
        let fence = fence();

        assert_eq!(
            memory.change_source.stats().unwrap(),
            spool.change_source.stats().unwrap()
        );
        assert_eq!(
            transaction_manifest(
                fence,
                "slot",
                &memory,
                memory.change_source.stats().unwrap()
            ),
            transaction_manifest(fence, "slot", &spool, spool.change_source.stats().unwrap())
        );
        assert_eq!(memory_tables.len(), 1);
        assert_eq!(memory_tables[0].rows, spool_tables[0].rows);

        if let ChangeSource::Spool(handle) = &spool.change_source {
            handle.remove().unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resumes_pk_move_chain_at_the_durable_record_boundary() {
        let registry =
            TableBindingRegistry::new([binding(7, 3, "items", "items", "stage_items")]).unwrap();
        let committed = CommittedTransaction::from(transaction(
            20,
            vec![
                move_key(7, 3, "1", "2", "first"),
                move_key(7, 3, "2", "3", "second"),
            ],
        ));
        let chunk = committed
            .change_source
            .chunks_from(
                1,
                ChunkLimits {
                    max_records: 1,
                    max_bytes: 1024,
                },
            )
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let tables = build_chunk_tables(&registry, &chunk).unwrap();

        assert_eq!((chunk.start_seq, chunk.end_seq), (1, 2));
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].rows.len(), 1);
        assert_eq!(
            tables[0].rows[0].operation,
            cloudberry_etl_target_cloudberry::apply::StageOperation::Move
        );
        assert_eq!(tables[0].rows[0].old_key, Some(vec![text("2")]));
        assert_eq!(tables[0].rows[0].cells, [text("3"), text("second")]);
    }

    #[test]
    fn scans_spooled_barriers_before_chunk_planning() {
        let ddl = TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![7],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "changed".to_owned(),
        });
        let (root, committed) = spooled_transaction(&[insert(7, 3, "1", "a"), ddl]);
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(committed.clone()).unwrap();
        batcher
            .push(transaction(21, vec![insert(7, 3, "2", "later")]))
            .unwrap();
        let batch = batcher.flush().unwrap();

        assert_eq!(batch.transactions().len(), 2);
        assert!(matches!(
            reject_schema_barriers(&batch, &DdlScope::default()),
            Err(PipelineError::SchemaBarrier { reason, .. }) if reason.contains("ALTER TABLE")
        ));
        assert_eq!(committed.change_source.stats().unwrap().change_count, 2);

        if let ChangeSource::Spool(handle) = &committed.change_source {
            handle.remove().unwrap();
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn target_resume_sequence_must_be_bounded_and_monotonic() {
        assert!(validate_resume_sequence(0, 3, 3).is_ok());
        assert!(validate_resume_sequence(0, 3, 0).is_err());
        assert!(validate_resume_sequence(1, 3, 2).is_ok());
        assert!(validate_resume_sequence(1, 3, 3).is_ok());
        assert!(validate_resume_sequence(1, 3, 1).is_err());
        assert!(validate_resume_sequence(2, 3, 1).is_err());
        assert!(validate_resume_sequence(2, 3, 4).is_err());
    }

    #[test]
    fn target_step_completion_matches_the_durable_record_boundary() {
        let applied = DataChunkDisposition::Applied {
            stats: cloudberry_etl_target_cloudberry::apply::ApplyStats::default(),
        };
        assert!(validate_chunk_step(0, 3, 1, 1, applied, false).is_ok());
        assert!(validate_chunk_step(0, 3, 1, 2, applied, false).is_err());
        assert!(
            validate_chunk_step(1, 3, 3, 3, DataChunkDisposition::AlreadyCommitted, true).is_ok()
        );
        assert!(validate_chunk_step(0, 3, 1, 3, DataChunkDisposition::ResumeAt, true).is_ok());
        assert!(validate_chunk_step(0, 3, 1, 3, DataChunkDisposition::ResumeAt, false).is_err());
        assert!(validate_chunk_step(0, 3, 1, 1, DataChunkDisposition::ResumeAt, true).is_err());
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
            Err(PipelineError::SchemaBarrier { reason, .. }) if reason.contains("ALTER TABLE")
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
            Err(PipelineError::SchemaBarrier { reason, .. }) if reason.contains("ALTER PUBLICATION")
        ));

        let truncate = batch(vec![TransactionChange::Truncate {
            relation_ids: vec![7],
            cascade: false,
            restart_identity: false,
        }]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &truncate),
            Err(PipelineError::SchemaBarrier { reason, .. }) if reason.contains("TRUNCATE")
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
            Err(PipelineError::SchemaBarrier { .. })
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
    fn replication_irrelevant_ddl_never_raises_a_barrier() {
        let registry = TableBindingRegistry::new([]).unwrap();
        // CREATE INDEX on an in-scope managed schema must not trigger a rebuild:
        // it cannot change the mirrored relation's columns, types, PK, or rows.
        let create_index = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "CREATE INDEX".to_owned(),
            relation_ids: vec![7],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "abc".to_owned(),
        })]);
        let request =
            build_apply_request(fence(), "slot", &registry, &create_index).unwrap();
        assert!(request.tables.is_empty());
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));

        // An unrelated privilege change is likewise ignored under an include list.
        let grant = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "GRANT".to_owned(),
            relation_ids: vec![],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "abc".to_owned(),
        })]);
        let scope = DdlScope::new(Some(HashSet::from(["public".to_owned()])), HashSet::new());
        let request =
            build_apply_request_scoped(fence(), "slot", &registry, &scope, &grant).unwrap();
        assert!(request.tables.is_empty());
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

    #[test]
    fn binding_rejects_unpersistable_managed_table_identity() {
        let target = || QualifiedName::new("target", "items").unwrap();
        assert!(matches!(
            TableBinding::new(
                schema(7, 3, "items"),
                target(),
                "stage_items",
                u64::MAX,
                "sha256:test"
            ),
            Err(AdapterConfigError::InvalidTableGeneration(u64::MAX))
        ));
        assert!(matches!(
            TableBinding::new(schema(7, 3, "items"), target(), "stage_items", 4, ""),
            Err(AdapterConfigError::InvalidSchemaFingerprint)
        ));
        assert!(matches!(
            TableBinding::new(
                schema(7, 3, "items"),
                target(),
                "stage_items",
                4,
                "sha256:test\0invalid"
            ),
            Err(AdapterConfigError::InvalidSchemaFingerprint)
        ));
    }
}
