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
    catalog::{CatalogOptions, load_tables_by_relation_ids},
    spool::{ChangeChunk, ChangeStats, ChunkLimits},
    wal::CommittedTransaction,
};
use cloudberry_etl_target_cloudberry::{
    apply::{
        ApplyPlan, ApplyPlanError, ApplyRequest, DataChunkDisposition, LedgeredCommitObserver,
        LedgeredDataChunkOutcome, LedgeredDataChunkRequest, LedgeredEmptyTransactionOutcome,
        TableApplyBatch, execute_ledgered_data_chunk, execute_ledgered_data_chunk_observed,
        execute_ledgered_empty_transaction, execute_ledgered_empty_transaction_observed,
        plan_apply_with_storage,
    },
    checkpoint::{CheckpointKey, NodeCheckpoint, PipelineFence},
    chunk::{DataChunkIdentity, TransactionChunkKey, TransactionChunkManifest},
    managed::TableApplyIdentity,
    storage::TargetStorage,
};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::{
    batch::TransactionBatch,
    normalize::normalize_table_changes,
    pipeline::{PipelineError, SchemaEventKey, TransactionSink},
    schema_transition::{plan_schema_capabilities, plan_schema_transaction, prepare_schema_plan},
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
    #[error("source metadata schema cannot be empty or contain NUL")]
    InvalidMetadataSchema,
    #[error("relation {0} has more than one active schema binding")]
    AmbiguousActiveRelation(u32),
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

    pub(crate) fn requires_barrier(
        &self,
        message: &cloudberry_etl_core::change::DdlMessage,
    ) -> bool {
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
        storage: TargetStorage,
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
        let plan = plan_apply_with_storage(&schema, target, &staging_name, storage)?;
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

/// Registry of active source-schema to target-table bindings for one source-node
/// sink. Bindings can be swapped at runtime by a DDL table transition; every
/// mutation preserves the same invariants the constructor enforces — unique
/// `(relation_id, generation)` key, unique target, and unique staging name — so
/// the row hot path can look up a binding without any catalog access.
#[derive(Debug, Clone)]
pub struct TableBindingRegistry {
    bindings: HashMap<(u32, u64), TableBinding>,
    /// Derived uniqueness indexes kept in sync with `bindings` so insert/remove
    /// stay O(1) and cannot admit a duplicate target or staging name.
    targets: HashSet<QualifiedName>,
    staging_names: HashSet<String>,
}

impl TableBindingRegistry {
    pub fn new(
        bindings: impl IntoIterator<Item = TableBinding>,
    ) -> Result<Self, AdapterConfigError> {
        let mut registry = Self {
            bindings: HashMap::new(),
            targets: HashSet::new(),
            staging_names: HashSet::new(),
        };
        for binding in bindings {
            registry.insert(binding)?;
        }
        Ok(registry)
    }

    /// Add a binding, rejecting a duplicate key, target, or staging name. The
    /// registry is left unchanged when the binding is rejected.
    pub fn insert(&mut self, binding: TableBinding) -> Result<(), AdapterConfigError> {
        let key = binding.key();
        if self.bindings.contains_key(&key) {
            return Err(AdapterConfigError::DuplicateBinding {
                relation_id: key.0,
                generation: key.1,
            });
        }
        let target = binding.plan.table.target.clone();
        if self.targets.contains(&target) {
            return Err(AdapterConfigError::DuplicateTarget(target.to_string()));
        }
        if self.staging_names.contains(&binding.plan.staging_name) {
            return Err(AdapterConfigError::DuplicateStagingName(
                binding.plan.staging_name.clone(),
            ));
        }
        self.targets.insert(target);
        self.staging_names.insert(binding.plan.staging_name.clone());
        self.bindings.insert(key, binding);
        Ok(())
    }

    /// Remove the binding for a `(relation_id, generation)` key, returning it if
    /// present and releasing its target and staging-name reservations.
    pub fn remove(&mut self, relation_id: u32, generation: u64) -> Option<TableBinding> {
        let binding = self.bindings.remove(&(relation_id, generation))?;
        self.targets.remove(&binding.plan.table.target);
        self.staging_names.remove(&binding.plan.staging_name);
        Some(binding)
    }

    /// Atomically replace the binding at `(previous_relation_id,
    /// previous_generation)` with `binding` after a table transition. The old
    /// binding is removed first so the new one may reuse its target or staging
    /// name; on validation failure the old binding is restored and the registry
    /// is left unchanged.
    pub fn swap(
        &mut self,
        previous_relation_id: u32,
        previous_generation: u64,
        binding: TableBinding,
    ) -> Result<Option<TableBinding>, AdapterConfigError> {
        let removed = self.remove(previous_relation_id, previous_generation);
        match self.insert(binding) {
            Ok(()) => Ok(removed),
            Err(error) => {
                // Restore the prior state so a rejected swap is a no-op.
                if let Some(previous) = removed {
                    self.insert(previous).expect(
                        "restoring the removed binding cannot conflict after its own removal",
                    );
                }
                Err(error)
            }
        }
    }

    #[must_use]
    pub fn get(&self, relation_id: u32, generation: u64) -> Option<&TableBinding> {
        self.bindings.get(&(relation_id, generation))
    }

    #[must_use]
    pub fn contains_relation(&self, relation_id: u32) -> bool {
        self.bindings
            .keys()
            .any(|(bound_relation, _)| *bound_relation == relation_id)
    }

    /// Snapshot the one active before-schema for each managed relation at a schema barrier.
    /// Multiple generations for one relation are valid only while a future transition executor
    /// explicitly selects the active generation; until then capability planning fails closed.
    pub fn active_schemas(&self) -> Result<BTreeMap<u32, TableSchema>, AdapterConfigError> {
        let mut schemas = BTreeMap::new();
        for binding in self.bindings.values() {
            if schemas
                .insert(binding.schema.relation_id, binding.schema.clone())
                .is_some()
            {
                return Err(AdapterConfigError::AmbiguousActiveRelation(
                    binding.schema.relation_id,
                ));
            }
        }
        Ok(schemas)
    }

    /// Classify a DDL against the currently bound schema for `(relation_id,
    /// generation)`, comparing the binding's mirrored (before) schema to the
    /// supplied post-DDL (after) schema. Returns `None` when no binding exists
    /// for the key (an unmanaged or already-superseded relation); otherwise the
    /// per-column transitions from [`classify_table_diff`], which the caller can
    /// check with `TransitionKind::is_online_safe` to decide follow vs rebuild.
    #[must_use]
    pub fn classify_relation_diff(
        &self,
        relation_id: u32,
        generation: u64,
        after: &TableSchema,
    ) -> Option<Vec<cloudberry_etl_core::change::TransitionKind>> {
        let binding = self.get(relation_id, generation)?;
        Some(cloudberry_etl_core::schema_diff::classify_table_diff(
            binding.schema(),
            after,
        ))
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
                        // A v2 event whose transitions are all online-safe could be followed with
                        // table transitions once that path is wired; until then it still rebuilds,
                        // but we surface the classification so the reason and telemetry distinguish
                        // "could be online" from "must rebuild".
                        let followable = message.all_transitions_online_safe();
                        let follow_note = if followable {
                            " (all transitions online-safe; rebuild pending online-follow support)"
                        } else {
                            ""
                        };
                        return Err(PipelineError::SchemaBarrier {
                            reason: format!(
                                "DDL `{}` in transaction {} for relations {:?} and schemas {:?}{}",
                                message.command_tag,
                                transaction.xid,
                                message.relation_ids,
                                message.affected_schemas,
                                follow_note
                            ),
                            command_tag: Some(message.command_tag.clone()),
                            schema_event: None,
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
                        schema_event: None,
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
    schema_source: Option<SchemaSource>,
    fence: PipelineFence,
    slot_name: String,
    registry: TableBindingRegistry,
    ddl_scope: DdlScope,
    chunk_limits: ChunkLimits,
    commit_observer: Option<Arc<dyn LedgeredCommitObserver>>,
}

struct SchemaSource {
    client: Client,
    options: CatalogOptions,
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
        Self::build(
            client,
            fence,
            slot_name,
            registry,
            ddl_scope,
            chunk_limits,
            None,
        )
    }

    pub fn new_with_chunk_limits_and_observer(
        client: Client,
        fence: PipelineFence,
        slot_name: impl Into<String>,
        registry: TableBindingRegistry,
        ddl_scope: DdlScope,
        chunk_limits: ChunkLimits,
        commit_observer: Arc<dyn LedgeredCommitObserver>,
    ) -> Result<Self, AdapterConfigError> {
        Self::build(
            client,
            fence,
            slot_name,
            registry,
            ddl_scope,
            chunk_limits,
            Some(commit_observer),
        )
    }

    fn build(
        client: Client,
        fence: PipelineFence,
        slot_name: impl Into<String>,
        registry: TableBindingRegistry,
        ddl_scope: DdlScope,
        chunk_limits: ChunkLimits,
        commit_observer: Option<Arc<dyn LedgeredCommitObserver>>,
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
            schema_source: None,
            fence,
            slot_name,
            registry,
            ddl_scope,
            chunk_limits,
            commit_observer,
        })
    }

    /// Enable transaction-level schema planning with a dedicated source SQL connection.
    /// The connection is queried only for an isolated schema barrier, never from row apply.
    pub fn with_schema_source(
        mut self,
        client: Client,
        options: CatalogOptions,
    ) -> Result<Self, AdapterConfigError> {
        if options.metadata_schema.is_empty() || options.metadata_schema.contains('\0') {
            return Err(AdapterConfigError::InvalidMetadataSchema);
        }
        self.schema_source = Some(SchemaSource { client, options });
        Ok(self)
    }

    fn plan_requires_barrier(
        &self,
        plan: &crate::schema_transition::SchemaTransactionPlan,
    ) -> bool {
        plan.changes.iter().any(|change| match change {
            crate::schema_transition::OrderedSchemaChange::Ddl { message, .. } => {
                self.ddl_scope.requires_barrier(message)
            }
            crate::schema_transition::OrderedSchemaChange::Truncate { relation_ids, .. } => {
                relation_ids
                    .iter()
                    .any(|relation_id| self.registry.contains_relation(*relation_id))
            }
        })
    }

    async fn prepare_schema_barrier(
        &self,
        client: &mut Client,
        batch: &TransactionBatch,
    ) -> Result<Option<PipelineError>, PipelineError> {
        if !batch.has_generation_barrier() {
            return Ok(None);
        }
        if batch.transactions().len() != 1 {
            return Err(PipelineError::Target(
                "schema barrier transaction was not isolated by the batcher".to_owned(),
            ));
        }
        let transaction = batch.final_transaction();
        let Some(plan) = plan_schema_transaction(transaction)
            .map_err(|error| PipelineError::Source(error.to_string()))?
        else {
            return Ok(None);
        };
        if !self.plan_requires_barrier(&plan) {
            return Ok(None);
        }
        let command_tag = plan.changes.iter().find_map(|change| match change {
            crate::schema_transition::OrderedSchemaChange::Ddl { message, .. } => {
                Some(message.command_tag.clone())
            }
            crate::schema_transition::OrderedSchemaChange::Truncate { .. } => None,
        });
        let reason = format!(
            "schema transaction {} at {} ({}) requires table transition for relations {:?} and schemas {:?}",
            plan.source_xid,
            plan.source_position.lsn,
            plan.command_summary(),
            plan.relation_ids(),
            plan.affected_schemas
        );
        let Some(source) = &self.schema_source else {
            return Ok(Some(PipelineError::SchemaBarrier {
                reason,
                command_tag,
                schema_event: None,
            }));
        };
        let prepared = prepare_schema_plan(
            &source.client,
            client,
            &source.options.metadata_schema,
            self.fence,
            plan,
        )
        .await
        .map_err(|error| PipelineError::Target(error.to_string()))?;
        let capability = if prepared.catalog_validation.unknown_scope
            || !prepared.catalog_validation.mismatches.is_empty()
        {
            plan_schema_capabilities(
                &prepared.plan,
                &prepared.catalog_validation,
                &BTreeMap::new(),
                &BTreeMap::new(),
            )
        } else {
            let present_ids = prepared
                .current_relations
                .iter()
                .filter_map(|(relation_id, current)| current.as_ref().map(|_| *relation_id))
                .collect::<Vec<_>>();
            let current =
                load_tables_by_relation_ids(&source.client, &source.options, &present_ids)
                    .await
                    .map_err(|error| PipelineError::Source(error.to_string()))?;
            let after = prepared
                .plan
                .relation_ids()
                .into_iter()
                .map(|relation_id| (relation_id, current.get(&relation_id).cloned()))
                .collect();
            let before = self
                .registry
                .active_schemas()
                .map_err(|error| PipelineError::Target(error.to_string()))?;
            plan_schema_capabilities(
                &prepared.plan,
                &prepared.catalog_validation,
                &before,
                &after,
            )
        }
        .map_err(|error| PipelineError::Source(error.to_string()))?;
        let reason = format!("{reason}; capability plan: {}", capability.action_summary());
        Ok(Some(PipelineError::SchemaBarrier {
            reason,
            command_tag,
            schema_event: Some(SchemaEventKey {
                source_lsn: prepared.plan.source_position.lsn,
                source_xid: u64::from(prepared.plan.source_xid),
            }),
        }))
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
            let outcome = match &self.commit_observer {
                Some(observer) => {
                    execute_ledgered_empty_transaction_observed(
                        client,
                        self.fence,
                        &manifest,
                        observer.as_ref(),
                    )
                    .await
                }
                None => execute_ledgered_empty_transaction(client, self.fence, &manifest).await,
            }
            .map_err(|error| PipelineError::Target(error.to_string()))?;
            return match outcome {
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
            let outcome = match &self.commit_observer {
                Some(observer) => {
                    execute_ledgered_data_chunk_observed(client, &request, observer.as_ref()).await
                }
                None => execute_ledgered_data_chunk(client, &request).await,
            }
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
        let mut client = self.client.lock().await;
        if let Some(barrier) = self.prepare_schema_barrier(&mut client, batch).await? {
            return Err(barrier);
        }
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
            TargetStorage::AoColumn,
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
            transitions: Vec::new(),
        });
        let (root, committed) = spooled_transaction(&[insert(7, 3, "1", "a"), ddl]);
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(committed.clone()).unwrap();
        let batch = batcher
            .push(transaction(21, vec![insert(7, 3, "2", "later")]))
            .unwrap()
            .expect("the transaction after a barrier flushes the isolated DDL batch");

        assert_eq!(batch.transactions().len(), 1);
        assert!(matches!(
            reject_schema_barriers(&batch, &DdlScope::default()),
            Err(PipelineError::SchemaBarrier { reason, .. }) if reason.contains("ALTER TABLE")
        ));
        let following = batcher.flush().unwrap();
        assert_eq!(following.transactions().len(), 1);
        assert!(!following.has_generation_barrier());
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
            transitions: Vec::new(),
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
            transitions: Vec::new(),
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
            transitions: Vec::new(),
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
            transitions: Vec::new(),
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
            transitions: Vec::new(),
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
            transitions: Vec::new(),
        })]);
        let request = build_apply_request(fence(), "slot", &registry, &create_index).unwrap();
        assert!(request.tables.is_empty());
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));

        // An unrelated privilege change is likewise ignored under an include list.
        let grant = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "GRANT".to_owned(),
            relation_ids: vec![],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "abc".to_owned(),
            transitions: Vec::new(),
        })]);
        let scope = DdlScope::new(Some(HashSet::from(["public".to_owned()])), HashSet::new());
        let request =
            build_apply_request_scoped(fence(), "slot", &registry, &scope, &grant).unwrap();
        assert!(request.tables.is_empty());
        assert_eq!(request.checkpoint.applied_lsn, PgLsn::new(20));
    }

    #[test]
    fn online_safe_v2_ddl_barrier_reason_notes_followability() {
        use cloudberry_etl_core::change::{TableTransition, TransitionKind};
        let registry = TableBindingRegistry::new([]).unwrap();
        let add_column = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 2,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![7],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "after".to_owned(),
            transitions: vec![TableTransition {
                relation_id: 7,
                before_generation: Some(1),
                after_generation: Some(2),
                before_fingerprint: Some("before".to_owned()),
                after_fingerprint: Some("after".to_owned()),
                after_schema: None,
                kind: TransitionKind::AddColumn {
                    name: "note".to_owned(),
                    nullable_or_defaulted: true,
                },
            }],
        })]);
        // Still a barrier today (online-follow path not wired), but the reason must flag it.
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &add_column),
            Err(PipelineError::SchemaBarrier { reason, .. })
                if reason.contains("online-safe")
        ));

        // An unknown transition must NOT be flagged followable.
        let unknown = batch(vec![TransactionChange::Ddl(DdlMessage {
            version: 2,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![7],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "after".to_owned(),
            transitions: vec![TableTransition {
                relation_id: 7,
                before_generation: Some(1),
                after_generation: Some(2),
                before_fingerprint: Some("before".to_owned()),
                after_fingerprint: Some("after".to_owned()),
                after_schema: None,
                kind: TransitionKind::Unknown,
            }],
        })]);
        assert!(matches!(
            build_apply_request(fence(), "slot", &registry, &unknown),
            Err(PipelineError::SchemaBarrier { reason, .. })
                if !reason.contains("online-safe")
        ));
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
    fn registry_insert_and_remove_maintain_uniqueness() {
        let mut registry = TableBindingRegistry::new([]).unwrap();
        registry
            .insert(binding(7, 3, "items", "items", "stage_items"))
            .unwrap();
        assert_eq!(registry.len(), 1);
        assert!(registry.get(7, 3).is_some());

        // Duplicate target is rejected and leaves the registry unchanged.
        assert!(matches!(
            registry.insert(binding(8, 1, "other", "items", "stage_other")),
            Err(AdapterConfigError::DuplicateTarget(_))
        ));
        assert_eq!(registry.len(), 1);

        // Removing frees the target and staging name for reuse.
        let removed = registry.remove(7, 3).expect("binding present");
        assert_eq!(removed.key(), (7, 3));
        assert!(registry.is_empty());
        registry
            .insert(binding(8, 1, "other", "items", "stage_items"))
            .expect("target and staging name are free after removal");
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_swap_replaces_binding_and_reuses_reservations() {
        let mut registry =
            TableBindingRegistry::new([binding(7, 3, "items", "items", "stage_items")]).unwrap();

        // A new generation of the same table reuses the same target and staging name.
        let previous = registry
            .swap(7, 3, binding(7, 4, "items", "items", "stage_items"))
            .expect("swap succeeds")
            .expect("previous binding returned");
        assert_eq!(previous.key(), (7, 3));
        assert!(registry.get(7, 3).is_none());
        assert_eq!(registry.get(7, 4).unwrap().key(), (7, 4));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn registry_swap_restores_previous_binding_on_conflict() {
        let mut registry = TableBindingRegistry::new([
            binding(7, 3, "items", "items", "stage_items"),
            binding(8, 1, "other", "other", "stage_other"),
        ])
        .unwrap();

        // Swapping table 7 to a binding whose target collides with table 8 must fail and
        // leave the original table-7 binding intact.
        let result = registry.swap(7, 3, binding(7, 4, "items", "other", "stage_new"));
        assert!(matches!(
            result,
            Err(AdapterConfigError::DuplicateTarget(_))
        ));
        assert_eq!(registry.get(7, 3).unwrap().key(), (7, 3));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn classify_relation_diff_uses_the_bound_before_schema() {
        use cloudberry_etl_core::change::TransitionKind;
        let registry =
            TableBindingRegistry::new([binding(7, 3, "items", "items", "stage_items")]).unwrap();

        // Unmanaged key -> None.
        assert!(
            registry
                .classify_relation_diff(99, 1, &schema(99, 1, "x"))
                .is_none()
        );

        // Add a nullable column to the bound schema (id pk + payload) -> online-safe AddColumn.
        let mut after = schema(7, 3, "items");
        after.columns.push(column(3, "note", None));
        let diff = registry
            .classify_relation_diff(7, 3, &after)
            .expect("managed relation");
        assert_eq!(diff.len(), 1);
        assert!(matches!(&diff[0], TransitionKind::AddColumn { name, .. } if name == "note"));
        assert!(diff[0].is_online_safe());

        // Identical schema -> no transitions.
        assert!(
            registry
                .classify_relation_diff(7, 3, &schema(7, 3, "items"))
                .expect("managed relation")
                .is_empty()
        );
    }

    #[test]
    fn binding_rejects_unpersistable_managed_table_identity() {
        let target = || QualifiedName::new("target", "items").unwrap();
        assert!(matches!(
            TableBinding::new(
                schema(7, 3, "items"),
                target(),
                "stage_items",
                TargetStorage::AoColumn,
                u64::MAX,
                "sha256:test"
            ),
            Err(AdapterConfigError::InvalidTableGeneration(u64::MAX))
        ));
        assert!(matches!(
            TableBinding::new(
                schema(7, 3, "items"),
                target(),
                "stage_items",
                TargetStorage::AoColumn,
                4,
                "",
            ),
            Err(AdapterConfigError::InvalidSchemaFingerprint)
        ));
        assert!(matches!(
            TableBinding::new(
                schema(7, 3, "items"),
                target(),
                "stage_items",
                TargetStorage::AoColumn,
                4,
                "sha256:test\0invalid"
            ),
            Err(AdapterConfigError::InvalidSchemaFingerprint)
        ));
    }
}
