//! Transaction-scoped schema planning for ordered DDL and TRUNCATE barriers.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Write,
};

use cloudberry_etl_core::{
    change::{
        DdlMessage, DdlReplicationImpact, RelationSchemaSnapshot, SourcePosition, TableTransition,
        TransactionChange, TransitionKind,
    },
    lsn::PgLsn,
    schema::TableSchema,
    schema_diff::classify_table_diff,
};
use cloudberry_etl_source_postgres::{
    SourceError,
    catalog::{CatalogOptions, load_tables_by_relation_ids},
    ddl::{CurrentRelationSchema, load_current_relation_schemas},
    wal::CommittedTransaction,
};
use cloudberry_etl_target_cloudberry::{
    checkpoint::PipelineFence,
    schema_event::{
        RecordOutcome, SchemaEventError, SchemaEventRecord, record_schema_event,
        record_schema_event_in_transaction,
    },
    table_transition::{
        TableTransitionAction, TableTransitionError, TableTransitionRecord,
        record_table_transition_in_transaction,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::{Client, GenericClient};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum SchemaPlanError {
    #[error("schema transaction change source failed: {0}")]
    ChangeSource(String),
    #[error("schema transaction contains invalid DDL: {0}")]
    InvalidDdl(String),
    #[error("schema transaction payload cannot be encoded: {0}")]
    Encode(String),
    #[error("catalog validation omitted relation {0}")]
    MissingCatalogRelation(u32),
}

#[derive(Debug, Error)]
pub enum SchemaCoordinatorError {
    #[error(transparent)]
    Plan(#[from] SchemaPlanError),
    #[error(transparent)]
    Source(#[from] SourceError),
    #[error(transparent)]
    Target(#[from] SchemaEventError),
    #[error(transparent)]
    TableTransition(#[from] TableTransitionError),
    #[error("schema coordinator database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
}

/// One schema-sensitive source change and its zero-based ordinal among all transaction changes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "change", rename_all = "snake_case")]
pub enum OrderedSchemaChange {
    Ddl {
        ordinal: u64,
        message: DdlMessage,
    },
    Truncate {
        ordinal: u64,
        relation_ids: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
}

impl OrderedSchemaChange {
    #[must_use]
    pub const fn ordinal(&self) -> u64 {
        match self {
            Self::Ddl { ordinal, .. } | Self::Truncate { ordinal, .. } => *ordinal,
        }
    }
}

/// Last source-transaction state captured for one relation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CapturedRelationState {
    /// A v1 event or TRUNCATE names the relation but cannot prove its terminal schema.
    Unknown {
        last_ordinal: u64,
    },
    Present {
        last_ordinal: u64,
        fingerprint: String,
        schema: RelationSchemaSnapshot,
    },
    Dropped {
        last_ordinal: u64,
    },
}

impl CapturedRelationState {
    #[must_use]
    pub const fn last_ordinal(&self) -> u64 {
        match self {
            Self::Unknown { last_ordinal }
            | Self::Present { last_ordinal, .. }
            | Self::Dropped { last_ordinal } => *last_ordinal,
        }
    }
}

/// Durable, deterministic plan input for one committed source transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaTransactionPlan {
    pub source_position: SourcePosition,
    pub commit_lsn: PgLsn,
    pub source_xid: u32,
    pub changes: Vec<OrderedSchemaChange>,
    pub terminal_relations: BTreeMap<u32, CapturedRelationState>,
    pub reload_relations: BTreeSet<u32>,
    pub affected_schemas: Vec<String>,
    /// A schema-sensitive event had no table identity (for example ALTER TYPE or v1 scope loss).
    pub unknown_scope: bool,
    /// SHA-256 over the ordered `changes` JSON, independent of memory/spool representation.
    pub payload_fingerprint: String,
}

impl SchemaTransactionPlan {
    #[must_use]
    pub fn relation_ids(&self) -> Vec<u32> {
        self.terminal_relations.keys().copied().collect()
    }

    #[must_use]
    pub fn command_summary(&self) -> String {
        let mut tags = self.changes.iter().map(|change| match change {
            OrderedSchemaChange::Ddl { message, .. } => message.command_tag.as_str(),
            OrderedSchemaChange::Truncate { .. } => "TRUNCATE",
        });
        let Some(first) = tags.next() else {
            return "SCHEMA TRANSACTION".to_owned();
        };
        if tags.next().is_none() {
            first.to_owned()
        } else {
            "MULTI SCHEMA CHANGE".to_owned()
        }
    }

    pub fn ledger_payload(&self) -> Result<serde_json::Value, SchemaPlanError> {
        serde_json::to_value(self).map_err(|error| SchemaPlanError::Encode(error.to_string()))
    }

    pub fn schema_event_record(
        &self,
        fence: PipelineFence,
    ) -> Result<SchemaEventRecord, SchemaPlanError> {
        Ok(SchemaEventRecord {
            event_id: schema_event_id(fence, self),
            fence,
            source_lsn: self.source_position.lsn,
            source_xid: u64::from(self.source_xid),
            command_tag: self.command_summary(),
            schema_fingerprint: self.payload_fingerprint.clone(),
            transitions: self.ledger_payload()?,
        })
    }

    /// Compare captured terminal states with the single authoritative catalog read performed
    /// after commit. A mismatch means a later DDL may already be visible or catalog drift exists;
    /// the caller must coalesce/replan or fail closed, never apply the captured state online.
    pub fn validate_catalog(
        &self,
        current: &BTreeMap<u32, Option<CurrentRelationSchema>>,
    ) -> Result<CatalogValidation, SchemaPlanError> {
        let mut matched_relations = Vec::new();
        let mut unverifiable_relations = Vec::new();
        let mut mismatches = Vec::new();
        for (relation_id, expected) in &self.terminal_relations {
            let actual = current
                .get(relation_id)
                .ok_or(SchemaPlanError::MissingCatalogRelation(*relation_id))?;
            match (expected, actual) {
                (CapturedRelationState::Unknown { .. }, _) => {
                    unverifiable_relations.push(*relation_id);
                }
                (CapturedRelationState::Dropped { .. }, None) => {
                    matched_relations.push(*relation_id);
                }
                (CapturedRelationState::Dropped { .. }, Some(_)) => {
                    mismatches.push(CatalogMismatch {
                        relation_id: *relation_id,
                        kind: CatalogMismatchKind::ExpectedDropped,
                    });
                }
                (
                    CapturedRelationState::Present {
                        fingerprint,
                        schema,
                        ..
                    },
                    Some(actual),
                ) if actual.fingerprint == *fingerprint && actual.schema == *schema => {
                    matched_relations.push(*relation_id);
                }
                (CapturedRelationState::Present { .. }, None) => {
                    mismatches.push(CatalogMismatch {
                        relation_id: *relation_id,
                        kind: CatalogMismatchKind::ExpectedPresent,
                    });
                }
                (CapturedRelationState::Present { .. }, Some(_)) => {
                    mismatches.push(CatalogMismatch {
                        relation_id: *relation_id,
                        kind: CatalogMismatchKind::DifferentPresentState,
                    });
                }
            }
        }
        Ok(CatalogValidation {
            matched_relations,
            unverifiable_relations,
            mismatches,
            unknown_scope: self.unknown_scope,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogMismatchKind {
    ExpectedPresent,
    ExpectedDropped,
    DifferentPresentState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogMismatch {
    pub relation_id: u32,
    pub kind: CatalogMismatchKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogValidation {
    pub matched_relations: Vec<u32>,
    pub unverifiable_relations: Vec<u32>,
    pub mismatches: Vec<CatalogMismatch>,
    pub unknown_scope: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSchemaEvent {
    pub event_id: Uuid,
    pub plan: SchemaTransactionPlan,
    pub catalog_validation: CatalogValidation,
    pub current_relations: BTreeMap<u32, Option<CurrentRelationSchema>>,
    pub record_outcome: RecordOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSchemaCapabilityEvent {
    pub event: PreparedSchemaEvent,
    pub capability: SchemaCapabilityPlan,
}

/// Deterministic table-local action selected from the bound schema and the authoritative
/// post-commit source catalog. Execution remains a separate, fenced concern.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case")]
pub enum SchemaAction {
    Noop,
    Online {
        transitions: Vec<TransitionKind>,
        after: TableSchema,
    },
    Reload {
        after: TableSchema,
        reason: ReloadReason,
    },
    Drop,
    Add {
        after: TableSchema,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReloadReason {
    ExplicitFallback,
    UnsafeDiff,
    UnverifiableEvent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "blocker", rename_all = "snake_case")]
pub enum CapabilityBlocker {
    UnknownScope,
    CatalogMismatch { mismatch: CatalogMismatch },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaCapabilityPlan {
    pub actions: BTreeMap<u32, SchemaAction>,
    pub blockers: Vec<CapabilityBlocker>,
}

impl SchemaCapabilityPlan {
    #[must_use]
    pub fn is_table_local(&self) -> bool {
        self.blockers.is_empty()
    }

    #[must_use]
    pub fn action_summary(&self) -> String {
        if !self.blockers.is_empty() {
            return format!("blocked({})", self.blockers.len());
        }
        let mut counts = BTreeMap::<&'static str, usize>::new();
        for action in self.actions.values() {
            let name = match action {
                SchemaAction::Noop => "noop",
                SchemaAction::Online { .. } => "online",
                SchemaAction::Reload { .. } => "reload",
                SchemaAction::Drop => "drop",
                SchemaAction::Add { .. } => "add",
            };
            *counts.entry(name).or_default() += 1;
        }
        counts
            .into_iter()
            .map(|(name, count)| format!("{name}={count}"))
            .collect::<Vec<_>>()
            .join(",")
    }
}

/// Bind a persisted schema event to complete before/after schemas.
///
/// `before` contains active managed relations. `after` must contain one entry for every relation
/// in the event; `None` means the OID no longer exists. Catalog mismatch and unknown scope are
/// pipeline-level correctness failures, so no table-local actions are emitted in those cases.
pub fn plan_schema_capabilities(
    plan: &SchemaTransactionPlan,
    validation: &CatalogValidation,
    before: &BTreeMap<u32, TableSchema>,
    after: &BTreeMap<u32, Option<TableSchema>>,
) -> Result<SchemaCapabilityPlan, SchemaPlanError> {
    let mut blockers = Vec::new();
    if validation.unknown_scope {
        blockers.push(CapabilityBlocker::UnknownScope);
    }
    blockers.extend(
        validation
            .mismatches
            .iter()
            .cloned()
            .map(|mismatch| CapabilityBlocker::CatalogMismatch { mismatch }),
    );
    if !blockers.is_empty() {
        return Ok(SchemaCapabilityPlan {
            actions: BTreeMap::new(),
            blockers,
        });
    }

    let unverifiable = validation
        .unverifiable_relations
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut actions = BTreeMap::new();
    for (relation_id, captured) in &plan.terminal_relations {
        let current = after
            .get(relation_id)
            .ok_or(SchemaPlanError::MissingCatalogRelation(*relation_id))?;
        let bound = before.get(relation_id);
        let action = if unverifiable.contains(relation_id) {
            match (bound, current) {
                (Some(_), Some(after)) => SchemaAction::Reload {
                    after: after.clone(),
                    reason: ReloadReason::UnverifiableEvent,
                },
                (Some(_), None) => SchemaAction::Drop,
                (None, Some(after)) => SchemaAction::Add {
                    after: after.clone(),
                },
                (None, None) => SchemaAction::Noop,
            }
        } else {
            match (captured, bound, current) {
                (CapturedRelationState::Dropped { .. }, Some(_), None) => SchemaAction::Drop,
                (CapturedRelationState::Dropped { .. }, None, None) => SchemaAction::Noop,
                (CapturedRelationState::Present { .. }, None, Some(after)) => SchemaAction::Add {
                    after: after.clone(),
                },
                (CapturedRelationState::Present { .. }, Some(before), Some(after)) => {
                    let transitions = classify_table_diff(before, after);
                    if plan.reload_relations.contains(relation_id) {
                        SchemaAction::Reload {
                            after: after.clone(),
                            reason: ReloadReason::ExplicitFallback,
                        }
                    } else if transitions.is_empty() {
                        SchemaAction::Noop
                    } else if transitions.iter().all(TransitionKind::is_online_safe) {
                        SchemaAction::Online {
                            transitions,
                            after: after.clone(),
                        }
                    } else {
                        SchemaAction::Reload {
                            after: after.clone(),
                            reason: ReloadReason::UnsafeDiff,
                        }
                    }
                }
                // These shapes contradict an otherwise mismatch-free terminal validation.
                _ => return Err(SchemaPlanError::MissingCatalogRelation(*relation_id)),
            }
        };
        actions.insert(*relation_id, action);
    }
    Ok(SchemaCapabilityPlan { actions, blockers })
}

impl CatalogValidation {
    #[must_use]
    pub fn is_exact_match(&self) -> bool {
        !self.unknown_scope && self.unverifiable_relations.is_empty() && self.mismatches.is_empty()
    }
}

/// Persist one committed schema transaction after a single terminal catalog read.
///
/// The returned validation is deliberately separate from persistence: exact matches may proceed
/// to capability planning, while rapid-advance/drift and unverifiable v1 scopes remain durable
/// pending work for coalescing or shadow reload. This function never advances a checkpoint.
pub async fn prepare_schema_event<C>(
    source: &C,
    target: &mut Client,
    metadata_schema: &str,
    fence: PipelineFence,
    transaction: &CommittedTransaction,
) -> Result<Option<PreparedSchemaEvent>, SchemaCoordinatorError>
where
    C: GenericClient + Sync,
{
    let Some(plan) = plan_schema_transaction(transaction)? else {
        return Ok(None);
    };
    prepare_schema_plan(source, target, metadata_schema, fence, plan)
        .await
        .map(Some)
}

/// Validate and persist an already planned schema transaction.
///
/// Callers use this split form to discard schema events outside their managed scope before the
/// source catalog is queried or the target ledger is mutated.
pub async fn prepare_schema_plan<C>(
    source: &C,
    target: &mut Client,
    metadata_schema: &str,
    fence: PipelineFence,
    plan: SchemaTransactionPlan,
) -> Result<PreparedSchemaEvent, SchemaCoordinatorError>
where
    C: GenericClient + Sync,
{
    let current =
        load_current_relation_schemas(source, metadata_schema, &plan.relation_ids()).await?;
    let catalog_validation = plan.validate_catalog(&current)?;
    let record = plan.schema_event_record(fence)?;
    let record_outcome = record_schema_event(target, &record).await?;
    Ok(PreparedSchemaEvent {
        event_id: record.event_id,
        plan,
        catalog_validation,
        current_relations: current,
        record_outcome,
    })
}

/// Read the compact terminal fingerprint and complete type-resolved schemas from one caller-owned
/// source snapshot, bind them to active before-schemas, then persist the schema event. No target
/// event is written if full capability planning fails.
pub async fn prepare_schema_capability_event<C>(
    source: &C,
    target: &mut Client,
    options: &CatalogOptions,
    fence: PipelineFence,
    plan: SchemaTransactionPlan,
    before: &BTreeMap<u32, TableSchema>,
) -> Result<PreparedSchemaCapabilityEvent, SchemaCoordinatorError>
where
    C: GenericClient + Sync,
{
    let current =
        load_current_relation_schemas(source, &options.metadata_schema, &plan.relation_ids())
            .await?;
    let catalog_validation = plan.validate_catalog(&current)?;
    let capability =
        if catalog_validation.unknown_scope || !catalog_validation.mismatches.is_empty() {
            plan_schema_capabilities(
                &plan,
                &catalog_validation,
                &BTreeMap::new(),
                &BTreeMap::new(),
            )?
        } else {
            let present_ids = current
                .iter()
                .filter_map(|(relation_id, state)| state.as_ref().map(|_| *relation_id))
                .collect::<Vec<_>>();
            let full = load_tables_by_relation_ids(source, options, &present_ids).await?;
            let after = plan
                .relation_ids()
                .into_iter()
                .map(|relation_id| (relation_id, full.get(&relation_id).cloned()))
                .collect();
            plan_schema_capabilities(&plan, &catalog_validation, before, &after)?
        };
    let record = plan.schema_event_record(fence)?;
    let target_transaction = target.transaction().await?;
    let record_outcome = record_schema_event_in_transaction(&target_transaction, &record).await?;
    for (relation_id, action) in &capability.actions {
        let action_record = TableTransitionRecord {
            event_id: record.event_id,
            fence,
            source_lsn: plan.source_position.lsn,
            source_xid: u64::from(plan.source_xid),
            source_relation_id: *relation_id,
            action: table_transition_action(action),
            plan: serde_json::to_value(action)
                .map_err(|error| SchemaPlanError::Encode(error.to_string()))?,
            barrier_lsn: plan.source_position.lsn,
            active_table_generation: None,
            pending_table_generation: None,
            snapshot_group_id: None,
        };
        record_table_transition_in_transaction(&target_transaction, &action_record).await?;
    }
    target_transaction.commit().await?;
    Ok(PreparedSchemaCapabilityEvent {
        event: PreparedSchemaEvent {
            event_id: record.event_id,
            plan,
            catalog_validation,
            current_relations: current,
            record_outcome,
        },
        capability,
    })
}

const fn table_transition_action(action: &SchemaAction) -> TableTransitionAction {
    match action {
        SchemaAction::Noop => TableTransitionAction::Noop,
        SchemaAction::Online { .. } => TableTransitionAction::Online,
        SchemaAction::Reload { .. } => TableTransitionAction::Reload,
        SchemaAction::Drop => TableTransitionAction::Drop,
        SchemaAction::Add { .. } => TableTransitionAction::Add,
    }
}

/// Scan one committed transaction without materializing row changes. Returns `None` when it has
/// no schema-sensitive DDL or TRUNCATE.
pub fn plan_schema_transaction(
    transaction: &CommittedTransaction,
) -> Result<Option<SchemaTransactionPlan>, SchemaPlanError> {
    let reader = transaction
        .change_source
        .reader()
        .map_err(|error| SchemaPlanError::ChangeSource(error.to_string()))?;
    let mut changes = Vec::new();
    let mut terminal_relations = BTreeMap::new();
    let mut reload_relations = BTreeSet::new();
    let mut affected_schemas = BTreeSet::new();
    let mut unknown_scope = false;

    for (ordinal, change) in reader.enumerate() {
        let ordinal = u64::try_from(ordinal).unwrap_or(u64::MAX);
        let change = change.map_err(|error| SchemaPlanError::ChangeSource(error.to_string()))?;
        match change {
            TransactionChange::Row(_) => {}
            TransactionChange::Truncate {
                mut relation_ids,
                cascade,
                restart_identity,
            } => {
                canonicalize_relation_ids(&mut relation_ids, "TRUNCATE")?;
                if relation_ids.is_empty() {
                    return Err(SchemaPlanError::InvalidDdl(
                        "TRUNCATE has no relation identity".to_owned(),
                    ));
                }
                for relation_id in &relation_ids {
                    reload_relations.insert(*relation_id);
                    terminal_relations
                        .entry(*relation_id)
                        .and_modify(|state| {
                            if matches!(state, CapturedRelationState::Unknown { .. }) {
                                *state = CapturedRelationState::Unknown {
                                    last_ordinal: ordinal,
                                };
                            }
                        })
                        .or_insert(CapturedRelationState::Unknown {
                            last_ordinal: ordinal,
                        });
                }
                changes.push(OrderedSchemaChange::Truncate {
                    ordinal,
                    relation_ids,
                    cascade,
                    restart_identity,
                });
            }
            TransactionChange::Ddl(mut message) => {
                if message.replication_impact() == DdlReplicationImpact::Irrelevant {
                    continue;
                }
                validate_and_apply_ddl(
                    &mut message,
                    ordinal,
                    &mut terminal_relations,
                    &mut reload_relations,
                )?;
                affected_schemas.extend(message.affected_schemas.iter().cloned());
                unknown_scope |= message.relation_ids.is_empty();
                changes.push(OrderedSchemaChange::Ddl { ordinal, message });
            }
        }
    }
    if changes.is_empty() {
        return Ok(None);
    }
    let encoded =
        serde_json::to_vec(&changes).map_err(|error| SchemaPlanError::Encode(error.to_string()))?;
    let payload_fingerprint = sha256_fingerprint(&encoded);
    Ok(Some(SchemaTransactionPlan {
        source_position: transaction.final_position.clone(),
        commit_lsn: transaction.commit_lsn,
        source_xid: transaction.xid,
        changes,
        terminal_relations,
        reload_relations,
        affected_schemas: affected_schemas.into_iter().collect(),
        unknown_scope,
        payload_fingerprint,
    }))
}

fn validate_and_apply_ddl(
    message: &mut DdlMessage,
    ordinal: u64,
    terminal_relations: &mut BTreeMap<u32, CapturedRelationState>,
    reload_relations: &mut BTreeSet<u32>,
) -> Result<(), SchemaPlanError> {
    if !matches!(message.version, 1 | 2) {
        return Err(SchemaPlanError::InvalidDdl(format!(
            "unsupported DDL payload version {}",
            message.version
        )));
    }
    if message.command_tag.is_empty() || message.schema_fingerprint.is_empty() {
        return Err(SchemaPlanError::InvalidDdl(
            "DDL command tag and fingerprint must be non-empty".to_owned(),
        ));
    }
    canonicalize_relation_ids(&mut message.relation_ids, "DDL")?;
    let mut seen = HashSet::with_capacity(message.transitions.len());
    for transition in &message.transitions {
        if !seen.insert(transition.relation_id) {
            return Err(SchemaPlanError::InvalidDdl(format!(
                "DDL repeats relation {}",
                transition.relation_id
            )));
        }
        if !message.relation_ids.contains(&transition.relation_id) {
            return Err(SchemaPlanError::InvalidDdl(format!(
                "DDL transition relation {} is outside relation_ids",
                transition.relation_id
            )));
        }
        let state = captured_state(transition, ordinal)?;
        if matches!(state, CapturedRelationState::Unknown { .. }) {
            reload_relations.insert(transition.relation_id);
        }
        terminal_relations.insert(transition.relation_id, state);
    }
    for relation_id in &message.relation_ids {
        if !seen.contains(relation_id) {
            terminal_relations.insert(
                *relation_id,
                CapturedRelationState::Unknown {
                    last_ordinal: ordinal,
                },
            );
            reload_relations.insert(*relation_id);
        }
    }
    Ok(())
}

fn captured_state(
    transition: &TableTransition,
    ordinal: u64,
) -> Result<CapturedRelationState, SchemaPlanError> {
    if transition.relation_id == 0 {
        return Err(SchemaPlanError::InvalidDdl(
            "DDL transition has relation OID zero".to_owned(),
        ));
    }
    match (
        &transition.kind,
        &transition.after_fingerprint,
        &transition.after_schema,
    ) {
        (TransitionKind::DropTable, None, None) => Ok(CapturedRelationState::Dropped {
            last_ordinal: ordinal,
        }),
        (TransitionKind::DropTable, _, _) => Err(SchemaPlanError::InvalidDdl(format!(
            "DROP relation {} carries an after-state",
            transition.relation_id
        ))),
        (_, Some(fingerprint), Some(schema))
            if !fingerprint.is_empty() && schema.relation_id == transition.relation_id =>
        {
            Ok(CapturedRelationState::Present {
                last_ordinal: ordinal,
                fingerprint: fingerprint.clone(),
                schema: schema.clone(),
            })
        }
        (_, _, _) => Ok(CapturedRelationState::Unknown {
            last_ordinal: ordinal,
        }),
    }
}

fn canonicalize_relation_ids(
    relation_ids: &mut Vec<u32>,
    context: &str,
) -> Result<(), SchemaPlanError> {
    if relation_ids.contains(&0) {
        return Err(SchemaPlanError::InvalidDdl(format!(
            "{context} contains relation OID zero"
        )));
    }
    relation_ids.sort_unstable();
    relation_ids.dedup();
    Ok(())
}

fn sha256_fingerprint(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut value = String::with_capacity(7 + digest.len() * 2);
    value.push_str("sha256:");
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn schema_event_id(fence: PipelineFence, plan: &SchemaTransactionPlan) -> Uuid {
    let mut digest = Sha256::new();
    digest.update(fence.pipeline_id.as_uuid().as_bytes());
    digest.update(fence.topology_generation.to_be_bytes());
    digest.update(plan.source_position.node_id.to_be_bytes());
    digest.update(plan.source_position.system_identifier.to_be_bytes());
    digest.update(plan.source_position.timeline.to_be_bytes());
    digest.update(plan.commit_lsn.as_u64().to_be_bytes());
    digest.update(plan.source_position.lsn.as_u64().to_be_bytes());
    digest.update(plan.source_xid.to_be_bytes());
    digest.update(plan.payload_fingerprint.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    // RFC 9562 UUIDv8 keeps custom payload bytes with standard version and variant bits.
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use chrono::Utc;
    use cloudberry_etl_core::{
        change::{RelationColumnSnapshot, SourceTransaction, TableChange, Tuple},
        id::PipelineId,
        schema::{
            ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
            ReplicaIdentity, TableKind,
        },
    };
    use cloudberry_etl_source_postgres::spool::{
        ChangeSource, SpoolIdentity, SpoolJournal, SpoolLimits,
    };
    use uuid::Uuid;

    use super::*;

    fn snapshot(relation_id: u32, column: &str) -> RelationSchemaSnapshot {
        RelationSchemaSnapshot {
            relation_id,
            name: QualifiedName::new("public", "items").unwrap(),
            relation_kind: "r".to_owned(),
            replica_identity: "d".to_owned(),
            columns: vec![RelationColumnSnapshot {
                attnum: 1,
                name: column.to_owned(),
                type_oid: 23,
                type_name: QualifiedName::new("pg_catalog", "int4").unwrap(),
                type_kind: "b".to_owned(),
                type_modifier: -1,
                nullable: false,
                generated: String::new(),
                identity: String::new(),
                collation: None,
                default_expression: None,
            }],
            primary_key: vec![1],
            partition_key: Vec::new(),
        }
    }

    fn table(relation_id: u32, columns: &[(&str, bool)]) -> TableSchema {
        TableSchema {
            relation_id,
            generation: 1,
            name: QualifiedName::new("public", "items").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: columns
                .iter()
                .enumerate()
                .map(|(index, (name, nullable))| ColumnSchema {
                    attnum: i16::try_from(index + 1).unwrap(),
                    name: (*name).to_owned(),
                    data_type: PgType {
                        oid: 23,
                        name: QualifiedName::new("pg_catalog", "int4").unwrap(),
                        kind: PgTypeKind::Int4,
                    },
                    nullable: *nullable,
                    primary_key_ordinal: (index == 0).then_some(1),
                    generated: GeneratedColumn::None,
                    identity: IdentityColumn::None,
                    collation: None,
                })
                .collect(),
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn ddl(relation_id: u32, fingerprint: &str, schema: RelationSchemaSnapshot) -> DdlMessage {
        DdlMessage {
            version: 2,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![relation_id],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: fingerprint.to_owned(),
            transitions: vec![TableTransition {
                relation_id,
                before_generation: None,
                after_generation: None,
                before_fingerprint: None,
                after_fingerprint: Some(fingerprint.to_owned()),
                after_schema: Some(schema),
                kind: TransitionKind::Unknown,
            }],
        }
    }

    fn committed(changes: Vec<TransactionChange>) -> CommittedTransaction {
        CommittedTransaction::from_memory(
            SourceTransaction {
                xid: 7,
                commit_time: Utc::now(),
                final_position: SourcePosition {
                    node_id: 0,
                    system_identifier: 99,
                    timeline: 1,
                    lsn: PgLsn::new(20),
                },
                changes,
            },
            PgLsn::new(19),
        )
    }

    fn spooled(changes: &[TransactionChange]) -> (PathBuf, CommittedTransaction) {
        let root = std::env::temp_dir().join(format!("pg2cb-schema-plan-{}", Uuid::new_v4()));
        let journal = SpoolJournal::open(
            &root,
            SpoolIdentity {
                pipeline_id: PipelineId::new(),
                topology_generation: 1,
                node_id: 0,
                system_identifier: 99,
                timeline: 1,
            },
            SpoolLimits::default(),
        )
        .unwrap();
        let mut writer = journal.begin(7, PgLsn::new(10)).unwrap();
        for change in changes {
            writer.append(change).unwrap();
        }
        let handle = writer.finish(PgLsn::new(19), PgLsn::new(20)).unwrap();
        (
            root,
            CommittedTransaction {
                transaction: SourceTransaction {
                    xid: 7,
                    commit_time: Utc::now(),
                    final_position: SourcePosition {
                        node_id: 0,
                        system_identifier: 99,
                        timeline: 1,
                        lsn: PgLsn::new(20),
                    },
                    changes: Vec::new(),
                },
                commit_lsn: PgLsn::new(19),
                change_source: ChangeSource::Spool(handle),
            },
        )
    }

    #[test]
    fn keeps_ordered_intermediate_states_and_uses_the_terminal_relation_state() {
        let first = ddl(42, "first", snapshot(42, "note"));
        let second = ddl(42, "second", snapshot(42, "description"));
        let drop = DdlMessage {
            version: 2,
            command_tag: "DROP TABLE".to_owned(),
            relation_ids: vec![42],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "drop".to_owned(),
            transitions: vec![TableTransition {
                relation_id: 42,
                before_generation: None,
                after_generation: None,
                before_fingerprint: None,
                after_fingerprint: None,
                after_schema: None,
                kind: TransitionKind::DropTable,
            }],
        };
        let row = TransactionChange::Row(TableChange {
            relation_id: 42,
            generation: 1,
            change: cloudberry_etl_core::change::RowChange::Insert {
                new: Tuple { cells: Vec::new() },
            },
        });
        let plan = plan_schema_transaction(&committed(vec![
            row,
            TransactionChange::Ddl(first),
            TransactionChange::Ddl(second),
            TransactionChange::Ddl(drop),
        ]))
        .unwrap()
        .unwrap();

        assert_eq!(
            plan.changes
                .iter()
                .map(OrderedSchemaChange::ordinal)
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(
            plan.terminal_relations.get(&42),
            Some(&CapturedRelationState::Dropped { last_ordinal: 3 })
        );
        assert_eq!(plan.command_summary(), "MULTI SCHEMA CHANGE");
        assert!(plan.payload_fingerprint.starts_with("sha256:"));
        assert_eq!(plan.ledger_payload().unwrap()["source_xid"], 7);
    }

    #[test]
    fn validates_exact_present_and_dropped_terminal_catalog_states() {
        let after = snapshot(42, "description");
        let plan = plan_schema_transaction(&committed(vec![
            TransactionChange::Ddl(ddl(42, "after", after.clone())),
            TransactionChange::Ddl(DdlMessage {
                version: 2,
                command_tag: "DROP TABLE".to_owned(),
                relation_ids: vec![84],
                affected_schemas: vec!["public".to_owned()],
                schema_fingerprint: "drop".to_owned(),
                transitions: vec![TableTransition {
                    relation_id: 84,
                    before_generation: None,
                    after_generation: None,
                    before_fingerprint: None,
                    after_fingerprint: None,
                    after_schema: None,
                    kind: TransitionKind::DropTable,
                }],
            }),
        ]))
        .unwrap()
        .unwrap();
        let current = BTreeMap::from([
            (
                42,
                Some(CurrentRelationSchema {
                    fingerprint: "after".to_owned(),
                    schema: after,
                }),
            ),
            (84, None),
        ]);
        let validation = plan.validate_catalog(&current).unwrap();
        assert!(validation.is_exact_match());
        assert_eq!(validation.matched_relations, [42, 84]);
    }

    #[test]
    fn v1_and_truncate_are_unverifiable_and_force_reload() {
        let legacy = DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![42],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "legacy".to_owned(),
            transitions: Vec::new(),
        };
        let plan = plan_schema_transaction(&committed(vec![
            TransactionChange::Ddl(legacy),
            TransactionChange::Truncate {
                relation_ids: vec![84],
                cascade: false,
                restart_identity: false,
            },
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(plan.reload_relations, BTreeSet::from([42, 84]));
        let validation = plan
            .validate_catalog(&BTreeMap::from([(42, None), (84, None)]))
            .unwrap();
        assert!(!validation.is_exact_match());
        assert_eq!(validation.unverifiable_relations, [42, 84]);
    }

    #[test]
    fn catalog_difference_is_rapid_advance_or_drift_not_a_match() {
        let plan = plan_schema_transaction(&committed(vec![TransactionChange::Ddl(ddl(
            42,
            "captured",
            snapshot(42, "before"),
        ))]))
        .unwrap()
        .unwrap();
        let current = BTreeMap::from([(
            42,
            Some(CurrentRelationSchema {
                fingerprint: "later".to_owned(),
                schema: snapshot(42, "after"),
            }),
        )]);
        let validation = plan.validate_catalog(&current).unwrap();
        assert!(!validation.is_exact_match());
        assert_eq!(
            validation.mismatches,
            [CatalogMismatch {
                relation_id: 42,
                kind: CatalogMismatchKind::DifferentPresentState,
            }]
        );
    }

    #[test]
    fn irrelevant_ddl_does_not_create_a_schema_plan() {
        let message = DdlMessage {
            version: 2,
            command_tag: "CREATE INDEX".to_owned(),
            relation_ids: Vec::new(),
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "index".to_owned(),
            transitions: Vec::new(),
        };
        assert!(
            plan_schema_transaction(&committed(vec![TransactionChange::Ddl(message)]))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn memory_and_spool_produce_the_same_schema_plan() {
        let changes = vec![
            TransactionChange::Ddl(ddl(42, "first", snapshot(42, "note"))),
            TransactionChange::Truncate {
                relation_ids: vec![42],
                cascade: false,
                restart_identity: false,
            },
            TransactionChange::Ddl(ddl(42, "last", snapshot(42, "description"))),
        ];
        let memory = plan_schema_transaction(&committed(changes.clone()))
            .unwrap()
            .unwrap();
        let (root, transaction) = spooled(&changes);
        let disk = plan_schema_transaction(&transaction).unwrap().unwrap();
        assert_eq!(disk, memory);
        transaction.cleanup_spool().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn schema_event_record_identity_is_deterministic_and_payload_bound() {
        let fence = PipelineFence {
            pipeline_id: PipelineId::new(),
            topology_generation: 3,
            fencing_token: 9,
        };
        let plan = plan_schema_transaction(&committed(vec![TransactionChange::Ddl(ddl(
            42,
            "after",
            snapshot(42, "description"),
        ))]))
        .unwrap()
        .unwrap();
        let first = plan.schema_event_record(fence).unwrap();
        let replay = plan.schema_event_record(fence).unwrap();
        assert_eq!(first, replay);
        assert_eq!(first.event_id.get_version_num(), 8);
        assert_eq!(first.source_lsn, PgLsn::new(20));
        assert_eq!(first.schema_fingerprint, plan.payload_fingerprint);

        let changed = plan_schema_transaction(&committed(vec![TransactionChange::Ddl(ddl(
            42,
            "later",
            snapshot(42, "later"),
        ))]))
        .unwrap()
        .unwrap()
        .schema_event_record(fence)
        .unwrap();
        assert_ne!(changed.event_id, first.event_id);
    }

    #[test]
    fn capability_plan_selects_online_reload_drop_add_and_noop() {
        let mut add_column = ddl(42, "after", snapshot(42, "description"));
        add_column.transitions[0].kind = TransitionKind::AddColumn {
            name: "note".to_owned(),
            nullable_or_defaulted: true,
        };
        let mut plan =
            plan_schema_transaction(&committed(vec![TransactionChange::Ddl(add_column)]))
                .unwrap()
                .unwrap();
        let validation = CatalogValidation {
            matched_relations: vec![42],
            unverifiable_relations: Vec::new(),
            mismatches: Vec::new(),
            unknown_scope: false,
        };
        let before = BTreeMap::from([(42, table(42, &[("id", false)]))]);
        let after_schema = table(42, &[("id", false), ("note", true)]);
        let after = BTreeMap::from([(42, Some(after_schema.clone()))]);
        let capability = plan_schema_capabilities(&plan, &validation, &before, &after).unwrap();
        assert!(capability.is_table_local());
        assert_eq!(capability.action_summary(), "online=1");
        assert!(matches!(
            capability.actions.get(&42),
            Some(SchemaAction::Online { transitions, after })
                if transitions.len() == 1 && after == &after_schema
        ));

        let add = plan_schema_capabilities(
            &plan,
            &validation,
            &BTreeMap::new(),
            &BTreeMap::from([(42, Some(after_schema.clone()))]),
        )
        .unwrap();
        assert!(matches!(
            add.actions.get(&42),
            Some(SchemaAction::Add { .. })
        ));

        let noop = plan_schema_capabilities(
            &plan,
            &validation,
            &BTreeMap::from([(42, after_schema.clone())]),
            &BTreeMap::from([(42, Some(after_schema.clone()))]),
        )
        .unwrap();
        assert!(matches!(noop.actions.get(&42), Some(SchemaAction::Noop)));

        plan.reload_relations.insert(42);
        let explicit = plan_schema_capabilities(&plan, &validation, &before, &after).unwrap();
        assert!(matches!(
            explicit.actions.get(&42),
            Some(SchemaAction::Reload {
                reason: ReloadReason::ExplicitFallback,
                ..
            })
        ));
        plan.reload_relations.clear();

        let mut unsafe_after = after_schema;
        unsafe_after.columns[1].nullable = false;
        let capability = plan_schema_capabilities(
            &plan,
            &validation,
            &before,
            &BTreeMap::from([(42, Some(unsafe_after))]),
        )
        .unwrap();
        assert!(matches!(
            capability.actions.get(&42),
            Some(SchemaAction::Reload {
                reason: ReloadReason::UnsafeDiff,
                ..
            })
        ));

        let drop_message = DdlMessage {
            version: 2,
            command_tag: "DROP TABLE".to_owned(),
            relation_ids: vec![42],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "drop".to_owned(),
            transitions: vec![TableTransition {
                relation_id: 42,
                before_generation: None,
                after_generation: None,
                before_fingerprint: None,
                after_fingerprint: None,
                after_schema: None,
                kind: TransitionKind::DropTable,
            }],
        };
        let drop_plan =
            plan_schema_transaction(&committed(vec![TransactionChange::Ddl(drop_message)]))
                .unwrap()
                .unwrap();
        let drop = plan_schema_capabilities(
            &drop_plan,
            &validation,
            &before,
            &BTreeMap::from([(42, None)]),
        )
        .unwrap();
        assert!(matches!(drop.actions.get(&42), Some(SchemaAction::Drop)));
    }

    #[test]
    fn capability_plan_keeps_unknown_relations_local_but_blocks_unknown_scope_and_drift() {
        let legacy = DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![42],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "legacy".to_owned(),
            transitions: Vec::new(),
        };
        let plan = plan_schema_transaction(&committed(vec![TransactionChange::Ddl(legacy)]))
            .unwrap()
            .unwrap();
        let local = plan_schema_capabilities(
            &plan,
            &CatalogValidation {
                matched_relations: Vec::new(),
                unverifiable_relations: vec![42],
                mismatches: Vec::new(),
                unknown_scope: false,
            },
            &BTreeMap::from([(42, table(42, &[("id", false)]))]),
            &BTreeMap::from([(42, Some(table(42, &[("id", false)])))]),
        )
        .unwrap();
        assert!(matches!(
            local.actions.get(&42),
            Some(SchemaAction::Reload {
                reason: ReloadReason::UnverifiableEvent,
                ..
            })
        ));

        let blocked = plan_schema_capabilities(
            &plan,
            &CatalogValidation {
                matched_relations: Vec::new(),
                unverifiable_relations: vec![42],
                mismatches: vec![CatalogMismatch {
                    relation_id: 42,
                    kind: CatalogMismatchKind::DifferentPresentState,
                }],
                unknown_scope: true,
            },
            &BTreeMap::new(),
            &BTreeMap::new(),
        )
        .unwrap();
        assert!(!blocked.is_table_local());
        assert!(blocked.actions.is_empty());
        assert_eq!(blocked.action_summary(), "blocked(2)");
        assert!(serde_json::to_value(&blocked).is_ok());
    }
}
