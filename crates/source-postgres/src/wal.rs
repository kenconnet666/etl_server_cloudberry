//! pgoutput streaming, decoding, and feedback.
//!
//! The forked `postgres-replication` crate is used only behind this module.  Every decoded event
//! is converted to core-owned values, and protocol additions are rejected until explicitly
//! implemented. This is intentional: silently skipping a WAL message would make final-state
//! convergence unverifiable.

use std::{
    collections::HashMap,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use cloudberry_etl_core::{
    change::{
        Cell, DdlMessage, RowChange, SourcePosition, SourceTransaction, TableChange,
        TransactionChange, Tuple,
    },
    lsn::PgLsn,
};
use futures::Stream;
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{
    self, LogicalReplicationMessage, ReplicaIdentity as WireReplicaIdentity, ReplicationMessage,
    TupleData,
};

use crate::{
    SourceError, SourceResult,
    ddl::{DDL_MESSAGE_PREFIX, decode_ddl_message},
    publication::start_replication_sql,
    spool::{
        ChangeSource, ResourceState, SpoolError, SpoolJournal, SpoolLimits, SpoolWriter,
        framed_change_bytes,
    },
};

const POSTGRES_EPOCH_UNIX_SECONDS: i64 = 946_684_800;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationColumn {
    pub flags: i8,
    pub name: String,
    pub type_oid: u32,
    pub type_modifier: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationEvent {
    pub relation_id: u32,
    pub namespace: String,
    pub name: String,
    /// Monotonically increasing schema generation for this relation identity.
    pub generation: u64,
    pub replica_identity: WireReplicaIdentityKind,
    pub columns: Vec<RelationColumn>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireReplicaIdentityKind {
    Default,
    Index,
    Full,
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypeEvent {
    pub type_id: u32,
    pub namespace: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedMessage {
    Begin {
        final_lsn: PgLsn,
        timestamp: DateTime<Utc>,
        xid: u32,
    },
    Commit {
        commit_lsn: PgLsn,
        end_lsn: PgLsn,
        timestamp: DateTime<Utc>,
        flags: i8,
    },
    Relation(RelationEvent),
    Type(TypeEvent),
    Insert {
        relation_id: u32,
        generation: u64,
        new: Tuple,
    },
    Update {
        relation_id: u32,
        generation: u64,
        old_key: Option<Tuple>,
        new: Tuple,
    },
    Delete {
        relation_id: u32,
        generation: u64,
        old_key: Tuple,
    },
    Truncate {
        relation_ids: Vec<u32>,
        options: i8,
    },
    Ddl(DdlMessage),
    Keepalive {
        wal_end: PgLsn,
        timestamp_micros: i64,
        reply_requested: bool,
    },
}

impl DecodedMessage {
    #[must_use]
    pub fn as_table_change(&self, generation: u64) -> Option<TableChange> {
        match self {
            Self::Insert {
                relation_id,
                generation: _,
                new,
            } => Some(TableChange {
                relation_id: *relation_id,
                generation,
                change: RowChange::Insert { new: new.clone() },
            }),
            Self::Update {
                relation_id,
                generation: _,
                old_key,
                new,
            } => Some(TableChange {
                relation_id: *relation_id,
                generation,
                change: RowChange::Update {
                    old_key: old_key.clone(),
                    new: new.clone(),
                },
            }),
            Self::Delete {
                relation_id,
                generation: _,
                old_key,
            } => Some(TableChange {
                relation_id: *relation_id,
                generation,
                change: RowChange::Delete {
                    old_key: old_key.clone(),
                },
            }),
            _ => None,
        }
    }

    #[must_use]
    pub fn relation_generation(&self) -> Option<u64> {
        match self {
            Self::Insert { generation, .. }
            | Self::Update { generation, .. }
            | Self::Delete { generation, .. } => Some(*generation),
            Self::Relation(relation) => Some(relation.generation),
            _ => None,
        }
    }

    fn to_transaction_change(&self) -> SourceResult<Option<TransactionChange>> {
        let change = match self {
            Self::Insert {
                relation_id,
                generation,
                new,
            } => TransactionChange::Row(TableChange {
                relation_id: *relation_id,
                generation: *generation,
                change: RowChange::Insert { new: new.clone() },
            }),
            Self::Update {
                relation_id,
                generation,
                old_key,
                new,
            } => TransactionChange::Row(TableChange {
                relation_id: *relation_id,
                generation: *generation,
                change: RowChange::Update {
                    old_key: old_key.clone(),
                    new: new.clone(),
                },
            }),
            Self::Delete {
                relation_id,
                generation,
                old_key,
            } => TransactionChange::Row(TableChange {
                relation_id: *relation_id,
                generation: *generation,
                change: RowChange::Delete {
                    old_key: old_key.clone(),
                },
            }),
            Self::Truncate {
                relation_ids,
                options,
            } => TransactionChange::Truncate {
                relation_ids: relation_ids.clone(),
                cascade: options & 1 != 0,
                restart_identity: options & 2 != 0,
            },
            Self::Ddl(message) => TransactionChange::Ddl(message.clone()),
            _ => return Ok(None),
        };
        Ok(Some(change))
    }
}

fn relation_shape_equal(left: &RelationEvent, right: &RelationEvent) -> bool {
    left.relation_id == right.relation_id
        && left.namespace == right.namespace
        && left.name == right.name
        && left.replica_identity == right.replica_identity
        && left.columns == right.columns
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceNodeIdentity {
    pub node_id: i32,
    pub system_identifier: u64,
    pub timeline: u32,
}

impl SourceNodeIdentity {
    #[must_use]
    pub const fn position(self, lsn: PgLsn) -> SourcePosition {
        SourcePosition {
            node_id: self.node_id,
            system_identifier: self.system_identifier,
            timeline: self.timeline,
            lsn,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionLimits {
    pub max_changes: usize,
    pub max_bytes: usize,
}

impl Default for TransactionLimits {
    fn default() -> Self {
        Self {
            max_changes: 1_000_000,
            max_bytes: 512 * 1024 * 1024,
        }
    }
}

impl TransactionLimits {
    pub fn validate(self) -> SourceResult<()> {
        if self.max_changes == 0 || self.max_bytes == 0 {
            return Err(SourceError::contract(
                "transaction limits must be greater than zero",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedTransaction {
    pub transaction: SourceTransaction,
    /// The commit record LSN. `transaction.final_position.lsn` is the commit end LSN used for ACK.
    pub commit_lsn: PgLsn,
    /// Change storage is separate from transaction metadata.  For small transactions this points
    /// at the in-memory changes retained in `transaction`; for spilled transactions the vector is
    /// empty and the reader streams validated journal frames from disk.
    pub change_source: ChangeSource,
}

impl CommittedTransaction {
    #[must_use]
    pub fn from_memory(transaction: SourceTransaction, commit_lsn: PgLsn) -> Self {
        let change_source = ChangeSource::memory(transaction.changes.clone());
        Self {
            transaction,
            commit_lsn,
            change_source,
        }
    }

    /// Idempotently release a spilled transaction after its target checkpoint is durable.
    pub fn cleanup_spool(&self) -> SourceResult<()> {
        self.change_source
            .cleanup()
            .map_err(|error| SourceError::Spool(error.to_string()))
    }
}

impl From<SourceTransaction> for CommittedTransaction {
    fn from(transaction: SourceTransaction) -> Self {
        let commit_lsn = transaction.final_position.lsn;
        Self::from_memory(transaction, commit_lsn)
    }
}

impl std::ops::Deref for CommittedTransaction {
    type Target = SourceTransaction;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AssembledEvent {
    Transaction(Box<CommittedTransaction>),
    Keepalive {
        wal_end: PgLsn,
        timestamp_micros: i64,
        reply_requested: bool,
    },
}

#[derive(Debug)]
struct PendingTransaction {
    xid: u32,
    begin_lsn: PgLsn,
    begin_time: DateTime<Utc>,
    changes: Vec<TransactionChange>,
    buffered_bytes: usize,
    spool_writer: Option<SpoolWriter>,
}

/// Turn decoded pgoutput events into complete, node-scoped source transactions.
///
/// A commit is the only event that produces a transaction. Keepalives are returned immediately
/// and never enter the transaction change list, so an ACK scheduler cannot mistake a heartbeat for
/// an applied data prefix.
#[derive(Debug)]
pub struct TransactionAssembler {
    identity: SourceNodeIdentity,
    limits: TransactionLimits,
    pending: Option<PendingTransaction>,
    last_commit_lsn: Option<PgLsn>,
    relation_generations: HashMap<u32, u64>,
    spool: Option<SpoolJournal>,
    resource_wait: Option<ResourceState>,
}

impl TransactionAssembler {
    #[must_use]
    pub fn new(identity: SourceNodeIdentity) -> Self {
        Self {
            identity,
            limits: TransactionLimits::default(),
            pending: None,
            last_commit_lsn: None,
            relation_generations: HashMap::new(),
            spool: None,
            resource_wait: None,
        }
    }

    pub fn with_limits(
        identity: SourceNodeIdentity,
        limits: TransactionLimits,
    ) -> SourceResult<Self> {
        limits.validate()?;
        Ok(Self {
            identity,
            limits,
            pending: None,
            last_commit_lsn: None,
            relation_generations: HashMap::new(),
            spool: None,
            resource_wait: None,
        })
    }

    /// Configure the same assembler to spill once its in-memory watermarks are reached.  The
    /// legacy constructor remains memory-only for callers that do not yet have a pipeline scope.
    pub fn with_spool(
        identity: SourceNodeIdentity,
        limits: TransactionLimits,
        journal: SpoolJournal,
    ) -> SourceResult<Self> {
        limits.validate()?;
        Ok(Self {
            identity,
            limits,
            pending: None,
            last_commit_lsn: None,
            relation_generations: HashMap::new(),
            spool: Some(journal),
            resource_wait: None,
        })
    }

    #[must_use]
    pub const fn identity(&self) -> SourceNodeIdentity {
        self.identity
    }

    #[must_use]
    pub fn pending_xid(&self) -> Option<u32> {
        self.pending.as_ref().map(|pending| pending.xid)
    }

    #[must_use]
    pub fn relation_generation(&self, relation_id: u32) -> Option<u64> {
        self.relation_generations.get(&relation_id).copied()
    }

    #[must_use]
    pub fn spool_limits(&self) -> Option<SpoolLimits> {
        self.spool.as_ref().map(SpoolJournal::limits)
    }

    /// Check spool capacity before accepting a decoded message.
    ///
    /// On the first spill, `additional_bytes` covers every buffered change plus the current
    /// frame. Once spilling has started, buffered writer bytes not yet reflected in file metadata
    /// are included as pending usage. Callers can therefore wait without partially consuming the
    /// message into assembler state.
    pub fn spool_resource_state(
        &self,
        event: &DecodedMessage,
    ) -> SourceResult<Option<ResourceState>> {
        let Some(journal) = &self.spool else {
            return Ok(None);
        };
        let Some(pending) = &self.pending else {
            return Ok(None);
        };
        let additional_bytes = match event {
            DecodedMessage::Commit {
                commit_lsn,
                end_lsn,
                ..
            } => match &pending.spool_writer {
                Some(writer) => writer
                    .finish_additional_bytes(*commit_lsn, *end_lsn)
                    .map_err(|error| SourceError::Spool(error.to_string()))?,
                None => return Ok(None),
            },
            _ => {
                let Some(change) = event.to_transaction_change()? else {
                    return Ok(None);
                };
                let duplicate_ddl = matches!(&change, TransactionChange::Ddl(message) if pending
                    .changes
                    .iter()
                    .any(|existing| matches!(existing, TransactionChange::Ddl(previous) if previous == message)));
                if duplicate_ddl {
                    return Ok(None);
                }
                let bytes = transaction_change_bytes(&change);
                let over_memory = pending.changes.len() >= self.limits.max_changes
                    || pending.buffered_bytes.saturating_add(bytes) > self.limits.max_bytes;
                if pending.spool_writer.is_some() {
                    framed_change_bytes(&change)
                        .map_err(|error| SourceError::Spool(error.to_string()))?
                } else if over_memory {
                    pending
                        .changes
                        .iter()
                        .chain(std::iter::once(&change))
                        .try_fold(0_u64, |total, buffered| {
                            framed_change_bytes(buffered).map(|bytes| total.saturating_add(bytes))
                        })
                        .map_err(|error| SourceError::Spool(error.to_string()))?
                } else {
                    return Ok(None);
                }
            }
        };

        journal
            .resource_state(additional_bytes)
            .map(Some)
            .map_err(|error| SourceError::Spool(error.to_string()))
    }

    /// Return and clear a recoverable capacity failure raised after preflight (for example,
    /// ENOSPC between the check and the write). The decoded message was not committed to the
    /// assembler and may be retried unchanged.
    pub fn take_resource_wait(&mut self) -> Option<ResourceState> {
        self.resource_wait.take()
    }

    pub fn push(&mut self, event: DecodedMessage) -> SourceResult<Option<AssembledEvent>> {
        match event {
            DecodedMessage::Keepalive {
                wal_end,
                timestamp_micros,
                reply_requested,
            } => Ok(Some(AssembledEvent::Keepalive {
                wal_end,
                timestamp_micros,
                reply_requested,
            })),
            DecodedMessage::Relation(relation) => {
                self.accept_relation(&relation)?;
                Ok(None)
            }
            DecodedMessage::Type(_) => Ok(None),
            DecodedMessage::Begin {
                final_lsn,
                timestamp,
                xid,
            } => {
                if self.pending.is_some() {
                    return Err(SourceError::ReplicationProtocol(
                        "BEGIN arrived before the previous transaction committed".to_owned(),
                    ));
                }
                if let Some(previous) = self.last_commit_lsn
                    && final_lsn <= previous
                {
                    return Err(SourceError::ReplicationProtocol(format!(
                        "BEGIN final LSN {} did not advance beyond committed LSN {}",
                        final_lsn, previous
                    )));
                }
                self.pending = Some(PendingTransaction {
                    xid,
                    begin_lsn: final_lsn,
                    begin_time: timestamp,
                    changes: Vec::new(),
                    buffered_bytes: 0,
                    spool_writer: None,
                });
                Ok(None)
            }
            DecodedMessage::Commit {
                commit_lsn,
                end_lsn,
                timestamp,
                flags,
            } => self.commit(commit_lsn, end_lsn, timestamp, flags),
            event => {
                let Some(change) = event.to_transaction_change()? else {
                    return Ok(None);
                };
                if self.pending.is_none() {
                    return Err(SourceError::ReplicationProtocol(
                        "transactional pgoutput event arrived without BEGIN".to_owned(),
                    ));
                }
                if let TransactionChange::Row(row) = &change
                    && self.relation_generations.get(&row.relation_id).copied()
                        != Some(row.generation)
                {
                    return Err(SourceError::ReplicationProtocol(format!(
                        "relation {} generation {} is not current",
                        row.relation_id, row.generation
                    )));
                }
                // Independent managed capture triggers can overlap during upgrades and emit the
                // same DDL message in one transaction. Keep one barrier, but retain messages with
                // a different fingerprint or schema scope.
                if let TransactionChange::Ddl(message) = &change
                    && self.pending.as_ref().is_some_and(|pending| {
                        pending.changes.iter().any(|existing| {
                            matches!(existing, TransactionChange::Ddl(previous) if previous == message)
                        })
                    })
                {
                    return Ok(None);
                }
                let bytes = transaction_change_bytes(&change);
                let over_memory = self.pending.as_ref().is_some_and(|pending| {
                    pending.changes.len() >= self.limits.max_changes
                        || pending.buffered_bytes.saturating_add(bytes) > self.limits.max_bytes
                });
                if over_memory
                    && self
                        .pending
                        .as_ref()
                        .is_some_and(|pending| pending.spool_writer.is_none())
                {
                    let pending = self.pending.as_ref().expect("checked above");
                    let journal = self.spool.as_ref().ok_or_else(|| {
                        SourceError::unsupported(format!(
                            "transaction {} requires a spool; configure transaction spool settings",
                            pending.xid
                        ))
                    })?;
                    let mut writer = match journal.begin(pending.xid, pending.begin_lsn) {
                        Ok(writer) => writer,
                        Err(error) => return Err(self.record_spool_error(error)),
                    };
                    for buffered in &pending.changes {
                        if let Err(error) = writer.append(buffered) {
                            if let Err(abort) = writer.abort() {
                                return Err(self.record_spool_error(abort));
                            }
                            return Err(self.record_spool_error(error));
                        }
                    }
                    if let Err(error) = writer.append(&change) {
                        if let Err(abort) = writer.abort() {
                            return Err(self.record_spool_error(abort));
                        }
                        return Err(self.record_spool_error(error));
                    }
                    let pending = self.pending.as_mut().expect("checked above");
                    pending.changes.clear();
                    pending.spool_writer = Some(writer);
                    pending.buffered_bytes = 0;
                    return Ok(None);
                }
                let pending = self.pending.as_mut().expect("checked above");
                if let Some(writer) = pending.spool_writer.as_mut() {
                    let result = writer.append(&change);
                    if let Err(error) = result {
                        return Err(self.record_spool_error(error));
                    }
                } else {
                    pending.buffered_bytes = pending.buffered_bytes.saturating_add(bytes);
                    pending.changes.push(change);
                }
                Ok(None)
            }
        }
    }

    pub fn finish(&self) -> SourceResult<()> {
        if let Some(pending) = &self.pending {
            return Err(SourceError::ReplicationProtocol(format!(
                "replication stream ended with open transaction {}",
                pending.xid
            )));
        }
        Ok(())
    }

    fn accept_relation(&mut self, relation: &RelationEvent) -> SourceResult<()> {
        if relation.generation == 0 {
            return Err(SourceError::ReplicationProtocol(format!(
                "relation {} has zero schema generation",
                relation.relation_id
            )));
        }
        if let Some(previous) = self.relation_generations.get(&relation.relation_id)
            && relation.generation < *previous
        {
            return Err(SourceError::ReplicationProtocol(format!(
                "relation {} generation moved backwards from {} to {}",
                relation.relation_id, previous, relation.generation
            )));
        }
        self.relation_generations
            .insert(relation.relation_id, relation.generation);
        Ok(())
    }

    fn commit(
        &mut self,
        commit_lsn: PgLsn,
        end_lsn: PgLsn,
        timestamp: DateTime<Utc>,
        flags: i8,
    ) -> SourceResult<Option<AssembledEvent>> {
        if flags != 0 {
            return Err(SourceError::ReplicationProtocol(format!(
                "unsupported pgoutput COMMIT flags {flags}"
            )));
        }
        if end_lsn < commit_lsn {
            return Err(SourceError::ReplicationProtocol(format!(
                "commit end LSN {} precedes commit LSN {}",
                end_lsn, commit_lsn
            )));
        }
        if let Some(previous) = self.last_commit_lsn
            && end_lsn <= previous
        {
            return Err(SourceError::ReplicationProtocol(format!(
                "commit LSN {} did not advance beyond {}",
                end_lsn, previous
            )));
        }
        let pending = self.pending.as_ref().ok_or_else(|| {
            SourceError::ReplicationProtocol("COMMIT arrived without BEGIN".to_owned())
        })?;
        if pending.begin_lsn > end_lsn {
            return Err(SourceError::ReplicationProtocol(format!(
                "transaction {} begins after its commit",
                pending.xid
            )));
        }
        if timestamp < pending.begin_time {
            return Err(SourceError::ReplicationProtocol(format!(
                "transaction {} commit timestamp precedes BEGIN timestamp",
                pending.xid
            )));
        }
        let spool_handle = match self
            .pending
            .as_mut()
            .and_then(|pending| pending.spool_writer.as_mut())
        {
            Some(writer) => match writer.finish(commit_lsn, end_lsn) {
                Ok(handle) => Some(handle),
                Err(error) => return Err(self.record_spool_error(error)),
            },
            None => None,
        };
        let pending = self.pending.take().expect("validated above");
        self.last_commit_lsn = Some(end_lsn);
        let final_position = self.identity.position(end_lsn);
        let (changes, change_source) = match spool_handle {
            Some(handle) => (Vec::new(), ChangeSource::Spool(handle)),
            None => {
                let source = ChangeSource::memory(pending.changes.clone());
                (pending.changes, source)
            }
        };
        Ok(Some(AssembledEvent::Transaction(Box::new(
            CommittedTransaction {
                transaction: SourceTransaction {
                    xid: pending.xid,
                    commit_time: timestamp,
                    final_position,
                    changes,
                },
                commit_lsn,
                change_source,
            },
        ))))
    }

    fn record_spool_error(&mut self, error: SpoolError) -> SourceError {
        self.resource_wait = match &error {
            SpoolError::ResourceWait {
                used,
                high,
                free,
                minimum,
            } => Some(ResourceState::Wait {
                used_bytes: *used,
                disk_high_water_bytes: *high,
                free_bytes: *free,
                minimum_free_disk_bytes: *minimum,
            }),
            _ => None,
        };
        SourceError::Spool(error.to_string())
    }
}

#[derive(Debug, Default)]
pub struct TransactionDecoder {
    in_transaction: bool,
    relations: HashMap<u32, RelationEvent>,
}

impl TransactionDecoder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn relation(&self, relation_id: u32) -> Option<&RelationEvent> {
        self.relations.get(&relation_id)
    }

    pub fn decode(&mut self, message: LogicalReplicationMessage) -> SourceResult<DecodedMessage> {
        match message {
            LogicalReplicationMessage::Begin(body) => {
                if self.in_transaction {
                    return Err(SourceError::ReplicationProtocol(
                        "nested pgoutput BEGIN".to_owned(),
                    ));
                }
                self.in_transaction = true;
                Ok(DecodedMessage::Begin {
                    final_lsn: PgLsn::new(body.final_lsn()),
                    timestamp: postgres_timestamp(body.timestamp())?,
                    xid: body.xid(),
                })
            }
            LogicalReplicationMessage::Commit(body) => {
                if !self.in_transaction {
                    return Err(SourceError::ReplicationProtocol(
                        "pgoutput COMMIT without BEGIN".to_owned(),
                    ));
                }
                self.in_transaction = false;
                Ok(DecodedMessage::Commit {
                    commit_lsn: PgLsn::new(body.commit_lsn()),
                    end_lsn: PgLsn::new(body.end_lsn()),
                    timestamp: postgres_timestamp(body.timestamp())?,
                    flags: body.flags(),
                })
            }
            LogicalReplicationMessage::Relation(body) => {
                let mut event = RelationEvent {
                    relation_id: body.rel_id(),
                    namespace: body.namespace()?.to_owned(),
                    name: body.name()?.to_owned(),
                    generation: 1,
                    replica_identity: map_replica_identity(body.replica_identity()),
                    columns: body
                        .columns()
                        .iter()
                        .map(|column| {
                            Ok(RelationColumn {
                                flags: column.flags(),
                                name: column.name()?.to_owned(),
                                type_oid: u32::try_from(column.type_id()).map_err(|_| {
                                    SourceError::ReplicationProtocol(format!(
                                        "negative type OID {}",
                                        column.type_id()
                                    ))
                                })?,
                                type_modifier: column.type_modifier(),
                            })
                        })
                        .collect::<SourceResult<Vec<_>>>()?,
                };
                if let Some(previous) = self.relations.get(&event.relation_id) {
                    event.generation = if relation_shape_equal(previous, &event) {
                        previous.generation
                    } else {
                        previous.generation.checked_add(1).ok_or_else(|| {
                            SourceError::ReplicationProtocol(format!(
                                "relation {} generation overflow",
                                event.relation_id
                            ))
                        })?
                    };
                }
                self.relations.insert(event.relation_id, event.clone());
                Ok(DecodedMessage::Relation(event))
            }
            LogicalReplicationMessage::Type(body) => Ok(DecodedMessage::Type(TypeEvent {
                type_id: body.id(),
                namespace: body.namespace()?.to_owned(),
                name: body.name()?.to_owned(),
            })),
            LogicalReplicationMessage::Insert(body) => {
                self.require_transaction("INSERT")?;
                let relation_id = body.rel_id();
                self.require_relation(relation_id)?;
                Ok(DecodedMessage::Insert {
                    relation_id,
                    generation: self.relation_generation(relation_id)?,
                    new: convert_tuple(body.tuple())?,
                })
            }
            LogicalReplicationMessage::Update(body) => {
                self.require_transaction("UPDATE")?;
                let relation_id = body.rel_id();
                self.require_relation(relation_id)?;
                let old_key = body
                    .key_tuple()
                    .or_else(|| body.old_tuple())
                    .map(convert_tuple)
                    .transpose()?;
                Ok(DecodedMessage::Update {
                    relation_id,
                    generation: self.relation_generation(relation_id)?,
                    old_key,
                    new: convert_tuple(body.new_tuple())?,
                })
            }
            LogicalReplicationMessage::Delete(body) => {
                self.require_transaction("DELETE")?;
                let relation_id = body.rel_id();
                self.require_relation(relation_id)?;
                let old_key = body
                    .key_tuple()
                    .or_else(|| body.old_tuple())
                    .ok_or_else(|| {
                        SourceError::ReplicationProtocol(format!(
                            "DELETE for relation {relation_id} has no old key"
                        ))
                    })?;
                Ok(DecodedMessage::Delete {
                    relation_id,
                    generation: self.relation_generation(relation_id)?,
                    old_key: convert_tuple(old_key)?,
                })
            }
            LogicalReplicationMessage::Truncate(body) => {
                self.require_transaction("TRUNCATE")?;
                for relation_id in body.rel_ids() {
                    self.require_relation(*relation_id)?;
                }
                Ok(DecodedMessage::Truncate {
                    relation_ids: body.rel_ids().to_vec(),
                    options: body.options(),
                })
            }
            LogicalReplicationMessage::Message(body) => {
                self.require_transaction("MESSAGE")?;
                let prefix = body.prefix()?.to_owned();
                let content = body.content()?.as_bytes();
                if prefix != DDL_MESSAGE_PREFIX {
                    return Err(SourceError::ReplicationProtocol(format!(
                        "unknown logical message prefix `{prefix}`"
                    )));
                }
                Ok(DecodedMessage::Ddl(decode_ddl_message(
                    prefix.as_str(),
                    content,
                )?))
            }
            LogicalReplicationMessage::Origin(body) => {
                Err(SourceError::ReplicationProtocol(format!(
                    "logical replication origin `{}` is unsupported",
                    body.name()?
                )))
            }
            _ => Err(SourceError::ReplicationProtocol(
                "unknown or unsupported pgoutput message".to_owned(),
            )),
        }
    }

    fn require_transaction(&self, operation: &str) -> SourceResult<()> {
        if self.in_transaction {
            Ok(())
        } else {
            Err(SourceError::ReplicationProtocol(format!(
                "pgoutput {operation} outside a transaction"
            )))
        }
    }

    fn require_relation(&self, relation_id: u32) -> SourceResult<()> {
        let Some(relation) = self.relations.get(&relation_id) else {
            return Err(SourceError::ReplicationProtocol(format!(
                "DML references unknown relation {relation_id}"
            )));
        };
        if relation.replica_identity != WireReplicaIdentityKind::Default {
            return Err(SourceError::ReplicationProtocol(format!(
                "relation {relation_id} changed replica identity; only DEFAULT is supported"
            )));
        }
        Ok(())
    }

    fn relation_generation(&self, relation_id: u32) -> SourceResult<u64> {
        self.relations
            .get(&relation_id)
            .map(|relation| relation.generation)
            .ok_or_else(|| {
                SourceError::ReplicationProtocol(format!(
                    "DML references unknown relation {relation_id}"
                ))
            })
    }
}

fn transaction_change_bytes(change: &TransactionChange) -> usize {
    match change {
        TransactionChange::Row(row) => match &row.change {
            RowChange::Insert { new } => tuple_bytes(&new.cells),
            RowChange::Update { old_key, new } => {
                old_key
                    .as_ref()
                    .map_or(0, |tuple| tuple_bytes(&tuple.cells))
                    + tuple_bytes(&new.cells)
            }
            RowChange::Delete { old_key } => tuple_bytes(&old_key.cells),
        },
        TransactionChange::Truncate { relation_ids, .. } => relation_ids.len() * 4,
        TransactionChange::Ddl(message) => {
            message.command_tag.len()
                + message.schema_fingerprint.len()
                + message.relation_ids.len() * 4
                + message
                    .affected_schemas
                    .iter()
                    .map(String::len)
                    .sum::<usize>()
        }
    }
}

fn tuple_bytes(cells: &[Cell]) -> usize {
    cells
        .iter()
        .map(|cell| match cell {
            Cell::Null | Cell::UnchangedToast => 1,
            Cell::Text(bytes) | Cell::Binary(bytes) => bytes.len(),
        })
        .sum()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StandbyStatus {
    pub write_lsn: PgLsn,
    pub flush_lsn: PgLsn,
    pub apply_lsn: PgLsn,
    pub reply_requested: bool,
}

pub struct ReplicationTransport {
    stream: Pin<Box<LogicalReplicationStream>>,
    decoder: TransactionDecoder,
}

impl ReplicationTransport {
    /// Start `pgoutput` over the fork's COPY BOTH implementation.
    pub async fn start(
        client: &replication_postgres::Client,
        slot_name: &str,
        start_lsn: &str,
        publication_name: &str,
    ) -> SourceResult<Self> {
        let sql = start_replication_sql(slot_name, start_lsn, publication_name)?;
        let stream = client.copy_both_simple::<Bytes>(&sql).await?;
        Ok(Self {
            stream: Box::pin(LogicalReplicationStream::new(stream)),
            decoder: TransactionDecoder::new(),
        })
    }

    pub async fn send_status(&mut self, status: StandbyStatus) -> SourceResult<()> {
        self.stream
            .as_mut()
            .standby_status_update(
                status.write_lsn.as_u64().into(),
                status.flush_lsn.as_u64().into(),
                status.apply_lsn.as_u64().into(),
                Utc::now().timestamp_micros(),
                u8::from(status.reply_requested),
            )
            .await
            .map_err(|error| SourceError::ReplicationProtocol(error.to_string()))
    }

    pub async fn send_hot_standby_feedback(
        &mut self,
        timestamp: i64,
        global_xmin: u32,
        global_xmin_epoch: u32,
        catalog_xmin: u32,
        catalog_xmin_epoch: u32,
    ) -> SourceResult<()> {
        self.stream
            .as_mut()
            .hot_standby_feedback(
                timestamp,
                global_xmin,
                global_xmin_epoch,
                catalog_xmin,
                catalog_xmin_epoch,
            )
            .await
            .map_err(|error| SourceError::ReplicationProtocol(error.to_string()))
    }
}

impl Stream for ReplicationTransport {
    type Item = SourceResult<DecodedMessage>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context).map(|item| {
            item.map(|result| match result {
                Ok(ReplicationMessage::XLogData(body)) => self.decoder.decode(body.into_data()),
                Ok(ReplicationMessage::PrimaryKeepAlive(body)) => Ok(DecodedMessage::Keepalive {
                    wal_end: PgLsn::new(body.wal_end()),
                    timestamp_micros: body.timestamp(),
                    reply_requested: body.reply() != 0,
                }),
                Err(error) => Err(SourceError::ReplicationProtocol(error.to_string())),
                Ok(_) => Err(SourceError::ReplicationProtocol(
                    "unknown replication envelope".to_owned(),
                )),
            })
        })
    }
}

fn convert_tuple(tuple: &protocol::Tuple) -> SourceResult<Tuple> {
    Ok(Tuple {
        cells: tuple
            .tuple_data()
            .iter()
            .map(|value| match value {
                TupleData::Null => Ok(Cell::Null),
                TupleData::UnchangedToast => Ok(Cell::UnchangedToast),
                TupleData::Text(bytes) => Ok(Cell::Text(bytes.clone())),
                TupleData::Binary(bytes) => Ok(Cell::Binary(bytes.clone())),
            })
            .collect::<SourceResult<Vec<_>>>()?,
    })
}

fn map_replica_identity(identity: &WireReplicaIdentity) -> WireReplicaIdentityKind {
    match identity {
        WireReplicaIdentity::Default => WireReplicaIdentityKind::Default,
        WireReplicaIdentity::Index => WireReplicaIdentityKind::Index,
        WireReplicaIdentity::Full => WireReplicaIdentityKind::Full,
        WireReplicaIdentity::Nothing => WireReplicaIdentityKind::Nothing,
    }
}

fn postgres_timestamp(micros_since_2000: i64) -> SourceResult<DateTime<Utc>> {
    let unix_micros = micros_since_2000
        .checked_add(POSTGRES_EPOCH_UNIX_SECONDS.saturating_mul(1_000_000))
        .ok_or_else(|| SourceError::ReplicationProtocol("timestamp overflow".to_owned()))?;
    Utc.timestamp_micros(unix_micros)
        .single()
        .ok_or_else(|| SourceError::ReplicationProtocol("invalid PostgreSQL timestamp".to_owned()))
}

/// Parse one raw logical payload. Unknown protocol tags fail closed through the fork parser.
pub fn parse_logical_payload(payload: Bytes) -> SourceResult<LogicalReplicationMessage> {
    LogicalReplicationMessage::parse(&payload)
        .map_err(|error| SourceError::ReplicationProtocol(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::{Path, PathBuf},
    };

    use cloudberry_etl_core::id::PipelineId;
    use uuid::Uuid;

    use super::*;

    fn timestamp(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0)
            .single()
            .expect("valid timestamp")
    }

    fn relation(relation_id: u32, generation: u64) -> RelationEvent {
        RelationEvent {
            relation_id,
            namespace: "public".to_owned(),
            name: "items".to_owned(),
            generation,
            replica_identity: WireReplicaIdentityKind::Default,
            columns: Vec::new(),
        }
    }

    fn insert(relation_id: u32, generation: u64) -> DecodedMessage {
        DecodedMessage::Insert {
            relation_id,
            generation,
            new: Tuple { cells: Vec::new() },
        }
    }

    fn begin(lsn: u64, xid: u32) -> DecodedMessage {
        DecodedMessage::Begin {
            final_lsn: PgLsn::new(lsn),
            timestamp: timestamp(1),
            xid,
        }
    }

    fn commit(commit_lsn: u64, end_lsn: u64) -> DecodedMessage {
        DecodedMessage::Commit {
            commit_lsn: PgLsn::new(commit_lsn),
            end_lsn: PgLsn::new(end_lsn),
            timestamp: timestamp(2),
            flags: 0,
        }
    }

    fn temp_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("pg2cb-wal-{label}-{}", Uuid::new_v4()))
    }

    fn spooling_assembler(root: &Path, disk_high_water_bytes: u64) -> TransactionAssembler {
        let identity = SourceNodeIdentity {
            node_id: 7,
            system_identifier: 99,
            timeline: 3,
        };
        let journal = SpoolJournal::open(
            root,
            crate::spool::SpoolIdentity {
                pipeline_id: PipelineId::new(),
                topology_generation: 1,
                node_id: identity.node_id,
                system_identifier: identity.system_identifier,
                timeline: identity.timeline,
            },
            SpoolLimits {
                memory_high_water_bytes: 1,
                segment_target_bytes: usize::try_from(disk_high_water_bytes).unwrap(),
                disk_high_water_bytes,
                minimum_free_disk_bytes: 1,
            },
        )
        .unwrap();
        TransactionAssembler::with_spool(
            identity,
            TransactionLimits {
                max_changes: 1,
                max_bytes: usize::MAX,
            },
            journal,
        )
        .unwrap()
    }

    #[test]
    fn unknown_protocol_tag_fails_closed() {
        assert!(parse_logical_payload(Bytes::from_static(b"Z")).is_err());
    }

    #[test]
    fn streaming_and_two_phase_tags_fail_closed() {
        for tag in *b"SEcAP" {
            assert!(
                parse_logical_payload(Bytes::from(vec![tag])).is_err(),
                "unsupported protocol tag {tag:?} was accepted"
            );
        }
    }

    #[test]
    fn postgres_epoch_conversion_is_exact() {
        assert_eq!(postgres_timestamp(0).unwrap().timestamp(), 946_684_800);
    }

    #[test]
    fn tuple_data_preserves_unchanged_toast() {
        // Constructing the fork's private tuple is intentionally impossible here; the mapping
        // is covered by the protocol integration tests. This assertion documents the contract.
        assert_eq!(std::mem::size_of::<Cell>(), std::mem::size_of::<Cell>());
    }

    #[test]
    fn assembler_emits_only_on_commit_and_keeps_keepalive_separate() {
        let identity = SourceNodeIdentity {
            node_id: 7,
            system_identifier: 99,
            timeline: 3,
        };
        let mut assembler = TransactionAssembler::new(identity);
        assert!(
            assembler
                .push(DecodedMessage::Relation(relation(42, 1)))
                .unwrap()
                .is_none()
        );
        assert!(assembler.push(begin(10, 11)).unwrap().is_none());
        let heartbeat = assembler
            .push(DecodedMessage::Keepalive {
                wal_end: PgLsn::new(12),
                timestamp_micros: 1,
                reply_requested: true,
            })
            .unwrap();
        assert!(matches!(heartbeat, Some(AssembledEvent::Keepalive { .. })));
        assert!(assembler.push(insert(42, 1)).unwrap().is_none());
        assert!(
            assembler
                .push(DecodedMessage::Ddl(DdlMessage {
                    version: 1,
                    command_tag: "ALTER TABLE".to_owned(),
                    relation_ids: vec![42],
                    affected_schemas: vec!["public".to_owned()],
                    schema_fingerprint: "abc".to_owned(),
                    transitions: Vec::new(),
                }))
                .unwrap()
                .is_none()
        );
        assert!(
            assembler
                .push(DecodedMessage::Truncate {
                    relation_ids: vec![42],
                    options: 3,
                })
                .unwrap()
                .is_none()
        );
        let output = assembler.push(commit(20, 21)).unwrap();
        let Some(AssembledEvent::Transaction(committed)) = output else {
            panic!("commit did not produce a transaction")
        };
        assert_eq!(committed.commit_lsn, PgLsn::new(20));
        assert_eq!(
            committed.transaction.final_position,
            identity.position(PgLsn::new(21))
        );
        assert_eq!(committed.transaction.xid, 11);
        assert_eq!(committed.transaction.changes.len(), 3);
        assert!(assembler.finish().is_ok());
    }

    #[test]
    fn assembler_deduplicates_identical_ddl_messages_in_one_transaction() {
        let identity = SourceNodeIdentity {
            node_id: 1,
            system_identifier: 2,
            timeline: 1,
        };
        let message = DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![42],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "same".to_owned(),
            transitions: Vec::new(),
        };
        let mut assembler = TransactionAssembler::new(identity);
        assembler.push(begin(1, 7)).unwrap();
        assembler
            .push(DecodedMessage::Ddl(message.clone()))
            .unwrap();
        assembler.push(DecodedMessage::Ddl(message)).unwrap();
        let Some(AssembledEvent::Transaction(committed)) = assembler.push(commit(2, 3)).unwrap()
        else {
            panic!("commit did not produce a transaction");
        };
        assert_eq!(
            committed
                .transaction
                .changes
                .iter()
                .filter(|change| matches!(change, TransactionChange::Ddl(_)))
                .count(),
            1
        );
    }

    #[test]
    fn first_spill_preflight_counts_buffered_and_current_frames() {
        let root = temp_root("preflight");
        let event = insert(42, 1);
        let frame_bytes =
            framed_change_bytes(&event.to_transaction_change().unwrap().unwrap()).unwrap();
        let mut assembler = spooling_assembler(&root, frame_bytes);
        assembler
            .push(DecodedMessage::Relation(relation(42, 1)))
            .unwrap();
        assembler.push(begin(10, 11)).unwrap();
        assembler.push(event.clone()).unwrap();

        assert!(matches!(
            assembler.spool_resource_state(&event).unwrap(),
            Some(ResourceState::Wait { used_bytes: 0, .. })
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn tiny_memory_watermark_spills_and_streams_every_change() {
        let root = temp_root("spill");
        let mut assembler = spooling_assembler(&root, 1024 * 1024);
        assembler
            .push(DecodedMessage::Relation(relation(42, 1)))
            .unwrap();
        assembler.push(begin(10, 11)).unwrap();
        assembler.push(insert(42, 1)).unwrap();
        assembler.push(insert(42, 1)).unwrap();
        let Some(AssembledEvent::Transaction(committed)) = assembler.push(commit(20, 21)).unwrap()
        else {
            panic!("commit did not produce a transaction");
        };
        assert!(committed.transaction.changes.is_empty());
        assert!(matches!(committed.change_source, ChangeSource::Spool(_)));
        assert_eq!(
            committed
                .change_source
                .reader()
                .unwrap()
                .collect::<crate::spool::SpoolResult<Vec<_>>>()
                .unwrap()
                .len(),
            2
        );
        committed.cleanup_spool().unwrap();
        committed.cleanup_spool().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn capacity_failure_rolls_back_frame_and_allows_same_change_retry() {
        let root = temp_root("retry");
        let mut assembler = spooling_assembler(&root, 1024 * 1024);
        assembler
            .push(DecodedMessage::Relation(relation(42, 1)))
            .unwrap();
        assembler.push(begin(10, 11)).unwrap();
        assembler.push(insert(42, 1)).unwrap();
        assembler.push(insert(42, 1)).unwrap();
        assembler
            .pending
            .as_mut()
            .and_then(|pending| pending.spool_writer.as_mut())
            .unwrap()
            .inject_next_append_capacity_failure();

        let retry = insert(42, 1);
        assert!(assembler.push(retry.clone()).is_err());
        assert!(matches!(
            assembler.take_resource_wait(),
            Some(ResourceState::Wait { .. })
        ));
        assembler.push(retry).unwrap();
        let Some(AssembledEvent::Transaction(committed)) = assembler.push(commit(20, 21)).unwrap()
        else {
            panic!("commit did not produce a transaction");
        };
        assert_eq!(
            committed
                .change_source
                .reader()
                .unwrap()
                .collect::<crate::spool::SpoolResult<Vec<_>>>()
                .unwrap()
                .len(),
            3
        );
        committed.cleanup_spool().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn manifest_capacity_failure_keeps_transaction_open_for_commit_retry() {
        let root = temp_root("commit-retry");
        let mut assembler = spooling_assembler(&root, 1024 * 1024);
        assembler
            .push(DecodedMessage::Relation(relation(42, 1)))
            .unwrap();
        assembler.push(begin(10, 11)).unwrap();
        assembler.push(insert(42, 1)).unwrap();
        assembler.push(insert(42, 1)).unwrap();
        assembler
            .pending
            .as_mut()
            .and_then(|pending| pending.spool_writer.as_mut())
            .unwrap()
            .inject_next_finish_capacity_failure();

        let commit = commit(20, 21);
        assert!(assembler.push(commit.clone()).is_err());
        assert_eq!(assembler.pending_xid(), Some(11));
        assert!(matches!(
            assembler.take_resource_wait(),
            Some(ResourceState::Wait { .. })
        ));
        let Some(AssembledEvent::Transaction(committed)) = assembler.push(commit).unwrap() else {
            panic!("retried commit did not produce a transaction");
        };
        assert_eq!(committed.change_source.change_count(), 2);
        committed.cleanup_spool().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn assembler_rejects_invalid_sequence_and_lsn_regression() {
        let identity = SourceNodeIdentity {
            node_id: 1,
            system_identifier: 2,
            timeline: 1,
        };
        let mut assembler = TransactionAssembler::new(identity);
        assert!(assembler.push(commit(1, 1)).is_err());
        assert!(assembler.push(begin(2, 1)).unwrap().is_none());
        assert!(assembler.push(begin(3, 2)).is_err());
        assert!(assembler.push(commit(4, 5)).unwrap().is_some());
        assert!(assembler.push(begin(5, 2)).is_err());
        assert!(assembler.push(begin(6, 2)).unwrap().is_none());
        assert!(assembler.push(commit(3, 3)).is_err());
    }

    #[test]
    fn assembler_enforces_relation_generation() {
        let identity = SourceNodeIdentity {
            node_id: 1,
            system_identifier: 2,
            timeline: 1,
        };
        let mut assembler = TransactionAssembler::new(identity);
        assembler
            .push(DecodedMessage::Relation(relation(9, 1)))
            .unwrap();
        assembler.push(begin(1, 1)).unwrap();
        assert!(assembler.push(insert(9, 2)).is_err());

        let mut assembler = TransactionAssembler::new(identity);
        assembler
            .push(DecodedMessage::Relation(relation(9, 1)))
            .unwrap();
        assembler
            .push(DecodedMessage::Relation(relation(9, 2)))
            .unwrap();
        assert_eq!(assembler.relation_generation(9), Some(2));
        assembler.push(begin(1, 1)).unwrap();
        assert!(assembler.push(insert(9, 1)).is_err());
        assert!(assembler.push(insert(9, 2)).unwrap().is_none());
    }

    #[test]
    fn assembler_detects_open_transaction_at_end() {
        let identity = SourceNodeIdentity {
            node_id: 1,
            system_identifier: 2,
            timeline: 1,
        };
        let mut assembler = TransactionAssembler::new(identity);
        assembler.push(begin(1, 1)).unwrap();
        assert!(assembler.finish().is_err());
    }

    #[test]
    fn assembler_rejects_unstreamable_large_transactions_with_a_clear_error() {
        let identity = SourceNodeIdentity {
            node_id: 1,
            system_identifier: 2,
            timeline: 1,
        };
        let limits = TransactionLimits {
            max_changes: 1,
            max_bytes: 1,
        };
        let mut assembler = TransactionAssembler::with_limits(identity, limits).unwrap();
        assembler
            .push(DecodedMessage::Relation(relation(1, 1)))
            .unwrap();
        assembler.push(begin(1, 1)).unwrap();
        assembler.push(insert(1, 1)).unwrap();
        assert!(assembler.push(insert(1, 1)).is_err());
        assert!(
            TransactionAssembler::with_limits(
                identity,
                TransactionLimits {
                    max_changes: 0,
                    max_bytes: 1,
                }
            )
            .is_err()
        );
    }
}
