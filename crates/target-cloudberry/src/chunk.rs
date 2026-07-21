//! Durable target-side progress for bounded source-transaction chunks.
//!
//! Sequence ranges are half-open: a chunk covering records 10 through 19 has
//! `start_seq = 10` and `end_seq = 20`.  The progress row and user-table DML must be committed in
//! the same caller-owned [`Transaction`].  Call [`record_data_chunk`] before executing the DML;
//! any later statement failure then rolls both changes back, while a successful commit cannot
//! publish the DML without its durable chunk identity.

use std::str::FromStr as _;

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use thiserror::Error;
use tokio_postgres::{Row, Transaction};

use crate::checkpoint::{
    AdvanceOutcome, CheckpointError, CheckpointKey, NodeCheckpoint, PipelineFence,
    advance_node_checkpoint, checkpoint_reached, load_node_checkpoint_locked, lock_pipeline_fence,
};

pub const LOCK_TRANSACTION_PROGRESS_SQL: &str = r#"
SELECT pipeline_id, topology_generation, node_id, end_lsn::text AS end_lsn,
       system_identifier::text AS system_identifier, timeline, slot_name, xid,
       manifest_version, record_count, manifest_digest, next_seq, fencing_token
FROM pg2cb_meta.transaction_chunk_progress
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
  AND end_lsn = $4::text::pg_lsn
FOR UPDATE
"#;

pub const INSERT_TRANSACTION_PROGRESS_SQL: &str = r#"
INSERT INTO pg2cb_meta.transaction_chunk_progress (
    pipeline_id, topology_generation, node_id, end_lsn, system_identifier,
    timeline, slot_name, xid, manifest_version, record_count, manifest_digest,
    next_seq, fencing_token
)
VALUES ($1, $2, $3, $4::text::pg_lsn, $5::text::numeric,
        $6, $7, $8, $9, $10, $11, 0, $12)
"#;

pub const LOCK_COMMITTED_CHUNK_SQL: &str = r#"
SELECT end_seq, chunk_digest
FROM pg2cb_meta.transaction_committed_chunks
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
  AND end_lsn = $4::text::pg_lsn AND start_seq = $5
FOR UPDATE
"#;

pub const ADVANCE_TRANSACTION_PROGRESS_SQL: &str = r#"
UPDATE pg2cb_meta.transaction_chunk_progress
SET next_seq = $6, fencing_token = $7, updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
  AND end_lsn = $4::text::pg_lsn AND next_seq = $5
  AND manifest_digest = $8
"#;

pub const INSERT_COMMITTED_CHUNK_SQL: &str = r#"
INSERT INTO pg2cb_meta.transaction_committed_chunks (
    pipeline_id, topology_generation, node_id, end_lsn,
    start_seq, end_seq, chunk_digest, fencing_token
)
VALUES ($1, $2, $3, $4::text::pg_lsn, $5, $6, $7, $8)
"#;

pub const DELETE_TRANSACTION_COMMITTED_CHUNKS_SQL: &str = r#"
DELETE FROM pg2cb_meta.transaction_committed_chunks
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
  AND end_lsn = $4::text::pg_lsn
"#;

pub const DELETE_TRANSACTION_PROGRESS_SQL: &str = r#"
DELETE FROM pg2cb_meta.transaction_chunk_progress
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
  AND end_lsn = $4::text::pg_lsn
  AND system_identifier = $5::text::numeric AND timeline = $6 AND slot_name = $7
  AND xid = $8 AND manifest_version = $9 AND record_count = $10
  AND manifest_digest = $11 AND next_seq = $10 AND fencing_token = $12
"#;

const UPDATE_PROGRESS_FENCE_SQL: &str = r#"
UPDATE pg2cb_meta.transaction_chunk_progress
SET fencing_token = $5, updated_at = clock_timestamp()
WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3
  AND end_lsn = $4::text::pg_lsn
"#;

/// Durable key for one committed source transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionChunkKey {
    pub pipeline_id: PipelineId,
    pub topology_generation: u64,
    pub node_id: i32,
    pub end_lsn: PgLsn,
}

/// Immutable identity of the spool manifest being applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionChunkManifest {
    pub key: TransactionChunkKey,
    pub system_identifier: u64,
    pub timeline: u32,
    pub slot_name: String,
    pub xid: u32,
    pub manifest_version: u16,
    pub record_count: u64,
    pub manifest_digest: [u8; 32],
}

impl TransactionChunkManifest {
    /// Derives the only checkpoint that this immutable manifest may publish.
    ///
    /// Keeping this conversion beside the manifest prevents a caller from pairing a chunk
    /// receipt with a checkpoint for a different source transaction. Validation still happens at
    /// the completion boundary before any row is published.
    #[must_use]
    pub fn node_checkpoint(&self) -> NodeCheckpoint {
        NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: self.key.pipeline_id,
                topology_generation: self.key.topology_generation,
                node_id: self.key.node_id,
            },
            system_identifier: self.system_identifier,
            timeline: self.timeline,
            slot_name: self.slot_name.clone(),
            applied_lsn: self.key.end_lsn,
        }
    }
}

/// Identity of one half-open record range in an immutable transaction manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataChunkIdentity {
    pub start_seq: u64,
    pub end_seq: u64,
    pub digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressRegistration {
    Registered,
    Existing {
        next_seq: u64,
    },
    /// The target-authoritative checkpoint already covers this manifest.
    AlreadyCheckpointed {
        applied_lsn: PgLsn,
    },
}

/// Result of locking the ledger before user-table DML starts.
#[derive(Debug)]
pub enum PrepareDataChunkOutcome {
    Apply(PreparedDataChunk),
    AlreadyCommitted {
        next_seq: u64,
    },
    ResumeAt {
        next_seq: u64,
    },
    /// The ledger was retired with a checkpoint at or beyond this manifest.
    AlreadyCheckpointed {
        applied_lsn: PgLsn,
    },
}

/// Capability proving that a chunk is exactly the next range in a locked progress row.
///
/// It has no public constructor and is consumed by [`record_data_chunk`].
#[derive(Debug)]
pub struct PreparedDataChunk {
    fence: PipelineFence,
    manifest_digest: [u8; 32],
    key: TransactionChunkKey,
    chunk: DataChunkIdentity,
}

impl PreparedDataChunk {
    #[must_use]
    pub const fn identity(&self) -> DataChunkIdentity {
        self.chunk
    }

    #[must_use]
    pub const fn next_seq(&self) -> u64 {
        self.chunk.end_seq
    }
}

/// Capability proving that the locked manifest has no unapplied record.
///
/// It has no public constructor and must be consumed by
/// [`complete_transaction_checkpoint`], which also verifies that the checkpoint exactly identifies
/// this source transaction.
#[derive(Debug)]
pub struct PreparedTransactionCompletion {
    fence: PipelineFence,
    manifest: TransactionChunkManifest,
}

impl PreparedTransactionCompletion {
    #[must_use]
    pub const fn end_lsn(&self) -> PgLsn {
        self.manifest.key.end_lsn
    }
}

#[derive(Debug, Error)]
pub enum ChunkLedgerError {
    #[error("target chunk ledger database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error("the transaction manifest key does not match the active pipeline fence")]
    FenceKeyMismatch,
    #[error("topology generation {0} exceeds the target bigint range")]
    GenerationOutOfRange(u64),
    #[error("transaction end LSN must be non-zero")]
    InvalidEndLsn,
    #[error("source timeline must be non-zero")]
    InvalidTimeline,
    #[error("replication slot name cannot be empty or contain NUL")]
    InvalidSlotName,
    #[error("source transaction ID must be non-zero")]
    InvalidXid,
    #[error("transaction manifest version must be non-zero")]
    InvalidManifestVersion,
    #[error("{field} value {value} exceeds the target bigint range")]
    SequenceOutOfRange { field: &'static str, value: u64 },
    #[error("data chunk range must be non-empty")]
    EmptyChunk,
    #[error("data chunk end sequence {end_seq} exceeds manifest record count {record_count}")]
    ChunkPastManifest { end_seq: u64, record_count: u64 },
    #[error("persisted transaction manifest differs in {field}")]
    ManifestMismatch { field: &'static str },
    #[error("transaction chunk progress has not been registered")]
    MissingProgress,
    #[error("data chunk starts at {start_seq}, leaving a gap after sequence {next_seq}")]
    SequenceGap { next_seq: u64, start_seq: u64 },
    #[error("committed chunk identity at sequence {start_seq} does not match the replay")]
    ChunkIdentityMismatch { start_seq: u64 },
    #[error("persisted chunk progress changed after it was prepared")]
    ProgressChanged,
    #[error(
        "transaction is incomplete: next sequence is {next_seq}, record count is {record_count}"
    )]
    IncompleteTransaction { next_seq: u64, record_count: u64 },
    #[error("checkpoint source identity does not match the completed transaction manifest")]
    CheckpointIdentityMismatch,
    #[error("checkpoint LSN {checkpoint_lsn} does not equal transaction end LSN {end_lsn}")]
    CheckpointLsnMismatch {
        checkpoint_lsn: PgLsn,
        end_lsn: PgLsn,
    },
    #[error("persisted chunk ledger contains invalid {field}: {value}")]
    InvalidPersistedValue { field: &'static str, value: String },
    #[error("chunk ledger write affected {0} rows instead of one")]
    UnexpectedWriteCount(u64),
    #[error("chunk ledger retirement deleted {0} progress rows instead of one")]
    UnexpectedProgressRetirementCount(u64),
    #[error(
        "chunk ledger retirement deleted {deleted_chunks} receipts for a {record_count}-record manifest"
    )]
    UnexpectedCommittedChunkRetirementCount {
        record_count: u64,
        deleted_chunks: u64,
    },
}

/// Registers and locks an immutable transaction manifest in a caller-owned target transaction.
///
/// Re-registration is allowed only for the exact same manifest. A newer active fencing owner may
/// adopt an existing progress row, but it cannot change any source or spool identity field.
pub async fn register_transaction_progress(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<ProgressRegistration, ChunkLedgerError> {
    match lock_or_register_progress(transaction, fence, manifest).await? {
        LockOrRegisterProgressOutcome::Progress {
            progress,
            registered,
        } => Ok(if registered {
            ProgressRegistration::Registered
        } else {
            ProgressRegistration::Existing {
                next_seq: progress.next_seq,
            }
        }),
        LockOrRegisterProgressOutcome::AlreadyCheckpointed { applied_lsn } => {
            Ok(ProgressRegistration::AlreadyCheckpointed { applied_lsn })
        }
    }
}

/// Locks or registers progress and decides whether a chunk may execute.
///
/// `Apply` carries the only value accepted by [`record_data_chunk`]. An exact historical identity
/// returns `AlreadyCommitted`; a request starting inside or before a differently partitioned
/// committed prefix returns `ResumeAt` so the caller can discard it and read from the durable
/// boundary. A changed digest at an already recorded start sequence is corruption and fails closed.
pub async fn prepare_data_chunk(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
    chunk: DataChunkIdentity,
) -> Result<PrepareDataChunkOutcome, ChunkLedgerError> {
    validate_manifest(fence, manifest)?;
    validate_chunk(manifest.record_count, chunk)?;
    let progress = match lock_or_register_progress(transaction, fence, manifest).await? {
        LockOrRegisterProgressOutcome::Progress { progress, .. } => progress,
        LockOrRegisterProgressOutcome::AlreadyCheckpointed { applied_lsn } => {
            return Ok(PrepareDataChunkOutcome::AlreadyCheckpointed { applied_lsn });
        }
    };

    let committed = if chunk.start_seq < progress.next_seq {
        load_committed_chunk(transaction, manifest.key, chunk.start_seq).await?
    } else {
        None
    };
    match classify_chunk(progress.next_seq, committed.as_ref(), chunk)? {
        ChunkClassification::Apply => Ok(PrepareDataChunkOutcome::Apply(PreparedDataChunk {
            fence,
            manifest_digest: manifest.manifest_digest,
            key: manifest.key,
            chunk,
        })),
        ChunkClassification::AlreadyCommitted => Ok(PrepareDataChunkOutcome::AlreadyCommitted {
            next_seq: progress.next_seq,
        }),
        ChunkClassification::ResumeAt => Ok(PrepareDataChunkOutcome::ResumeAt {
            next_seq: progress.next_seq,
        }),
    }
}

/// Durably records a prepared chunk inside the transaction that will execute its user-table DML.
///
/// Call this before DML, then commit only after the DML succeeds. A later SQL error aborts the
/// whole PostgreSQL transaction and rolls the ledger update back with the data changes.
pub async fn record_data_chunk(
    transaction: &Transaction<'_>,
    prepared: PreparedDataChunk,
) -> Result<(), ChunkLedgerError> {
    lock_pipeline_fence(transaction, prepared.fence).await?;
    let database = DatabaseKey::new(prepared.key)?;
    let start_seq = database_sequence("start_seq", prepared.chunk.start_seq)?;
    let end_seq = database_sequence("end_seq", prepared.chunk.end_seq)?;

    let advanced = transaction
        .execute(
            ADVANCE_TRANSACTION_PROGRESS_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &prepared.key.node_id,
                &database.end_lsn,
                &start_seq,
                &end_seq,
                &prepared.fence.fencing_token,
                &&prepared.manifest_digest[..],
            ],
        )
        .await?;
    if advanced != 1 {
        return Err(ChunkLedgerError::ProgressChanged);
    }

    let inserted = transaction
        .execute(
            INSERT_COMMITTED_CHUNK_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &prepared.key.node_id,
                &database.end_lsn,
                &start_seq,
                &end_seq,
                &&prepared.chunk.digest[..],
                &prepared.fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_row(inserted)
}

/// Locks an existing progress row and returns proof that every manifest record committed.
///
/// This function never creates progress. Empty transactions must first be explicitly registered
/// with [`register_transaction_progress`], preventing an empty apply request from inventing a
/// completion and advancing a checkpoint on its own.
pub async fn prepare_transaction_completion(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<PreparedTransactionCompletion, ChunkLedgerError> {
    validate_manifest(fence, manifest)?;
    lock_pipeline_fence(transaction, fence).await?;
    let Some(mut progress) = load_progress_locked(transaction, manifest.key).await? else {
        return Err(ChunkLedgerError::MissingProgress);
    };
    ensure_same_manifest(&progress.manifest, manifest)?;
    adopt_fence(transaction, fence, &mut progress).await?;
    ensure_complete(&progress)?;
    Ok(PreparedTransactionCompletion {
        fence,
        manifest: manifest.clone(),
    })
}

/// Advances a checkpoint and retires its chunk ledger in the same caller-owned transaction.
///
/// The target checkpoint is the recovery authority: replication always restarts from that
/// checkpoint, and manifest/chunk entry points check it before creating a new ledger. Therefore a
/// commit response lost after this operation is safe even though its receipts have been deleted.
/// The checkpoint update, receipt deletion, and progress deletion become durable only when the
/// caller commits this transaction.
pub async fn complete_transaction_checkpoint(
    transaction: &Transaction<'_>,
    completion: PreparedTransactionCompletion,
    checkpoint: &NodeCheckpoint,
) -> Result<AdvanceOutcome, ChunkLedgerError> {
    ensure_checkpoint_matches(&completion.manifest, checkpoint)?;
    lock_pipeline_fence(transaction, completion.fence).await?;
    let Some(progress) = load_progress_locked(transaction, completion.manifest.key).await? else {
        return Err(ChunkLedgerError::MissingProgress);
    };
    ensure_same_manifest(&progress.manifest, &completion.manifest)?;
    ensure_complete(&progress)?;
    let outcome = advance_node_checkpoint(transaction, completion.fence, checkpoint)
        .await
        .map_err(ChunkLedgerError::from)?;
    retire_transaction_ledger(transaction, completion.fence, &completion.manifest).await?;
    Ok(outcome)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredProgress {
    manifest: TransactionChunkManifest,
    next_seq: u64,
    fencing_token: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StoredCommittedChunk {
    end_seq: u64,
    digest: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkClassification {
    Apply,
    AlreadyCommitted,
    ResumeAt,
}

enum LockOrRegisterProgressOutcome {
    Progress {
        progress: StoredProgress,
        registered: bool,
    },
    AlreadyCheckpointed {
        applied_lsn: PgLsn,
    },
}

struct DatabaseKey {
    pipeline_id: uuid::Uuid,
    generation: i64,
    end_lsn: String,
}

impl DatabaseKey {
    fn new(key: TransactionChunkKey) -> Result<Self, ChunkLedgerError> {
        Ok(Self {
            pipeline_id: key.pipeline_id.as_uuid(),
            generation: database_generation(key.topology_generation)?,
            end_lsn: key.end_lsn.to_string(),
        })
    }
}

async fn lock_or_register_progress(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<LockOrRegisterProgressOutcome, ChunkLedgerError> {
    validate_manifest(fence, manifest)?;
    lock_pipeline_fence(transaction, fence).await?;
    if let Some(mut progress) = load_progress_locked(transaction, manifest.key).await? {
        ensure_same_manifest(&progress.manifest, manifest)?;
        adopt_fence(transaction, fence, &mut progress).await?;
        return Ok(LockOrRegisterProgressOutcome::Progress {
            progress,
            registered: false,
        });
    }
    if let Some(applied_lsn) = manifest_checkpoint_reached_locked(transaction, manifest).await? {
        return Ok(LockOrRegisterProgressOutcome::AlreadyCheckpointed { applied_lsn });
    }

    let database = DatabaseKey::new(manifest.key)?;
    let system_identifier = manifest.system_identifier.to_string();
    let timeline = i64::from(manifest.timeline);
    let xid = i64::from(manifest.xid);
    let manifest_version = i32::from(manifest.manifest_version);
    let record_count = database_sequence("record_count", manifest.record_count)?;
    let inserted = transaction
        .execute(
            INSERT_TRANSACTION_PROGRESS_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &manifest.key.node_id,
                &database.end_lsn,
                &system_identifier,
                &timeline,
                &manifest.slot_name,
                &xid,
                &manifest_version,
                &record_count,
                &&manifest.manifest_digest[..],
                &fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_row(inserted)?;
    Ok(LockOrRegisterProgressOutcome::Progress {
        progress: StoredProgress {
            manifest: manifest.clone(),
            next_seq: 0,
            fencing_token: fence.fencing_token,
        },
        registered: true,
    })
}

async fn load_progress_locked(
    transaction: &Transaction<'_>,
    key: TransactionChunkKey,
) -> Result<Option<StoredProgress>, ChunkLedgerError> {
    let database = DatabaseKey::new(key)?;
    transaction
        .query_opt(
            LOCK_TRANSACTION_PROGRESS_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &key.node_id,
                &database.end_lsn,
            ],
        )
        .await?
        .map(progress_from_row)
        .transpose()
}

async fn load_committed_chunk(
    transaction: &Transaction<'_>,
    key: TransactionChunkKey,
    start_seq: u64,
) -> Result<Option<StoredCommittedChunk>, ChunkLedgerError> {
    let database = DatabaseKey::new(key)?;
    let start_seq = database_sequence("start_seq", start_seq)?;
    transaction
        .query_opt(
            LOCK_COMMITTED_CHUNK_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &key.node_id,
                &database.end_lsn,
                &start_seq,
            ],
        )
        .await?
        .map(committed_chunk_from_row)
        .transpose()
}

async fn manifest_checkpoint_reached_locked(
    transaction: &Transaction<'_>,
    manifest: &TransactionChunkManifest,
) -> Result<Option<PgLsn>, ChunkLedgerError> {
    let checkpoint = manifest.node_checkpoint();
    let current = load_node_checkpoint_locked(transaction, checkpoint.key).await?;
    if checkpoint_reached(current.as_ref(), &checkpoint)? {
        Ok(current.map(|stored| stored.checkpoint.applied_lsn))
    } else {
        Ok(None)
    }
}

async fn retire_transaction_ledger(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<(), ChunkLedgerError> {
    let database = DatabaseKey::new(manifest.key)?;
    let timeline = i64::from(manifest.timeline);
    let xid = i64::from(manifest.xid);
    let manifest_version = i32::from(manifest.manifest_version);
    let record_count = database_sequence("record_count", manifest.record_count)?;
    let system_identifier = manifest.system_identifier.to_string();

    // Committed chunk receipts may legitimately be empty for a zero-record transaction.
    let deleted_chunks = transaction
        .execute(
            DELETE_TRANSACTION_COMMITTED_CHUNKS_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &manifest.key.node_id,
                &database.end_lsn,
            ],
        )
        .await?;
    let deleted_progress = transaction
        .execute(
            DELETE_TRANSACTION_PROGRESS_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &manifest.key.node_id,
                &database.end_lsn,
                &system_identifier,
                &timeline,
                &manifest.slot_name,
                &xid,
                &manifest_version,
                &record_count,
                &&manifest.manifest_digest[..],
                &fence.fencing_token,
            ],
        )
        .await?;
    ensure_retirement_counts(manifest.record_count, deleted_chunks, deleted_progress)
}

async fn adopt_fence(
    transaction: &Transaction<'_>,
    fence: PipelineFence,
    progress: &mut StoredProgress,
) -> Result<(), ChunkLedgerError> {
    if progress.fencing_token == fence.fencing_token {
        return Ok(());
    }
    let database = DatabaseKey::new(progress.manifest.key)?;
    let updated = transaction
        .execute(
            UPDATE_PROGRESS_FENCE_SQL,
            &[
                &database.pipeline_id,
                &database.generation,
                &progress.manifest.key.node_id,
                &database.end_lsn,
                &fence.fencing_token,
            ],
        )
        .await?;
    ensure_one_row(updated)?;
    progress.fencing_token = fence.fencing_token;
    Ok(())
}

fn validate_manifest(
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<(), ChunkLedgerError> {
    database_generation(manifest.key.topology_generation)?;
    database_sequence("record_count", manifest.record_count)?;
    if manifest.key.pipeline_id != fence.pipeline_id
        || manifest.key.topology_generation != fence.topology_generation
    {
        return Err(ChunkLedgerError::FenceKeyMismatch);
    }
    if manifest.key.end_lsn == PgLsn::ZERO {
        return Err(ChunkLedgerError::InvalidEndLsn);
    }
    if manifest.timeline == 0 {
        return Err(ChunkLedgerError::InvalidTimeline);
    }
    if manifest.slot_name.is_empty() || manifest.slot_name.contains('\0') {
        return Err(ChunkLedgerError::InvalidSlotName);
    }
    if manifest.xid == 0 {
        return Err(ChunkLedgerError::InvalidXid);
    }
    if manifest.manifest_version == 0 {
        return Err(ChunkLedgerError::InvalidManifestVersion);
    }
    Ok(())
}

fn validate_chunk(record_count: u64, chunk: DataChunkIdentity) -> Result<(), ChunkLedgerError> {
    database_sequence("start_seq", chunk.start_seq)?;
    database_sequence("end_seq", chunk.end_seq)?;
    if chunk.start_seq >= chunk.end_seq {
        return Err(ChunkLedgerError::EmptyChunk);
    }
    if chunk.end_seq > record_count {
        return Err(ChunkLedgerError::ChunkPastManifest {
            end_seq: chunk.end_seq,
            record_count,
        });
    }
    Ok(())
}

fn ensure_same_manifest(
    current: &TransactionChunkManifest,
    proposed: &TransactionChunkManifest,
) -> Result<(), ChunkLedgerError> {
    let mismatch = if current.key != proposed.key {
        Some("key")
    } else if current.system_identifier != proposed.system_identifier {
        Some("system_identifier")
    } else if current.timeline != proposed.timeline {
        Some("timeline")
    } else if current.slot_name != proposed.slot_name {
        Some("slot_name")
    } else if current.xid != proposed.xid {
        Some("xid")
    } else if current.manifest_version != proposed.manifest_version {
        Some("manifest_version")
    } else if current.record_count != proposed.record_count {
        Some("record_count")
    } else if current.manifest_digest != proposed.manifest_digest {
        Some("manifest_digest")
    } else {
        None
    };
    match mismatch {
        Some(field) => Err(ChunkLedgerError::ManifestMismatch { field }),
        None => Ok(()),
    }
}

fn classify_chunk(
    next_seq: u64,
    committed: Option<&StoredCommittedChunk>,
    proposed: DataChunkIdentity,
) -> Result<ChunkClassification, ChunkLedgerError> {
    match proposed.start_seq.cmp(&next_seq) {
        std::cmp::Ordering::Greater => Err(ChunkLedgerError::SequenceGap {
            next_seq,
            start_seq: proposed.start_seq,
        }),
        std::cmp::Ordering::Equal => Ok(ChunkClassification::Apply),
        std::cmp::Ordering::Less => {
            let Some(committed) = committed else {
                return Ok(ChunkClassification::ResumeAt);
            };
            if committed.end_seq > next_seq {
                return Err(ChunkLedgerError::InvalidPersistedValue {
                    field: "committed_chunk.end_seq",
                    value: committed.end_seq.to_string(),
                });
            }
            if committed.end_seq != proposed.end_seq {
                // Chunk sizing is an execution detail, not part of the immutable transaction
                // manifest. A restarted worker may use different limits; resume at the durable
                // prefix and rebuild the next chunk from there.
                return Ok(ChunkClassification::ResumeAt);
            }
            if committed.digest == proposed.digest {
                Ok(ChunkClassification::AlreadyCommitted)
            } else {
                Err(ChunkLedgerError::ChunkIdentityMismatch {
                    start_seq: proposed.start_seq,
                })
            }
        }
    }
}

fn ensure_complete(progress: &StoredProgress) -> Result<(), ChunkLedgerError> {
    if progress.next_seq == progress.manifest.record_count {
        Ok(())
    } else {
        Err(ChunkLedgerError::IncompleteTransaction {
            next_seq: progress.next_seq,
            record_count: progress.manifest.record_count,
        })
    }
}

fn ensure_checkpoint_matches(
    manifest: &TransactionChunkManifest,
    checkpoint: &NodeCheckpoint,
) -> Result<(), ChunkLedgerError> {
    if checkpoint.key.pipeline_id != manifest.key.pipeline_id
        || checkpoint.key.topology_generation != manifest.key.topology_generation
        || checkpoint.key.node_id != manifest.key.node_id
        || checkpoint.system_identifier != manifest.system_identifier
        || checkpoint.timeline != manifest.timeline
        || checkpoint.slot_name != manifest.slot_name
    {
        return Err(ChunkLedgerError::CheckpointIdentityMismatch);
    }
    if checkpoint.applied_lsn != manifest.key.end_lsn {
        return Err(ChunkLedgerError::CheckpointLsnMismatch {
            checkpoint_lsn: checkpoint.applied_lsn,
            end_lsn: manifest.key.end_lsn,
        });
    }
    Ok(())
}

fn progress_from_row(row: Row) -> Result<StoredProgress, ChunkLedgerError> {
    let key = key_from_row(&row)?;
    let system_identifier = parse_persisted::<u64>(&row, "system_identifier")?;
    let timeline = persisted_u32(&row, "timeline")?;
    let xid = persisted_u32(&row, "xid")?;
    let manifest_version = persisted_u16(&row, "manifest_version")?;
    let record_count = persisted_u64(&row, "record_count")?;
    let next_seq = persisted_u64(&row, "next_seq")?;
    if next_seq > record_count {
        return Err(ChunkLedgerError::InvalidPersistedValue {
            field: "next_seq",
            value: next_seq.to_string(),
        });
    }
    Ok(StoredProgress {
        manifest: TransactionChunkManifest {
            key,
            system_identifier,
            timeline,
            slot_name: row.try_get("slot_name")?,
            xid,
            manifest_version,
            record_count,
            manifest_digest: persisted_digest(&row, "manifest_digest")?,
        },
        next_seq,
        fencing_token: row.try_get("fencing_token")?,
    })
}

fn committed_chunk_from_row(row: Row) -> Result<StoredCommittedChunk, ChunkLedgerError> {
    Ok(StoredCommittedChunk {
        end_seq: persisted_u64(&row, "end_seq")?,
        digest: persisted_digest(&row, "chunk_digest")?,
    })
}

fn key_from_row(row: &Row) -> Result<TransactionChunkKey, ChunkLedgerError> {
    let generation = persisted_u64(row, "topology_generation")?;
    let lsn_text: String = row.try_get("end_lsn")?;
    let end_lsn =
        PgLsn::from_str(&lsn_text).map_err(|_| ChunkLedgerError::InvalidPersistedValue {
            field: "end_lsn",
            value: lsn_text,
        })?;
    Ok(TransactionChunkKey {
        pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
        topology_generation: generation,
        node_id: row.try_get("node_id")?,
        end_lsn,
    })
}

fn persisted_digest(row: &Row, field: &'static str) -> Result<[u8; 32], ChunkLedgerError> {
    let value: Vec<u8> = row.try_get(field)?;
    value
        .try_into()
        .map_err(|value: Vec<u8>| ChunkLedgerError::InvalidPersistedValue {
            field,
            value: format!("{} bytes", value.len()),
        })
}

fn parse_persisted<T>(row: &Row, field: &'static str) -> Result<T, ChunkLedgerError>
where
    T: std::str::FromStr,
{
    let value: String = row.try_get(field)?;
    value
        .parse()
        .map_err(|_| ChunkLedgerError::InvalidPersistedValue { field, value })
}

fn persisted_u64(row: &Row, field: &'static str) -> Result<u64, ChunkLedgerError> {
    let value: i64 = row.try_get(field)?;
    u64::try_from(value).map_err(|_| ChunkLedgerError::InvalidPersistedValue {
        field,
        value: value.to_string(),
    })
}

fn persisted_u32(row: &Row, field: &'static str) -> Result<u32, ChunkLedgerError> {
    let value: i64 = row.try_get(field)?;
    u32::try_from(value).map_err(|_| ChunkLedgerError::InvalidPersistedValue {
        field,
        value: value.to_string(),
    })
}

fn persisted_u16(row: &Row, field: &'static str) -> Result<u16, ChunkLedgerError> {
    let value: i32 = row.try_get(field)?;
    u16::try_from(value).map_err(|_| ChunkLedgerError::InvalidPersistedValue {
        field,
        value: value.to_string(),
    })
}

fn database_generation(generation: u64) -> Result<i64, ChunkLedgerError> {
    i64::try_from(generation).map_err(|_| ChunkLedgerError::GenerationOutOfRange(generation))
}

fn database_sequence(field: &'static str, value: u64) -> Result<i64, ChunkLedgerError> {
    i64::try_from(value).map_err(|_| ChunkLedgerError::SequenceOutOfRange { field, value })
}

fn ensure_one_row(written: u64) -> Result<(), ChunkLedgerError> {
    if written == 1 {
        Ok(())
    } else {
        Err(ChunkLedgerError::UnexpectedWriteCount(written))
    }
}

fn ensure_retirement_counts(
    record_count: u64,
    deleted_chunks: u64,
    deleted_progress: u64,
) -> Result<(), ChunkLedgerError> {
    if (record_count == 0 && deleted_chunks != 0) || (record_count > 0 && deleted_chunks == 0) {
        return Err(ChunkLedgerError::UnexpectedCommittedChunkRetirementCount {
            record_count,
            deleted_chunks,
        });
    }
    if deleted_progress != 1 {
        return Err(ChunkLedgerError::UnexpectedProgressRetirementCount(
            deleted_progress,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cloudberry_etl_core::id::PipelineId;

    fn fence() -> PipelineFence {
        PipelineFence {
            pipeline_id: PipelineId::new(),
            topology_generation: 7,
            fencing_token: 11,
        }
    }

    fn manifest(fence: PipelineFence) -> TransactionChunkManifest {
        TransactionChunkManifest {
            key: TransactionChunkKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: 2,
                end_lsn: PgLsn::new(0x1234),
            },
            system_identifier: u64::MAX,
            timeline: 3,
            slot_name: "pg2cb_slot_2".to_owned(),
            xid: u32::MAX,
            manifest_version: 1,
            record_count: 30,
            manifest_digest: [0xA5; 32],
        }
    }

    fn chunk(start_seq: u64, end_seq: u64, byte: u8) -> DataChunkIdentity {
        DataChunkIdentity {
            start_seq,
            end_seq,
            digest: [byte; 32],
        }
    }

    #[test]
    fn validates_manifest_identity_and_target_ranges() {
        let fence = fence();
        let mut value = manifest(fence);
        assert!(validate_manifest(fence, &value).is_ok());

        value.key.pipeline_id = PipelineId::new();
        assert!(matches!(
            validate_manifest(fence, &value),
            Err(ChunkLedgerError::FenceKeyMismatch)
        ));
        value = manifest(fence);
        value.manifest_version = 0;
        assert!(matches!(
            validate_manifest(fence, &value),
            Err(ChunkLedgerError::InvalidManifestVersion)
        ));
        value = manifest(fence);
        value.record_count = u64::MAX;
        assert!(matches!(
            validate_manifest(fence, &value),
            Err(ChunkLedgerError::SequenceOutOfRange {
                field: "record_count",
                ..
            })
        ));
    }

    #[test]
    fn validates_non_empty_bounded_chunk_ranges() {
        assert!(validate_chunk(20, chunk(0, 20, 1)).is_ok());
        assert!(matches!(
            validate_chunk(20, chunk(5, 5, 1)),
            Err(ChunkLedgerError::EmptyChunk)
        ));
        assert!(matches!(
            validate_chunk(20, chunk(10, 21, 1)),
            Err(ChunkLedgerError::ChunkPastManifest {
                end_seq: 21,
                record_count: 20
            })
        ));
    }

    #[test]
    fn exact_next_chunk_applies_and_gaps_fail_closed() {
        assert_eq!(
            classify_chunk(10, None, chunk(10, 20, 2)).unwrap(),
            ChunkClassification::Apply
        );
        assert!(matches!(
            classify_chunk(10, None, chunk(11, 20, 2)),
            Err(ChunkLedgerError::SequenceGap {
                next_seq: 10,
                start_seq: 11
            })
        ));
    }

    #[test]
    fn historical_digest_distinguishes_replay_corruption_from_repartition() {
        let committed = StoredCommittedChunk {
            end_seq: 10,
            digest: [3; 32],
        };
        assert_eq!(
            classify_chunk(20, Some(&committed), chunk(0, 10, 3)).unwrap(),
            ChunkClassification::AlreadyCommitted
        );
        assert!(matches!(
            classify_chunk(20, Some(&committed), chunk(0, 10, 4)),
            Err(ChunkLedgerError::ChunkIdentityMismatch { start_seq: 0 })
        ));
        assert_eq!(
            classify_chunk(20, Some(&committed), chunk(0, 15, 4)).unwrap(),
            ChunkClassification::ResumeAt
        );
        assert_eq!(
            classify_chunk(20, None, chunk(5, 15, 9)).unwrap(),
            ChunkClassification::ResumeAt
        );
    }

    #[test]
    fn manifest_replay_requires_every_immutable_field() {
        let fence = fence();
        let current = manifest(fence);
        assert!(ensure_same_manifest(&current, &current).is_ok());

        let mut changed = current.clone();
        changed.manifest_digest[0] ^= 0xFF;
        assert!(matches!(
            ensure_same_manifest(&current, &changed),
            Err(ChunkLedgerError::ManifestMismatch {
                field: "manifest_digest"
            })
        ));
        changed = current.clone();
        changed.system_identifier -= 1;
        assert!(matches!(
            ensure_same_manifest(&current, &changed),
            Err(ChunkLedgerError::ManifestMismatch {
                field: "system_identifier"
            })
        ));
    }

    #[test]
    fn completion_requires_full_prefix_and_exact_checkpoint_identity() {
        let fence = fence();
        let manifest = manifest(fence);
        let mut progress = StoredProgress {
            manifest: manifest.clone(),
            next_seq: 29,
            fencing_token: fence.fencing_token,
        };
        assert!(matches!(
            ensure_complete(&progress),
            Err(ChunkLedgerError::IncompleteTransaction {
                next_seq: 29,
                record_count: 30
            })
        ));
        progress.next_seq = 30;
        assert!(ensure_complete(&progress).is_ok());

        let mut checkpoint = manifest.node_checkpoint();
        assert_eq!(checkpoint.key.pipeline_id, fence.pipeline_id);
        assert_eq!(checkpoint.key.node_id, manifest.key.node_id);
        assert_eq!(checkpoint.system_identifier, manifest.system_identifier);
        assert_eq!(checkpoint.timeline, manifest.timeline);
        assert_eq!(checkpoint.slot_name, manifest.slot_name);
        assert!(ensure_checkpoint_matches(&manifest, &checkpoint).is_ok());
        checkpoint.applied_lsn = PgLsn::new(manifest.key.end_lsn.as_u64() + 1);
        assert!(matches!(
            ensure_checkpoint_matches(&manifest, &checkpoint),
            Err(ChunkLedgerError::CheckpointLsnMismatch { .. })
        ));
    }

    #[test]
    fn retirement_sql_filters_the_complete_transaction_identity() {
        for sql in [
            DELETE_TRANSACTION_COMMITTED_CHUNKS_SQL,
            DELETE_TRANSACTION_PROGRESS_SQL,
        ] {
            assert!(sql.contains("pipeline_id = $1"));
            assert!(sql.contains("topology_generation = $2"));
            assert!(sql.contains("node_id = $3"));
            assert!(sql.contains("end_lsn = $4::text::pg_lsn"));
        }
        assert!(DELETE_TRANSACTION_PROGRESS_SQL.contains("system_identifier = $5::text::numeric"));
        assert!(DELETE_TRANSACTION_PROGRESS_SQL.contains("manifest_digest = $11"));
        assert!(DELETE_TRANSACTION_PROGRESS_SQL.contains("next_seq = $10"));
        assert!(DELETE_TRANSACTION_PROGRESS_SQL.contains("fencing_token = $12"));
    }

    #[test]
    fn retirement_allows_empty_or_non_empty_receipts_but_requires_one_progress_row() {
        assert!(ensure_retirement_counts(0, 0, 1).is_ok());
        assert!(ensure_retirement_counts(30, 1, 1).is_ok());
        assert!(ensure_retirement_counts(30, 7, 1).is_ok());
        assert!(matches!(
            ensure_retirement_counts(0, 1, 1),
            Err(ChunkLedgerError::UnexpectedCommittedChunkRetirementCount {
                record_count: 0,
                deleted_chunks: 1
            })
        ));
        assert!(matches!(
            ensure_retirement_counts(30, 0, 1),
            Err(ChunkLedgerError::UnexpectedCommittedChunkRetirementCount {
                record_count: 30,
                deleted_chunks: 0
            })
        ));
        assert!(matches!(
            ensure_retirement_counts(0, 0, 0),
            Err(ChunkLedgerError::UnexpectedProgressRetirementCount(0))
        ));
        assert!(matches!(
            ensure_retirement_counts(30, 7, 2),
            Err(ChunkLedgerError::UnexpectedProgressRetirementCount(2))
        ));
    }
}
