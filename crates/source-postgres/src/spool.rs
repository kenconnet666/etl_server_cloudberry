//! Bounded transaction storage for logical replication.
//!
//! The journal is deliberately small and synchronous at its API boundary.  WAL decoding already
//! has to preserve message order; a buffered writer keeps the hot path sequential while the
//! manifest and segment renames make a committed transaction discoverable after a process restart.
//! The payload is bincode (binary), never JSON.  JSON remains suitable for control-plane metadata,
//! but it is too expensive and too easy to partially publish on this data path.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bincode::config;
use cloudberry_etl_core::{
    change::{
        Cell, DdlMessage, RelationSchemaSnapshot, RowChange, TableChange, TableTransition,
        TransactionChange, TransitionKind, Tuple,
    },
    id::PipelineId,
    lsn::PgLsn,
};
use fs2::available_space;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use uuid::Uuid;

const MAGIC: [u8; 8] = *b"PG2CBSPL";
const FORMAT_VERSION: u16 = 2;
const RECORD_CHANGE: u16 = 1;
const HEADER_BYTES: usize = 8 + 2 + 2 + 8 + 4;
const MAX_RECORD_BYTES: u64 = 1 << 34;

// The core values use internally tagged serde enums for APIs.  Binary formats such as bincode do
// not support serde's `deserialize_any`, so the journal owns an explicit, versioned wire schema.
#[derive(Debug, Serialize, Deserialize)]
enum WireCell {
    Null,
    UnchangedToast,
    Text(Vec<u8>),
    Binary(Vec<u8>),
}

#[derive(Debug, Serialize, Deserialize)]
struct WireTuple {
    cells: Vec<WireCell>,
}

#[derive(Debug, Serialize, Deserialize)]
enum WireRowChange {
    Insert {
        new: WireTuple,
    },
    Update {
        old_key: Option<WireTuple>,
        new: WireTuple,
    },
    Delete {
        old_key: WireTuple,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct WireTableChange {
    relation_id: u32,
    generation: u64,
    change: WireRowChange,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireDdlMessage {
    version: u16,
    command_tag: String,
    relation_ids: Vec<u32>,
    affected_schemas: Vec<String>,
    schema_fingerprint: String,
    transitions: Vec<WireTableTransition>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WireTableTransition {
    relation_id: u32,
    before_generation: Option<u64>,
    after_generation: Option<u64>,
    before_fingerprint: Option<String>,
    after_fingerprint: Option<String>,
    after_schema: Option<RelationSchemaSnapshot>,
    kind: WireTransitionKind,
}

#[derive(Debug, Serialize, Deserialize)]
enum WireTransitionKind {
    AddColumn {
        name: String,
        nullable_or_defaulted: bool,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        from: String,
        to: String,
    },
    AlterColumnType {
        name: String,
        widening: bool,
    },
    AddTable,
    DropTable,
    Unknown,
}

#[derive(Debug, Serialize, Deserialize)]
enum WireTransactionChange {
    Row(WireTableChange),
    Truncate {
        relation_ids: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
    Ddl(WireDdlMessage),
}

#[derive(Debug, Error)]
pub enum SpoolError {
    #[error("spool I/O failed: {0}")]
    Io(#[from] io::Error),
    #[error("spool serialization failed: {0}")]
    Codec(String),
    #[error("spool format version {0} is not supported")]
    UnsupportedVersion(u16),
    #[error("spool record has invalid magic")]
    InvalidMagic,
    #[error("spool record kind {0} is not supported")]
    UnsupportedRecord(u16),
    #[error("spool record length {0} exceeds the safety limit")]
    RecordTooLarge(u64),
    #[error("spool record checksum mismatch")]
    ChecksumMismatch,
    #[error("spool manifest is inconsistent: {0}")]
    InvalidManifest(String),
    #[error(
        "spool resource watermark reached: used={used} high={high} free={free:?} minimum={minimum}"
    )]
    ResourceWait {
        used: u64,
        high: u64,
        free: Option<u64>,
        minimum: u64,
    },
}

pub type SpoolResult<T> = Result<T, SpoolError>;

/// Identity that scopes local files.  A file from another topology generation is never reused.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpoolIdentity {
    pub pipeline_id: PipelineId,
    pub topology_generation: u64,
    pub node_id: i32,
    pub system_identifier: u64,
    pub timeline: u32,
}

impl SpoolIdentity {
    fn directory_name(self) -> String {
        format!(
            "{}-{}-{}-{}-{}",
            self.node_id,
            self.system_identifier,
            self.timeline,
            self.pipeline_id,
            self.topology_generation
        )
    }
}

/// Resource thresholds are watermarks, not transaction-size rejection limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpoolLimits {
    pub memory_high_water_bytes: usize,
    pub segment_target_bytes: usize,
    pub disk_high_water_bytes: u64,
    pub minimum_free_disk_bytes: u64,
}

impl Default for SpoolLimits {
    fn default() -> Self {
        Self {
            memory_high_water_bytes: 64 * 1024 * 1024,
            segment_target_bytes: 64 * 1024 * 1024,
            disk_high_water_bytes: 256 * 1024 * 1024 * 1024,
            minimum_free_disk_bytes: 8 * 1024 * 1024 * 1024,
        }
    }
}

impl SpoolLimits {
    pub fn validate(self) -> SpoolResult<()> {
        if self.memory_high_water_bytes == 0
            || self.segment_target_bytes == 0
            || self.disk_high_water_bytes == 0
            || self.minimum_free_disk_bytes >= self.disk_high_water_bytes
        {
            return Err(SpoolError::InvalidManifest(
                "spool watermarks must be positive and minimum free space must be below disk high watermark"
                    .to_owned(),
            ));
        }
        if u64::try_from(self.segment_target_bytes).unwrap_or(u64::MAX) > self.disk_high_water_bytes
        {
            return Err(SpoolError::InvalidManifest(
                "segment target exceeds disk high watermark".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceState {
    Ready {
        used_bytes: u64,
    },
    Wait {
        used_bytes: u64,
        disk_high_water_bytes: u64,
        free_bytes: Option<u64>,
        minimum_free_disk_bytes: u64,
    },
}

impl ResourceState {
    #[must_use]
    pub const fn used_bytes(self) -> u64 {
        match self {
            Self::Ready { used_bytes } | Self::Wait { used_bytes, .. } => used_bytes,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionManifest {
    pub format_version: u16,
    pub transaction_id: Uuid,
    pub xid: u32,
    pub begin_lsn: PgLsn,
    pub commit_lsn: PgLsn,
    pub end_lsn: PgLsn,
    pub change_count: u64,
    pub encoded_bytes: u64,
    pub digest: [u8; 32],
    pub row_count: u64,
    pub has_generation_barrier: bool,
    pub segments: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SpoolHandle {
    manifest_path: PathBuf,
    manifest: TransactionManifest,
    usage: Arc<AtomicU64>,
}

impl PartialEq for SpoolHandle {
    fn eq(&self, other: &Self) -> bool {
        self.manifest_path == other.manifest_path && self.manifest == other.manifest
    }
}

impl Eq for SpoolHandle {}

impl SpoolHandle {
    #[must_use]
    pub fn manifest(&self) -> &TransactionManifest {
        &self.manifest
    }

    #[must_use]
    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    /// Validate and open a bounded reader over all committed segment frames.
    pub fn reader(&self) -> SpoolResult<ChangeReader> {
        if self.manifest.format_version != FORMAT_VERSION {
            return Err(SpoolError::UnsupportedVersion(self.manifest.format_version));
        }
        let parent = self
            .manifest_path
            .parent()
            .ok_or_else(|| SpoolError::InvalidManifest("manifest has no parent".to_owned()))?;
        let segments = self
            .manifest
            .segments
            .iter()
            .map(|name| parent.join(name))
            .collect();
        Ok(ChangeReader {
            source: ReaderSource::Spool {
                segments,
                segment_index: 0,
                reader: None,
                remaining: self.manifest.change_count,
            },
        })
    }

    /// Remove a transaction after its target checkpoint has durably advanced.
    ///
    /// The manifest is removed first so an interrupted cleanup can only leave unreferenced
    /// segments. Repeating cleanup is therefore safe, including after a process restart.
    pub fn remove(&self) -> SpoolResult<()> {
        let parent = self
            .manifest_path
            .parent()
            .ok_or_else(|| SpoolError::InvalidManifest("manifest has no parent".to_owned()))?;
        remove_tracked_file(&self.manifest_path, &self.usage)?;
        for segment in &self.manifest.segments {
            let path = parent.join(segment);
            remove_tracked_file(&path, &self.usage)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChangeSource {
    Memory(Arc<[TransactionChange]>),
    Spool(SpoolHandle),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChangeStats {
    pub change_count: u64,
    pub row_count: u64,
    pub encoded_bytes: u64,
    pub digest: [u8; 32],
    pub has_generation_barrier: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLimits {
    pub max_records: usize,
    pub max_bytes: usize,
}

impl ChunkLimits {
    pub fn validate(self) -> SpoolResult<()> {
        if self.max_records == 0 || self.max_bytes == 0 {
            return Err(SpoolError::InvalidManifest(
                "change chunk limits must be positive".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeChunk {
    pub start_seq: u64,
    pub end_seq: u64,
    pub encoded_bytes: u64,
    pub digest: [u8; 32],
    pub changes: Vec<TransactionChange>,
}

impl ChangeSource {
    #[must_use]
    pub fn memory(changes: Vec<TransactionChange>) -> Self {
        Self::Memory(Arc::from(changes.into_boxed_slice()))
    }

    #[must_use]
    pub fn change_count(&self) -> u64 {
        match self {
            Self::Memory(changes) => changes.len() as u64,
            Self::Spool(handle) => handle.manifest.change_count,
        }
    }

    pub fn reader(&self) -> SpoolResult<ChangeReader> {
        match self {
            Self::Memory(changes) => Ok(ChangeReader {
                source: ReaderSource::Memory {
                    changes: Arc::clone(changes),
                    index: 0,
                },
            }),
            Self::Spool(handle) => handle.reader(),
        }
    }

    pub fn stats(&self) -> SpoolResult<ChangeStats> {
        match self {
            Self::Memory(changes) => change_stats(changes),
            Self::Spool(handle) => Ok(ChangeStats {
                change_count: handle.manifest.change_count,
                row_count: handle.manifest.row_count,
                encoded_bytes: handle.manifest.encoded_bytes,
                digest: handle.manifest.digest,
                has_generation_barrier: handle.manifest.has_generation_barrier,
            }),
        }
    }

    pub fn chunks_from(
        &self,
        start_seq: u64,
        limits: ChunkLimits,
    ) -> SpoolResult<ChangeChunkReader> {
        limits.validate()?;
        let mut reader = self.reader()?;
        for _ in 0..start_seq {
            match reader.next() {
                Some(change) => {
                    change?;
                }
                None => {
                    return Err(SpoolError::InvalidManifest(format!(
                        "resume sequence {start_seq} exceeds change count"
                    )));
                }
            }
        }
        Ok(ChangeChunkReader {
            reader,
            limits,
            next_seq: start_seq,
            pending: None,
        })
    }

    /// Idempotently release durable local storage after the target checkpoint is committed.
    pub fn cleanup(&self) -> SpoolResult<()> {
        match self {
            Self::Memory(_) => Ok(()),
            Self::Spool(handle) => handle.remove(),
        }
    }
}

/// A streaming reader.  It owns at most one decoded change and one frame buffer at a time.
pub struct ChangeReader {
    source: ReaderSource,
}

enum ReaderSource {
    Memory {
        changes: Arc<[TransactionChange]>,
        index: usize,
    },
    Spool {
        segments: Vec<PathBuf>,
        segment_index: usize,
        reader: Option<BufReader<File>>,
        remaining: u64,
    },
}

impl std::fmt::Debug for ChangeReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ChangeReader")
    }
}

impl Iterator for ChangeReader {
    type Item = SpoolResult<TransactionChange>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.source {
            ReaderSource::Memory { changes, index } => {
                let change = changes.get(*index).cloned();
                *index = index.saturating_add(usize::from(change.is_some()));
                change.map(Ok)
            }
            ReaderSource::Spool {
                segments,
                segment_index,
                reader,
                remaining,
            } => {
                if *remaining == 0 {
                    return None;
                }
                loop {
                    if reader.is_none() {
                        let path = segments.get(*segment_index)?.clone();
                        match File::open(path).map(BufReader::new) {
                            Ok(opened) => *reader = Some(opened),
                            Err(error) => return Some(Err(error.into())),
                        }
                    }
                    let current = reader.as_mut().expect("reader initialized");
                    match read_frame(current) {
                        Ok(Some(change)) => {
                            *remaining = remaining.saturating_sub(1);
                            return Some(Ok(change));
                        }
                        Ok(None) => {
                            *reader = None;
                            *segment_index = segment_index.saturating_add(1);
                            continue;
                        }
                        Err(error) => return Some(Err(error)),
                    }
                }
            }
        }
    }
}

pub struct ChangeChunkReader {
    reader: ChangeReader,
    limits: ChunkLimits,
    next_seq: u64,
    pending: Option<(TransactionChange, Vec<u8>)>,
}

impl std::fmt::Debug for ChangeChunkReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ChangeChunkReader")
    }
}

impl Iterator for ChangeChunkReader {
    type Item = SpoolResult<ChangeChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        let start_seq = self.next_seq;
        let mut changes = Vec::new();
        let mut encoded_bytes = 0_u64;
        let mut digest = Sha256::new();
        loop {
            let next = match self.pending.take() {
                Some(pending) => Some(Ok(pending)),
                None => self.reader.next().map(|result| {
                    let change = result?;
                    let encoded = encode_change(&change)?;
                    Ok((change, encoded))
                }),
            };
            let Some(next) = next else {
                break;
            };
            let (change, encoded) = match next {
                Ok(change) => change,
                Err(error) => return Some(Err(error)),
            };
            let would_exceed = !changes.is_empty()
                && (changes.len() >= self.limits.max_records
                    || encoded_bytes.saturating_add(encoded.len() as u64)
                        > self.limits.max_bytes as u64);
            if would_exceed {
                self.pending = Some((change, encoded));
                break;
            }
            digest.update((encoded.len() as u64).to_le_bytes());
            digest.update(&encoded);
            encoded_bytes = encoded_bytes.saturating_add(encoded.len() as u64);
            changes.push(change);
        }
        if changes.is_empty() {
            return None;
        }
        self.next_seq = self.next_seq.saturating_add(changes.len() as u64);
        Some(Ok(ChangeChunk {
            start_seq,
            end_seq: self.next_seq,
            encoded_bytes,
            digest: digest.finalize().into(),
            changes,
        }))
    }
}

#[derive(Debug)]
pub struct SpoolJournal {
    directory: PathBuf,
    limits: SpoolLimits,
    usage: Arc<AtomicU64>,
}

impl SpoolJournal {
    pub fn open(
        root: impl AsRef<Path>,
        identity: SpoolIdentity,
        limits: SpoolLimits,
    ) -> SpoolResult<Self> {
        limits.validate()?;
        let directory = root
            .as_ref()
            .join(identity.pipeline_id.to_string())
            .join(identity.topology_generation.to_string())
            .join(identity.directory_name());
        fs::create_dir_all(&directory)?;
        let mut journal = Self {
            directory,
            limits,
            usage: Arc::new(AtomicU64::new(0)),
        };
        journal.remove_unpublished_temps()?;
        journal
            .usage
            .store(scan_used_bytes(&journal.directory)?, Ordering::Relaxed);
        journal.validate_committed_manifests()?;
        // Reclaim disk from topology generations this identity has already superseded. A lower
        // generation is definitionally dead: a new generation only exists after a fresh snapshot,
        // and the slot always replays under the current generation. Best-effort so a cleanup error
        // never blocks the data path.
        remove_superseded_generations(root.as_ref(), identity);
        Ok(journal)
    }

    /// Open an empty journal after the caller has proved the managed slot can replay from the
    /// target checkpoint. The proof is essential: this removes every spool artifact for the
    /// exact pipeline/topology/source identity, including an interrupted prior cleanup.
    pub fn open_after_wal_replay_verified(
        root: impl AsRef<Path>,
        identity: SpoolIdentity,
        limits: SpoolLimits,
    ) -> SpoolResult<Self> {
        limits.validate()?;
        let directory = root
            .as_ref()
            .join(identity.pipeline_id.to_string())
            .join(identity.topology_generation.to_string())
            .join(identity.directory_name());
        fs::create_dir_all(&directory)?;
        let journal = Self {
            directory,
            limits,
            usage: Arc::new(AtomicU64::new(0)),
        };
        journal.remove_all_artifacts()?;
        Ok(journal)
    }

    #[must_use]
    pub fn directory(&self) -> &Path {
        &self.directory
    }

    #[must_use]
    pub const fn limits(&self) -> SpoolLimits {
        self.limits
    }

    pub fn resource_state(&self, additional_bytes: u64) -> SpoolResult<ResourceState> {
        let used = self.usage.load(Ordering::Relaxed);
        let free = available_space(&self.directory).ok();
        let over_high = used.saturating_add(additional_bytes) > self.limits.disk_high_water_bytes;
        let under_reserve = free.is_some_and(|bytes| {
            bytes
                < self
                    .limits
                    .minimum_free_disk_bytes
                    .saturating_add(additional_bytes)
        });
        if over_high || under_reserve {
            return Ok(ResourceState::Wait {
                used_bytes: used,
                disk_high_water_bytes: self.limits.disk_high_water_bytes,
                free_bytes: free,
                minimum_free_disk_bytes: self.limits.minimum_free_disk_bytes,
            });
        }
        Ok(ResourceState::Ready { used_bytes: used })
    }

    pub fn begin(&self, xid: u32, begin_lsn: PgLsn) -> SpoolResult<SpoolWriter> {
        let transaction_id = Uuid::new_v4();
        let first = self.segment_path(transaction_id, 0, true);
        let file = match OpenOptions::new().create_new(true).write(true).open(first) {
            Ok(file) => file,
            Err(error) if is_capacity_error(&error) => return Err(self.capacity_wait()),
            Err(error) => return Err(error.into()),
        };
        Ok(SpoolWriter {
            directory: self.directory.clone(),
            limits: self.limits,
            usage: Arc::clone(&self.usage),
            transaction_id,
            xid,
            begin_lsn,
            next_segment: 0,
            writer: Some(file),
            current_segment_bytes: 0,
            segments: Vec::new(),
            change_count: 0,
            encoded_bytes: 0,
            digest: Sha256::new(),
            row_count: 0,
            has_generation_barrier: false,
            #[cfg(test)]
            fail_next_append_capacity: false,
            #[cfg(test)]
            fail_next_finish_capacity: false,
        })
    }

    pub fn load_manifest(&self, path: impl AsRef<Path>) -> SpoolResult<SpoolHandle> {
        let manifest_path = path.as_ref().to_owned();
        let bytes = fs::read(&manifest_path)?;
        let (manifest, consumed): (TransactionManifest, usize) =
            bincode::serde::decode_from_slice(&bytes, config::standard())
                .map_err(|error| SpoolError::Codec(error.to_string()))?;
        if consumed != bytes.len() {
            return Err(SpoolError::InvalidManifest("trailing bytes".to_owned()));
        }
        validate_manifest(&manifest, &manifest_path)?;
        Ok(SpoolHandle {
            manifest_path,
            manifest,
            usage: Arc::clone(&self.usage),
        })
    }

    fn segment_path(&self, transaction_id: Uuid, index: u32, temporary: bool) -> PathBuf {
        let suffix = if temporary { ".tmp" } else { ".seg" };
        self.directory
            .join(format!("{transaction_id}-{index}{suffix}"))
    }

    pub fn used_bytes(&self) -> SpoolResult<u64> {
        Ok(self.usage.load(Ordering::Relaxed))
    }

    fn capacity_wait(&self) -> SpoolError {
        capacity_wait(&self.directory, self.limits, &self.usage)
    }

    fn remove_all_artifacts(&self) -> SpoolResult<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            let path = entry.path();
            let managed_artifact = path.extension().is_some_and(|extension| {
                extension == "manifest" || extension == "seg" || extension == "tmp"
            });
            if managed_artifact && entry.metadata()?.is_file() {
                remove_tracked_file(&path, &self.usage)?;
            }
        }
        Ok(())
    }

    fn remove_unpublished_temps(&mut self) -> SpoolResult<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "tmp")
            {
                remove_tracked_file(&entry.path(), &self.usage)?;
            }
        }
        Ok(())
    }

    fn validate_committed_manifests(&self) -> SpoolResult<()> {
        for entry in fs::read_dir(&self.directory)? {
            let entry = entry?;
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "manifest")
            {
                let handle = self.load_manifest(entry.path())?;
                validate_handle_frames(&handle)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct SpoolWriter {
    directory: PathBuf,
    limits: SpoolLimits,
    usage: Arc<AtomicU64>,
    transaction_id: Uuid,
    xid: u32,
    begin_lsn: PgLsn,
    next_segment: u32,
    writer: Option<File>,
    current_segment_bytes: u64,
    segments: Vec<String>,
    change_count: u64,
    encoded_bytes: u64,
    digest: Sha256,
    row_count: u64,
    has_generation_barrier: bool,
    #[cfg(test)]
    fail_next_append_capacity: bool,
    #[cfg(test)]
    fail_next_finish_capacity: bool,
}

impl SpoolWriter {
    pub fn append(&mut self, change: &TransactionChange) -> SpoolResult<()> {
        let payload = encode_change(change)?;
        let frame_bytes = u64::try_from(HEADER_BYTES + payload.len()).unwrap_or(u64::MAX);
        if frame_bytes > MAX_RECORD_BYTES {
            return Err(SpoolError::RecordTooLarge(frame_bytes));
        }
        if self.current_segment_bytes > 0
            && self.current_segment_bytes.saturating_add(frame_bytes)
                > self.limits.segment_target_bytes as u64
        {
            self.rotate_segment()?;
        }
        self.ensure_current_segment()?;
        let mut frame = Vec::with_capacity(HEADER_BYTES + payload.len());
        frame.extend_from_slice(&MAGIC);
        frame.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        frame.extend_from_slice(&RECORD_CHANGE.to_le_bytes());
        frame.extend_from_slice(&(payload.len() as u64).to_le_bytes());
        frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        frame.extend_from_slice(&payload);
        let frame_start = self.current_segment_bytes;
        let writer = self.writer.as_mut().ok_or_else(|| {
            SpoolError::InvalidManifest("append after writer finalization".to_owned())
        })?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_next_append_capacity) {
            let partial = frame.len().div_ceil(2);
            writer.write_all(&frame[..partial])?;
            let error = io::Error::new(io::ErrorKind::WriteZero, "injected capacity failure");
            writer.set_len(frame_start)?;
            writer.seek(SeekFrom::Start(frame_start))?;
            return Err(self.map_io_error(error));
        }
        if let Err(error) = writer.write_all(&frame) {
            if let Err(rollback) = writer
                .set_len(frame_start)
                .and_then(|()| writer.seek(SeekFrom::Start(frame_start)).map(|_| ()))
            {
                return Err(rollback.into());
            }
            return Err(self.map_io_error(error));
        }
        self.usage.fetch_add(frame_bytes, Ordering::Relaxed);
        self.current_segment_bytes = self.current_segment_bytes.saturating_add(frame_bytes);
        self.change_count = self.change_count.saturating_add(1);
        self.encoded_bytes = self.encoded_bytes.saturating_add(payload.len() as u64);
        self.digest.update((payload.len() as u64).to_le_bytes());
        self.digest.update(&payload);
        self.row_count = self
            .row_count
            .saturating_add(u64::from(matches!(change, TransactionChange::Row(_))));
        self.has_generation_barrier |= change.requires_generation_barrier();
        Ok(())
    }

    pub fn finish(&mut self, commit_lsn: PgLsn, end_lsn: PgLsn) -> SpoolResult<SpoolHandle> {
        self.close_segment()?;
        let manifest = self.preview_manifest(commit_lsn, end_lsn);
        let manifest_name = format!("{}.manifest", self.transaction_id);
        let manifest_path = self.directory.join(&manifest_name);
        let temporary = self.directory.join(format!("{manifest_name}.tmp"));
        let bytes = bincode::serde::encode_to_vec(&manifest, config::standard())
            .map_err(|error| SpoolError::Codec(error.to_string()))?;
        #[cfg(test)]
        let inject_capacity_failure = std::mem::take(&mut self.fail_next_finish_capacity);
        #[cfg(not(test))]
        let inject_capacity_failure = false;
        let publish = (|| -> io::Result<()> {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)?;
            if inject_capacity_failure {
                file.write_all(&bytes[..bytes.len().div_ceil(2)])?;
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "injected manifest capacity failure",
                ));
            }
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temporary, &manifest_path)
        })();
        if let Err(error) = publish {
            let _ = fs::remove_file(&temporary);
            return Err(self.map_io_error(error));
        }
        self.usage.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let handle = SpoolHandle {
            manifest_path,
            manifest,
            usage: Arc::clone(&self.usage),
        };
        validate_handle_frames(&handle)?;
        Ok(handle)
    }

    pub(crate) fn finish_additional_bytes(
        &self,
        commit_lsn: PgLsn,
        end_lsn: PgLsn,
    ) -> SpoolResult<u64> {
        let manifest = self.preview_manifest(commit_lsn, end_lsn);
        bincode::serde::encode_to_vec(&manifest, config::standard())
            .map(|bytes| bytes.len() as u64)
            .map_err(|error| SpoolError::Codec(error.to_string()))
    }

    pub fn abort(mut self) -> SpoolResult<()> {
        self.writer.take();
        let temporary = self
            .directory
            .join(format!("{}-{}.tmp", self.transaction_id, self.next_segment));
        remove_tracked_file(&temporary, &self.usage)?;
        for name in self.segments {
            remove_tracked_file(&self.directory.join(name), &self.usage)?;
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> SpoolResult<()> {
        self.close_segment()?;
        self.next_segment = self.next_segment.saturating_add(1);
        self.current_segment_bytes = 0;
        self.ensure_current_segment()
    }

    fn close_segment(&mut self) -> SpoolResult<()> {
        if let Some(writer) = self.writer.as_mut() {
            if let Err(error) = writer.sync_all() {
                return Err(self.map_io_error(error));
            }
            self.writer.take();
        }
        let temporary = self.current_temporary_path();
        if !temporary.exists() {
            return Ok(());
        }
        let final_name = format!("{}-{}.seg", self.transaction_id, self.next_segment);
        let final_path = self.directory.join(&final_name);
        if let Err(error) = fs::rename(&temporary, final_path) {
            self.writer = OpenOptions::new().write(true).open(&temporary).ok();
            if let Some(writer) = self.writer.as_mut() {
                let _ = writer.seek(SeekFrom::End(0));
            }
            return Err(self.map_io_error(error));
        }
        self.segments.push(final_name);
        Ok(())
    }

    fn ensure_current_segment(&mut self) -> SpoolResult<()> {
        if self.writer.is_some() {
            return Ok(());
        }
        let path = self.current_temporary_path();
        if path.exists() {
            let mut file = OpenOptions::new().write(true).open(path)?;
            file.seek(SeekFrom::End(0))?;
            self.writer = Some(file);
            return Ok(());
        }
        match OpenOptions::new().create_new(true).write(true).open(path) {
            Ok(file) => {
                self.writer = Some(file);
                Ok(())
            }
            Err(error) if is_capacity_error(&error) => Err(self.capacity_wait()),
            Err(error) => Err(error.into()),
        }
    }

    fn preview_manifest(&self, commit_lsn: PgLsn, end_lsn: PgLsn) -> TransactionManifest {
        let mut segments = self.segments.clone();
        if self.writer.is_some() || self.current_temporary_path().exists() {
            segments.push(format!("{}-{}.seg", self.transaction_id, self.next_segment));
        }
        TransactionManifest {
            format_version: FORMAT_VERSION,
            transaction_id: self.transaction_id,
            xid: self.xid,
            begin_lsn: self.begin_lsn,
            commit_lsn,
            end_lsn,
            change_count: self.change_count,
            encoded_bytes: self.encoded_bytes,
            digest: self.digest.clone().finalize().into(),
            row_count: self.row_count,
            has_generation_barrier: self.has_generation_barrier,
            segments,
        }
    }

    fn capacity_wait(&self) -> SpoolError {
        capacity_wait(&self.directory, self.limits, &self.usage)
    }

    fn map_io_error(&self, error: io::Error) -> SpoolError {
        if is_capacity_error(&error) {
            self.capacity_wait()
        } else {
            error.into()
        }
    }

    fn current_temporary_path(&self) -> PathBuf {
        self.directory
            .join(format!("{}-{}.tmp", self.transaction_id, self.next_segment))
    }

    #[cfg(test)]
    pub(crate) fn inject_next_append_capacity_failure(&mut self) {
        self.fail_next_append_capacity = true;
    }

    #[cfg(test)]
    pub(crate) fn inject_next_finish_capacity_failure(&mut self) {
        self.fail_next_finish_capacity = true;
    }
}

/// Removes spool directories for topology generations older than `identity`'s current one.
///
/// The on-disk layout is `root/<pipeline_id>/<topology_generation>/<identity_dir>`. Once a pipeline
/// advances to a new generation the older generation's spool can never replay again, so its
/// directory is dead weight. This is best-effort: any I/O error is logged and skipped rather than
/// propagated, because failing here must not stop a healthy pipeline from starting.
fn remove_superseded_generations(root: &Path, identity: SpoolIdentity) {
    let pipeline_root = root.join(identity.pipeline_id.to_string());
    let entries = match fs::read_dir(&pipeline_root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return,
        Err(error) => {
            tracing::warn!(%error, path = %pipeline_root.display(), "spool generation scan failed");
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let is_older_generation = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.parse::<u64>().ok())
            .is_some_and(|generation| generation < identity.topology_generation);
        if is_older_generation && entry.metadata().is_ok_and(|metadata| metadata.is_dir()) {
            if let Err(error) = fs::remove_dir_all(&path) {
                tracing::warn!(%error, path = %path.display(), "spool generation cleanup failed");
            } else {
                tracing::info!(path = %path.display(), "removed superseded spool generation");
            }
        }
    }
}

fn scan_used_bytes(directory: &Path) -> SpoolResult<u64> {
    let mut total = 0_u64;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total = total.saturating_add(metadata.len());
        }
    }
    Ok(total)
}

fn remove_tracked_file(path: &Path, usage: &AtomicU64) -> SpoolResult<()> {
    let bytes = match fs::metadata(path) {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    match fs::remove_file(path) {
        Ok(()) => {
            let _ = usage.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(bytes))
            });
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn capacity_wait(directory: &Path, limits: SpoolLimits, usage: &AtomicU64) -> SpoolError {
    SpoolError::ResourceWait {
        used: usage.load(Ordering::Relaxed),
        high: limits.disk_high_water_bytes,
        free: available_space(directory).ok(),
        minimum: limits.minimum_free_disk_bytes,
    }
}

fn is_capacity_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::WriteZero | io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
    ) || matches!(error.raw_os_error(), Some(28 | 39 | 69 | 112 | 122))
}

fn read_frame(reader: &mut BufReader<File>) -> SpoolResult<Option<TransactionChange>> {
    let mut magic = [0_u8; 8];
    match reader.read_exact(&mut magic) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    if magic != MAGIC {
        return Err(SpoolError::InvalidMagic);
    }
    let version = read_u16(reader)?;
    if version != FORMAT_VERSION {
        return Err(SpoolError::UnsupportedVersion(version));
    }
    let kind = read_u16(reader)?;
    if kind != RECORD_CHANGE {
        return Err(SpoolError::UnsupportedRecord(kind));
    }
    let length = read_u64(reader)?;
    if length > MAX_RECORD_BYTES {
        return Err(SpoolError::RecordTooLarge(length));
    }
    let expected = read_u32(reader)?;
    let mut payload =
        vec![0_u8; usize::try_from(length).map_err(|_| SpoolError::RecordTooLarge(length))?];
    reader.read_exact(&mut payload)?;
    if crc32fast::hash(&payload) != expected {
        return Err(SpoolError::ChecksumMismatch);
    }
    let (change, consumed): (WireTransactionChange, usize) =
        bincode::serde::decode_from_slice(&payload, config::standard())
            .map_err(|error| SpoolError::Codec(error.to_string()))?;
    if consumed != payload.len() {
        return Err(SpoolError::InvalidManifest(
            "trailing change bytes".to_owned(),
        ));
    }
    Ok(Some(change.into()))
}

fn read_u16(reader: &mut BufReader<File>) -> SpoolResult<u16> {
    let mut bytes = [0_u8; 2];
    reader.read_exact(&mut bytes)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(reader: &mut BufReader<File>) -> SpoolResult<u32> {
    let mut bytes = [0_u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(reader: &mut BufReader<File>) -> SpoolResult<u64> {
    let mut bytes = [0_u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn validate_manifest(manifest: &TransactionManifest, path: &Path) -> SpoolResult<()> {
    if manifest.format_version != FORMAT_VERSION {
        return Err(SpoolError::UnsupportedVersion(manifest.format_version));
    }
    if manifest.segments.is_empty() && manifest.change_count != 0 {
        return Err(SpoolError::InvalidManifest(format!(
            "{} has changes but no segments",
            path.display()
        )));
    }
    if manifest.segments.iter().any(|segment| {
        segment.is_empty()
            || Path::new(segment)
                .file_name()
                .is_none_or(|name| name.to_string_lossy() != segment.as_str())
    }) {
        return Err(SpoolError::InvalidManifest(
            "segment path escapes manifest directory".to_owned(),
        ));
    }
    Ok(())
}

fn validate_handle_frames(handle: &SpoolHandle) -> SpoolResult<()> {
    let reader = handle.reader()?;
    let mut count = 0_u64;
    let mut digest = Sha256::new();
    for change in reader {
        let change = change?;
        let bytes = encode_change(&change)?;
        digest.update((bytes.len() as u64).to_le_bytes());
        digest.update(&bytes);
        count = count.saturating_add(1);
    }
    if count != handle.manifest.change_count
        || <[u8; 32]>::from(digest.finalize()) != handle.manifest.digest
    {
        return Err(SpoolError::InvalidManifest(format!(
            "transaction {} frame count/checksum mismatch",
            handle.manifest.transaction_id
        )));
    }
    Ok(())
}

fn encode_change(change: &TransactionChange) -> SpoolResult<Vec<u8>> {
    bincode::serde::encode_to_vec(WireTransactionChange::from(change), config::standard())
        .map_err(|error| SpoolError::Codec(error.to_string()))
}

pub(crate) fn framed_change_bytes(change: &TransactionChange) -> SpoolResult<u64> {
    let payload = encode_change(change)?;
    let frame_bytes = u64::try_from(HEADER_BYTES + payload.len()).unwrap_or(u64::MAX);
    if frame_bytes > MAX_RECORD_BYTES {
        return Err(SpoolError::RecordTooLarge(frame_bytes));
    }
    Ok(frame_bytes)
}

fn change_stats(changes: &[TransactionChange]) -> SpoolResult<ChangeStats> {
    let mut digest = Sha256::new();
    let mut encoded_bytes = 0_u64;
    let mut row_count = 0_u64;
    let mut has_generation_barrier = false;
    for change in changes {
        let encoded = encode_change(change)?;
        digest.update((encoded.len() as u64).to_le_bytes());
        digest.update(&encoded);
        encoded_bytes = encoded_bytes.saturating_add(encoded.len() as u64);
        row_count =
            row_count.saturating_add(u64::from(matches!(change, TransactionChange::Row(_))));
        has_generation_barrier |= change.requires_generation_barrier();
    }
    Ok(ChangeStats {
        change_count: changes.len() as u64,
        row_count,
        encoded_bytes,
        digest: digest.finalize().into(),
        has_generation_barrier,
    })
}

impl From<&Cell> for WireCell {
    fn from(cell: &Cell) -> Self {
        match cell {
            Cell::Null => Self::Null,
            Cell::UnchangedToast => Self::UnchangedToast,
            Cell::Text(value) => Self::Text(value.to_vec()),
            Cell::Binary(value) => Self::Binary(value.to_vec()),
        }
    }
}

impl From<WireCell> for Cell {
    fn from(cell: WireCell) -> Self {
        match cell {
            WireCell::Null => Self::Null,
            WireCell::UnchangedToast => Self::UnchangedToast,
            WireCell::Text(value) => Self::Text(value.into()),
            WireCell::Binary(value) => Self::Binary(value.into()),
        }
    }
}

impl From<&Tuple> for WireTuple {
    fn from(tuple: &Tuple) -> Self {
        Self {
            cells: tuple.cells.iter().map(WireCell::from).collect(),
        }
    }
}

impl From<WireTuple> for Tuple {
    fn from(tuple: WireTuple) -> Self {
        Self {
            cells: tuple.cells.into_iter().map(Cell::from).collect(),
        }
    }
}

impl From<&RowChange> for WireRowChange {
    fn from(change: &RowChange) -> Self {
        match change {
            RowChange::Insert { new } => Self::Insert {
                new: WireTuple::from(new),
            },
            RowChange::Update { old_key, new } => Self::Update {
                old_key: old_key.as_ref().map(WireTuple::from),
                new: WireTuple::from(new),
            },
            RowChange::Delete { old_key } => Self::Delete {
                old_key: WireTuple::from(old_key),
            },
        }
    }
}

impl From<WireRowChange> for RowChange {
    fn from(change: WireRowChange) -> Self {
        match change {
            WireRowChange::Insert { new } => Self::Insert { new: new.into() },
            WireRowChange::Update { old_key, new } => Self::Update {
                old_key: old_key.map(Tuple::from),
                new: new.into(),
            },
            WireRowChange::Delete { old_key } => Self::Delete {
                old_key: old_key.into(),
            },
        }
    }
}

impl From<&TableChange> for WireTableChange {
    fn from(change: &TableChange) -> Self {
        Self {
            relation_id: change.relation_id,
            generation: change.generation,
            change: WireRowChange::from(&change.change),
        }
    }
}

impl From<WireTableChange> for TableChange {
    fn from(change: WireTableChange) -> Self {
        Self {
            relation_id: change.relation_id,
            generation: change.generation,
            change: change.change.into(),
        }
    }
}

impl From<&DdlMessage> for WireDdlMessage {
    fn from(message: &DdlMessage) -> Self {
        Self {
            version: message.version,
            command_tag: message.command_tag.clone(),
            relation_ids: message.relation_ids.clone(),
            affected_schemas: message.affected_schemas.clone(),
            schema_fingerprint: message.schema_fingerprint.clone(),
            transitions: message
                .transitions
                .iter()
                .map(WireTableTransition::from)
                .collect(),
        }
    }
}

impl From<WireDdlMessage> for DdlMessage {
    fn from(message: WireDdlMessage) -> Self {
        Self {
            version: message.version,
            command_tag: message.command_tag,
            relation_ids: message.relation_ids,
            affected_schemas: message.affected_schemas,
            schema_fingerprint: message.schema_fingerprint,
            transitions: message
                .transitions
                .into_iter()
                .map(TableTransition::from)
                .collect(),
        }
    }
}

impl From<&TableTransition> for WireTableTransition {
    fn from(transition: &TableTransition) -> Self {
        Self {
            relation_id: transition.relation_id,
            before_generation: transition.before_generation,
            after_generation: transition.after_generation,
            before_fingerprint: transition.before_fingerprint.clone(),
            after_fingerprint: transition.after_fingerprint.clone(),
            after_schema: transition.after_schema.clone(),
            kind: WireTransitionKind::from(&transition.kind),
        }
    }
}

impl From<WireTableTransition> for TableTransition {
    fn from(transition: WireTableTransition) -> Self {
        Self {
            relation_id: transition.relation_id,
            before_generation: transition.before_generation,
            after_generation: transition.after_generation,
            before_fingerprint: transition.before_fingerprint,
            after_fingerprint: transition.after_fingerprint,
            after_schema: transition.after_schema,
            kind: TransitionKind::from(transition.kind),
        }
    }
}

impl From<&TransitionKind> for WireTransitionKind {
    fn from(kind: &TransitionKind) -> Self {
        match kind {
            TransitionKind::AddColumn {
                name,
                nullable_or_defaulted,
            } => Self::AddColumn {
                name: name.clone(),
                nullable_or_defaulted: *nullable_or_defaulted,
            },
            TransitionKind::DropColumn { name } => Self::DropColumn { name: name.clone() },
            TransitionKind::RenameColumn { from, to } => Self::RenameColumn {
                from: from.clone(),
                to: to.clone(),
            },
            TransitionKind::AlterColumnType { name, widening } => Self::AlterColumnType {
                name: name.clone(),
                widening: *widening,
            },
            TransitionKind::AddTable => Self::AddTable,
            TransitionKind::DropTable => Self::DropTable,
            TransitionKind::Unknown => Self::Unknown,
        }
    }
}

impl From<WireTransitionKind> for TransitionKind {
    fn from(kind: WireTransitionKind) -> Self {
        match kind {
            WireTransitionKind::AddColumn {
                name,
                nullable_or_defaulted,
            } => Self::AddColumn {
                name,
                nullable_or_defaulted,
            },
            WireTransitionKind::DropColumn { name } => Self::DropColumn { name },
            WireTransitionKind::RenameColumn { from, to } => Self::RenameColumn { from, to },
            WireTransitionKind::AlterColumnType { name, widening } => {
                Self::AlterColumnType { name, widening }
            }
            WireTransitionKind::AddTable => Self::AddTable,
            WireTransitionKind::DropTable => Self::DropTable,
            WireTransitionKind::Unknown => Self::Unknown,
        }
    }
}

impl From<&TransactionChange> for WireTransactionChange {
    fn from(change: &TransactionChange) -> Self {
        match change {
            TransactionChange::Row(change) => Self::Row(WireTableChange::from(change)),
            TransactionChange::Truncate {
                relation_ids,
                cascade,
                restart_identity,
            } => Self::Truncate {
                relation_ids: relation_ids.clone(),
                cascade: *cascade,
                restart_identity: *restart_identity,
            },
            TransactionChange::Ddl(message) => Self::Ddl(WireDdlMessage::from(message)),
        }
    }
}

impl From<WireTransactionChange> for TransactionChange {
    fn from(change: WireTransactionChange) -> Self {
        match change {
            WireTransactionChange::Row(change) => Self::Row(change.into()),
            WireTransactionChange::Truncate {
                relation_ids,
                cascade,
                restart_identity,
            } => Self::Truncate {
                relation_ids,
                cascade,
                restart_identity,
            },
            WireTransactionChange::Ddl(message) => Self::Ddl(message.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use bytes::Bytes;
    use cloudberry_etl_core::{
        change::{
            Cell, DdlMessage, RelationSchemaSnapshot, RowChange, TableChange, TableTransition,
            TransactionChange, TransitionKind, Tuple,
        },
        id::PipelineId,
        schema::QualifiedName,
    };

    use super::*;

    fn temp_root() -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("pg2cb-spool-{suffix}"))
    }

    fn change(value: u8) -> TransactionChange {
        TransactionChange::Row(TableChange {
            relation_id: 42,
            generation: 1,
            change: RowChange::Insert {
                new: Tuple {
                    cells: vec![Cell::Binary(Bytes::from(vec![value; 32]))],
                },
            },
        })
    }

    fn ddl_change() -> TransactionChange {
        TransactionChange::Ddl(DdlMessage {
            version: 2,
            command_tag: "ALTER TABLE".to_owned(),
            relation_ids: vec![42],
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "event-fingerprint".to_owned(),
            transitions: vec![TableTransition {
                relation_id: 42,
                before_generation: Some(1),
                after_generation: Some(2),
                before_fingerprint: Some("before".to_owned()),
                after_fingerprint: Some("after".to_owned()),
                after_schema: Some(RelationSchemaSnapshot {
                    relation_id: 42,
                    name: QualifiedName::new("public", "items").unwrap(),
                    relation_kind: "r".to_owned(),
                    replica_identity: "d".to_owned(),
                    columns: Vec::new(),
                    primary_key: Vec::new(),
                    partition_key: Vec::new(),
                }),
                kind: TransitionKind::RenameColumn {
                    from: "old_name".to_owned(),
                    to: "new_name".to_owned(),
                },
            }],
        })
    }

    fn identity() -> SpoolIdentity {
        SpoolIdentity {
            pipeline_id: PipelineId::new(),
            topology_generation: 1,
            node_id: 0,
            system_identifier: 9,
            timeline: 1,
        }
    }

    #[test]
    fn framed_segments_round_trip_and_remove() {
        let root = temp_root();
        let journal = SpoolJournal::open(
            &root,
            identity(),
            SpoolLimits {
                memory_high_water_bytes: 1,
                segment_target_bytes: 64,
                disk_high_water_bytes: 1024 * 1024,
                minimum_free_disk_bytes: 1,
            },
        )
        .unwrap();
        let mut writer = journal.begin(7, PgLsn::new(10)).unwrap();
        for value in 0..10 {
            writer.append(&change(value)).unwrap();
        }
        let handle = writer.finish(PgLsn::new(20), PgLsn::new(21)).unwrap();
        let changes = handle
            .reader()
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        assert_eq!(changes.len(), 10);
        assert!(handle.manifest().segments.len() > 1);
        handle.remove().unwrap();
        handle.remove().unwrap();
        assert!(!handle.manifest_path().exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn ddl_transitions_survive_spool_round_trip() {
        let root = temp_root();
        let journal = SpoolJournal::open(&root, identity(), SpoolLimits::default()).unwrap();
        let expected = ddl_change();
        let mut writer = journal.begin(7, PgLsn::new(10)).unwrap();
        writer.append(&expected).unwrap();
        let handle = writer.finish(PgLsn::new(20), PgLsn::new(21)).unwrap();
        let restored = handle
            .reader()
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        assert_eq!(restored, [expected]);
        handle.remove().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn verified_wal_replay_discards_partial_and_orphaned_artifacts() {
        let root = temp_root();
        let identity = identity();
        let limits = SpoolLimits::default();
        let journal = SpoolJournal::open(&root, identity, limits).unwrap();
        let mut writer = journal.begin(7, PgLsn::new(10)).unwrap();
        writer.append(&change(1)).unwrap();
        let handle = writer.finish(PgLsn::new(20), PgLsn::new(21)).unwrap();
        let missing_segment = handle
            .manifest()
            .segments
            .first()
            .map(|name| handle.manifest_path().parent().unwrap().join(name))
            .unwrap();
        fs::remove_file(missing_segment).unwrap();
        fs::write(journal.directory().join("interrupted.seg"), b"orphan").unwrap();
        // A binary upgrade may leave an artifact whose manifest cannot be decoded by the new
        // format. Once WAL replay has been proved, reset must remove it without trying to parse it.
        fs::write(journal.directory().join("legacy.manifest"), b"format-v1").unwrap();

        let reset = SpoolJournal::open_after_wal_replay_verified(&root, identity, limits).unwrap();
        assert_eq!(reset.used_bytes().unwrap(), 0);
        assert!(fs::read_dir(reset.directory()).unwrap().next().is_none());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn opening_a_new_generation_removes_superseded_generations() {
        let root = temp_root();
        let base = identity();

        // Generation 1 writes a committed transaction to disk.
        let gen1 = SpoolJournal::open(&root, base, SpoolLimits::default()).unwrap();
        let mut writer = gen1.begin(1, PgLsn::new(1)).unwrap();
        writer.append(&change(1)).unwrap();
        writer.finish(PgLsn::new(2), PgLsn::new(3)).unwrap();
        let gen1_dir = gen1.directory().to_owned();
        assert!(gen1_dir.exists());

        // Advancing to generation 2 must reclaim generation 1's directory tree.
        let gen2_identity = SpoolIdentity {
            topology_generation: 2,
            ..base
        };
        let gen2 = SpoolJournal::open(&root, gen2_identity, SpoolLimits::default()).unwrap();
        assert!(gen2.directory().exists());
        let gen1_generation_root = gen1_dir.parent().unwrap();
        assert!(!gen1_generation_root.exists());

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn checksum_corruption_is_rejected() {
        let root = temp_root();
        let journal = SpoolJournal::open(&root, identity(), SpoolLimits::default()).unwrap();
        let mut writer = journal.begin(1, PgLsn::new(1)).unwrap();
        writer.append(&change(1)).unwrap();
        let handle = writer.finish(PgLsn::new(2), PgLsn::new(3)).unwrap();
        let segment = handle
            .manifest()
            .segments
            .first()
            .map(|name| handle.manifest_path().parent().unwrap().join(name))
            .unwrap();
        let mut bytes = fs::read(&segment).unwrap();
        *bytes.last_mut().unwrap() ^= 0x01;
        fs::write(segment, bytes).unwrap();
        assert!(matches!(
            handle.reader().unwrap().next(),
            Some(Err(SpoolError::ChecksumMismatch))
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn resource_watermark_is_fail_closed() {
        let root = temp_root();
        let journal = SpoolJournal::open(
            &root,
            identity(),
            SpoolLimits {
                memory_high_water_bytes: 1,
                segment_target_bytes: 1,
                disk_high_water_bytes: 10,
                minimum_free_disk_bytes: 1,
            },
        )
        .unwrap();
        assert!(matches!(
            journal.resource_state(11).unwrap(),
            ResourceState::Wait { .. }
        ));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn memory_and_spool_sources_produce_the_same_stable_chunks() {
        let changes = (0..7).map(change).collect::<Vec<_>>();
        let memory = ChangeSource::memory(changes.clone());
        let root = temp_root();
        let journal = SpoolJournal::open(
            &root,
            identity(),
            SpoolLimits {
                segment_target_bytes: 80,
                ..SpoolLimits::default()
            },
        )
        .unwrap();
        let mut writer = journal.begin(1, PgLsn::new(1)).unwrap();
        for change in &changes {
            writer.append(change).unwrap();
        }
        let disk = ChangeSource::Spool(writer.finish(PgLsn::new(2), PgLsn::new(3)).unwrap());
        assert_eq!(memory.stats().unwrap(), disk.stats().unwrap());

        let limits = ChunkLimits {
            max_records: 3,
            max_bytes: usize::MAX,
        };
        let memory_chunks = memory
            .chunks_from(0, limits)
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        let disk_chunks = disk
            .chunks_from(0, limits)
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        assert_eq!(memory_chunks, disk_chunks);
        assert_eq!(
            memory_chunks
                .iter()
                .map(|chunk| chunk.start_seq)
                .collect::<Vec<_>>(),
            [0, 3, 6]
        );
        let resumed = disk
            .chunks_from(3, limits)
            .unwrap()
            .collect::<SpoolResult<Vec<_>>>()
            .unwrap();
        assert_eq!(resumed, disk_chunks[1..]);
        let _ = fs::remove_dir_all(root);
    }
}
