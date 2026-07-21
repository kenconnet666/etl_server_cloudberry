//! Per-source-node completion tracking for chunked transaction apply.
//!
//! Transactions may finish out of order, but a source checkpoint may only move
//! through a contiguous prefix of registered transactions. Chunk identifiers
//! are observational: record-range coverage is the correctness condition. A
//! registration's chunk count describes that token's immutable chunk plan.

use std::{collections::BTreeMap, ops::Range};

use cloudberry_etl_core::lsn::PgLsn;
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeIdentity {
    node_id: i32,
    system_identifier: u64,
    timeline: u32,
}

impl NodeIdentity {
    #[must_use]
    pub const fn new(node_id: i32, system_identifier: u64, timeline: u32) -> Self {
        Self {
            node_id,
            system_identifier,
            timeline,
        }
    }

    #[must_use]
    pub const fn node_id(self) -> i32 {
        self.node_id
    }

    #[must_use]
    pub const fn system_identifier(self) -> u64 {
        self.system_identifier
    }

    #[must_use]
    pub const fn timeline(self) -> u32 {
        self.timeline
    }
}

/// Immutable completion metadata for one committed source transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CompletionManifest {
    identity: NodeIdentity,
    end_lsn: PgLsn,
    total_chunks: u32,
    total_records: u64,
}

impl CompletionManifest {
    #[must_use]
    pub const fn new(
        identity: NodeIdentity,
        end_lsn: PgLsn,
        total_chunks: u32,
        total_records: u64,
    ) -> Self {
        Self {
            identity,
            end_lsn,
            total_chunks,
            total_records,
        }
    }

    #[must_use]
    pub const fn identity(self) -> NodeIdentity {
        self.identity
    }

    #[must_use]
    pub const fn end_lsn(self) -> PgLsn {
        self.end_lsn
    }

    #[must_use]
    pub const fn total_chunks(self) -> u32 {
        self.total_chunks
    }

    #[must_use]
    pub const fn total_records(self) -> u64 {
        self.total_records
    }
}

/// Opaque proof that a transaction was registered with a particular tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionToken {
    tracker_id: Uuid,
    sequence: u64,
    manifest: CompletionManifest,
}

impl TransactionToken {
    #[must_use]
    pub const fn identity(self) -> NodeIdentity {
        self.manifest.identity
    }

    #[must_use]
    pub const fn end_lsn(self) -> PgLsn {
        self.manifest.end_lsn
    }

    #[must_use]
    pub const fn total_chunks(self) -> u32 {
        self.manifest.total_chunks
    }

    #[must_use]
    pub const fn total_records(self) -> u64 {
        self.manifest.total_records
    }
}

/// A tracker-issued capability to advance this node's durable checkpoint.
///
/// All fields are private so downstream code cannot construct a prefix from an
/// arbitrary LSN. Persist only values obtained from a tracker accessor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletedPrefix {
    tracker_id: Uuid,
    identity: NodeIdentity,
    end_lsn: PgLsn,
}

impl CompletedPrefix {
    #[must_use]
    pub const fn identity(self) -> NodeIdentity {
        self.identity
    }

    #[must_use]
    pub const fn node_id(self) -> i32 {
        self.identity.node_id
    }

    #[must_use]
    pub const fn system_identifier(self) -> u64 {
        self.identity.system_identifier
    }

    #[must_use]
    pub const fn timeline(self) -> u32 {
        self.identity.timeline
    }

    #[must_use]
    pub const fn end_lsn(self) -> PgLsn {
        self.end_lsn
    }
}

/// Describes an out-of-order completion blocked by an earlier transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompletionGap {
    identity: NodeIdentity,
    blocking_end_lsn: PgLsn,
    completed_after_lsn: PgLsn,
    blocking_missing_records: u64,
}

impl CompletionGap {
    #[must_use]
    pub const fn identity(self) -> NodeIdentity {
        self.identity
    }

    #[must_use]
    pub const fn blocking_end_lsn(self) -> PgLsn {
        self.blocking_end_lsn
    }

    #[must_use]
    pub const fn completed_after_lsn(self) -> PgLsn {
        self.completed_after_lsn
    }

    #[must_use]
    pub const fn blocking_missing_records(self) -> u64 {
        self.blocking_missing_records
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum CompletionError {
    #[error("node {actual} does not match completion tracker node {expected}")]
    WrongNode { expected: i32, actual: i32 },
    #[error("PostgreSQL system identifier does not match the completion tracker")]
    WrongSystemIdentifier,
    #[error("timeline {actual} does not match completion tracker timeline {expected}")]
    WrongTimeline { expected: u32, actual: u32 },
    #[error("transaction token was issued by a different completion tracker")]
    ForeignToken,
    #[error("transaction end LSN {actual} is not after {previous}")]
    NonMonotonicLsn { previous: PgLsn, actual: PgLsn },
    #[error("a transaction with records must declare at least one chunk")]
    MissingChunks,
    #[error("completion tracker exhausted its transaction sequence")]
    SequenceExhausted,
    #[error("transaction token is not registered with this completion tracker")]
    UnknownTransaction,
    #[error("chunk index {chunk_index} is outside the declared chunk count {total_chunks}")]
    ChunkOutOfRange { chunk_index: u32, total_chunks: u32 },
    #[error("completed record range must not be empty")]
    EmptyRecordRange,
    #[error("completed record range {start}..{end} exceeds total record count {total_records}")]
    RecordRangeOutOfBounds {
        start: u64,
        end: u64,
        total_records: u64,
    },
}

#[derive(Debug)]
struct TransactionState {
    manifest: CompletionManifest,
    completed_ranges: BTreeMap<u64, u64>,
    covered_records: u64,
    completed_chunks: BTreeMap<u32, ()>,
    explicitly_completed: bool,
}

impl TransactionState {
    fn new(manifest: CompletionManifest) -> Self {
        Self {
            manifest,
            completed_ranges: BTreeMap::new(),
            covered_records: 0,
            completed_chunks: BTreeMap::new(),
            explicitly_completed: false,
        }
    }

    fn mark_range(&mut self, chunk_index: u32, range: Range<u64>) {
        self.completed_chunks.insert(chunk_index, ());

        let mut merged_start = range.start;
        let mut merged_end = range.end;
        let overlaps = self
            .completed_ranges
            .range(..=range.end)
            .filter(|(_, end)| **end >= range.start)
            .map(|(start, end)| (*start, *end))
            .collect::<Vec<_>>();

        for (start, end) in overlaps {
            self.completed_ranges.remove(&start);
            self.covered_records -= end - start;
            merged_start = merged_start.min(start);
            merged_end = merged_end.max(end);
        }

        self.completed_ranges.insert(merged_start, merged_end);
        self.covered_records += merged_end - merged_start;
    }

    const fn is_complete(&self) -> bool {
        self.explicitly_completed
            || (self.manifest.total_records > 0
                && self.covered_records == self.manifest.total_records)
    }

    const fn missing_records(&self) -> u64 {
        self.manifest
            .total_records
            .saturating_sub(self.covered_records)
    }
}

/// Tracks the contiguous completed prefix for exactly one source node identity.
#[derive(Debug)]
pub struct NodeCompletionTracker {
    tracker_id: Uuid,
    identity: NodeIdentity,
    prefix_lsn: PgLsn,
    last_registered_lsn: PgLsn,
    next_sequence: u64,
    retired_through: u64,
    pending: BTreeMap<u64, TransactionState>,
}

impl NodeCompletionTracker {
    #[must_use]
    pub fn new(identity: NodeIdentity, initial_prefix: PgLsn) -> Self {
        Self {
            tracker_id: Uuid::new_v4(),
            identity,
            prefix_lsn: initial_prefix,
            last_registered_lsn: initial_prefix,
            next_sequence: 1,
            retired_through: 0,
            pending: BTreeMap::new(),
        }
    }

    #[must_use]
    pub const fn identity(&self) -> NodeIdentity {
        self.identity
    }

    /// Returns the currently completed prefix, including the initial prefix.
    #[must_use]
    pub const fn completed_prefix(&self) -> CompletedPrefix {
        CompletedPrefix {
            tracker_id: self.tracker_id,
            identity: self.identity,
            end_lsn: self.prefix_lsn,
        }
    }

    pub fn register(
        &mut self,
        manifest: CompletionManifest,
    ) -> Result<TransactionToken, CompletionError> {
        self.validate_identity(manifest.identity)?;
        if manifest.end_lsn <= self.last_registered_lsn {
            return Err(CompletionError::NonMonotonicLsn {
                previous: self.last_registered_lsn,
                actual: manifest.end_lsn,
            });
        }
        if manifest.total_records > 0 && manifest.total_chunks == 0 {
            return Err(CompletionError::MissingChunks);
        }

        let sequence = self.next_sequence;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or(CompletionError::SequenceExhausted)?;
        self.pending
            .insert(sequence, TransactionState::new(manifest));
        self.last_registered_lsn = manifest.end_lsn;

        Ok(TransactionToken {
            tracker_id: self.tracker_id,
            sequence,
            manifest,
        })
    }

    /// Records one durably applied record range for a chunk.
    ///
    /// Ranges are half-open offsets in the transaction's record stream. They
    /// may arrive out of order or overlap; duplicate reports are idempotent.
    pub fn mark_chunk_completed(
        &mut self,
        token: &TransactionToken,
        chunk_index: u32,
        record_range: Range<u64>,
    ) -> Result<Option<CompletedPrefix>, CompletionError> {
        self.validate_token(token)?;
        if token.sequence <= self.retired_through {
            return Ok(None);
        }
        if chunk_index >= token.manifest.total_chunks {
            return Err(CompletionError::ChunkOutOfRange {
                chunk_index,
                total_chunks: token.manifest.total_chunks,
            });
        }
        if record_range.start >= record_range.end {
            return Err(CompletionError::EmptyRecordRange);
        }
        if record_range.end > token.manifest.total_records {
            return Err(CompletionError::RecordRangeOutOfBounds {
                start: record_range.start,
                end: record_range.end,
                total_records: token.manifest.total_records,
            });
        }

        let state = self
            .pending
            .get_mut(&token.sequence)
            .ok_or(CompletionError::UnknownTransaction)?;
        state.mark_range(chunk_index, record_range);
        Ok(self.advance_prefix())
    }

    /// Marks a transaction complete using an external durable whole-transaction proof.
    ///
    /// This is also the completion path for transactions with zero records.
    pub fn mark_completed(
        &mut self,
        token: &TransactionToken,
    ) -> Result<Option<CompletedPrefix>, CompletionError> {
        self.validate_token(token)?;
        if token.sequence <= self.retired_through {
            return Ok(None);
        }
        let state = self
            .pending
            .get_mut(&token.sequence)
            .ok_or(CompletionError::UnknownTransaction)?;
        state.explicitly_completed = true;
        Ok(self.advance_prefix())
    }

    /// Returns a gap only when a completed transaction is blocked by an earlier one.
    #[must_use]
    pub fn gap(&self) -> Option<CompletionGap> {
        let (&blocking_sequence, blocking) = self.pending.first_key_value()?;
        if blocking.is_complete() {
            return None;
        }
        let completed_after = self
            .pending
            .range((blocking_sequence + 1)..)
            .find_map(|(_, state)| state.is_complete().then_some(state))?;

        Some(CompletionGap {
            identity: self.identity,
            blocking_end_lsn: blocking.manifest.end_lsn,
            completed_after_lsn: completed_after.manifest.end_lsn,
            blocking_missing_records: blocking.missing_records(),
        })
    }

    fn validate_identity(&self, actual: NodeIdentity) -> Result<(), CompletionError> {
        if actual.node_id != self.identity.node_id {
            return Err(CompletionError::WrongNode {
                expected: self.identity.node_id,
                actual: actual.node_id,
            });
        }
        if actual.system_identifier != self.identity.system_identifier {
            return Err(CompletionError::WrongSystemIdentifier);
        }
        if actual.timeline != self.identity.timeline {
            return Err(CompletionError::WrongTimeline {
                expected: self.identity.timeline,
                actual: actual.timeline,
            });
        }
        Ok(())
    }

    fn validate_token(&self, token: &TransactionToken) -> Result<(), CompletionError> {
        self.validate_identity(token.manifest.identity)?;
        if token.tracker_id != self.tracker_id {
            return Err(CompletionError::ForeignToken);
        }
        Ok(())
    }

    fn advance_prefix(&mut self) -> Option<CompletedPrefix> {
        let mut advanced = false;
        loop {
            let next_sequence = self.retired_through + 1;
            let Some(state) = self.pending.get(&next_sequence) else {
                break;
            };
            if !state.is_complete() {
                break;
            }

            let state = self
                .pending
                .remove(&next_sequence)
                .expect("the next transaction was just observed");
            self.retired_through = next_sequence;
            self.prefix_lsn = state.manifest.end_lsn;
            advanced = true;
        }

        advanced.then(|| self.completed_prefix())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const IDENTITY: NodeIdentity = NodeIdentity::new(1, 99, 3);

    fn manifest(identity: NodeIdentity, end_lsn: u64, records: u64) -> CompletionManifest {
        CompletionManifest::new(identity, PgLsn::new(end_lsn), 2, records)
    }

    #[test]
    fn later_completion_creates_gap_until_head_completes() {
        let mut tracker = NodeCompletionTracker::new(IDENTITY, PgLsn::new(5));
        let first = tracker.register(manifest(IDENTITY, 10, 4)).unwrap();
        let second = tracker.register(manifest(IDENTITY, 20, 4)).unwrap();

        assert_eq!(tracker.mark_completed(&second).unwrap(), None);
        let gap = tracker
            .gap()
            .expect("the completed second transaction is blocked");
        assert_eq!(gap.blocking_end_lsn(), PgLsn::new(10));
        assert_eq!(gap.completed_after_lsn(), PgLsn::new(20));
        assert_eq!(gap.blocking_missing_records(), 4);

        let prefix = tracker
            .mark_completed(&first)
            .unwrap()
            .expect("both transactions are now a contiguous prefix");
        assert_eq!(prefix.end_lsn(), PgLsn::new(20));
        assert_eq!(prefix.identity(), IDENTITY);
        assert_eq!(tracker.gap(), None);
    }

    #[test]
    fn duplicate_completion_is_idempotent_before_and_after_retirement() {
        let mut tracker = NodeCompletionTracker::new(IDENTITY, PgLsn::ZERO);
        let first = tracker.register(manifest(IDENTITY, 10, 2)).unwrap();
        let second = tracker.register(manifest(IDENTITY, 20, 2)).unwrap();

        assert_eq!(tracker.mark_completed(&second).unwrap(), None);
        assert_eq!(tracker.mark_completed(&second).unwrap(), None);
        assert_eq!(
            tracker.mark_completed(&first).unwrap().unwrap().end_lsn(),
            PgLsn::new(20)
        );
        assert_eq!(tracker.mark_completed(&first).unwrap(), None);
        assert_eq!(tracker.mark_completed(&second).unwrap(), None);
        assert_eq!(tracker.completed_prefix().end_lsn(), PgLsn::new(20));
    }

    #[test]
    fn rejects_wrong_node_and_database_identity() {
        let mut tracker = NodeCompletionTracker::new(IDENTITY, PgLsn::ZERO);
        assert!(matches!(
            tracker.register(manifest(NodeIdentity::new(2, 99, 3), 10, 1)),
            Err(CompletionError::WrongNode { .. })
        ));
        assert_eq!(
            tracker.register(manifest(NodeIdentity::new(1, 100, 3), 10, 1)),
            Err(CompletionError::WrongSystemIdentifier)
        );
        assert!(matches!(
            tracker.register(manifest(NodeIdentity::new(1, 99, 4), 10, 1)),
            Err(CompletionError::WrongTimeline { .. })
        ));

        let mut other = NodeCompletionTracker::new(NodeIdentity::new(1, 100, 3), PgLsn::ZERO);
        let foreign = other
            .register(manifest(NodeIdentity::new(1, 100, 3), 10, 1))
            .unwrap();
        assert_eq!(
            tracker.mark_completed(&foreign),
            Err(CompletionError::WrongSystemIdentifier)
        );
    }

    #[test]
    fn rejects_lsn_regression_and_duplicate_lsn_registration() {
        let mut tracker = NodeCompletionTracker::new(IDENTITY, PgLsn::new(5));
        tracker.register(manifest(IDENTITY, 20, 1)).unwrap();

        assert!(matches!(
            tracker.register(manifest(IDENTITY, 10, 1)),
            Err(CompletionError::NonMonotonicLsn { .. })
        ));
        assert!(matches!(
            tracker.register(manifest(IDENTITY, 20, 1)),
            Err(CompletionError::NonMonotonicLsn { .. })
        ));
    }

    #[test]
    fn nodes_advance_independently_and_tokens_cannot_be_mixed() {
        let second_identity = NodeIdentity::new(2, 199, 1);
        let mut first_node = NodeCompletionTracker::new(IDENTITY, PgLsn::ZERO);
        let mut second_node = NodeCompletionTracker::new(second_identity, PgLsn::new(100));
        let first = first_node.register(manifest(IDENTITY, 10, 1)).unwrap();
        let second = second_node
            .register(manifest(second_identity, 120, 1))
            .unwrap();

        assert!(matches!(
            first_node.mark_completed(&second),
            Err(CompletionError::WrongNode { .. })
        ));
        assert_eq!(
            second_node
                .mark_completed(&second)
                .unwrap()
                .unwrap()
                .end_lsn(),
            PgLsn::new(120)
        );
        assert_eq!(first_node.completed_prefix().end_lsn(), PgLsn::ZERO);
        assert_eq!(
            first_node
                .mark_completed(&first)
                .unwrap()
                .unwrap()
                .end_lsn(),
            PgLsn::new(10)
        );
    }

    #[test]
    fn out_of_order_and_overlapping_record_ranges_complete_exactly_once() {
        let mut tracker = NodeCompletionTracker::new(IDENTITY, PgLsn::ZERO);
        let token = tracker.register(manifest(IDENTITY, 50, 10)).unwrap();

        assert_eq!(
            tracker.mark_chunk_completed(&token, 1, 5..10).unwrap(),
            None
        );
        assert_eq!(
            tracker.mark_chunk_completed(&token, 1, 5..10).unwrap(),
            None
        );
        let prefix = tracker
            .mark_chunk_completed(&token, 0, 0..7)
            .unwrap()
            .expect("the union covers every record");
        assert_eq!(prefix.end_lsn(), PgLsn::new(50));
        assert_eq!(tracker.mark_chunk_completed(&token, 0, 0..7).unwrap(), None);
    }

    #[test]
    fn same_identity_token_from_another_tracker_is_rejected() {
        let mut first = NodeCompletionTracker::new(IDENTITY, PgLsn::ZERO);
        let mut second = NodeCompletionTracker::new(IDENTITY, PgLsn::ZERO);
        let token = second.register(manifest(IDENTITY, 10, 1)).unwrap();

        assert_eq!(
            first.mark_completed(&token),
            Err(CompletionError::ForeignToken)
        );
    }
}
