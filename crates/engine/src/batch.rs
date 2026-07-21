//! Bounded, node-ordered change batching.

use std::{collections::HashMap, time::Duration};

use cloudberry_etl_core::lsn::PgLsn;
use cloudberry_etl_source_postgres::wal::CommittedTransaction;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    pub max_rows: usize,
    pub max_bytes: usize,
    pub max_delay: Duration,
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self {
            max_rows: 10_000,
            max_bytes: 16 * 1024 * 1024,
            max_delay: Duration::from_millis(250),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BatchError {
    #[error("batch limits must be non-zero")]
    InvalidLimits,
    #[error("transactions from node {actual} cannot enter a node {expected} batch")]
    MixedNodes { expected: i32, actual: i32 },
    #[error(
        "source node {node_id} changed PostgreSQL system identifier from {expected} to {actual}"
    )]
    MixedSystems {
        node_id: i32,
        expected: u64,
        actual: u64,
    },
    #[error("source node {node_id} changed timeline from {expected} to {actual}")]
    MixedTimelines {
        node_id: i32,
        expected: u32,
        actual: u32,
    },
    #[error("transaction LSN did not advance beyond {previous}: got {actual}")]
    NonMonotonicLsn { previous: String, actual: String },
    #[error("transaction change source failed: {0}")]
    ChangeSource(String),
}

#[derive(Debug, Clone)]
pub struct TransactionBatch {
    transactions: Vec<CommittedTransaction>,
    row_count: usize,
    estimated_bytes: usize,
    has_generation_barrier: bool,
}

impl TransactionBatch {
    #[must_use]
    pub fn transactions(&self) -> &[CommittedTransaction] {
        &self.transactions
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub const fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    #[must_use]
    pub const fn has_generation_barrier(&self) -> bool {
        self.has_generation_barrier
    }

    #[must_use]
    pub fn final_transaction(&self) -> &CommittedTransaction {
        self.transactions
            .last()
            .expect("a constructed batch always has a transaction")
    }
}

#[derive(Debug)]
pub struct Batcher {
    limits: BatchLimits,
    pending: Vec<CommittedTransaction>,
    pending_rows: usize,
    pending_bytes: usize,
    pending_node_id: Option<i32>,
    node_watermarks: HashMap<i32, NodeWatermark>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NodeWatermark {
    system_identifier: u64,
    timeline: u32,
    end_lsn: PgLsn,
}

impl Batcher {
    pub fn new(limits: BatchLimits) -> Result<Self, BatchError> {
        if limits.max_rows == 0 || limits.max_bytes == 0 || limits.max_delay.is_zero() {
            return Err(BatchError::InvalidLimits);
        }
        Ok(Self {
            limits,
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            pending_node_id: None,
            node_watermarks: HashMap::new(),
        })
    }

    pub fn push<T>(&mut self, transaction: T) -> Result<Option<TransactionBatch>, BatchError>
    where
        T: Into<CommittedTransaction>,
    {
        let transaction = transaction.into();
        self.validate_order(&transaction)?;
        let stats = transaction
            .change_source
            .stats()
            .map_err(|error| BatchError::ChangeSource(error.to_string()))?;
        let rows = usize::try_from(stats.row_count).unwrap_or(usize::MAX);
        let bytes = usize::try_from(stats.encoded_bytes).unwrap_or(usize::MAX);
        let barrier = stats.has_generation_barrier;
        let must_flush = !self.pending.is_empty()
            && (barrier
                || self.pending_rows.saturating_add(rows) > self.limits.max_rows
                || self.pending_bytes.saturating_add(bytes) > self.limits.max_bytes
                // Empty transactions still carry a commit position; cap their count so they
                // cannot accumulate indefinitely when a source emits no row changes.
                || self.pending.len() >= self.limits.max_rows);
        let full = must_flush.then(|| self.take_pending());

        let position = &transaction.final_position;
        self.pending_node_id = Some(position.node_id);
        self.node_watermarks.insert(
            position.node_id,
            NodeWatermark {
                system_identifier: position.system_identifier,
                timeline: position.timeline,
                end_lsn: position.lsn,
            },
        );
        self.pending_rows = self.pending_rows.saturating_add(rows);
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.pending.push(transaction);
        Ok(full)
    }

    #[must_use]
    pub fn flush(&mut self) -> Option<TransactionBatch> {
        (!self.pending.is_empty()).then(|| self.take_pending())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    #[must_use]
    pub const fn max_delay(&self) -> Duration {
        self.limits.max_delay
    }

    fn validate_order(&self, transaction: &CommittedTransaction) -> Result<(), BatchError> {
        let position = &transaction.final_position;
        if let Some(node_id) = self.pending_node_id
            && node_id != position.node_id
        {
            return Err(BatchError::MixedNodes {
                expected: node_id,
                actual: position.node_id,
            });
        }
        if let Some(previous) = self.node_watermarks.get(&position.node_id) {
            if previous.system_identifier != position.system_identifier {
                return Err(BatchError::MixedSystems {
                    node_id: position.node_id,
                    expected: previous.system_identifier,
                    actual: position.system_identifier,
                });
            }
            if previous.timeline != position.timeline {
                return Err(BatchError::MixedTimelines {
                    node_id: position.node_id,
                    expected: previous.timeline,
                    actual: position.timeline,
                });
            }
            if position.lsn <= previous.end_lsn {
                return Err(BatchError::NonMonotonicLsn {
                    previous: previous.end_lsn.to_string(),
                    actual: position.lsn.to_string(),
                });
            }
        }
        Ok(())
    }

    fn take_pending(&mut self) -> TransactionBatch {
        let transactions = std::mem::take(&mut self.pending);
        let row_count = std::mem::take(&mut self.pending_rows);
        let estimated_bytes = std::mem::take(&mut self.pending_bytes);
        let has_generation_barrier = transactions.iter().any(|transaction| {
            transaction
                .change_source
                .stats()
                .is_ok_and(|stats| stats.has_generation_barrier)
        });
        self.pending_node_id = None;
        TransactionBatch {
            transactions,
            row_count,
            estimated_bytes,
            has_generation_barrier,
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use cloudberry_etl_core::{
        change::{DdlMessage, SourcePosition, SourceTransaction, TransactionChange},
        lsn::PgLsn,
    };

    use super::*;

    fn transaction(lsn: u64, changes: Vec<TransactionChange>) -> SourceTransaction {
        transaction_at(1, 9, 1, lsn, changes)
    }

    fn transaction_at(
        node_id: i32,
        system_identifier: u64,
        timeline: u32,
        lsn: u64,
        changes: Vec<TransactionChange>,
    ) -> SourceTransaction {
        SourceTransaction {
            xid: lsn as u32,
            commit_time: Utc::now(),
            final_position: SourcePosition {
                node_id,
                system_identifier,
                timeline,
                lsn: PgLsn::new(lsn),
            },
            changes,
        }
    }

    #[test]
    fn generation_barrier_flushes_prior_transactions() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction(1, vec![])).unwrap();
        let ddl = TransactionChange::Ddl(DdlMessage {
            version: 1,
            command_tag: "ALTER TABLE".into(),
            relation_ids: vec![7],
            affected_schemas: vec!["public".into()],
            schema_fingerprint: "abc".into(),
        });
        let full = batcher.push(transaction(2, vec![ddl])).unwrap().unwrap();
        assert_eq!(full.transactions().len(), 1);

        let barrier = batcher.flush().unwrap();
        assert!(barrier.has_generation_barrier());
    }

    #[test]
    fn rejects_non_increasing_transaction_lsn() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction(2, vec![])).unwrap();
        assert!(matches!(
            batcher.push(transaction(2, vec![])),
            Err(BatchError::NonMonotonicLsn { .. })
        ));
        assert!(matches!(
            batcher.push(transaction(1, vec![])),
            Err(BatchError::NonMonotonicLsn { .. })
        ));
    }

    #[test]
    fn rejects_non_increasing_lsn_after_flush() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction(5, vec![])).unwrap();
        assert!(batcher.flush().is_some());
        assert!(batcher.is_empty());

        assert!(matches!(
            batcher.push(transaction(5, vec![])),
            Err(BatchError::NonMonotonicLsn { .. })
        ));
        assert!(matches!(
            batcher.push(transaction(4, vec![])),
            Err(BatchError::NonMonotonicLsn { .. })
        ));
        assert!(batcher.push(transaction(6, vec![])).unwrap().is_none());
    }

    #[test]
    fn tracks_lsn_watermarks_independently_per_node() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction_at(1, 9, 1, 100, vec![])).unwrap();
        batcher.flush().unwrap();

        assert!(
            batcher
                .push(transaction_at(2, 10, 3, 1, vec![]))
                .unwrap()
                .is_none()
        );
        batcher.flush().unwrap();
        assert!(
            batcher
                .push(transaction_at(1, 9, 1, 101, vec![]))
                .unwrap()
                .is_none()
        );
        batcher.flush().unwrap();
        assert!(matches!(
            batcher.push(transaction_at(2, 10, 3, 1, vec![])),
            Err(BatchError::NonMonotonicLsn { .. })
        ));
    }

    #[test]
    fn rejects_node_identity_changes_across_flushes() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction_at(1, 9, 1, 10, vec![])).unwrap();
        batcher.flush().unwrap();

        assert!(matches!(
            batcher.push(transaction_at(1, 10, 1, 11, vec![])),
            Err(BatchError::MixedSystems {
                node_id: 1,
                expected: 9,
                actual: 10
            })
        ));
        assert!(matches!(
            batcher.push(transaction_at(1, 9, 2, 11, vec![])),
            Err(BatchError::MixedTimelines {
                node_id: 1,
                expected: 1,
                actual: 2
            })
        ));
    }

    #[test]
    fn rejects_mixed_nodes_only_while_a_batch_is_pending() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction_at(1, 9, 1, 10, vec![])).unwrap();
        assert!(matches!(
            batcher.push(transaction_at(2, 10, 1, 1, vec![])),
            Err(BatchError::MixedNodes {
                expected: 1,
                actual: 2
            })
        ));
        batcher.flush().unwrap();
        assert!(
            batcher
                .push(transaction_at(2, 10, 1, 1, vec![]))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn empty_transactions_are_bounded_by_the_row_limit() {
        let mut batcher = Batcher::new(BatchLimits {
            max_rows: 2,
            max_bytes: 1024,
            max_delay: Duration::from_secs(1),
        })
        .unwrap();
        assert!(batcher.push(transaction(1, vec![])).unwrap().is_none());
        assert!(batcher.push(transaction(2, vec![])).unwrap().is_none());
        let full = batcher.push(transaction(3, vec![])).unwrap().unwrap();
        assert_eq!(full.transactions().len(), 2);
        assert_eq!(full.row_count(), 0);
    }
}
