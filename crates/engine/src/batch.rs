//! Bounded, node-ordered change batching.

use std::time::Duration;

use cloudberry_etl_core::change::{Cell, RowChange, SourceTransaction, TransactionChange};
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
    #[error("transactions from different PostgreSQL system identifiers cannot share a batch")]
    MixedSystems,
    #[error("transaction LSN moved backwards from {previous} to {actual}")]
    NonMonotonicLsn { previous: String, actual: String },
}

#[derive(Debug, Clone)]
pub struct TransactionBatch {
    transactions: Vec<SourceTransaction>,
    row_count: usize,
    estimated_bytes: usize,
    has_generation_barrier: bool,
}

impl TransactionBatch {
    #[must_use]
    pub fn transactions(&self) -> &[SourceTransaction] {
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
    pub fn final_transaction(&self) -> &SourceTransaction {
        self.transactions
            .last()
            .expect("a constructed batch always has a transaction")
    }
}

#[derive(Debug)]
pub struct Batcher {
    limits: BatchLimits,
    pending: Vec<SourceTransaction>,
    pending_rows: usize,
    pending_bytes: usize,
    node_identity: Option<(i32, u64)>,
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
            node_identity: None,
        })
    }

    pub fn push(
        &mut self,
        transaction: SourceTransaction,
    ) -> Result<Option<TransactionBatch>, BatchError> {
        self.validate_order(&transaction)?;
        let rows = transaction_row_count(&transaction);
        let bytes = transaction_estimated_bytes(&transaction);
        let barrier = transaction
            .changes
            .iter()
            .any(TransactionChange::requires_generation_barrier);
        let must_flush = !self.pending.is_empty()
            && (barrier
                || self.pending_rows.saturating_add(rows) > self.limits.max_rows
                || self.pending_bytes.saturating_add(bytes) > self.limits.max_bytes);
        let full = must_flush.then(|| self.take_pending());

        self.node_identity = Some((
            transaction.final_position.node_id,
            transaction.final_position.system_identifier,
        ));
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

    fn validate_order(&self, transaction: &SourceTransaction) -> Result<(), BatchError> {
        if let Some((node_id, system_identifier)) = self.node_identity {
            if node_id != transaction.final_position.node_id {
                return Err(BatchError::MixedNodes {
                    expected: node_id,
                    actual: transaction.final_position.node_id,
                });
            }
            if system_identifier != transaction.final_position.system_identifier {
                return Err(BatchError::MixedSystems);
            }
        }
        if let Some(previous) = self.pending.last()
            && transaction.final_position.lsn < previous.final_position.lsn
        {
            return Err(BatchError::NonMonotonicLsn {
                previous: previous.final_position.lsn.to_string(),
                actual: transaction.final_position.lsn.to_string(),
            });
        }
        Ok(())
    }

    fn take_pending(&mut self) -> TransactionBatch {
        let transactions = std::mem::take(&mut self.pending);
        let row_count = std::mem::take(&mut self.pending_rows);
        let estimated_bytes = std::mem::take(&mut self.pending_bytes);
        let has_generation_barrier = transactions.iter().any(|transaction| {
            transaction
                .changes
                .iter()
                .any(TransactionChange::requires_generation_barrier)
        });
        self.node_identity = None;
        TransactionBatch {
            transactions,
            row_count,
            estimated_bytes,
            has_generation_barrier,
        }
    }
}

fn transaction_row_count(transaction: &SourceTransaction) -> usize {
    transaction
        .changes
        .iter()
        .filter(|change| matches!(change, TransactionChange::Row(_)))
        .count()
}

fn transaction_estimated_bytes(transaction: &SourceTransaction) -> usize {
    transaction
        .changes
        .iter()
        .map(|change| match change {
            TransactionChange::Row(change) => match &change.change {
                RowChange::Insert { new } => tuple_bytes(&new.cells),
                RowChange::Update { old_key, new } => {
                    old_key.as_ref().map_or(0, |key| tuple_bytes(&key.cells))
                        + tuple_bytes(&new.cells)
                }
                RowChange::Delete { old_key } => tuple_bytes(&old_key.cells),
            },
            TransactionChange::Truncate { relation_ids, .. } => relation_ids.len() * 4,
            TransactionChange::Ddl(message) => {
                message.command_tag.len()
                    + message.schema_fingerprint.len()
                    + message.relation_ids.len() * 4
            }
        })
        .sum()
}

fn tuple_bytes(cells: &[Cell]) -> usize {
    cells
        .iter()
        .map(|cell| match cell {
            Cell::Null | Cell::UnchangedToast => 1,
            Cell::Text(value) | Cell::Binary(value) => value.len(),
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use cloudberry_etl_core::{
        change::{DdlMessage, SourcePosition, TransactionChange},
        lsn::PgLsn,
    };

    use super::*;

    fn transaction(lsn: u64, changes: Vec<TransactionChange>) -> SourceTransaction {
        SourceTransaction {
            xid: lsn as u32,
            commit_time: Utc::now(),
            final_position: SourcePosition {
                node_id: 1,
                system_identifier: 9,
                timeline: 1,
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
            schema_fingerprint: "abc".into(),
        });
        let full = batcher.push(transaction(2, vec![ddl])).unwrap().unwrap();
        assert_eq!(full.transactions().len(), 1);

        let barrier = batcher.flush().unwrap();
        assert!(barrier.has_generation_barrier());
    }

    #[test]
    fn rejects_lsn_regression() {
        let mut batcher = Batcher::new(BatchLimits::default()).unwrap();
        batcher.push(transaction(2, vec![])).unwrap();
        assert!(matches!(
            batcher.push(transaction(1, vec![])),
            Err(BatchError::NonMonotonicLsn { .. })
        ));
    }
}
