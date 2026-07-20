//! Count and primary-key chunk reconciliation.

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use thiserror::Error;

use cloudberry_etl_core::change::Cell;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRow {
    pub key: Vec<Bytes>,
    pub values: Vec<Cell>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDigest {
    pub row_count: u64,
    pub sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileResult {
    Match(ChunkDigest),
    Mismatch {
        source: ChunkDigest,
        target: ChunkDigest,
    },
}

#[derive(Debug, Error)]
pub enum ReconcileError {
    #[error("source reconciliation read failed: {0}")]
    Source(String),
    #[error("target reconciliation read failed: {0}")]
    Target(String),
    #[error("unchanged TOAST cannot appear in a materialized reconciliation row")]
    UnchangedToast,
}

#[async_trait]
pub trait ChunkReader: Send + Sync {
    async fn read_chunk(
        &self,
        start_after: Option<&[Bytes]>,
    ) -> Result<Vec<CanonicalRow>, ReconcileError>;
}

pub async fn compare_chunk(
    source: &dyn ChunkReader,
    target: &dyn ChunkReader,
    start_after: Option<&[Bytes]>,
) -> Result<ReconcileResult, ReconcileError> {
    let (source_rows, target_rows) = tokio::try_join!(
        source.read_chunk(start_after),
        target.read_chunk(start_after)
    )?;
    let source = digest_rows(&source_rows)?;
    let target = digest_rows(&target_rows)?;
    if source == target {
        Ok(ReconcileResult::Match(source))
    } else {
        Ok(ReconcileResult::Mismatch { source, target })
    }
}

pub fn digest_rows(rows: &[CanonicalRow]) -> Result<ChunkDigest, ReconcileError> {
    let mut hasher = Sha256::new();
    for row in rows {
        hasher.update((row.key.len() as u64).to_be_bytes());
        for key in &row.key {
            hash_bytes(&mut hasher, 1, key);
        }
        hasher.update((row.values.len() as u64).to_be_bytes());
        for value in &row.values {
            match value {
                Cell::Null => hasher.update([0]),
                Cell::Text(value) => hash_bytes(&mut hasher, 2, value),
                Cell::Binary(value) => hash_bytes(&mut hasher, 3, value),
                Cell::UnchangedToast => return Err(ReconcileError::UnchangedToast),
            }
        }
    }
    Ok(ChunkDigest {
        row_count: rows.len() as u64,
        sha256: hasher.finalize().into(),
    })
}

fn hash_bytes(hasher: &mut Sha256, tag: u8, value: &[u8]) {
    hasher.update([tag]);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_prefix_prevents_ambiguous_rows() {
        let first = CanonicalRow {
            key: vec![Bytes::from_static(b"a")],
            values: vec![Cell::Text(Bytes::from_static(b"bc"))],
        };
        let second = CanonicalRow {
            key: vec![Bytes::from_static(b"ab")],
            values: vec![Cell::Text(Bytes::from_static(b"c"))],
        };
        assert_ne!(
            digest_rows(&[first]).unwrap(),
            digest_rows(&[second]).unwrap()
        );
    }
}
