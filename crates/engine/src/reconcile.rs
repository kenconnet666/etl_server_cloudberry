//! Pure, bounded planning, digest, and diff primitives for primary-key range
//! reconciliation.
//!
//! This validation-gated module does not open database sessions, acquire a
//! pipeline fence, establish an authoritative source snapshot, apply repairs,
//! or advance reconciliation state. A runtime may use these primitives only
//! inside the separately proven fence/session/repair protocol.

use std::{cmp::Ordering, collections::HashSet, mem::size_of};

use async_trait::async_trait;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use thiserror::Error;

use cloudberry_etl_core::change::Cell;

const DIGEST_FORMAT: &[u8] = b"pg2cb-reconcile-digest-v1";

/// A materialized row produced by a canonical SQL text reader.
///
/// Keys are non-NULL text by construction. Values are checked again by every
/// public operation so callers cannot accidentally pass pgoutput binary or
/// unchanged-TOAST cells through reconciliation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalRow {
    pub key: Vec<Bytes>,
    pub values: Vec<Cell>,
}

impl CanonicalRow {
    /// Converts materialized cells into a canonical row, rejecting every key
    /// representation other than non-NULL text.
    pub fn try_from_cells(key: Vec<Cell>, values: Vec<Cell>) -> Result<Self, ReconcileError> {
        let key = key
            .into_iter()
            .enumerate()
            .map(|(index, cell)| match cell {
                Cell::Text(value) => {
                    validate_utf8("key", index, &value)?;
                    Ok(value)
                }
                Cell::Null => Err(ReconcileError::NullKey { index }),
                Cell::Binary(_) => Err(ReconcileError::BinaryCell {
                    component: "key",
                    index,
                }),
                Cell::UnchangedToast => Err(ReconcileError::UnchangedToast {
                    component: "key",
                    index,
                }),
            })
            .collect::<Result<Vec<_>, _>>()?;

        for (index, value) in values.iter().enumerate() {
            validate_value(index, value)?;
        }

        Ok(Self { key, values })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestColumn {
    pub ordinal: u32,
    pub portable_type_tag: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DigestContext {
    pub version_domain: String,
    pub schema_fingerprint: String,
    pub key_columns: Vec<DigestColumn>,
    pub value_columns: Vec<DigestColumn>,
}

impl DigestContext {
    pub fn validate(&self) -> Result<(), ReconcileError> {
        if self.version_domain.is_empty() {
            return Err(ReconcileError::EmptyDigestDomain);
        }
        if self.schema_fingerprint.is_empty() {
            return Err(ReconcileError::EmptySchemaFingerprint);
        }
        if self.key_columns.is_empty() {
            return Err(ReconcileError::MissingKeyColumns);
        }
        validate_columns("key", &self.key_columns)?;
        validate_columns("value", &self.value_columns)
    }
}

/// One source-derived page. `next_key` is the final returned row key, not the
/// lookahead key. A non-empty page always carries it, including the final page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    pub rows: Vec<CanonicalRow>,
    pub has_more: bool,
    pub next_key: Option<Vec<Bytes>>,
}

impl Page {
    /// Builds a page from at most `row_limit + 1` source rows. The optional
    /// lookahead row is validated and then removed; rows are never re-sorted.
    /// `compare_keys` must represent the admitted typed PK ordering, not a
    /// lexical ordering invented from canonical text.
    pub fn from_source_rows<F>(
        context: &DigestContext,
        mut rows_with_lookahead: Vec<CanonicalRow>,
        row_limit: usize,
        max_page_bytes: usize,
        compare_keys: &F,
    ) -> Result<Self, ReconcileError>
    where
        F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
    {
        validate_positive_limit("source row", row_limit)?;
        validate_positive_limit("page byte", max_page_bytes)?;
        context.validate()?;
        let lookahead_limit = row_limit
            .checked_add(1)
            .ok_or(ReconcileError::LimitOverflow)?;
        if rows_with_lookahead.len() > lookahead_limit {
            return Err(ReconcileError::SourcePageContract(format!(
                "reader returned {} rows for a row limit of {row_limit}",
                rows_with_lookahead.len()
            )));
        }

        validate_rows(context, &rows_with_lookahead, compare_keys)?;
        validate_page_bytes("source", &rows_with_lookahead, max_page_bytes)?;
        let has_more = rows_with_lookahead.len() > row_limit;
        if has_more {
            rows_with_lookahead.truncate(row_limit);
        }
        let next_key = rows_with_lookahead.last().map(|row| row.key.clone());

        Ok(Self {
            rows: rows_with_lookahead,
            has_more,
            next_key,
        })
    }

    /// Derives the only target range that may be compared with this page.
    /// Public fields make deserialization convenient, so this method rejects
    /// an invalid `has_more`/`next_key` shape before choosing an unbounded end.
    pub fn target_range(&self, start_after: Option<&[Bytes]>) -> Result<KeyRange, ReconcileError> {
        self.validate_basic_shape()?;
        Ok(KeyRange {
            start_exclusive: start_after.map(<[Bytes]>::to_vec),
            end_inclusive: self.has_more.then(|| self.next_key.clone()).flatten(),
        })
    }

    fn validate_basic_shape(&self) -> Result<(), ReconcileError> {
        if self.rows.is_empty() && self.has_more {
            return Err(ReconcileError::SourcePageContract(
                "an empty source page cannot have more rows".to_owned(),
            ));
        }
        let expected_next_key = self.rows.last().map(|row| &row.key);
        if expected_next_key != self.next_key.as_ref() {
            return Err(ReconcileError::SourcePageContract(
                "next_key must equal the final returned row key".to_owned(),
            ));
        }
        Ok(())
    }
}

/// A target key range in database PK order: `(start_exclusive, end_inclusive]`.
/// A missing end means the source page reached the table tail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRange {
    pub start_exclusive: Option<Vec<Bytes>>,
    pub end_inclusive: Option<Vec<Bytes>>,
}

/// Bounded target rows. Readers use one-row lookahead internally and set
/// `has_more` rather than returning more than the requested limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRows {
    pub rows: Vec<CanonicalRow>,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLimits {
    pub source_row_limit: usize,
    pub max_page_bytes: usize,
    pub max_repair_rows: usize,
}

impl PageLimits {
    fn validate(self) -> Result<(), ReconcileError> {
        validate_positive_limit("source row", self.source_row_limit)?;
        validate_positive_limit("page byte", self.max_page_bytes)?;
        validate_positive_limit("repair row", self.max_repair_rows)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChunkDigest {
    pub row_count: u64,
    pub sha256: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairOperation {
    Upsert(CanonicalRow),
    Delete(Vec<Bytes>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconcileResult {
    Match(ChunkDigest),
    Mismatch {
        source: ChunkDigest,
        target: ChunkDigest,
        repairs: Vec<RepairOperation>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageComparison {
    pub source_page: Page,
    pub target_range: KeyRange,
    pub result: ReconcileResult,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ReconcileError {
    #[error("source reconciliation read failed: {0}")]
    Source(String),
    #[error("target reconciliation read failed: {0}")]
    Target(String),
    #[error("{name} limit must be greater than zero")]
    InvalidLimit { name: &'static str },
    #[error("reconciliation limit arithmetic overflow")]
    LimitOverflow,
    #[error("digest version domain must not be empty")]
    EmptyDigestDomain,
    #[error("schema fingerprint must not be empty")]
    EmptySchemaFingerprint,
    #[error("digest context must contain at least one key column")]
    MissingKeyColumns,
    #[error("{component} column {ordinal} has an empty portable type tag")]
    EmptyPortableTypeTag {
        component: &'static str,
        ordinal: u32,
    },
    #[error("duplicate {component} column ordinal {ordinal} in digest context")]
    DuplicateColumnOrdinal {
        component: &'static str,
        ordinal: u32,
    },
    #[error("row {row} has {actual} {component} cells; expected {expected}")]
    Arity {
        row: usize,
        component: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("key column {index} is NULL")]
    NullKey { index: usize },
    #[error("binary {component} cell at column {index} is not canonical text")]
    BinaryCell {
        component: &'static str,
        index: usize,
    },
    #[error("unchanged TOAST {component} cell at column {index} is not materialized")]
    UnchangedToast {
        component: &'static str,
        index: usize,
    },
    #[error("{component} text at column {index} is not valid UTF-8")]
    InvalidUtf8 {
        component: &'static str,
        index: usize,
    },
    #[error("row {row} repeats the preceding primary key")]
    DuplicateKey { row: usize },
    #[error("row {row} is out of primary-key order")]
    KeyOutOfOrder { row: usize },
    #[error("source and target produced different canonical text for an equal primary key")]
    EqualKeyEncodingMismatch,
    #[error("source page contract violated: {0}")]
    SourcePageContract(String),
    #[error("target range contract violated: {0}")]
    TargetRangeContract(String),
    #[error("exact repair exceeds the configured maximum of {max_repair_rows} rows")]
    RepairLimitExceeded { max_repair_rows: usize },
    #[error(
        "{side} canonical page estimate of {estimated_bytes} bytes exceeds the configured maximum of {max_page_bytes} bytes"
    )]
    PageBytesExceeded {
        side: &'static str,
        estimated_bytes: usize,
        max_page_bytes: usize,
    },
    #[error("{side} canonical page byte estimate overflowed usize")]
    PageByteEstimateOverflow { side: &'static str },
    #[error("row or descriptor count cannot be represented in the digest format")]
    DigestLengthOverflow,
}

#[async_trait]
pub trait SourcePageReader: Send + Sync {
    /// Returns at most `row_limit` rows and reports whether source lookahead
    /// found another row. Implementations must use native database PK order
    /// and enforce `max_page_bytes` while decoding, before allocating an
    /// oversized value. They must define an explicit oversized-row policy;
    /// the post-read estimate in this module is only a second-line check.
    async fn read_page(
        &self,
        start_after: Option<&[Bytes]>,
        row_limit: usize,
        max_page_bytes: usize,
    ) -> Result<Page, ReconcileError>;
}

#[async_trait]
pub trait TargetRangeReader: Send + Sync {
    /// Reads exactly the supplied source-derived range in native database PK
    /// order, returning at most `row_limit` rows plus an exhaustion flag.
    /// `max_page_bytes` is an independent target budget and must be enforced
    /// during decoding, before allocation; hitting it cannot return a partial
    /// range for comparison and must follow the reader's oversized-row/error
    /// policy. Core validation below cannot undo an oversized allocation.
    async fn read_range(
        &self,
        range: &KeyRange,
        row_limit: usize,
        max_page_bytes: usize,
    ) -> Result<RangeRows, ReconcileError>;
}

/// Reads source first, derives its target boundary, and compares only that
/// range. `compare_keys` must come from the already-admitted typed database PK
/// semantics. Canonical-text byte ordering is suitable only for tests and is
/// not a production fallback; this module deliberately provides no default.
pub async fn compare_page<F>(
    source: &dyn SourcePageReader,
    target: &dyn TargetRangeReader,
    context: &DigestContext,
    start_after: Option<&[Bytes]>,
    limits: PageLimits,
    compare_keys: &F,
) -> Result<PageComparison, ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + Sync + ?Sized,
{
    limits.validate()?;
    context.validate()?;

    let source_page = source
        .read_page(start_after, limits.source_row_limit, limits.max_page_bytes)
        .await?;
    validate_page(
        context,
        &source_page,
        start_after,
        limits.source_row_limit,
        limits.max_page_bytes,
        compare_keys,
    )?;
    let target_range = source_page.target_range(start_after)?;

    // If more than source_rows + max_repair_rows target rows exist, at least
    // max_repair_rows + 1 of them must be target-only.
    let target_row_limit = source_page
        .rows
        .len()
        .checked_add(limits.max_repair_rows)
        .ok_or(ReconcileError::LimitOverflow)?;
    let target_rows = target
        .read_range(&target_range, target_row_limit, limits.max_page_bytes)
        .await?;
    validate_target_rows(
        context,
        &target_rows,
        &target_range,
        target_row_limit,
        limits.max_page_bytes,
        compare_keys,
    )?;
    if target_rows.has_more {
        return Err(ReconcileError::RepairLimitExceeded {
            max_repair_rows: limits.max_repair_rows,
        });
    }

    let source_digest = digest_rows(context, &source_page.rows, compare_keys)?;
    let target_digest = digest_rows(context, &target_rows.rows, compare_keys)?;
    let result = if source_digest == target_digest {
        ReconcileResult::Match(source_digest)
    } else {
        let repairs = exact_diff(
            context,
            &source_page.rows,
            &target_rows.rows,
            limits.max_repair_rows,
            compare_keys,
        )?;
        ReconcileResult::Mismatch {
            source: source_digest,
            target: target_digest,
            repairs,
        }
    };

    Ok(PageComparison {
        source_page,
        target_range,
        result,
    })
}

/// Produces a stable digest over the plan and materialized rows.
pub fn digest_rows<F>(
    context: &DigestContext,
    rows: &[CanonicalRow],
    compare_keys: &F,
) -> Result<ChunkDigest, ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
{
    context.validate()?;
    validate_rows(context, rows, compare_keys)?;

    let mut hasher = Sha256::new();
    hash_bytes(&mut hasher, DIGEST_FORMAT)?;
    hash_bytes(&mut hasher, context.version_domain.as_bytes())?;
    hash_bytes(&mut hasher, context.schema_fingerprint.as_bytes())?;
    hash_columns(&mut hasher, b'K', &context.key_columns)?;
    hash_columns(&mut hasher, b'V', &context.value_columns)?;
    hash_len(&mut hasher, rows.len())?;

    for row in rows {
        hasher.update(b"R");
        hash_len(&mut hasher, row.key.len())?;
        for (column, key) in context.key_columns.iter().zip(&row.key) {
            hash_typed_text(&mut hasher, b'K', column, key)?;
        }
        hash_len(&mut hasher, row.values.len())?;
        for (column, value) in context.value_columns.iter().zip(&row.values) {
            match value {
                Cell::Null => {
                    hasher.update(b"N");
                    hash_column_identity(&mut hasher, column)?;
                }
                Cell::Text(value) => hash_typed_text(&mut hasher, b'T', column, value)?,
                Cell::Binary(_) => {
                    return Err(ReconcileError::BinaryCell {
                        component: "value",
                        index: column.ordinal as usize,
                    });
                }
                Cell::UnchangedToast => {
                    return Err(ReconcileError::UnchangedToast {
                        component: "value",
                        index: column.ordinal as usize,
                    });
                }
            }
        }
    }

    Ok(ChunkDigest {
        row_count: u64::try_from(rows.len()).map_err(|_| ReconcileError::DigestLengthOverflow)?,
        sha256: hasher.finalize().into(),
    })
}

/// Computes a bounded, exact merge diff without building an unbounded key map.
pub fn exact_diff<F>(
    context: &DigestContext,
    source_rows: &[CanonicalRow],
    target_rows: &[CanonicalRow],
    max_repair_rows: usize,
    compare_keys: &F,
) -> Result<Vec<RepairOperation>, ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
{
    validate_positive_limit("repair row", max_repair_rows)?;
    context.validate()?;
    validate_rows(context, source_rows, compare_keys)?;
    validate_rows(context, target_rows, compare_keys)?;

    let mut repairs = Vec::with_capacity(
        max_repair_rows.min(source_rows.len().saturating_add(target_rows.len())),
    );
    let (mut source_index, mut target_index) = (0, 0);

    while source_index < source_rows.len() && target_index < target_rows.len() {
        let source = &source_rows[source_index];
        let target = &target_rows[target_index];
        match compare_keys(&source.key, &target.key) {
            Ordering::Less => {
                push_repair(
                    &mut repairs,
                    RepairOperation::Upsert(source.clone()),
                    max_repair_rows,
                )?;
                source_index += 1;
            }
            Ordering::Greater => {
                push_repair(
                    &mut repairs,
                    RepairOperation::Delete(target.key.clone()),
                    max_repair_rows,
                )?;
                target_index += 1;
            }
            Ordering::Equal => {
                if source.key != target.key {
                    return Err(ReconcileError::EqualKeyEncodingMismatch);
                }
                if source.values != target.values {
                    push_repair(
                        &mut repairs,
                        RepairOperation::Upsert(source.clone()),
                        max_repair_rows,
                    )?;
                }
                source_index += 1;
                target_index += 1;
            }
        }
    }

    for source in &source_rows[source_index..] {
        push_repair(
            &mut repairs,
            RepairOperation::Upsert(source.clone()),
            max_repair_rows,
        )?;
    }
    for target in &target_rows[target_index..] {
        push_repair(
            &mut repairs,
            RepairOperation::Delete(target.key.clone()),
            max_repair_rows,
        )?;
    }

    Ok(repairs)
}

fn validate_page<F>(
    context: &DigestContext,
    page: &Page,
    start_after: Option<&[Bytes]>,
    row_limit: usize,
    max_page_bytes: usize,
    compare_keys: &F,
) -> Result<(), ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
{
    if page.rows.len() > row_limit {
        return Err(ReconcileError::SourcePageContract(format!(
            "reader returned {} rows for a row limit of {row_limit}",
            page.rows.len()
        )));
    }
    page.validate_basic_shape()?;
    validate_rows(context, &page.rows, compare_keys)?;
    validate_page_bytes("source", &page.rows, max_page_bytes)?;
    validate_start_boundary(
        context,
        start_after,
        &page.rows,
        compare_keys,
        ReconcileError::SourcePageContract,
    )
}

fn validate_target_rows<F>(
    context: &DigestContext,
    target: &RangeRows,
    range: &KeyRange,
    row_limit: usize,
    max_page_bytes: usize,
    compare_keys: &F,
) -> Result<(), ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
{
    if target.rows.len() > row_limit {
        return Err(ReconcileError::TargetRangeContract(format!(
            "reader returned {} rows for a row limit of {row_limit}",
            target.rows.len()
        )));
    }
    if target.has_more && target.rows.len() != row_limit {
        return Err(ReconcileError::TargetRangeContract(
            "has_more requires a full target range page".to_owned(),
        ));
    }

    validate_rows(context, &target.rows, compare_keys)?;
    validate_page_bytes("target", &target.rows, max_page_bytes)?;
    validate_start_boundary(
        context,
        range.start_exclusive.as_deref(),
        &target.rows,
        compare_keys,
        ReconcileError::TargetRangeContract,
    )?;
    if let (Some(end), Some(last)) = (range.end_inclusive.as_deref(), target.rows.last())
        && compare_keys(&last.key, end) == Ordering::Greater
    {
        return Err(ReconcileError::TargetRangeContract(
            "reader returned a key beyond the source-derived end boundary".to_owned(),
        ));
    }
    Ok(())
}

fn validate_start_boundary<F>(
    context: &DigestContext,
    start_after: Option<&[Bytes]>,
    rows: &[CanonicalRow],
    compare_keys: &F,
    contract_error: fn(String) -> ReconcileError,
) -> Result<(), ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
{
    let Some(start_after) = start_after else {
        return Ok(());
    };
    validate_key_arity_and_text(context, start_after, 0)?;
    if let Some(first) = rows.first()
        && compare_keys(&first.key, start_after) != Ordering::Greater
    {
        return Err(contract_error(
            "reader returned a key outside the exclusive start boundary".to_owned(),
        ));
    }
    Ok(())
}

fn validate_rows<F>(
    context: &DigestContext,
    rows: &[CanonicalRow],
    compare_keys: &F,
) -> Result<(), ReconcileError>
where
    F: Fn(&[Bytes], &[Bytes]) -> Ordering + ?Sized,
{
    for (row_index, row) in rows.iter().enumerate() {
        validate_key_arity_and_text(context, &row.key, row_index)?;
        if row.values.len() != context.value_columns.len() {
            return Err(ReconcileError::Arity {
                row: row_index,
                component: "value",
                expected: context.value_columns.len(),
                actual: row.values.len(),
            });
        }
        for (index, value) in row.values.iter().enumerate() {
            validate_value(index, value)?;
        }

        if let Some(previous) = row_index.checked_sub(1).map(|index| &rows[index]) {
            match compare_keys(&previous.key, &row.key) {
                Ordering::Less => {}
                Ordering::Equal => {
                    return Err(ReconcileError::DuplicateKey { row: row_index });
                }
                Ordering::Greater => {
                    return Err(ReconcileError::KeyOutOfOrder { row: row_index });
                }
            }
        }
    }
    Ok(())
}

fn validate_key_arity_and_text(
    context: &DigestContext,
    key: &[Bytes],
    row: usize,
) -> Result<(), ReconcileError> {
    if key.len() != context.key_columns.len() {
        return Err(ReconcileError::Arity {
            row,
            component: "key",
            expected: context.key_columns.len(),
            actual: key.len(),
        });
    }
    for (index, value) in key.iter().enumerate() {
        validate_utf8("key", index, value)?;
    }
    Ok(())
}

fn validate_value(index: usize, value: &Cell) -> Result<(), ReconcileError> {
    match value {
        Cell::Null => Ok(()),
        Cell::Text(value) => validate_utf8("value", index, value),
        Cell::Binary(_) => Err(ReconcileError::BinaryCell {
            component: "value",
            index,
        }),
        Cell::UnchangedToast => Err(ReconcileError::UnchangedToast {
            component: "value",
            index,
        }),
    }
}

fn validate_utf8(
    component: &'static str,
    index: usize,
    value: &[u8],
) -> Result<(), ReconcileError> {
    std::str::from_utf8(value)
        .map(|_| ())
        .map_err(|_| ReconcileError::InvalidUtf8 { component, index })
}

fn validate_columns(
    component: &'static str,
    columns: &[DigestColumn],
) -> Result<(), ReconcileError> {
    let mut ordinals = HashSet::with_capacity(columns.len());
    for column in columns {
        if column.portable_type_tag.is_empty() {
            return Err(ReconcileError::EmptyPortableTypeTag {
                component,
                ordinal: column.ordinal,
            });
        }
        if !ordinals.insert(column.ordinal) {
            return Err(ReconcileError::DuplicateColumnOrdinal {
                component,
                ordinal: column.ordinal,
            });
        }
    }
    Ok(())
}

fn validate_positive_limit(name: &'static str, limit: usize) -> Result<(), ReconcileError> {
    if limit == 0 {
        Err(ReconcileError::InvalidLimit { name })
    } else {
        Ok(())
    }
}

/// Estimates retained canonical payload plus the row, cell, and `Bytes`
/// containers owned by the page. Shared backing allocation and allocator
/// metadata are intentionally not guessed, which is why readers must enforce
/// their transport/decoder budget before materialization as well.
fn estimate_materialized_bytes(rows: &[CanonicalRow]) -> Option<usize> {
    rows.iter().try_fold(0usize, |total, row| {
        let key_containers = row.key.len().checked_mul(size_of::<Bytes>())?;
        let value_containers = row.values.len().checked_mul(size_of::<Cell>())?;
        let key_payload = row
            .key
            .iter()
            .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?;
        let value_payload = row.values.iter().try_fold(0usize, |bytes, value| {
            let value_len = match value {
                Cell::Text(value) | Cell::Binary(value) => value.len(),
                Cell::Null | Cell::UnchangedToast => 0,
            };
            bytes.checked_add(value_len)
        })?;

        total
            .checked_add(size_of::<CanonicalRow>())?
            .checked_add(key_containers)?
            .checked_add(value_containers)?
            .checked_add(key_payload)?
            .checked_add(value_payload)
    })
}

fn validate_page_bytes(
    side: &'static str,
    rows: &[CanonicalRow],
    max_page_bytes: usize,
) -> Result<(), ReconcileError> {
    let estimated_bytes = estimate_materialized_bytes(rows)
        .ok_or(ReconcileError::PageByteEstimateOverflow { side })?;
    if estimated_bytes > max_page_bytes {
        return Err(ReconcileError::PageBytesExceeded {
            side,
            estimated_bytes,
            max_page_bytes,
        });
    }
    Ok(())
}

fn push_repair(
    repairs: &mut Vec<RepairOperation>,
    repair: RepairOperation,
    max_repair_rows: usize,
) -> Result<(), ReconcileError> {
    if repairs.len() == max_repair_rows {
        return Err(ReconcileError::RepairLimitExceeded { max_repair_rows });
    }
    repairs.push(repair);
    Ok(())
}

fn hash_columns(
    hasher: &mut Sha256,
    role: u8,
    columns: &[DigestColumn],
) -> Result<(), ReconcileError> {
    hasher.update([role]);
    hash_len(hasher, columns.len())?;
    for column in columns {
        hash_column_identity(hasher, column)?;
    }
    Ok(())
}

fn hash_typed_text(
    hasher: &mut Sha256,
    marker: u8,
    column: &DigestColumn,
    value: &[u8],
) -> Result<(), ReconcileError> {
    hasher.update([marker]);
    hash_column_identity(hasher, column)?;
    hash_bytes(hasher, value)
}

fn hash_column_identity(hasher: &mut Sha256, column: &DigestColumn) -> Result<(), ReconcileError> {
    hasher.update(column.ordinal.to_be_bytes());
    hash_bytes(hasher, column.portable_type_tag.as_bytes())
}

fn hash_len(hasher: &mut Sha256, len: usize) -> Result<(), ReconcileError> {
    let len = u64::try_from(len).map_err(|_| ReconcileError::DigestLengthOverflow)?;
    hasher.update(len.to_be_bytes());
    Ok(())
}

fn hash_bytes(hasher: &mut Sha256, value: &[u8]) -> Result<(), ReconcileError> {
    hash_len(hasher, value.len())?;
    hasher.update(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    const MAX_PAGE_BYTES: usize = 1024 * 1024;

    fn context() -> DigestContext {
        DigestContext {
            version_domain: "pg2cb-reconcile-v1".to_owned(),
            schema_fingerprint: "schema-1".to_owned(),
            key_columns: vec![DigestColumn {
                ordinal: 0,
                portable_type_tag: "pg_catalog.int8".to_owned(),
            }],
            value_columns: vec![DigestColumn {
                ordinal: 1,
                portable_type_tag: "pg_catalog.text".to_owned(),
            }],
        }
    }

    fn row(key: &'static [u8], value: Cell) -> CanonicalRow {
        CanonicalRow {
            key: vec![Bytes::from_static(key)],
            values: vec![value],
        }
    }

    fn text(value: &'static [u8]) -> Cell {
        Cell::Text(Bytes::from_static(value))
    }

    fn byte_order(left: &[Bytes], right: &[Bytes]) -> Ordering {
        left.cmp(right)
    }

    fn page_limits(max_page_bytes: usize, max_repair_rows: usize) -> PageLimits {
        PageLimits {
            source_row_limit: 2,
            max_page_bytes,
            max_repair_rows,
        }
    }

    #[test]
    fn source_page_derives_first_tail_and_empty_tail_ranges() {
        let context = context();
        let first = Page::from_source_rows(
            &context,
            vec![
                row(b"1", text(b"a")),
                row(b"2", text(b"b")),
                row(b"3", text(b"c")),
            ],
            2,
            MAX_PAGE_BYTES,
            &byte_order,
        )
        .unwrap();
        assert_eq!(first.rows.len(), 2);
        assert!(first.has_more);
        assert_eq!(first.next_key, Some(vec![Bytes::from_static(b"2")]));
        assert_eq!(
            first.target_range(None).unwrap(),
            KeyRange {
                start_exclusive: None,
                end_inclusive: Some(vec![Bytes::from_static(b"2")]),
            }
        );

        let tail = Page::from_source_rows(
            &context,
            vec![row(b"3", text(b"c"))],
            2,
            MAX_PAGE_BYTES,
            &byte_order,
        )
        .unwrap();
        assert!(!tail.has_more);
        assert_eq!(tail.next_key, Some(vec![Bytes::from_static(b"3")]));
        assert_eq!(
            tail.target_range(Some(&[Bytes::from_static(b"2")]))
                .unwrap(),
            KeyRange {
                start_exclusive: Some(vec![Bytes::from_static(b"2")]),
                end_inclusive: None,
            }
        );

        let empty =
            Page::from_source_rows(&context, vec![], 2, MAX_PAGE_BYTES, &byte_order).unwrap();
        assert!(!empty.has_more);
        assert_eq!(empty.next_key, None);
        assert_eq!(
            empty
                .target_range(Some(&[Bytes::from_static(b"3")]))
                .unwrap(),
            KeyRange {
                start_exclusive: Some(vec![Bytes::from_static(b"3")]),
                end_inclusive: None,
            }
        );
    }

    #[test]
    fn invalid_public_page_cannot_silently_derive_an_unbounded_range() {
        let empty_with_more = Page {
            rows: vec![],
            has_more: true,
            next_key: None,
        };
        assert!(matches!(
            empty_with_more.target_range(None),
            Err(ReconcileError::SourcePageContract(_))
        ));

        let missing_next_key = Page {
            rows: vec![row(b"1", text(b"a"))],
            has_more: true,
            next_key: None,
        };
        assert!(matches!(
            missing_next_key.target_range(None),
            Err(ReconcileError::SourcePageContract(_))
        ));
    }

    #[test]
    fn source_page_rejects_one_oversized_row_and_total_page_bytes() {
        assert_eq!(
            Page::from_source_rows(&context(), vec![], 1, 0, &byte_order),
            Err(ReconcileError::InvalidLimit { name: "page byte" })
        );

        let oversized = CanonicalRow {
            key: vec![Bytes::from_static(b"1")],
            values: vec![Cell::Text(Bytes::from(vec![b'x'; 4096]))],
        };
        let oversized_estimate = estimate_materialized_bytes(std::slice::from_ref(&oversized))
            .expect("small test estimate must fit");
        assert_eq!(
            Page::from_source_rows(
                &context(),
                vec![oversized],
                1,
                oversized_estimate - 1,
                &byte_order,
            ),
            Err(ReconcileError::PageBytesExceeded {
                side: "source",
                estimated_bytes: oversized_estimate,
                max_page_bytes: oversized_estimate - 1,
            })
        );

        let rows = vec![row(b"1", text(b"a")), row(b"2", text(b"b"))];
        let total_estimate = estimate_materialized_bytes(&rows).unwrap();
        let largest_row = rows
            .iter()
            .map(|row| estimate_materialized_bytes(std::slice::from_ref(row)).unwrap())
            .max()
            .unwrap();
        assert!(total_estimate > largest_row);
        assert!(matches!(
            Page::from_source_rows(&context(), rows, 2, total_estimate - 1, &byte_order,),
            Err(ReconcileError::PageBytesExceeded { side: "source", .. })
        ));
    }

    #[test]
    fn digest_length_prefix_prevents_ambiguous_rows() {
        let first = CanonicalRow {
            key: vec![Bytes::from_static(b"a")],
            values: vec![text(b"bc")],
        };
        let second = CanonicalRow {
            key: vec![Bytes::from_static(b"ab")],
            values: vec![text(b"c")],
        };
        assert_ne!(
            digest_rows(&context(), &[first], &byte_order).unwrap(),
            digest_rows(&context(), &[second], &byte_order).unwrap()
        );
    }

    #[test]
    fn digest_separates_domain_schema_type_tag_and_null_spellings() {
        let base = context();
        let rows = [row(b"1", Cell::Null)];
        let digest = digest_rows(&base, &rows, &byte_order).unwrap();

        let mut different_domain = base.clone();
        different_domain.version_domain.push_str("-v2");
        assert_ne!(
            digest,
            digest_rows(&different_domain, &rows, &byte_order).unwrap()
        );

        let mut different_schema = base.clone();
        different_schema.schema_fingerprint.push_str("-changed");
        assert_ne!(
            digest,
            digest_rows(&different_schema, &rows, &byte_order).unwrap()
        );

        let mut different_type = base.clone();
        different_type.value_columns[0].portable_type_tag = "pg_catalog.varchar".to_owned();
        assert_ne!(
            digest,
            digest_rows(&different_type, &rows, &byte_order).unwrap()
        );

        let empty = digest_rows(&base, &[row(b"1", text(b""))], &byte_order).unwrap();
        let copy_null = digest_rows(&base, &[row(b"1", text(b"\\N"))], &byte_order).unwrap();
        assert_ne!(digest, empty);
        assert_ne!(digest, copy_null);
        assert_ne!(empty, copy_null);
    }

    #[test]
    fn exact_diff_is_precise_for_all_row_relationships() {
        let source = vec![
            row(b"1", text(b"same")),
            row(b"2", text(b"new")),
            row(b"3", text(b"source-only")),
        ];
        let target = vec![
            row(b"1", text(b"same")),
            row(b"2", text(b"old")),
            row(b"4", text(b"target-only")),
        ];

        assert_eq!(
            exact_diff(&context(), &source, &target, 3, &byte_order).unwrap(),
            vec![
                RepairOperation::Upsert(row(b"2", text(b"new"))),
                RepairOperation::Upsert(row(b"3", text(b"source-only"))),
                RepairOperation::Delete(vec![Bytes::from_static(b"4")]),
            ]
        );
        assert!(
            exact_diff(&context(), &source, &source, 1, &byte_order)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn exact_diff_stops_at_repair_limit() {
        let source = vec![row(b"1", text(b"a")), row(b"2", text(b"b"))];
        let target = vec![row(b"3", text(b"c"))];
        assert_eq!(
            exact_diff(&context(), &source, &target, 2, &byte_order),
            Err(ReconcileError::RepairLimitExceeded { max_repair_rows: 2 })
        );
    }

    #[test]
    fn duplicate_and_out_of_order_keys_are_rejected() {
        let duplicate = vec![row(b"1", text(b"a")), row(b"1", text(b"b"))];
        assert_eq!(
            digest_rows(&context(), &duplicate, &byte_order),
            Err(ReconcileError::DuplicateKey { row: 1 })
        );

        let unordered = vec![row(b"2", text(b"a")), row(b"1", text(b"b"))];
        assert_eq!(
            digest_rows(&context(), &unordered, &byte_order),
            Err(ReconcileError::KeyOutOfOrder { row: 1 })
        );
    }

    #[test]
    fn key_and_value_arity_are_checked() {
        let missing_key = CanonicalRow {
            key: vec![],
            values: vec![text(b"a")],
        };
        assert!(matches!(
            digest_rows(&context(), &[missing_key], &byte_order),
            Err(ReconcileError::Arity {
                component: "key",
                expected: 1,
                actual: 0,
                ..
            })
        ));

        let missing_value = CanonicalRow {
            key: vec![Bytes::from_static(b"1")],
            values: vec![],
        };
        assert!(matches!(
            digest_rows(&context(), &[missing_value], &byte_order),
            Err(ReconcileError::Arity {
                component: "value",
                expected: 1,
                actual: 0,
                ..
            })
        ));
    }

    #[test]
    fn binary_toast_and_null_keys_are_rejected() {
        assert_eq!(
            CanonicalRow::try_from_cells(vec![Cell::Null], vec![text(b"a")]),
            Err(ReconcileError::NullKey { index: 0 })
        );
        assert!(matches!(
            CanonicalRow::try_from_cells(
                vec![Cell::Binary(Bytes::from_static(b"1"))],
                vec![text(b"a")]
            ),
            Err(ReconcileError::BinaryCell {
                component: "key",
                ..
            })
        ));
        assert!(matches!(
            digest_rows(
                &context(),
                &[row(b"1", Cell::Binary(Bytes::from_static(b"a")))],
                &byte_order
            ),
            Err(ReconcileError::BinaryCell {
                component: "value",
                ..
            })
        ));
        assert!(matches!(
            digest_rows(&context(), &[row(b"1", Cell::UnchangedToast)], &byte_order),
            Err(ReconcileError::UnchangedToast {
                component: "value",
                ..
            })
        ));
    }

    #[derive(Clone)]
    struct StubSource {
        page: Page,
    }

    #[async_trait]
    impl SourcePageReader for StubSource {
        async fn read_page(
            &self,
            _start_after: Option<&[Bytes]>,
            _row_limit: usize,
            _max_page_bytes: usize,
        ) -> Result<Page, ReconcileError> {
            Ok(self.page.clone())
        }
    }

    struct RecordingTarget {
        response: RangeRows,
        calls: Mutex<Vec<(KeyRange, usize, usize)>>,
    }

    #[async_trait]
    impl TargetRangeReader for RecordingTarget {
        async fn read_range(
            &self,
            range: &KeyRange,
            row_limit: usize,
            max_page_bytes: usize,
        ) -> Result<RangeRows, ReconcileError> {
            self.calls
                .lock()
                .unwrap()
                .push((range.clone(), row_limit, max_page_bytes));
            Ok(self.response.clone())
        }
    }

    #[tokio::test]
    async fn compare_page_reads_only_the_source_derived_target_range() {
        let source_rows = vec![row(b"1", text(b"a")), row(b"2", text(b"b"))];
        let source = StubSource {
            page: Page {
                rows: source_rows.clone(),
                has_more: true,
                next_key: Some(vec![Bytes::from_static(b"2")]),
            },
        };
        let target = RecordingTarget {
            response: RangeRows {
                rows: source_rows,
                has_more: false,
            },
            calls: Mutex::new(Vec::new()),
        };

        let comparison = compare_page(
            &source,
            &target,
            &context(),
            None,
            page_limits(MAX_PAGE_BYTES, 3),
            &byte_order,
        )
        .await
        .unwrap();
        assert!(matches!(comparison.result, ReconcileResult::Match(_)));
        assert_eq!(
            *target.calls.lock().unwrap(),
            vec![(
                KeyRange {
                    start_exclusive: None,
                    end_inclusive: Some(vec![Bytes::from_static(b"2")]),
                },
                5,
                MAX_PAGE_BYTES,
            )]
        );
    }

    #[tokio::test]
    async fn target_lookahead_proves_repair_limit_would_be_exceeded() {
        let source = StubSource {
            page: Page {
                rows: vec![],
                has_more: false,
                next_key: None,
            },
        };
        let target = RecordingTarget {
            response: RangeRows {
                rows: vec![row(b"1", text(b"a"))],
                has_more: true,
            },
            calls: Mutex::new(Vec::new()),
        };

        assert_eq!(
            compare_page(
                &source,
                &target,
                &context(),
                None,
                page_limits(MAX_PAGE_BYTES, 1),
                &byte_order,
            )
            .await,
            Err(ReconcileError::RepairLimitExceeded { max_repair_rows: 1 })
        );
    }

    #[tokio::test]
    async fn compare_page_applies_an_independent_target_byte_budget() {
        let source = StubSource {
            page: Page {
                rows: vec![],
                has_more: false,
                next_key: None,
            },
        };
        let target_rows = vec![row(b"1", text(b"a")), row(b"2", text(b"b"))];
        let target_estimate = estimate_materialized_bytes(&target_rows).unwrap();
        let max_page_bytes = target_estimate - 1;
        let target = RecordingTarget {
            response: RangeRows {
                rows: target_rows,
                has_more: false,
            },
            calls: Mutex::new(Vec::new()),
        };

        assert_eq!(
            compare_page(
                &source,
                &target,
                &context(),
                None,
                page_limits(max_page_bytes, 2),
                &byte_order,
            )
            .await,
            Err(ReconcileError::PageBytesExceeded {
                side: "target",
                estimated_bytes: target_estimate,
                max_page_bytes,
            })
        );
        assert_eq!(target.calls.lock().unwrap()[0].2, max_page_bytes);
    }
}
