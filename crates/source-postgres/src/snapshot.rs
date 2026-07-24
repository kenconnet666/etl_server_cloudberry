//! Exported-snapshot readers for initial loads and shadow rebuilds.
//!
//! Snapshot operations use the official `tokio-postgres` client. The replication fork is kept out
//! of this module so a snapshot transaction can never accidentally be passed to a replication
//! connection.
//!
//! Initial-load PK cursors are scoped to one live `SnapshotSession`. A cursor from an older
//! repeatable-read snapshot must never resume a fresh session: a PK move across that boundary can
//! otherwise be skipped or duplicated, and replay from the old consistent LSN does not repair the
//! gap. A fresh initial-load session restarts its scan from the beginning.

use std::{
    collections::HashSet,
    error::Error,
    marker::PhantomData,
    mem::size_of,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{BufMut, Bytes, BytesMut};
use cloudberry_etl_core::schema::{
    ColumnSchema, PgType, QualifiedName, TableSchema, validate_identifier,
};
use futures::{Stream, TryStreamExt};
use tokio_postgres::{
    Client, CopyOutStream, IsolationLevel, Transaction,
    types::{Format, IsNull, ToSql, Type},
};

use crate::{
    SourceError, SourceResult,
    catalog::{CatalogOptions, TableInventory, inspect_tables},
    sql::{quote_identifier, quote_literal, quote_qualified},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Binary,
    Text,
}

/// The only textual representations accepted by the initial-load contract.
///
/// Keeping these values typed prevents a caller from accidentally changing a session setting and
/// silently producing bytes that the strongly typed Cloudberry table interprets differently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateStyle {
    Iso,
}

impl DateStyle {
    const fn sql(self) -> &'static str {
        match self {
            Self::Iso => "ISO",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntervalStyle {
    Postgres,
}

impl IntervalStyle {
    const fn sql(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeZone {
    Utc,
}

impl TimeZone {
    const fn sql(self) -> &'static str {
        match self {
            Self::Utc => "UTC",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteaOutput {
    Hex,
}

impl ByteaOutput {
    const fn sql(self) -> &'static str {
        match self {
            Self::Hex => "hex",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientEncoding {
    Utf8,
}

impl ClientEncoding {
    const fn sql(self) -> &'static str {
        match self {
            Self::Utf8 => "UTF8",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtraFloatDigits {
    Three,
}

impl ExtraFloatDigits {
    const fn value(self) -> i8 {
        match self {
            Self::Three => 3,
        }
    }
}

/// Session settings shared by every source snapshot reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotSettings {
    pub date_style: DateStyle,
    pub interval_style: IntervalStyle,
    pub time_zone: TimeZone,
    pub extra_float_digits: ExtraFloatDigits,
    pub bytea_output: ByteaOutput,
    pub client_encoding: ClientEncoding,
}

impl Default for SnapshotSettings {
    fn default() -> Self {
        Self {
            date_style: DateStyle::Iso,
            interval_style: IntervalStyle::Postgres,
            time_zone: TimeZone::Utc,
            extra_float_digits: ExtraFloatDigits::Three,
            bytea_output: ByteaOutput::Hex,
            client_encoding: ClientEncoding::Utf8,
        }
    }
}

impl SnapshotSettings {
    fn sql(self) -> String {
        format!(
            "SET LOCAL DateStyle = '{}, YMD'; SET LOCAL IntervalStyle = '{}'; SET LOCAL TimeZone = '{}'; SET LOCAL extra_float_digits = {}; SET LOCAL bytea_output = '{}'; SET LOCAL client_encoding = '{}';",
            self.date_style.sql(),
            self.interval_style.sql(),
            self.time_zone.sql(),
            self.extra_float_digits.value(),
            self.bytea_output.sql(),
            self.client_encoding.sql(),
        )
    }
}

impl CopyFormat {
    fn sql(self) -> &'static str {
        match self {
            Self::Binary => "binary",
            Self::Text => "text",
        }
    }
}

/// A canonical value read through PostgreSQL's text output function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalSnapshotCell {
    Null,
    Text(Bytes),
}

/// One fully materialized source row. `values` follow `TableSchema::columns`;
/// `key` follows primary-key ordinal order and is always non-NULL text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotRow {
    pub key: Vec<Bytes>,
    pub values: Vec<CanonicalSnapshotCell>,
}

/// A source-derived keyset page. `materialized_bytes` is the conservative
/// retained-size estimate for `rows`; the reader also applies the same budget
/// to the discarded lookahead row before returning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPage {
    pub rows: Vec<CanonicalSnapshotRow>,
    pub has_more: bool,
    pub next_key: Option<Vec<Bytes>>,
    pub materialized_bytes: usize,
    snapshot_id: String,
    table_identity: SnapshotTableIdentity,
    start_exclusive: Option<Vec<Bytes>>,
}

impl SnapshotPage {
    #[must_use]
    pub fn start_exclusive(&self) -> Option<&[Bytes]> {
        self.start_exclusive.as_deref()
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<SnapshotCursor> {
        self.next_key.as_ref().map(|key| SnapshotCursor {
            key: key.clone(),
            snapshot_id: self.snapshot_id.clone(),
            table_identity: self.table_identity.clone(),
        })
    }
}

/// An opaque, session-bound source cursor. It cannot be reconstructed from a
/// durable raw key in a fresh repeatable-read snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotCursor {
    key: Vec<Bytes>,
    snapshot_id: String,
    table_identity: SnapshotTableIdentity,
}

impl SnapshotCursor {
    #[must_use]
    pub fn key(&self) -> &[Bytes] {
        &self.key
    }

    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CanonicalSnapshotKeyRow {
    pub key: Vec<Bytes>,
}

/// A lightweight source page used only to derive bounded COPY ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotKeyPage {
    pub rows: Vec<CanonicalSnapshotKeyRow>,
    pub has_more: bool,
    pub next_key: Option<Vec<Bytes>>,
    pub materialized_bytes: usize,
    snapshot_id: String,
    table_identity: SnapshotTableIdentity,
    start_exclusive: Option<Vec<Bytes>>,
}

impl SnapshotKeyPage {
    /// Derives `(start_exclusive, end_inclusive]`. A source tail deliberately
    /// has no end; the repeatable-read snapshot proves that the tail is fixed.
    pub fn copy_range(&self) -> SourceResult<SnapshotKeyRange> {
        if self.rows.is_empty() && self.has_more {
            return Err(SourceError::contract(
                "an empty canonical PK page cannot have more rows",
            ));
        }
        let expected_next_key = self.rows.last().map(|row| &row.key);
        if expected_next_key != self.next_key.as_ref() {
            return Err(SourceError::contract(
                "canonical PK page next_key must equal the final row key",
            ));
        }
        Ok(SnapshotKeyRange {
            start_exclusive: self.start_exclusive.clone(),
            end_inclusive: self.has_more.then(|| self.next_key.clone()).flatten(),
            snapshot_id: self.snapshot_id.clone(),
            table_identity: self.table_identity.clone(),
        })
    }

    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    #[must_use]
    pub fn next_cursor(&self) -> Option<SnapshotCursor> {
        self.next_key.as_ref().map(|key| SnapshotCursor {
            key: key.clone(),
            snapshot_id: self.snapshot_id.clone(),
            table_identity: self.table_identity.clone(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotKeyRange {
    start_exclusive: Option<Vec<Bytes>>,
    end_inclusive: Option<Vec<Bytes>>,
    snapshot_id: String,
    table_identity: SnapshotTableIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotTableIdentity {
    relation_id: u32,
    generation: u64,
    name: QualifiedName,
    key_columns: Vec<SnapshotKeyColumnIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SnapshotKeyColumnIdentity {
    attnum: i16,
    name: String,
    ordinal: u16,
    data_type: PgType,
    collation: Option<QualifiedName>,
}

impl SnapshotKeyRange {
    #[must_use]
    pub fn start_exclusive(&self) -> Option<&[Bytes]> {
        self.start_exclusive.as_deref()
    }

    #[must_use]
    pub fn end_inclusive(&self) -> Option<&[Bytes]> {
        self.end_inclusive.as_deref()
    }

    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPageLimits {
    pub row_limit: usize,
    pub max_page_bytes: usize,
}

impl SnapshotPageLimits {
    fn limit_plus_one(self) -> SourceResult<usize> {
        if self.row_limit == 0 {
            return Err(SourceError::contract(
                "canonical snapshot row_limit must be greater than zero",
            ));
        }
        if self.max_page_bytes == 0 {
            return Err(SourceError::contract(
                "canonical snapshot max_page_bytes must be greater than zero",
            ));
        }
        let limit = self.row_limit.checked_add(1).ok_or_else(|| {
            SourceError::contract("canonical snapshot row limit arithmetic overflow")
        })?;
        i64::try_from(limit).map_err(|_| {
            SourceError::contract("canonical snapshot row limit exceeds PostgreSQL LIMIT")
        })?;
        Ok(limit)
    }
}

#[derive(Debug)]
struct CanonicalTextParameter<'value>(&'value str);

impl ToSql for CanonicalTextParameter<'_> {
    fn to_sql(
        &self,
        _ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Send + Sync>> {
        out.put_slice(self.0.as_bytes());
        Ok(IsNull::No)
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }

    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut BytesMut,
    ) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.to_sql(ty, out)
    }

    fn encode_format(&self, _ty: &Type) -> Format {
        Format::Text
    }
}

struct CanonicalPagePlan {
    sql: String,
    key_indexes: Vec<usize>,
    value_count: usize,
    projection: CanonicalProjection,
}

struct CanonicalPageQuery {
    rows: Vec<CanonicalSnapshotRow>,
    has_more: bool,
}

#[derive(Debug, Clone, Copy)]
enum CanonicalProjection {
    KeysOnly,
    AllColumns,
}

/// The exact column contract consumed by the whole-table reconciliation digest.
///
/// Keys are ordered by primary-key ordinal. Values exclude the keys and are ordered by PostgreSQL
/// attribute number, so source and target readers can build identical digest contexts without
/// relying on catalog result order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationCopyColumns<'schema> {
    key_columns: Vec<&'schema ColumnSchema>,
    value_columns: Vec<&'schema ColumnSchema>,
}

impl<'schema> ReconciliationCopyColumns<'schema> {
    #[must_use]
    pub fn key_columns(&self) -> &[&'schema ColumnSchema] {
        &self.key_columns
    }

    #[must_use]
    pub fn value_columns(&self) -> &[&'schema ColumnSchema] {
        &self.value_columns
    }

    fn ordered_columns(&self) -> impl Iterator<Item = &'schema ColumnSchema> + '_ {
        self.key_columns.iter().chain(&self.value_columns).copied()
    }
}

/// Validate and derive the source projection used by whole-table reconciliation COPY.
///
/// This is deliberately stricter than the initial-load COPY helper. Reconciliation is allowed to
/// compare only a fully admitted table contract; malformed synthetic schemas fail before SQL is
/// prepared instead of risking a digest over ambiguous columns.
pub fn reconciliation_copy_columns(
    schema: &TableSchema,
) -> SourceResult<ReconciliationCopyColumns<'_>> {
    validate_reconciliation_identifiers(schema)?;
    schema
        .validate_supported()
        .map_err(|error| SourceError::contract(error.to_string()))?;

    let mut names = HashSet::with_capacity(schema.columns.len());
    let mut attnums = HashSet::with_capacity(schema.columns.len());
    for column in &schema.columns {
        if !names.insert(column.name.as_str()) {
            return Err(SourceError::contract(format!(
                "reconciliation COPY table {} has duplicate column name `{}`",
                schema.name, column.name
            )));
        }
        if column.attnum <= 0 {
            return Err(SourceError::contract(format!(
                "reconciliation COPY column {} has invalid attribute number {}",
                column.name, column.attnum
            )));
        }
        if !attnums.insert(column.attnum) {
            return Err(SourceError::contract(format!(
                "reconciliation COPY table {} has duplicate attribute number {}",
                schema.name, column.attnum
            )));
        }
    }

    let mut key_columns = schema.primary_key();
    if key_columns.is_empty() {
        return Err(SourceError::contract(format!(
            "reconciliation COPY table {} has no primary key",
            schema.name
        )));
    }
    key_columns.sort_by_key(|column| column.primary_key_ordinal);
    for (index, column) in key_columns.iter().enumerate() {
        let ordinal = u16::try_from(index + 1)
            .map_err(|_| SourceError::contract("reconciliation primary-key ordinal exceeds u16"))?;
        if column.primary_key_ordinal != Some(ordinal) {
            return Err(SourceError::contract(format!(
                "reconciliation COPY primary-key ordinals are not unique and contiguous at column {}",
                column.name
            )));
        }
        if column.nullable {
            return Err(SourceError::contract(format!(
                "reconciliation COPY primary-key column {} is nullable",
                column.name
            )));
        }
    }

    let mut value_columns = schema
        .columns
        .iter()
        .filter(|column| column.primary_key_ordinal.is_none())
        .collect::<Vec<_>>();
    value_columns.sort_by_key(|column| column.attnum);

    Ok(ReconciliationCopyColumns {
        key_columns,
        value_columns,
    })
}

fn validate_reconciliation_identifiers(schema: &TableSchema) -> SourceResult<()> {
    for identifier in std::iter::once(schema.name.schema.as_str())
        .chain(std::iter::once(schema.name.name.as_str()))
        .chain(schema.columns.iter().map(|column| column.name.as_str()))
    {
        validate_identifier(identifier)
            .map_err(|error| SourceError::contract(error.to_string()))?;
    }
    Ok(())
}

pub struct SnapshotSession<'client> {
    transaction: Transaction<'client>,
    snapshot_id: String,
}

impl<'client> SnapshotSession<'client> {
    #[must_use]
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Read the source catalog from the same MVCC snapshot used by table COPY streams.
    pub async fn inspect_tables(&self, options: &CatalogOptions) -> SourceResult<TableInventory> {
        inspect_tables(&self.transaction, options).await
    }

    /// Build a text COPY query with a stable, explicit source column order.
    pub fn copy_sql_columns(
        table: &QualifiedName,
        columns: &[String],
        format: CopyFormat,
    ) -> SourceResult<String> {
        if columns.is_empty() {
            return Err(SourceError::Contract(
                "snapshot COPY requires at least one column".to_owned(),
            ));
        }
        let qualified_table = quote_qualified(&table.schema, &table.name)?;
        let quoted_columns = columns
            .iter()
            .map(|column| quote_identifier(column))
            .collect::<SourceResult<Vec<_>>>()?
            .join(", ");
        Ok(format!(
            "COPY (SELECT {quoted_columns} FROM {qualified_table}) TO STDOUT WITH (FORMAT {}, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')",
            format.sql()
        ))
    }

    /// Build the text COPY query for a strongly typed source schema.
    pub fn copy_text_sql(schema: &TableSchema) -> SourceResult<String> {
        let columns = schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        Self::copy_sql_columns(&schema.name, &columns, CopyFormat::Text)
    }

    /// Apply the controlled representation settings to this snapshot transaction.
    pub async fn configure(&mut self, settings: SnapshotSettings) -> SourceResult<()> {
        self.transaction
            .batch_execute(&settings.sql())
            .await
            .map_err(SourceError::from)
    }

    /// Start an explicit-column text COPY stream for a typed source table.
    pub async fn copy_text_table<'session>(
        &'session mut self,
        schema: &TableSchema,
    ) -> SourceResult<SnapshotCopy<'session>> {
        let sql = Self::copy_text_sql(schema)?;
        let statement = self.transaction.prepare(&sql).await?;
        let stream = self.transaction.copy_out(&statement).await?;
        Ok(SnapshotCopy {
            stream: Box::pin(stream),
            _session: PhantomData,
        })
    }

    /// Build the whole-table canonical text COPY used only by reconciliation.
    ///
    /// Rows are intentionally unsorted: the reconciliation digest is a multiset digest. Avoiding
    /// `ORDER BY` prevents a full-table sort while the explicit projection keeps field order exact.
    pub fn reconciliation_copy_text_sql(schema: &TableSchema) -> SourceResult<String> {
        let projection = reconciliation_copy_columns(schema)?;
        let columns = projection
            .ordered_columns()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        Self::copy_sql_columns(&schema.name, &columns, CopyFormat::Text)
    }

    /// Stream a whole source table in canonical reconciliation column order.
    ///
    /// The stream remains owned by this repeatable-read `SnapshotSession`, including sessions that
    /// imported a temporary replication slot's exported snapshot.
    pub async fn copy_reconciliation_table<'session>(
        &'session mut self,
        schema: &TableSchema,
    ) -> SourceResult<SnapshotCopy<'session>> {
        let sql = Self::reconciliation_copy_text_sql(schema)?;
        let statement = self.transaction.prepare(&sql).await?;
        let stream = self.transaction.copy_out(&statement).await?;
        Ok(SnapshotCopy {
            stream: Box::pin(stream),
            _session: PhantomData,
        })
    }

    /// Build the validation-gated PK-only keyset query. This API plans SQL;
    /// it does not establish a target fence or make a repair authoritative.
    pub fn canonical_pk_page_sql(
        schema: &TableSchema,
        has_start: bool,
        limits: SnapshotPageLimits,
    ) -> SourceResult<String> {
        Ok(
            build_canonical_page_plan(schema, has_start, limits, CanonicalProjection::KeysOnly)?
                .sql,
        )
    }

    /// Read only a bounded set of canonical PKs plus one lookahead row. The
    /// returned boundary can feed `copy_text_pk_range` without materializing
    /// or re-encoding the page's non-key values.
    ///
    /// The optional cursor must be the preceding page's opaque token from this
    /// same live session. It is not a durable resume cursor. A fresh snapshot
    /// must restart an initial-load scan from `None`.
    pub async fn read_canonical_pk_page(
        &self,
        schema: &TableSchema,
        cursor: Option<&SnapshotCursor>,
        limits: SnapshotPageLimits,
    ) -> SourceResult<SnapshotKeyPage> {
        let table_identity = snapshot_table_identity(schema)?;
        validate_cursor_scope(cursor, &self.snapshot_id, &table_identity)?;
        let start_exclusive = cursor.map(|cursor| cursor.key.clone());
        let plan = build_canonical_page_plan(
            schema,
            cursor.is_some(),
            limits,
            CanonicalProjection::KeysOnly,
        )?;
        let page = self
            .query_canonical_page(plan, cursor, limits, &table_identity)
            .await?;
        let rows = page
            .rows
            .into_iter()
            .map(|row| CanonicalSnapshotKeyRow { key: row.key })
            .collect();
        finish_key_page(
            rows,
            limits.row_limit,
            page.has_more,
            &self.snapshot_id,
            table_identity,
            start_exclusive,
        )
    }

    /// Build the full-row canonical query reserved for reconciliation. Initial
    /// snapshots should use the PK boundary plus direct range COPY path.
    pub fn canonical_row_page_sql(
        schema: &TableSchema,
        has_start: bool,
        limits: SnapshotPageLimits,
    ) -> SourceResult<String> {
        Ok(
            build_canonical_page_plan(schema, has_start, limits, CanonicalProjection::AllColumns)?
                .sql,
        )
    }

    /// Materialize a bounded canonical full-row page for validation-gated
    /// reconciliation. This is intentionally not used by snapshot COPY.
    pub async fn read_canonical_row_page(
        &self,
        schema: &TableSchema,
        cursor: Option<&SnapshotCursor>,
        limits: SnapshotPageLimits,
    ) -> SourceResult<SnapshotPage> {
        let table_identity = snapshot_table_identity(schema)?;
        validate_cursor_scope(cursor, &self.snapshot_id, &table_identity)?;
        let start_exclusive = cursor.map(|cursor| cursor.key.clone());
        let plan = build_canonical_page_plan(
            schema,
            cursor.is_some(),
            limits,
            CanonicalProjection::AllColumns,
        )?;
        let page = self
            .query_canonical_page(plan, cursor, limits, &table_identity)
            .await?;
        finish_row_page(
            page.rows,
            limits.row_limit,
            page.has_more,
            &self.snapshot_id,
            table_identity,
            start_exclusive,
        )
    }

    /// Build a direct text COPY for one source-derived `(start, end]` range.
    /// Canonical keys are rendered only as hex data decoded by `pg_catalog`
    /// and cast to the catalog-qualified PK type; no user SQL is reused.
    pub fn copy_text_pk_range_sql(
        schema: &TableSchema,
        range: &SnapshotKeyRange,
    ) -> SourceResult<String> {
        build_copy_range_sql(schema, range)
    }

    /// Stream a range directly from PostgreSQL COPY. The same repeatable-read
    /// transaction owns both the preceding PK boundary read and this stream.
    pub async fn copy_text_pk_range<'session>(
        &'session mut self,
        schema: &TableSchema,
        range: &SnapshotKeyRange,
    ) -> SourceResult<SnapshotCopy<'session>> {
        if range.snapshot_id != self.snapshot_id {
            return Err(SourceError::contract(
                "snapshot PK range belongs to a different repeatable-read snapshot",
            ));
        }
        let table_identity = snapshot_table_identity(schema)?;
        if range.table_identity != table_identity {
            return Err(SourceError::contract(
                "snapshot PK range belongs to a different table or primary-key contract",
            ));
        }
        let sql = Self::copy_text_pk_range_sql(schema, range)?;
        let statement = self.transaction.prepare(&sql).await?;
        let stream = self.transaction.copy_out(&statement).await?;
        Ok(SnapshotCopy {
            stream: Box::pin(stream),
            _session: PhantomData,
        })
    }

    async fn query_canonical_page(
        &self,
        plan: CanonicalPagePlan,
        cursor: Option<&SnapshotCursor>,
        limits: SnapshotPageLimits,
        table_identity: &SnapshotTableIdentity,
    ) -> SourceResult<CanonicalPageQuery> {
        validate_cursor_scope(cursor, &self.snapshot_id, table_identity)?;
        let start_text = validate_key(
            "cursor",
            cursor.map(|cursor| cursor.key.as_slice()),
            plan.key_indexes.len(),
        )?;
        let parameters = start_text
            .iter()
            .map(|value| CanonicalTextParameter(value))
            .collect::<Vec<_>>();
        let parameter_refs = parameters
            .iter()
            .map(|value| value as &(dyn ToSql + Sync))
            .collect::<Vec<_>>();
        let statement = self.transaction.prepare(&plan.sql).await?;
        let stream = self
            .transaction
            .query_raw(&statement, parameter_refs)
            .await?;
        let mut stream = std::pin::pin!(stream);
        let mut rows = Vec::new();
        let mut materialized_bytes = 0usize;
        let mut previous_key: Option<Vec<Bytes>> = None;
        let mut returned_rows = 0usize;
        let mut truncated_for_budget = false;
        let mut oversized_row_bytes = None;

        while let Some(row) = stream.as_mut().try_next().await? {
            returned_rows = returned_rows.checked_add(1).ok_or_else(|| {
                SourceError::contract("canonical snapshot returned row count overflow")
            })?;
            let expected_arity = plan
                .value_count
                .checked_add(1)
                .ok_or_else(|| SourceError::contract("canonical snapshot result arity overflow"))?;
            if row.len() != expected_arity {
                return Err(SourceError::contract(format!(
                    "canonical snapshot returned {} fields; expected {expected_arity}",
                    row.len()
                )));
            }

            let strictly_ordered: bool = row.try_get(plan.value_count)?;
            let canonical = decode_canonical_row(&row, &plan.key_indexes, plan.value_count)?;
            if previous_key.as_ref() == Some(&canonical.key) {
                return Err(SourceError::contract(
                    "canonical snapshot page contains a duplicate primary key",
                ));
            }
            if !strictly_ordered {
                return Err(SourceError::contract(
                    "canonical snapshot page is not strictly ordered by the native primary key",
                ));
            }

            let row_bytes = match plan.projection {
                CanonicalProjection::KeysOnly => estimate_canonical_key_row_bytes(&canonical.key),
                CanonicalProjection::AllColumns => estimate_canonical_row_bytes(&canonical),
            }
            .ok_or_else(|| {
                SourceError::contract("canonical snapshot page byte estimate overflow")
            })?;
            let next_materialized_bytes =
                materialized_bytes.checked_add(row_bytes).ok_or_else(|| {
                    SourceError::contract("canonical snapshot page byte estimate overflow")
                })?;
            if !truncated_for_budget && next_materialized_bytes <= limits.max_page_bytes {
                materialized_bytes = next_materialized_bytes;
                previous_key = Some(canonical.key.clone());
                rows.push(canonical);
                continue;
            }

            previous_key = Some(canonical.key.clone());
            truncated_for_budget = true;
            if rows.is_empty() {
                oversized_row_bytes.get_or_insert(row_bytes);
            }
        }

        let max_rows = limits.limit_plus_one()?;
        if returned_rows > max_rows {
            return Err(SourceError::contract(format!(
                "canonical snapshot returned {returned_rows} rows for LIMIT {max_rows}"
            )));
        }
        if let Some(row_bytes) = oversized_row_bytes {
            return Err(SourceError::contract(format!(
                "canonical snapshot row requires {row_bytes} estimated bytes, exceeding max_page_bytes {}",
                limits.max_page_bytes
            )));
        }
        Ok(CanonicalPageQuery {
            rows,
            has_more: truncated_for_budget || returned_rows > limits.row_limit,
        })
    }

    /// Commit only after all COPY streams have been drained. Dropping this session rolls back.
    pub async fn commit(self) -> SourceResult<()> {
        self.transaction.commit().await.map_err(SourceError::from)
    }

    /// Explicitly abort and release the exported snapshot transaction.
    pub async fn rollback(self) -> SourceResult<()> {
        self.transaction.rollback().await.map_err(SourceError::from)
    }
}

fn build_canonical_page_plan(
    schema: &TableSchema,
    has_start: bool,
    limits: SnapshotPageLimits,
    projection: CanonicalProjection,
) -> SourceResult<CanonicalPagePlan> {
    let limit_plus_one = limits.limit_plus_one()?;
    let (key_columns, schema_key_indexes) = validated_primary_key(schema)?;
    let selected_columns = match projection {
        CanonicalProjection::KeysOnly => key_columns.clone(),
        CanonicalProjection::AllColumns => schema.columns.iter().collect(),
    };
    let key_indexes = match projection {
        CanonicalProjection::KeysOnly => (0..key_columns.len()).collect(),
        CanonicalProjection::AllColumns => schema_key_indexes,
    };

    let qualified_table = quote_qualified(&schema.name.schema, &schema.name.name)?;
    let selected = quote_columns(&selected_columns)?;
    let keys = quote_columns(&key_columns)?;
    let canonical_values = selected
        .iter()
        .enumerate()
        .map(|(index, column)| format!("p.{column}::text AS \"__pg2cb_value_{index}\""))
        .collect::<Vec<_>>()
        .join(", ");
    let raw_values = selected.join(", ");
    let quoted_keys = keys.join(", ");
    let predicate = if has_start {
        let parameters = key_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                let type_name =
                    quote_qualified(&column.data_type.name.schema, &column.data_type.name.name)?;
                Ok(format!("CAST(${} AS {type_name})", index + 1))
            })
            .collect::<SourceResult<Vec<_>>>()?
            .join(", ");
        format!(" WHERE ROW({quoted_keys}) > ROW({parameters})")
    } else {
        String::new()
    };
    let qualified_keys = keys
        .iter()
        .map(|key| format!("p.{key}"))
        .collect::<Vec<_>>();
    let qualified_key_list = qualified_keys.join(", ");
    let lagged_keys = qualified_keys
        .iter()
        .map(|key| format!("lag({key}) OVER \"__pg2cb_pk_window\""))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "WITH \"__pg2cb_page\" AS MATERIALIZED (SELECT {raw_values} FROM {qualified_table}{predicate} ORDER BY {quoted_keys} LIMIT {limit_plus_one}) SELECT {canonical_values}, (row_number() OVER \"__pg2cb_pk_window\" = 1 OR ROW({qualified_key_list}) > ROW({lagged_keys})) AS \"__pg2cb_pk_strict\" FROM \"__pg2cb_page\" AS p WINDOW \"__pg2cb_pk_window\" AS (ORDER BY {qualified_key_list}) ORDER BY {qualified_key_list}"
    );

    Ok(CanonicalPagePlan {
        sql,
        key_indexes,
        value_count: selected_columns.len(),
        projection,
    })
}

fn build_copy_range_sql(schema: &TableSchema, range: &SnapshotKeyRange) -> SourceResult<String> {
    let table_identity = snapshot_table_identity(schema)?;
    if range.table_identity != table_identity {
        return Err(SourceError::contract(
            "snapshot PK range does not match the requested table or primary-key contract",
        ));
    }
    let (key_columns, _) = validated_primary_key(schema)?;
    let selected_columns = schema.columns.iter().collect::<Vec<_>>();
    if selected_columns.is_empty() {
        return Err(SourceError::contract(
            "snapshot range COPY requires at least one column",
        ));
    }
    let qualified_table = quote_qualified(&schema.name.schema, &schema.name.name)?;
    let selected = quote_columns(&selected_columns)?.join(", ");
    let keys = quote_columns(&key_columns)?.join(", ");
    let mut predicates = Vec::with_capacity(2);
    if let Some(start) = range.start_exclusive.as_deref() {
        predicates.push(format!(
            "ROW({keys}) > ROW({})",
            render_typed_key(&key_columns, start, "start_exclusive")?
        ));
    }
    if let Some(end) = range.end_inclusive.as_deref() {
        predicates.push(format!(
            "ROW({keys}) <= ROW({})",
            render_typed_key(&key_columns, end, "end_inclusive")?
        ));
    }
    let predicate = if predicates.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", predicates.join(" AND "))
    };

    Ok(format!(
        "COPY (SELECT {selected} FROM {qualified_table}{predicate} ORDER BY {keys}) TO STDOUT WITH (FORMAT text, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')"
    ))
}

fn validated_primary_key(schema: &TableSchema) -> SourceResult<(Vec<&ColumnSchema>, Vec<usize>)> {
    if schema.columns.is_empty() {
        return Err(SourceError::contract(
            "canonical snapshot requires at least one source column",
        ));
    }
    let key_columns = schema.primary_key();
    if key_columns.is_empty() {
        return Err(SourceError::contract(format!(
            "canonical snapshot table {} has no primary key",
            schema.name
        )));
    }

    let mut key_indexes = Vec::with_capacity(key_columns.len());
    for (index, column) in key_columns.iter().enumerate() {
        let expected_ordinal = u16::try_from(index + 1)
            .map_err(|_| SourceError::contract("primary-key ordinal exceeds u16"))?;
        if column.primary_key_ordinal != Some(expected_ordinal) {
            return Err(SourceError::contract(format!(
                "canonical snapshot primary-key ordinals are not contiguous at column {}",
                column.name
            )));
        }
        if column.nullable {
            return Err(SourceError::contract(format!(
                "canonical snapshot primary-key column {} is nullable",
                column.name
            )));
        }
        if !column.data_type.kind.is_supported() {
            return Err(SourceError::contract(format!(
                "canonical snapshot primary-key column {} has an unsupported type",
                column.name
            )));
        }
        let schema_index = schema
            .columns
            .iter()
            .position(|candidate| candidate.attnum == column.attnum)
            .ok_or_else(|| {
                SourceError::contract(format!(
                    "primary-key column {} is absent from the source column list",
                    column.name
                ))
            })?;
        key_indexes.push(schema_index);
    }
    Ok((key_columns, key_indexes))
}

fn snapshot_table_identity(schema: &TableSchema) -> SourceResult<SnapshotTableIdentity> {
    let (key_columns, _) = validated_primary_key(schema)?;
    let key_columns = key_columns
        .into_iter()
        .map(|column| {
            let ordinal = column.primary_key_ordinal.ok_or_else(|| {
                SourceError::contract(format!(
                    "primary-key column {} lost its ordinal",
                    column.name
                ))
            })?;
            Ok(SnapshotKeyColumnIdentity {
                attnum: column.attnum,
                name: column.name.clone(),
                ordinal,
                data_type: column.data_type.clone(),
                collation: column.collation.clone(),
            })
        })
        .collect::<SourceResult<Vec<_>>>()?;
    Ok(SnapshotTableIdentity {
        relation_id: schema.relation_id,
        generation: schema.generation,
        name: schema.name.clone(),
        key_columns,
    })
}

fn quote_columns(columns: &[&ColumnSchema]) -> SourceResult<Vec<String>> {
    columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect()
}

fn validate_cursor_scope(
    cursor: Option<&SnapshotCursor>,
    snapshot_id: &str,
    table_identity: &SnapshotTableIdentity,
) -> SourceResult<()> {
    if let Some(cursor) = cursor {
        if cursor.snapshot_id != snapshot_id {
            return Err(SourceError::contract(
                "snapshot cursor belongs to a different repeatable-read snapshot",
            ));
        }
        if cursor.table_identity != *table_identity {
            return Err(SourceError::contract(
                "snapshot cursor belongs to a different table or primary-key contract",
            ));
        }
    }
    Ok(())
}

fn validate_key<'value>(
    label: &str,
    key: Option<&'value [Bytes]>,
    expected_arity: usize,
) -> SourceResult<Vec<&'value str>> {
    let Some(key) = key else {
        return Ok(Vec::new());
    };
    if key.len() != expected_arity {
        return Err(SourceError::contract(format!(
            "canonical snapshot {label} has {} fields; expected {expected_arity}",
            key.len()
        )));
    }
    key.iter()
        .enumerate()
        .map(|(index, value)| {
            let value = std::str::from_utf8(value).map_err(|_| {
                SourceError::contract(format!(
                    "canonical snapshot {label} field {index} is not UTF-8 text"
                ))
            })?;
            if value.contains('\0') {
                return Err(SourceError::contract(format!(
                    "canonical snapshot {label} field {index} contains NUL"
                )));
            }
            Ok(value)
        })
        .collect()
}

fn render_typed_key(columns: &[&ColumnSchema], key: &[Bytes], label: &str) -> SourceResult<String> {
    let values = validate_key(label, Some(key), columns.len())?;
    columns
        .iter()
        .zip(values)
        .map(|(column, value)| {
            let type_name = quote_qualified(
                &column.data_type.name.schema,
                &column.data_type.name.name,
            )?;
            let hex = encode_hex(value.as_bytes());
            Ok(format!(
                "CAST(pg_catalog.convert_from(pg_catalog.decode('{hex}', 'hex'), 'UTF8') AS {type_name})"
            ))
        })
        .collect::<SourceResult<Vec<_>>>()
        .map(|values| values.join(", "))
}

fn encode_hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_canonical_row(
    row: &tokio_postgres::Row,
    key_indexes: &[usize],
    value_count: usize,
) -> SourceResult<CanonicalSnapshotRow> {
    let values = (0..value_count)
        .map(|index| {
            row.try_get::<_, Option<String>>(index)
                .map(|value| match value {
                    Some(value) => CanonicalSnapshotCell::Text(Bytes::from(value)),
                    None => CanonicalSnapshotCell::Null,
                })
                .map_err(SourceError::from)
        })
        .collect::<SourceResult<Vec<_>>>()?;
    let key = key_indexes
        .iter()
        .map(|index| match values.get(*index) {
            Some(CanonicalSnapshotCell::Text(value)) => Ok(value.clone()),
            Some(CanonicalSnapshotCell::Null) => Err(SourceError::contract(format!(
                "canonical snapshot primary-key field {index} is NULL"
            ))),
            None => Err(SourceError::contract(format!(
                "canonical snapshot primary-key index {index} exceeds row arity {value_count}"
            ))),
        })
        .collect::<SourceResult<Vec<_>>>()?;
    if key.len() != key_indexes.len() {
        return Err(SourceError::contract(
            "canonical snapshot primary-key arity changed while decoding",
        ));
    }
    Ok(CanonicalSnapshotRow { key, values })
}

fn finish_key_page(
    mut rows: Vec<CanonicalSnapshotKeyRow>,
    row_limit: usize,
    has_more_hint: bool,
    snapshot_id: &str,
    table_identity: SnapshotTableIdentity,
    start_exclusive: Option<Vec<Bytes>>,
) -> SourceResult<SnapshotKeyPage> {
    let max_rows = row_limit
        .checked_add(1)
        .ok_or_else(|| SourceError::contract("canonical PK page row limit overflow"))?;
    if rows.len() > max_rows {
        return Err(SourceError::contract(format!(
            "canonical PK page returned {} rows for a row limit of {row_limit}",
            rows.len()
        )));
    }
    let has_more = has_more_hint || rows.len() > row_limit;
    if has_more {
        rows.truncate(row_limit);
    }
    let next_key = rows.last().map(|row| row.key.clone());
    let materialized_bytes = estimate_key_page_bytes(&rows)
        .ok_or_else(|| SourceError::contract("canonical PK page byte estimate overflow"))?;
    Ok(SnapshotKeyPage {
        rows,
        has_more,
        next_key,
        materialized_bytes,
        snapshot_id: snapshot_id.to_owned(),
        table_identity,
        start_exclusive,
    })
}

fn finish_row_page(
    mut rows: Vec<CanonicalSnapshotRow>,
    row_limit: usize,
    has_more_hint: bool,
    snapshot_id: &str,
    table_identity: SnapshotTableIdentity,
    start_exclusive: Option<Vec<Bytes>>,
) -> SourceResult<SnapshotPage> {
    let max_rows = row_limit
        .checked_add(1)
        .ok_or_else(|| SourceError::contract("canonical row page row limit overflow"))?;
    if rows.len() > max_rows {
        return Err(SourceError::contract(format!(
            "canonical row page returned {} rows for a row limit of {row_limit}",
            rows.len()
        )));
    }
    let has_more = has_more_hint || rows.len() > row_limit;
    if has_more {
        rows.truncate(row_limit);
    }
    let next_key = rows.last().map(|row| row.key.clone());
    let materialized_bytes = rows.iter().try_fold(0usize, |total, row| {
        total.checked_add(estimate_canonical_row_bytes(row)?)
    });
    let materialized_bytes = materialized_bytes
        .ok_or_else(|| SourceError::contract("canonical row page byte estimate overflow"))?;
    Ok(SnapshotPage {
        rows,
        has_more,
        next_key,
        materialized_bytes,
        snapshot_id: snapshot_id.to_owned(),
        table_identity,
        start_exclusive,
    })
}

fn estimate_key_page_bytes(rows: &[CanonicalSnapshotKeyRow]) -> Option<usize> {
    rows.iter().try_fold(0usize, |total, row| {
        let containers = row.key.len().checked_mul(size_of::<Bytes>())?;
        let payload = row
            .key
            .iter()
            .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?;
        total
            .checked_add(size_of::<CanonicalSnapshotKeyRow>())?
            .checked_add(containers)?
            .checked_add(payload)
    })
}

fn estimate_canonical_key_row_bytes(key: &[Bytes]) -> Option<usize> {
    let containers = key.len().checked_mul(size_of::<Bytes>())?;
    let payload = key
        .iter()
        .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?;
    size_of::<CanonicalSnapshotKeyRow>()
        .checked_add(containers)?
        .checked_add(payload)
}

fn estimate_canonical_row_bytes(row: &CanonicalSnapshotRow) -> Option<usize> {
    let key_containers = row.key.len().checked_mul(size_of::<Bytes>())?;
    let value_containers = row
        .values
        .len()
        .checked_mul(size_of::<CanonicalSnapshotCell>())?;
    let key_payload = row
        .key
        .iter()
        .try_fold(0usize, |bytes, value| bytes.checked_add(value.len()))?;
    let value_payload = row.values.iter().try_fold(0usize, |bytes, value| {
        let len = match value {
            CanonicalSnapshotCell::Null => 0,
            CanonicalSnapshotCell::Text(value) => value.len(),
        };
        bytes.checked_add(len)
    })?;
    size_of::<CanonicalSnapshotRow>()
        .checked_add(key_containers)?
        .checked_add(value_containers)?
        .checked_add(key_payload)?
        .checked_add(value_payload)
}

pub struct SnapshotCopy<'session> {
    stream: Pin<Box<CopyOutStream>>,
    _session: PhantomData<&'session mut SnapshotSession<'session>>,
}

impl Stream for SnapshotCopy<'_> {
    type Item = SourceResult<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context).map(|item| {
            item.map(|result| result.map_err(|error| SourceError::Copy(error.to_string())))
        })
    }
}

/// Start a repeatable-read, read-only transaction and export its snapshot.
pub async fn begin_exported_snapshot(client: &mut Client) -> SourceResult<SnapshotSession<'_>> {
    begin_exported_snapshot_with_settings(client, SnapshotSettings::default()).await
}

/// Start an exported snapshot and pin the approved textual representation settings.
pub async fn begin_exported_snapshot_with_settings(
    client: &mut Client,
    settings: SnapshotSettings,
) -> SourceResult<SnapshotSession<'_>> {
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    transaction.batch_execute(&settings.sql()).await?;
    let snapshot_id = transaction
        .query_one("SELECT pg_export_snapshot()", &[])
        .await?
        .try_get::<_, String>(0)?;
    validate_snapshot_id(&snapshot_id)?;
    Ok(SnapshotSession {
        transaction,
        snapshot_id,
    })
}

/// Start a child snapshot transaction on another official client connection.
pub async fn begin_at_exported_snapshot<'client>(
    client: &'client mut Client,
    snapshot_id: &str,
) -> SourceResult<SnapshotSession<'client>> {
    begin_at_exported_snapshot_with_settings(client, snapshot_id, SnapshotSettings::default()).await
}

/// Start a child snapshot transaction, import the exported snapshot first, then pin settings.
pub async fn begin_at_exported_snapshot_with_settings<'client>(
    client: &'client mut Client,
    snapshot_id: &str,
    settings: SnapshotSettings,
) -> SourceResult<SnapshotSession<'client>> {
    validate_snapshot_id(snapshot_id)?;
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    transaction
        .batch_execute(&format!(
            "SET TRANSACTION SNAPSHOT {}",
            quote_literal(snapshot_id)?
        ))
        .await?;
    transaction.batch_execute(&settings.sql()).await?;
    Ok(SnapshotSession {
        transaction,
        snapshot_id: snapshot_id.to_owned(),
    })
}

/// SQL used by connection pools that need to start the child transaction themselves.
pub fn set_snapshot_sql(snapshot_id: &str) -> SourceResult<String> {
    validate_snapshot_id(snapshot_id)?;
    Ok(format!(
        "SET TRANSACTION SNAPSHOT {}",
        quote_literal(snapshot_id)?
    ))
}

fn validate_snapshot_id(value: &str) -> SourceResult<()> {
    if value.is_empty() || value.contains('\0') || value.contains('\n') || value.contains('\r') {
        return Err(SourceError::Contract(format!(
            "invalid exported snapshot identifier `{value}`"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use cloudberry_etl_core::schema::{
        GeneratedColumn, IdentityColumn, PgType, PgTypeKind, ReplicaIdentity, TableKind,
    };

    use super::*;

    fn composite_schema() -> TableSchema {
        TableSchema {
            relation_id: 42,
            generation: 1,
            name: QualifiedName::new("Sales\"Data", "Order\"Rows").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                column(1, "Tenant\"Id", "text", PgTypeKind::Text, Some(1)),
                column(2, "Seq No", "int8", PgTypeKind::Int8, Some(2)),
                column(3, "Payload", "text", PgTypeKind::Text, None),
            ],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn reconciliation_schema() -> TableSchema {
        TableSchema {
            relation_id: 84,
            generation: 3,
            name: QualifiedName::new("Sales\"Data", "Order\"Rows").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            // Physical order deliberately differs from both PK ordinal order and the final
            // reconciliation projection.
            columns: vec![
                supported_column(1, "Payload", 25, "text", PgTypeKind::Text, None),
                supported_column(2, "Seq No", 20, "int8", PgTypeKind::Int8, Some(2)),
                supported_column(3, "Tenant\"Id", 23, "int4", PgTypeKind::Int4, Some(1)),
                supported_column(4, "Other Value", 25, "text", PgTypeKind::Text, None),
            ],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn column(
        attnum: i16,
        name: &str,
        type_name: &str,
        kind: PgTypeKind,
        primary_key_ordinal: Option<u16>,
    ) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid: u32::try_from(attnum).unwrap(),
                name: QualifiedName::new("pg_catalog", type_name).unwrap(),
                kind,
            },
            nullable: primary_key_ordinal.is_none(),
            primary_key_ordinal,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn supported_column(
        attnum: i16,
        name: &str,
        oid: u32,
        type_name: &str,
        kind: PgTypeKind,
        primary_key_ordinal: Option<u16>,
    ) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid,
                name: QualifiedName::new("pg_catalog", type_name).unwrap(),
                kind,
            },
            nullable: primary_key_ordinal.is_none(),
            primary_key_ordinal,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn limits(row_limit: usize) -> SnapshotPageLimits {
        SnapshotPageLimits {
            row_limit,
            max_page_bytes: 1024 * 1024,
        }
    }

    fn key(values: &[&'static [u8]]) -> CanonicalSnapshotKeyRow {
        CanonicalSnapshotKeyRow {
            key: values
                .iter()
                .map(|value| Bytes::from_static(value))
                .collect(),
        }
    }

    #[test]
    fn copy_sql_quotes_an_explicit_stable_column_list() {
        let table = QualifiedName::new("s\"x", "t\"x").unwrap();
        let columns = vec!["id".to_owned(), "pay\"load".to_owned()];
        let sql = SnapshotSession::copy_sql_columns(&table, &columns, CopyFormat::Text).unwrap();
        assert!(sql.contains("\"s\"\"x\".\"t\"\"x\""));
        assert!(sql.contains("SELECT \"id\", \"pay\"\"load\""));
        assert!(sql.contains("FORMAT text"));
        assert!(sql.contains("DELIMITER E'\\t'"));
        assert!(sql.contains("NULL E'\\\\N'"));
        assert!(!sql.contains("SELECT *"));
        assert!(SnapshotSession::copy_sql_columns(&table, &[], CopyFormat::Text).is_err());
    }

    #[test]
    fn reconciliation_copy_puts_composite_key_first_without_sorting_rows() {
        let schema = reconciliation_schema();
        let projection = reconciliation_copy_columns(&schema).unwrap();
        let key_names = projection
            .key_columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let value_names = projection
            .value_columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(key_names, ["Tenant\"Id", "Seq No"]);
        assert_eq!(value_names, ["Payload", "Other Value"]);

        let sql = SnapshotSession::reconciliation_copy_text_sql(&schema).unwrap();
        assert_eq!(
            sql,
            "COPY (SELECT \"Tenant\"\"Id\", \"Seq No\", \"Payload\", \"Other Value\" FROM \"Sales\"\"Data\".\"Order\"\"Rows\") TO STDOUT WITH (FORMAT text, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')"
        );
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn reconciliation_copy_rejects_ambiguous_or_unsupported_schemas() {
        let mut invalid = reconciliation_schema();
        invalid.columns.clear();
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        for column in &mut invalid.columns {
            column.primary_key_ordinal = None;
        }
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.columns[3].name = invalid.columns[0].name.clone();
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.columns[3].name.clear();
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.columns[3].attnum = invalid.columns[0].attnum;
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.columns[2].primary_key_ordinal = Some(2);
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.columns[2].nullable = true;
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.columns[0].data_type.kind = PgTypeKind::Unsupported {
            reason: "test-only unsupported payload".to_owned(),
        };
        assert!(reconciliation_copy_columns(&invalid).is_err());

        invalid = reconciliation_schema();
        invalid.name.name = "unsafe\0table".to_owned();
        assert!(reconciliation_copy_columns(&invalid).is_err());
    }

    #[test]
    fn controlled_settings_pin_every_textual_representation() {
        let sql = SnapshotSettings::default().sql();
        assert!(sql.contains("DateStyle = 'ISO, YMD'"));
        assert!(sql.contains("IntervalStyle = 'postgres'"));
        assert!(sql.contains("TimeZone = 'UTC'"));
        assert!(sql.contains("extra_float_digits = 3"));
        assert!(sql.contains("bytea_output = 'hex'"));
        assert!(sql.contains("client_encoding = 'UTF8'"));
        assert_eq!(sql.matches("SET LOCAL").count(), 6);
    }

    #[test]
    fn snapshot_literal_is_escaped() {
        let sql = set_snapshot_sql("000003A1-1'2").unwrap();
        assert!(sql.contains("'000003A1-1''2'"));
        assert!(set_snapshot_sql("bad\nvalue").is_err());
    }

    #[test]
    fn canonical_pk_sql_uses_typed_keyset_and_native_composite_order() {
        let schema = composite_schema();
        let sql = SnapshotSession::canonical_pk_page_sql(&schema, true, limits(2)).unwrap();

        // Check basic structure
        assert!(sql.contains("\"Tenant\"\"Id\""));
        assert!(sql.contains("\"Seq No\""));
        assert!(sql.contains("\"Sales\"\"Data\".\"Order\"\"Rows\""));
        assert!(sql.contains("ROW(\"Tenant\"\"Id\", \"Seq No\") > ROW("));
        assert!(sql.contains("ORDER BY \"Tenant\"\"Id\", \"Seq No\""));
        assert!(sql.contains("LIMIT 3")); // row_limit 2 + 1 lookahead

        let first_sql = SnapshotSession::canonical_pk_page_sql(&schema, false, limits(2)).unwrap();
        assert!(!first_sql.contains(" WHERE ROW("));
        assert!(!first_sql.contains("$1"));

        let row_sql = SnapshotSession::canonical_row_page_sql(&schema, true, limits(2)).unwrap();
        assert!(row_sql.contains("\"Payload\""));
    }

    #[test]
    fn canonical_page_limits_are_nonzero_and_add_lookahead_safely() {
        let schema = composite_schema();
        assert!(
            SnapshotSession::canonical_pk_page_sql(
                &schema,
                false,
                SnapshotPageLimits {
                    row_limit: 0,
                    max_page_bytes: 1,
                },
            )
            .is_err()
        );
        assert!(
            SnapshotSession::canonical_pk_page_sql(
                &schema,
                false,
                SnapshotPageLimits {
                    row_limit: 1,
                    max_page_bytes: 0,
                },
            )
            .is_err()
        );
        assert!(
            SnapshotSession::canonical_pk_page_sql(
                &schema,
                false,
                SnapshotPageLimits {
                    row_limit: usize::MAX,
                    max_page_bytes: 1,
                },
            )
            .is_err()
        );
    }

    #[test]
    fn key_pages_derive_bounded_first_and_unbounded_tail_copy_ranges() {
        let table_identity = SnapshotTableIdentity {
            relation_id: 12345,
            generation: 1,
            name: QualifiedName {
                schema: "public".to_owned(),
                name: "test".to_owned(),
            },
            key_columns: vec![SnapshotKeyColumnIdentity {
                attnum: 1,
                ordinal: 0,
                name: "id".to_owned(),
                data_type: PgType {
                    oid: 23,
                    name: QualifiedName {
                        schema: "pg_catalog".to_owned(),
                        name: "int4".to_owned(),
                    },
                    kind: cloudberry_etl_core::schema::PgTypeKind::Int4,
                },
                collation: None,
            }],
        };
        let first = finish_key_page(
            vec![key(&[b"a", b"1"]), key(&[b"a", b"2"]), key(&[b"a", b"3"])],
            2,
            false,
            "snapshot-a",
            table_identity.clone(),
            None,
        )
        .unwrap();
        assert!(first.has_more);
        assert_eq!(
            first.next_key,
            Some(vec![Bytes::from_static(b"a"), Bytes::from_static(b"2")])
        );
        assert_eq!(
            first.copy_range().unwrap(),
            SnapshotKeyRange {
                start_exclusive: None,
                end_inclusive: first.next_key.clone(),
                snapshot_id: "snapshot-a".to_owned(),
                table_identity: table_identity.clone(),
            }
        );

        let start = first.next_cursor().unwrap();
        let tail = finish_key_page(
            vec![key(&[b"b", b"1"])],
            2,
            false,
            "snapshot-a",
            table_identity.clone(),
            Some(start.key().to_vec()),
        )
        .unwrap();
        assert!(!tail.has_more);
        assert_eq!(
            tail.copy_range().unwrap(),
            SnapshotKeyRange {
                start_exclusive: Some(start.key().to_vec()),
                end_inclusive: None,
                snapshot_id: "snapshot-a".to_owned(),
                table_identity: table_identity.clone(),
            }
        );

        let tail_cursor = tail.next_cursor().unwrap();
        let empty = finish_key_page(
            Vec::new(),
            2,
            false,
            "snapshot-a",
            table_identity.clone(),
            Some(tail_cursor.key().to_vec()),
        )
        .unwrap();
        assert!(!empty.has_more);
        assert_eq!(
            empty.copy_range().unwrap(),
            SnapshotKeyRange {
                start_exclusive: Some(tail_cursor.key().to_vec()),
                end_inclusive: None,
                snapshot_id: "snapshot-a".to_owned(),
                table_identity: table_identity.clone(),
            }
        );
    }

    #[test]
    fn range_copy_uses_safe_typed_hex_keys_and_keeps_left_pk_bare() {
        let schema = composite_schema();
        let table_identity = SnapshotTableIdentity {
            relation_id: schema.relation_id,
            generation: schema.generation,
            name: schema.name.clone(),
            key_columns: vec![
                SnapshotKeyColumnIdentity {
                    attnum: 1,
                    ordinal: 1,
                    name: "Tenant\"Id".to_owned(),
                    data_type: PgType {
                        oid: 1,
                        name: QualifiedName {
                            schema: "pg_catalog".to_owned(),
                            name: "text".to_owned(),
                        },
                        kind: PgTypeKind::Text,
                    },
                    collation: None,
                },
                SnapshotKeyColumnIdentity {
                    attnum: 2,
                    ordinal: 2,
                    name: "Seq No".to_owned(),
                    data_type: PgType {
                        oid: 2,
                        name: QualifiedName {
                            schema: "pg_catalog".to_owned(),
                            name: "int8".to_owned(),
                        },
                        kind: PgTypeKind::Int8,
                    },
                    collation: None,
                },
            ],
        };
        let range = SnapshotKeyRange {
            start_exclusive: Some(vec![Bytes::from_static(b"a'\\"), Bytes::from_static(b"2")]),
            end_inclusive: Some(vec![Bytes::from_static(b"b"), Bytes::from_static(b"10")]),
            snapshot_id: "snapshot-a".to_owned(),
            table_identity,
        };
        let sql = SnapshotSession::copy_text_pk_range_sql(&schema, &range).unwrap();

        assert!(sql.starts_with(
            "COPY (SELECT \"Tenant\"\"Id\", \"Seq No\", \"Payload\" FROM \"Sales\"\"Data\".\"Order\"\"Rows\""
        ));
        assert!(sql.contains("ROW(\"Tenant\"\"Id\", \"Seq No\") > ROW("));
        assert!(sql.contains("ROW(\"Tenant\"\"Id\", \"Seq No\") <= ROW("));
        assert!(sql.contains("pg_catalog.decode('61275c', 'hex')"));
        assert!(sql.contains("AS \"pg_catalog\".\"int8\""));
        assert!(sql.contains("ORDER BY \"Tenant\"\"Id\", \"Seq No\""));
        assert!(!sql.contains("CAST(\"Tenant"));
        assert!(!sql.contains("a'\\"));
    }

    #[test]
    fn canonical_text_parameter_uses_postgres_text_format() {
        let value = CanonicalTextParameter("10");
        let mut encoded = BytesMut::new();
        assert!(matches!(value.encode_format(&Type::INT8), Format::Text));
        assert!(matches!(
            value.to_sql(&Type::INT8, &mut encoded).unwrap(),
            IsNull::No
        ));
        assert_eq!(&encoded[..], b"10");
    }
}
