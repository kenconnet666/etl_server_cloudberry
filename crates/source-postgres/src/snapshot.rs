//! Exported-snapshot readers for initial loads and shadow rebuilds.
//!
//! Snapshot operations use the official `tokio-postgres` client. The replication fork is kept out
//! of this module so a snapshot transaction can never accidentally be passed to a replication
//! connection.

use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use cloudberry_etl_core::schema::{QualifiedName, TableSchema};
use futures::Stream;
use tokio_postgres::{Client, CopyOutStream, IsolationLevel, Transaction};

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
            "SET LOCAL DateStyle = '{}'; SET LOCAL IntervalStyle = '{}'; SET LOCAL TimeZone = '{}'; SET LOCAL extra_float_digits = {}; SET LOCAL bytea_output = '{}'; SET LOCAL client_encoding = '{}';",
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

    /// Commit only after all COPY streams have been drained. Dropping this session rolls back.
    pub async fn commit(self) -> SourceResult<()> {
        self.transaction.commit().await.map_err(SourceError::from)
    }

    /// Explicitly abort and release the exported snapshot transaction.
    pub async fn rollback(self) -> SourceResult<()> {
        self.transaction.rollback().await.map_err(SourceError::from)
    }
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
    use super::*;

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
    fn controlled_settings_pin_every_textual_representation() {
        let sql = SnapshotSettings::default().sql();
        assert!(sql.contains("DateStyle = 'ISO'"));
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
}
