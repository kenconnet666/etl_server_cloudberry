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
use cloudberry_etl_core::schema::QualifiedName;
use futures::Stream;
use tokio_postgres::{Client, CopyOutStream, IsolationLevel, Transaction};

use crate::{
    SourceError, SourceResult,
    sql::{quote_literal, quote_qualified},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyFormat {
    Binary,
    Text,
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

    /// Build a parameter-free COPY query. Identifiers are quoted and the format is fixed to a
    /// closed enum, so data values never enter SQL text.
    pub fn copy_sql(table: &QualifiedName, format: CopyFormat) -> SourceResult<String> {
        Ok(format!(
            "COPY (SELECT * FROM {}) TO STDOUT WITH (FORMAT {}, HEADER false)",
            quote_qualified(&table.schema, &table.name)?,
            format.sql()
        ))
    }

    /// Start a COPY OUT stream. The borrow ties the stream to this transaction, preventing an
    /// accidental commit while PostgreSQL is still producing bytes.
    pub async fn copy_table<'session>(
        &'session mut self,
        table: &QualifiedName,
        format: CopyFormat,
    ) -> SourceResult<SnapshotCopy<'session>> {
        let sql = Self::copy_sql(table, format)?;
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
    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
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
    fn copy_sql_quotes_identifiers_and_has_no_values() {
        let table = QualifiedName::new("s\"x", "t\"x").unwrap();
        let sql = SnapshotSession::copy_sql(&table, CopyFormat::Binary).unwrap();
        assert!(sql.contains("\"s\"\"x\".\"t\"\"x\""));
        assert!(sql.contains("FORMAT binary"));
    }

    #[test]
    fn snapshot_literal_is_escaped() {
        let sql = set_snapshot_sql("000003A1-1'2").unwrap();
        assert!(sql.contains("'000003A1-1''2'"));
        assert!(set_snapshot_sql("bad\nvalue").is_err());
    }
}
