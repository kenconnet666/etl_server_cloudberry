//! Runtime-neutral helpers for canonical whole-table reconciliation.
//!
//! This module builds the shared digest contract and consumes bounded canonical
//! `COPY text` streams. It deliberately does not own database sessions, WAL
//! boundaries, durable state, scheduling, or reload decisions.

use std::fmt;

use cloudberry_etl_core::schema::{PgType, TableSchema};
use cloudberry_etl_source_postgres::{SourceError, snapshot::reconciliation_copy_columns};
use cloudberry_etl_target_cloudberry::reconciliation::{
    RECONCILIATION_DIGEST_BYTES, ReconciliationStats,
};
use futures::{Stream, StreamExt};
use serde_json::Value;
use thiserror::Error;

use crate::{
    canonical_copy::{CanonicalCopyError, CanonicalCopyTextDigestStream},
    reconcile::{CanonicalMultisetDigest, DigestColumn, DigestContext, ReconcileError},
};

use super::PlannedTable;

pub const RECONCILIATION_DIGEST_DOMAIN: &str = "pg2cb-canonical-copy-multiset-v1";
const PORTABLE_TYPE_TAG_DOMAIN: &str = "pg2cb-portable-pg-type-v1";

/// Identifies the database side that produced a canonical COPY stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationSide {
    Source,
    Target,
}

impl fmt::Display for ReconciliationSide {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Source => "source",
            Self::Target => "target",
        })
    }
}

#[derive(Debug, Error)]
pub enum RuntimeReconciliationError {
    #[error("failed to derive the source reconciliation projection for `{table}`: {source}")]
    Projection {
        table: String,
        #[source]
        source: SourceError,
    },
    #[error("failed to serialize the portable type identity for column `{column}`: {source}")]
    PortableTypeTag {
        column: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid reconciliation digest context: {0}")]
    DigestContext(#[from] ReconcileError),
    #[error("{side} reconciliation COPY stream failed: {message}")]
    Stream {
        side: ReconciliationSide,
        message: String,
    },
    #[error("{side} reconciliation COPY data is invalid: {source}")]
    CopyData {
        side: ReconciliationSide,
        #[source]
        source: CanonicalCopyError,
    },
}

/// Builds the canonical digest contract for one immutable table plan.
pub fn digest_context_for_planned_table(
    table: &PlannedTable,
) -> Result<DigestContext, RuntimeReconciliationError> {
    digest_context_for_source(&table.source, &table.schema_fingerprint)
}

/// Builds a strict key-first digest contract from the admitted source schema.
///
/// Key columns follow primary-key ordinal. Non-key columns follow PostgreSQL
/// attribute number. This is the same projection used by the source COPY
/// reader, and every digest column keeps its source attribute number as its
/// stable ordinal.
pub fn digest_context_for_source(
    schema: &TableSchema,
    schema_fingerprint: &str,
) -> Result<DigestContext, RuntimeReconciliationError> {
    let projection = reconciliation_copy_columns(schema).map_err(|source| {
        RuntimeReconciliationError::Projection {
            table: schema.name.to_string(),
            source,
        }
    })?;

    let key_columns = projection
        .key_columns()
        .iter()
        .copied()
        .map(digest_column)
        .collect::<Result<Vec<_>, _>>()?;
    let value_columns = projection
        .value_columns()
        .iter()
        .copied()
        .map(digest_column)
        .collect::<Result<Vec<_>, _>>()?;
    let context = DigestContext {
        version_domain: RECONCILIATION_DIGEST_DOMAIN.to_owned(),
        schema_fingerprint: schema_fingerprint.to_owned(),
        key_columns,
        value_columns,
    };
    context.validate()?;
    Ok(context)
}

/// Consumes an arbitrary-chunk COPY stream without retaining completed rows.
///
/// `max_row_bytes` is enforced by [`CanonicalCopyTextDigestStream`] across
/// chunk boundaries. Upstream transport failures and malformed COPY data retain
/// their source/target side in the returned error.
pub async fn digest_copy_stream<S, B, E>(
    side: ReconciliationSide,
    context: &DigestContext,
    stream: S,
    max_row_bytes: usize,
) -> Result<ReconciliationStats, RuntimeReconciliationError>
where
    S: Stream<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    E: fmt::Display,
{
    let mut digest = CanonicalCopyTextDigestStream::new(context, max_row_bytes)
        .map_err(|source| RuntimeReconciliationError::CopyData { side, source })?;
    futures::pin_mut!(stream);
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| RuntimeReconciliationError::Stream {
            side,
            message: error.to_string(),
        })?;
        digest
            .feed(chunk)
            .map_err(|source| RuntimeReconciliationError::CopyData { side, source })?;
    }
    let digest = digest
        .finish()
        .map_err(|source| RuntimeReconciliationError::CopyData { side, source })?;
    Ok(reconciliation_stats(digest))
}

/// Converts the engine digest into the durable target-side aggregate format.
#[must_use]
pub fn reconciliation_stats(digest: CanonicalMultisetDigest) -> ReconciliationStats {
    let mut bytes = [0_u8; RECONCILIATION_DIGEST_BYTES];
    bytes[..32].copy_from_slice(&digest.accumulator_a);
    bytes[32..].copy_from_slice(&digest.accumulator_b);
    ReconciliationStats {
        rows: digest.row_count,
        bytes: digest.payload_bytes,
        digest: bytes,
    }
}

/// Requires equality of row count, canonical payload bytes, and all digest bits.
#[must_use]
pub fn stats_exactly_match(source: &ReconciliationStats, target: &ReconciliationStats) -> bool {
    source == target
}

fn digest_column(
    column: &cloudberry_etl_core::schema::ColumnSchema,
) -> Result<DigestColumn, RuntimeReconciliationError> {
    Ok(DigestColumn {
        ordinal: column.attnum as u32,
        portable_type_tag: portable_type_tag(&column.name, &column.data_type)?,
    })
}

fn portable_type_tag(
    column: &str,
    data_type: &PgType,
) -> Result<String, RuntimeReconciliationError> {
    let mut identity = serde_json::to_value(data_type).map_err(|source| {
        RuntimeReconciliationError::PortableTypeTag {
            column: column.to_owned(),
            source,
        }
    })?;
    remove_local_oids(&mut identity);
    let identity = serde_json::to_string(&identity).map_err(|source| {
        RuntimeReconciliationError::PortableTypeTag {
            column: column.to_owned(),
            source,
        }
    })?;
    Ok(format!("{PORTABLE_TYPE_TAG_DOMAIN}:{identity}"))
}

fn remove_local_oids(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                remove_local_oids(value);
            }
        }
        Value::Object(object) => {
            object.remove("oid");
            for value in object.values_mut() {
                remove_local_oids(value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, io};

    use bytes::Bytes;
    use cloudberry_etl_core::schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgTypeKind, QualifiedName, ReplicaIdentity,
        TableKind,
    };
    use futures::stream;

    use super::*;
    use crate::runtime::TargetStorage;

    fn pg_type(oid: u32, schema: &str, name: &str, kind: PgTypeKind) -> PgType {
        PgType {
            oid,
            name: QualifiedName::new(schema, name).unwrap(),
            kind,
        }
    }

    fn column(
        attnum: i16,
        name: &str,
        data_type: PgType,
        primary_key_ordinal: Option<u16>,
    ) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type,
            nullable: primary_key_ordinal.is_none(),
            primary_key_ordinal,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn table(columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            relation_id: 42,
            generation: 3,
            name: QualifiedName::new("public", "events").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns,
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn composite_table() -> TableSchema {
        table(vec![
            column(
                1,
                "tenant",
                pg_type(23, "pg_catalog", "int4", PgTypeKind::Int4),
                Some(2),
            ),
            column(
                2,
                "payload",
                pg_type(25, "pg_catalog", "text", PgTypeKind::Text),
                None,
            ),
            column(
                3,
                "sequence",
                pg_type(20, "pg_catalog", "int8", PgTypeKind::Int8),
                Some(1),
            ),
            column(
                4,
                "note",
                pg_type(
                    1043,
                    "pg_catalog",
                    "varchar",
                    PgTypeKind::VarChar { length: Some(64) },
                ),
                None,
            ),
        ])
    }

    #[test]
    fn context_uses_pk_ordinal_then_non_key_attnum() {
        let source = composite_table();
        let context = digest_context_for_source(&source, "sha256:test").unwrap();

        assert_eq!(
            context
                .key_columns
                .iter()
                .map(|column| column.ordinal)
                .collect::<Vec<_>>(),
            [3, 1]
        );
        assert_eq!(
            context
                .value_columns
                .iter()
                .map(|column| column.ordinal)
                .collect::<Vec<_>>(),
            [2, 4]
        );

        let planned = PlannedTable {
            source,
            target: QualifiedName::new("analytics", "events").unwrap(),
            shadow: QualifiedName::new("analytics", "events_shadow").unwrap(),
            staging_name: "events_stage".to_owned(),
            storage: TargetStorage::AoColumn,
            schema_fingerprint: "sha256:test".to_owned(),
        };
        assert_eq!(digest_context_for_planned_table(&planned).unwrap(), context);
    }

    #[test]
    fn portable_tags_include_typmods_and_user_type_definitions_but_not_oids() {
        let varchar_64 = portable_type_tag(
            "value",
            &pg_type(
                1043,
                "pg_catalog",
                "varchar",
                PgTypeKind::VarChar { length: Some(64) },
            ),
        )
        .unwrap();
        let varchar_128 = portable_type_tag(
            "value",
            &pg_type(
                1043,
                "pg_catalog",
                "varchar",
                PgTypeKind::VarChar { length: Some(128) },
            ),
        )
        .unwrap();
        assert_ne!(varchar_64, varchar_128);
        assert!(varchar_64.contains("pg_catalog"));
        assert!(varchar_64.contains("varchar"));
        assert!(varchar_64.contains("64"));

        let first = pg_type(
            90_001,
            "application",
            "status",
            PgTypeKind::Enum {
                labels: vec!["open".to_owned(), "closed".to_owned()],
            },
        );
        let mut restored = first.clone();
        restored.oid = 91_337;
        assert_eq!(
            portable_type_tag("status", &first).unwrap(),
            portable_type_tag("status", &restored).unwrap()
        );
        restored.kind = PgTypeKind::Enum {
            labels: vec!["open".to_owned(), "closed".to_owned(), "held".to_owned()],
        };
        assert_ne!(
            portable_type_tag("status", &first).unwrap(),
            portable_type_tag("status", &restored).unwrap()
        );

        let domain = pg_type(
            90_002,
            "application",
            "positive_id",
            PgTypeKind::Domain {
                base: Box::new(pg_type(23, "pg_catalog", "int4", PgTypeKind::Int4)),
                constraints: vec!["CHECK (VALUE > 0)".to_owned()],
            },
        );
        let mut restored_domain = domain.clone();
        restored_domain.oid = 90_102;
        if let PgTypeKind::Domain { base, .. } = &mut restored_domain.kind {
            base.oid = 999;
        }
        assert_eq!(
            portable_type_tag("id", &domain).unwrap(),
            portable_type_tag("id", &restored_domain).unwrap()
        );
    }

    #[tokio::test]
    async fn source_and_target_match_across_row_order_and_arbitrary_chunks() {
        let context = digest_context_for_source(&composite_table(), "sha256:test").unwrap();
        let source_wire = b"1\t10\talpha\tnote-a\n2\t20\tbeta\t\\N\n";
        let target_wire = b"2\t20\tbeta\t\\N\n1\t10\talpha\tnote-a\n";
        let source_chunks = source_wire
            .iter()
            .copied()
            .map(|byte| Ok::<_, Infallible>(Bytes::from(vec![byte])));
        let target_chunks = target_wire
            .chunks(7)
            .map(|chunk| Ok::<_, Infallible>(Bytes::copy_from_slice(chunk)));

        let source = digest_copy_stream(
            ReconciliationSide::Source,
            &context,
            stream::iter(source_chunks),
            1024,
        )
        .await
        .unwrap();
        let target = digest_copy_stream(
            ReconciliationSide::Target,
            &context,
            stream::iter(target_chunks),
            1024,
        )
        .await
        .unwrap();

        assert!(stats_exactly_match(&source, &target));
        assert_eq!(source.rows, 2);
        assert_eq!(source.bytes, 21);

        let mut changed = target;
        changed.bytes += 1;
        assert!(!stats_exactly_match(&source, &changed));
    }

    #[tokio::test]
    async fn stream_and_copy_failures_retain_their_side_and_bounds() {
        let context = digest_context_for_source(&composite_table(), "sha256:test").unwrap();
        let upstream = stream::iter([Err::<Bytes, _>(io::Error::other("connection lost"))]);
        let error = digest_copy_stream(ReconciliationSide::Target, &context, upstream, 1024)
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            RuntimeReconciliationError::Stream {
                side: ReconciliationSide::Target,
                ..
            }
        ));
        assert!(error.to_string().contains("target"));
        assert!(error.to_string().contains("connection lost"));

        let oversized = stream::iter([Ok::<_, Infallible>(Bytes::from_static(
            b"1\t10\talpha\tnote-a\n",
        ))]);
        let error = digest_copy_stream(ReconciliationSide::Source, &context, oversized, 8)
            .await
            .unwrap_err();
        assert!(matches!(
            &error,
            RuntimeReconciliationError::CopyData {
                side: ReconciliationSide::Source,
                source: CanonicalCopyError::RowTooLarge { .. }
            }
        ));
        assert!(error.to_string().contains("source"));
    }
}
