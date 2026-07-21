//! Opt-in PostgreSQL 18 coverage for canonical PK paging and range COPY.
//!
//! Run explicitly with a disposable PG18 instance:
//! `PG2CB_TEST_SOURCE_DSN=postgres://... cargo test -p cloudberry-etl-source-postgres --test snapshot_page_pg18 -- --ignored --nocapture`

use bytes::Bytes;
use cloudberry_etl_core::schema::{
    ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
    ReplicaIdentity, TableKind, TableSchema,
};
use cloudberry_etl_source_postgres::{
    SourceError, SourceResult,
    snapshot::{CanonicalSnapshotCell, SnapshotPageLimits, begin_exported_snapshot},
};
use futures::StreamExt;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 18 instance and PG2CB_TEST_SOURCE_DSN"]
async fn canonical_composite_pk_pages_drive_direct_range_copy() -> SourceResult<()> {
    let dsn = std::env::var("PG2CB_TEST_SOURCE_DSN").map_err(|_| {
        SourceError::Contract(
            "PG2CB_TEST_SOURCE_DSN is required for the ignored integration test".to_owned(),
        )
    })?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("snapshot page connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let schema_name = format!("pg2cb_snapshot_page_{suffix}");
    let quoted_schema = quote_identifier(&schema_name);
    let quoted_table = quote_identifier("Order\"Rows");
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {quoted_schema};
             CREATE TABLE {quoted_schema}.{quoted_table} (
                 \"Tenant\"\"Id\" text NOT NULL,
                 \"Seq No\" bigint NOT NULL,
                 \"Payload\" text,
                 PRIMARY KEY (\"Tenant\"\"Id\", \"Seq No\")
             );
             INSERT INTO {quoted_schema}.{quoted_table} VALUES
                 ('a', 1, 'one'),
                 ('a', 2, NULL),
                 ('a', 10, 'ten'),
                 ('b', 1, 'bee');"
        ))
        .await?;

    let result = async {
        let schema = table_schema(&schema_name);
        let limits = SnapshotPageLimits {
            row_limit: 2,
            max_page_bytes: 1024 * 1024,
        };
        let mut session = begin_exported_snapshot(&mut client).await?;

        let first = session
            .read_canonical_pk_page(&schema, None, limits)
            .await?;
        assert!(first.has_more);
        assert_eq!(first.rows.len(), 2);
        assert_eq!(first.rows[0].key, key("a", "1"));
        assert_eq!(first.rows[1].key, key("a", "2"));
        assert_eq!(first.next_key, Some(key("a", "2")));
        assert!(first.materialized_bytes <= limits.max_page_bytes);

        let first_copy = {
            let range = first.copy_range()?;
            assert_eq!(range.start_exclusive(), None);
            assert_eq!(range.end_inclusive(), Some(key("a", "2").as_slice()));
            let stream = session.copy_text_pk_range(&schema, &range).await?;
            collect_copy(stream).await?
        };
        assert_eq!(first_copy, b"a\t1\tone\na\t2\t\\N\n");

        let start = first.next_cursor().expect("non-empty page has a cursor");
        let second = session
            .read_canonical_pk_page(&schema, Some(&start), limits)
            .await?;
        assert!(!second.has_more);
        assert_eq!(second.rows.len(), 2);
        // Native bigint order must put 10 after 2; lexical text order would not.
        assert_eq!(second.rows[0].key, key("a", "10"));
        assert_eq!(second.rows[1].key, key("b", "1"));
        let second_copy = {
            let range = second.copy_range()?;
            assert_eq!(range.start_exclusive(), Some(start.key()));
            assert_eq!(range.end_inclusive(), None);
            let stream = session.copy_text_pk_range(&schema, &range).await?;
            collect_copy(stream).await?
        };
        assert_eq!(second_copy, b"a\t10\tten\nb\t1\tbee\n");

        let tail_cursor = second.next_cursor().expect("non-empty page has a cursor");
        let empty = session
            .read_canonical_pk_page(&schema, Some(&tail_cursor), limits)
            .await?;
        assert!(empty.rows.is_empty());
        assert!(!empty.has_more);
        assert_eq!(empty.next_key, None);

        let full_rows = session
            .read_canonical_row_page(&schema, None, limits)
            .await?;
        assert_eq!(full_rows.rows.len(), 2);
        assert_eq!(full_rows.rows[1].values[2], CanonicalSnapshotCell::Null);

        let budget_error = session
            .read_canonical_pk_page(
                &schema,
                None,
                SnapshotPageLimits {
                    row_limit: 1,
                    max_page_bytes: 1,
                },
            )
            .await
            .expect_err("one canonical key row must exceed a one-byte page budget");
        assert!(budget_error.to_string().contains("max_page_bytes"));

        session.rollback().await?;
        Ok::<_, SourceError>(())
    }
    .await;

    let _ = client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
        .await;
    connection_task.abort();
    result
}

async fn collect_copy(
    stream: cloudberry_etl_source_postgres::snapshot::SnapshotCopy<'_>,
) -> SourceResult<Vec<u8>> {
    let mut stream = Box::pin(stream);
    let mut copied = Vec::new();
    while let Some(chunk) = stream.next().await {
        copied.extend_from_slice(&chunk?);
    }
    Ok(copied)
}

fn key(tenant: &str, sequence: &str) -> Vec<Bytes> {
    vec![
        Bytes::copy_from_slice(tenant.as_bytes()),
        Bytes::copy_from_slice(sequence.as_bytes()),
    ]
}

fn table_schema(schema_name: &str) -> TableSchema {
    TableSchema {
        relation_id: 42,
        generation: 1,
        name: QualifiedName::new(schema_name, "Order\"Rows").unwrap(),
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

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
