//! Opt-in PostgreSQL 18 coverage for whole-table reconciliation COPY.
//!
//! Run explicitly with a disposable PG18 instance:
//! `PG2CB_TEST_SOURCE_DSN=postgres://... cargo test -p cloudberry-etl-source-postgres --test reconciliation_copy_pg18 -- --ignored --nocapture`

use cloudberry_etl_core::schema::{
    ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
    ReplicaIdentity, TableKind, TableSchema,
};
use cloudberry_etl_source_postgres::{
    SourceError, SourceResult,
    snapshot::{begin_at_exported_snapshot, begin_exported_snapshot},
};
use futures::StreamExt;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 18 instance and PG2CB_TEST_SOURCE_DSN"]
async fn imported_snapshot_reconciliation_copy_is_canonical_and_isolated() -> SourceResult<()> {
    let dsn = std::env::var("PG2CB_TEST_SOURCE_DSN").map_err(|_| {
        SourceError::Contract(
            "PG2CB_TEST_SOURCE_DSN is required for the ignored integration test".to_owned(),
        )
    })?;
    let (mut exporter_client, exporter_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let exporter_task = tokio::spawn(async move {
        if let Err(error) = exporter_connection.await {
            eprintln!("reconciliation snapshot exporter connection ended: {error}");
        }
    });
    let (mut reader_client, reader_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let reader_task = tokio::spawn(async move {
        if let Err(error) = reader_connection.await {
            eprintln!("reconciliation snapshot reader connection ended: {error}");
        }
    });
    let (writer_client, writer_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let writer_task = tokio::spawn(async move {
        if let Err(error) = writer_connection.await {
            eprintln!("reconciliation snapshot writer connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let schema_name = format!("pg2cb_reconcile_copy_{suffix}");
    let table_name = "Order\"Rows";
    let quoted_schema = quote_identifier(&schema_name);
    let quoted_table = quote_identifier(table_name);
    writer_client
        .batch_execute(&format!(
            "CREATE SCHEMA {quoted_schema};
             CREATE TABLE {quoted_schema}.{quoted_table} (
                 \"Payload\" text,
                 \"Seq No\" bigint NOT NULL,
                 \"Tenant\"\"Id\" integer NOT NULL,
                 \"Other Value\" text,
                 PRIMARY KEY (\"Tenant\"\"Id\", \"Seq No\")
             );
             INSERT INTO {quoted_schema}.{quoted_table}
                 (\"Payload\", \"Seq No\", \"Tenant\"\"Id\", \"Other Value\")
             VALUES (E'before\\tline', 2, 7, E'slash\\\\end');"
        ))
        .await?;

    let result = async {
        let exporter = begin_exported_snapshot(&mut exporter_client).await?;
        let snapshot_id = exporter.snapshot_id().to_owned();
        let mut reader = begin_at_exported_snapshot(&mut reader_client, &snapshot_id).await?;

        writer_client
            .batch_execute(&format!(
                "UPDATE {quoted_schema}.{quoted_table} SET \"Payload\" = 'after';
                 INSERT INTO {quoted_schema}.{quoted_table}
                     (\"Payload\", \"Seq No\", \"Tenant\"\"Id\", \"Other Value\")
                 VALUES ('new', 3, 8, 'new');"
            ))
            .await?;

        let schema = table_schema(&schema_name, table_name);
        let copy = reader.copy_reconciliation_table(&schema).await?;
        let mut copy = Box::pin(copy);
        let mut bytes = Vec::new();
        while let Some(chunk) = copy.next().await {
            bytes.extend_from_slice(&chunk?);
        }

        // PK ordinal order is Tenant, Seq. Non-key values then follow attnum order: Payload, Other.
        // The later UPDATE and INSERT are both invisible through the imported exported snapshot.
        assert_eq!(bytes, b"7\t2\tbefore\\tline\tslash\\\\end\n");

        reader.rollback().await?;
        exporter.rollback().await?;
        Ok::<_, SourceError>(())
    }
    .await;

    let _ = writer_client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
        .await;
    exporter_task.abort();
    reader_task.abort();
    writer_task.abort();
    result
}

fn table_schema(schema_name: &str, table_name: &str) -> TableSchema {
    TableSchema {
        relation_id: 42,
        generation: 1,
        name: QualifiedName::new(schema_name, table_name).unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            column(1, "Payload", 25, "text", PgTypeKind::Text, None),
            column(2, "Seq No", 20, "int8", PgTypeKind::Int8, Some(2)),
            column(3, "Tenant\"Id", 23, "int4", PgTypeKind::Int4, Some(1)),
            column(4, "Other Value", 25, "text", PgTypeKind::Text, None),
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

fn column(
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

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
