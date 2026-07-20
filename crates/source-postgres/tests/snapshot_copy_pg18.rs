//! Opt-in PostgreSQL 18 coverage for controlled text snapshot COPY.

use cloudberry_etl_core::schema::{
    ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
    ReplicaIdentity, TableKind, TableSchema,
};
use cloudberry_etl_source_postgres::{
    SourceError, SourceResult, snapshot::begin_exported_snapshot,
};
use futures::{SinkExt, StreamExt};
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 18 instance and PG2CB_TEST_SOURCE_DSN"]
async fn controlled_text_copy_round_trips_strong_types() -> SourceResult<()> {
    let dsn = std::env::var("PG2CB_TEST_SOURCE_DSN").map_err(|_| {
        SourceError::Contract(
            "PG2CB_TEST_SOURCE_DSN is required for the ignored integration test".to_owned(),
        )
    })?;
    let (mut source_client, source_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let source_task = tokio::spawn(async move {
        if let Err(error) = source_connection.await {
            eprintln!("snapshot source connection ended: {error}");
        }
    });
    let (target_client, target_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let target_task = tokio::spawn(async move {
        if let Err(error) = target_connection.await {
            eprintln!("snapshot target connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let schema_name = format!("pg2cb_snapshot_copy_{suffix}");
    let quoted_schema = quote_identifier(&schema_name);
    let result = async {
        source_client
            .batch_execute(&format!(
                "CREATE SCHEMA {quoted_schema};
                 CREATE TABLE {quoted_schema}.source_rows (
                     id bigint PRIMARY KEY,
                     happened_on date,
                     happened_at timestamptz,
                     elapsed interval,
                     ratio double precision,
                     raw bytea,
                     payload text
                 );
                 CREATE TABLE {quoted_schema}.target_rows (
                     id bigint PRIMARY KEY,
                     happened_on date,
                     happened_at timestamptz,
                     elapsed interval,
                     ratio double precision,
                     raw bytea,
                     payload text
                 );
                 INSERT INTO {quoted_schema}.source_rows VALUES
                    (1, DATE '2026-07-20', TIMESTAMPTZ '2026-07-20 12:34:56.123456+08',
                     INTERVAL '1 day 02:03:04.5', 1.2345678901234567,
                     decode('0009ff', 'hex'), E'tab\\tnewline\\nslash\\\\'),
                    (2, NULL, NULL, NULL, 'Infinity'::float8, NULL, NULL);"
            ))
            .await?;

        let source_schema = table_schema(&schema_name);
        let mut session = begin_exported_snapshot(&mut source_client).await?;
        let copied_rows = {
            let stream = session.copy_text_table(&source_schema).await?;
            let mut stream = Box::pin(stream);
            let copy_sql = format!(
                "COPY {quoted_schema}.target_rows (id, happened_on, happened_at, elapsed, ratio, raw, payload) FROM STDIN WITH (FORMAT text, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')"
            );
            let sink = target_client.copy_in(&copy_sql).await?;
            let mut sink = std::pin::pin!(sink);
            while let Some(chunk) = stream.next().await {
                sink.send(chunk?).await?;
            }
            sink.finish().await?
        };
        session.commit().await?;
        assert_eq!(copied_rows, 2);

        let identical: bool = target_client
            .query_one(
                &format!(
                    "SELECT count(*) = 2 AND bool_and(
                         s.happened_on IS NOT DISTINCT FROM t.happened_on
                         AND s.happened_at IS NOT DISTINCT FROM t.happened_at
                         AND s.elapsed IS NOT DISTINCT FROM t.elapsed
                         AND s.ratio IS NOT DISTINCT FROM t.ratio
                         AND s.raw IS NOT DISTINCT FROM t.raw
                         AND s.payload IS NOT DISTINCT FROM t.payload)
                     FROM {quoted_schema}.source_rows AS s
                     JOIN {quoted_schema}.target_rows AS t USING (id)"
                ),
                &[],
            )
            .await?
            .try_get(0)?;
        assert!(identical);
        Ok::<_, SourceError>(())
    }
    .await;

    let _ = target_client
        .batch_execute(&format!("DROP SCHEMA IF EXISTS {quoted_schema} CASCADE"))
        .await;
    source_task.abort();
    target_task.abort();
    result
}

fn table_schema(schema_name: &str) -> TableSchema {
    TableSchema {
        relation_id: 42,
        generation: 1,
        name: QualifiedName::new(schema_name, "source_rows").unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            column(1, "id", PgTypeKind::Int8, Some(1)),
            column(2, "happened_on", PgTypeKind::Date, None),
            column(
                3,
                "happened_at",
                PgTypeKind::Timestamp {
                    precision: Some(6),
                    with_time_zone: true,
                },
                None,
            ),
            column(
                4,
                "elapsed",
                PgTypeKind::Interval { precision: Some(6) },
                None,
            ),
            column(5, "ratio", PgTypeKind::Float8, None),
            column(6, "raw", PgTypeKind::Bytea, None),
            column(7, "payload", PgTypeKind::Text, None),
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

fn column(attnum: i16, name: &str, kind: PgTypeKind, pk: Option<u16>) -> ColumnSchema {
    ColumnSchema {
        attnum,
        name: name.to_owned(),
        data_type: PgType {
            oid: u32::try_from(attnum).unwrap(),
            name: QualifiedName::new("pg_catalog", name).unwrap(),
            kind,
        },
        nullable: pk.is_none(),
        primary_key_ordinal: pk,
        generated: GeneratedColumn::None,
        identity: IdentityColumn::None,
        collation: None,
    }
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
