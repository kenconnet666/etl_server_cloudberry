//! End-to-end bounded-snapshot test across a real PostgreSQL 18 source and a
//! real Apache Cloudberry 2.1 target.
//!
//! Mirrors `runtime::job::load_table_snapshot`: page the source by canonical PK
//! range, stream each range through the target `SnapshotPageLoader`, activate the
//! group, then assert the Cloudberry table matches the PG18 source row-for-row.
//!
//! Opt-in — requires both databases:
//! `PG2CB_TEST_SOURCE_DSN=postgres://... PG2CB_TEST_TARGET_DSN=postgres://... \
//!   cargo test -p cloudberry-etl-engine --test snapshot_e2e -- --ignored --nocapture`

use std::error::Error;

use bytes::Bytes;
use cloudberry_etl_core::schema::{
    ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
    ReplicaIdentity, TableKind, TableSchema,
};
use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use cloudberry_etl_source_postgres::snapshot::{SnapshotPageLimits, begin_exported_snapshot};
use cloudberry_etl_target_cloudberry::{
    checkpoint::{CheckpointKey, NodeCheckpoint, PipelineFence, activate_pipeline_fence},
    migration::migrate_target_database,
    snapshot::{
        SnapshotActivationRequest, SnapshotOwnership, SnapshotPageApplyOutcome,
        activate_snapshot_group, begin_snapshot_group, begin_snapshot_pages, plan_snapshot_target,
    },
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const PAGE_ROWS: usize = 3;

#[tokio::test]
#[ignore = "requires real PG18 (PG2CB_TEST_SOURCE_DSN) and Cloudberry 2.1 (PG2CB_TEST_TARGET_DSN)"]
async fn bounded_snapshot_pg18_to_cloudberry_matches_source() -> Result<(), Box<dyn Error>> {
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;

    let (mut source, source_conn) = tokio_postgres::connect(&source_dsn, NoTls).await?;
    let source_task = tokio::spawn(async move {
        if let Err(error) = source_conn.await {
            eprintln!("source connection ended: {error}");
        }
    });
    let (mut target, target_conn) = tokio_postgres::connect(&target_dsn, NoTls).await?;
    let target_task = tokio::spawn(async move {
        if let Err(error) = target_conn.await {
            eprintln!("target connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let source_schema_name = format!("pg2cb_e2e_src_{suffix}");
    let target_schema_name = format!("pg2cb_e2e_dst_{suffix}");
    let pipeline_id = PipelineId::new();

    let result = run_e2e(
        &mut source,
        &mut target,
        &source_schema_name,
        &target_schema_name,
        pipeline_id,
    )
    .await;

    // Best-effort cleanup on both ends.
    let _ = source
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {source_schema_name} CASCADE"
        ))
        .await;
    let _ = target
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {target_schema_name} CASCADE"
        ))
        .await;
    let _ = target
        .execute(
            "DELETE FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await;
    source_task.abort();
    target_task.abort();
    result
}

async fn run_e2e(
    source: &mut Client,
    target: &mut Client,
    source_schema_name: &str,
    target_schema_name: &str,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    // --- Seed the PG18 source: composite-PK table with rows spanning several pages.
    source
        .batch_execute(&format!(
            "CREATE SCHEMA {src};
             CREATE TABLE {src}.orders (
                 tenant text COLLATE \"C\" NOT NULL,
                 seq bigint NOT NULL,
                 payload text,
                 PRIMARY KEY (tenant, seq)
             );
             INSERT INTO {src}.orders (tenant, seq, payload)
             SELECT t.tenant, g.seq,
                    CASE WHEN g.seq % 4 = 0 THEN NULL ELSE 'p-' || t.tenant || '-' || g.seq END
               FROM (VALUES ('a'), ('b')) AS t(tenant),
                    generate_series(1, 5) AS g(seq);",
            src = quote_ident(source_schema_name)
        ))
        .await?;

    let source_relation_id: u32 = source
        .query_one(
            "SELECT c.oid::int8 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname = $1 AND c.relname = 'orders'",
            &[&source_schema_name],
        )
        .await?
        .get::<_, i64>(0) as u32;

    let schema = orders_schema(source_schema_name, source_relation_id);

    // --- Target migration + fence.
    migrate_target_database(target).await?;
    let fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 7,
    };
    activate_pipeline_fence(target, fence).await?;

    let target_name = QualifiedName::new(target_schema_name, "orders").unwrap();
    let shadow_name = QualifiedName::new(target_schema_name, "orders__shadow").unwrap();
    let plan = plan_snapshot_target(&schema, target_name, shadow_name)?;
    let snapshot_group_id = Uuid::now_v7();
    let ownership = SnapshotOwnership {
        fence,
        snapshot_group_id,
        schema_fingerprint: "sha256:e2e-orders".to_owned(),
    };
    let request = SnapshotActivationRequest {
        snapshot_group_id,
        fence,
        tables: vec![plan.activation_table("sha256:e2e-orders".to_owned())],
        initial_checkpoints: vec![NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: 0,
            },
            system_identifier: 1,
            timeline: 1,
            slot_name: "pg2cb_e2e_slot".to_owned(),
            applied_lsn: PgLsn::new(1),
        }],
    };
    begin_snapshot_group(target, &request).await?;

    // --- Page the source and load each range into the target shadow, exactly as
    //     runtime::job::load_table_snapshot does.
    let mut loader = begin_snapshot_pages(target, plan.clone(), &ownership).await?;
    let limits = SnapshotPageLimits {
        row_limit: PAGE_ROWS,
        max_page_bytes: 1024 * 1024,
    };
    let mut session = begin_exported_snapshot(source).await?;
    let mut cursor = None;
    let mut pages = 0u64;
    loop {
        let page = session
            .read_canonical_pk_page(&schema, cursor.as_ref(), limits)
            .await?;
        let has_more = page.has_more;
        let next_cursor_token = page.next_cursor();
        let next_cursor_text = match &next_cursor_token {
            Some(token) => key_to_text(token.key()),
            None => loader.cursor().to_vec(),
        };
        let range = page.copy_range()?;
        let completed = !has_more;
        let stream = session.copy_text_pk_range(&schema, &range).await?;
        let outcome = loader
            .apply_page(target, next_cursor_text, completed, stream)
            .await?;
        assert!(
            matches!(outcome, SnapshotPageApplyOutcome::Applied(_)),
            "page {pages} should apply: {outcome:?}"
        );
        pages += 1;
        if completed {
            break;
        }
        cursor = next_cursor_token;
    }
    session.commit().await?;
    assert!(
        pages >= 2,
        "10 rows / {PAGE_ROWS} per page should be >1 page"
    );

    // --- Activate the loaded shadow as the live target table.
    let activation = activate_snapshot_group(target, &request).await?;
    assert_eq!(
        activation.disposition,
        cloudberry_etl_target_cloudberry::snapshot::SnapshotActivationDisposition::Activated
    );

    // --- Verify the Cloudberry table matches the PG18 source row-for-row.
    let source_rows = source
        .query(
            &format!(
                "SELECT tenant, seq, payload FROM {src}.orders ORDER BY tenant, seq",
                src = quote_ident(source_schema_name)
            ),
            &[],
        )
        .await?;
    let target_rows = target
        .query(
            &format!(
                "SELECT tenant, seq, payload FROM {dst}.orders ORDER BY tenant, seq",
                dst = quote_ident(target_schema_name)
            ),
            &[],
        )
        .await?;
    assert_eq!(
        source_rows.len(),
        target_rows.len(),
        "row count must match ({} source vs {} target)",
        source_rows.len(),
        target_rows.len()
    );
    assert_eq!(source_rows.len(), 10, "seeded 10 rows");
    for (s, t) in source_rows.iter().zip(&target_rows) {
        let (st, ss, sp): (String, i64, Option<String>) = (s.get(0), s.get(1), s.get(2));
        let (tt, ts, tp): (String, i64, Option<String>) = (t.get(0), t.get(1), t.get(2));
        assert_eq!((st, ss, sp), (tt, ts, tp), "row mismatch after snapshot");
    }
    Ok(())
}

fn orders_schema(schema_name: &str, relation_id: u32) -> TableSchema {
    TableSchema {
        relation_id,
        generation: 1,
        name: QualifiedName::new(schema_name, "orders").unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            typed_column(1, "tenant", 25, "text", PgTypeKind::Text, Some(1), false),
            typed_column(2, "seq", 20, "int8", PgTypeKind::Int8, Some(2), false),
            typed_column(3, "payload", 25, "text", PgTypeKind::Text, None, true),
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

fn typed_column(
    attnum: i16,
    name: &str,
    type_oid: u32,
    type_name: &str,
    kind: PgTypeKind,
    primary_key_ordinal: Option<u16>,
    nullable: bool,
) -> ColumnSchema {
    let stable_text_key = primary_key_ordinal.is_some() && kind == PgTypeKind::Text;
    ColumnSchema {
        attnum,
        name: name.to_owned(),
        data_type: PgType {
            oid: type_oid,
            name: QualifiedName::new("pg_catalog", type_name).unwrap(),
            kind,
        },
        nullable,
        primary_key_ordinal,
        generated: GeneratedColumn::None,
        identity: IdentityColumn::None,
        collation: stable_text_key.then(|| QualifiedName::new("pg_catalog", "C").unwrap()),
    }
}

fn key_to_text(key: &[Bytes]) -> Vec<String> {
    key.iter()
        .map(|value| String::from_utf8(value.to_vec()).expect("canonical key is UTF-8"))
        .collect()
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
