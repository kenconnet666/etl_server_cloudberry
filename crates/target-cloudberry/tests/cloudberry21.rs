//! Opt-in Apache Cloudberry 2.1 integration coverage.

use std::{collections::BTreeMap, error::Error, io, sync::Arc};

use bytes::Bytes;
use cloudberry_etl_core::{
    change::Cell,
    id::PipelineId,
    lsn::PgLsn,
    schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
        ReplicaIdentity, TableKind, TableSchema,
    },
};
use cloudberry_etl_target_cloudberry::{
    apply::{
        ApplyError, ApplyRequest, AtomicApplyOutcome, DataChunkDisposition, LedgeredCommitKind,
        LedgeredCommitObserver, LedgeredCommitPhase, LedgeredDataChunkOutcome,
        LedgeredDataChunkRequest, StageOperation, StagingRow, TableApplyBatch, execute_apply,
        execute_atomic_apply, execute_atomic_apply_observed, execute_ledgered_data_chunk,
        plan_apply_with_storage,
    },
    checkpoint::{
        AdvanceOutcome, CheckpointKey, NodeCheckpoint, PipelineFence, activate_pipeline_fence,
        load_node_checkpoint,
    },
    chunk::{DataChunkIdentity, TransactionChunkKey, TransactionChunkManifest},
    managed::{ManagedTableError, TableApplyIdentity},
    migration::migrate_target_database,
    schema::plan_create_table_with_storage,
    snapshot::{
        ActiveTableRequirement, SnapshotActivationDisposition, SnapshotActivationRequest,
        SnapshotApplyMode, SnapshotApplyOutcome, SnapshotGroupCleanupRequest,
        SnapshotGroupRegistrationDisposition, SnapshotGroupStatus, SnapshotOwnership,
        SnapshotPageApplyOutcome, SnapshotTargetError, SnapshotTargetPlan, activate_snapshot_group,
        activate_table_snapshot_group, adopt_table_snapshot_replay_group, begin_snapshot_apply,
        begin_snapshot_group, begin_snapshot_pages, cleanup_loading_snapshot_group,
        cleanup_stale_snapshot_groups, load_snapshot_group_manifest, plan_snapshot_target,
        reset_interrupted_table_snapshot_group, validate_active_snapshot_group,
        validate_active_tables,
    },
    storage::{TargetStorage, load_relation_storage},
};
use futures::{SinkExt, future::try_join_all};
use serde_json::json;
use tokio::time::Instant;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const APPLY_SCHEMA_FINGERPRINT: &str = "sha256:cloudberry21-typed-apply-v1";

#[tokio::test]
#[ignore = "benchmark: requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_parallel_aoco_copy_benchmark() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let rows_per_table = benchmark_usize("PG2CB_PARALLEL_COPY_ROWS", 100_000)?;
    let table_count = benchmark_usize("PG2CB_PARALLEL_COPY_TABLES", 4)?;
    let samples = benchmark_usize("PG2CB_PARALLEL_COPY_SAMPLES", 3)?;
    if rows_per_table == 0 || !(2..=16).contains(&table_count) || samples == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "parallel COPY requires positive rows/samples and between 2 and 16 tables",
        )
        .into());
    }

    let (coordinator, coordinator_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let coordinator_task = tokio::spawn(log_connection(coordinator_connection));
    let mut workers = Vec::with_capacity(table_count);
    let mut worker_tasks = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
        workers.push(client);
        worker_tasks.push(tokio::spawn(log_connection(connection)));
    }

    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_parallel_{}", &suffix[..12]);
    let result = async {
        coordinator
            .batch_execute(&format!(
                "CREATE SCHEMA {}",
                quote_identifier(&target_schema)
            ))
            .await?;
        for table_index in 0..table_count {
            coordinator
                .batch_execute(&format!(
                    "CREATE TABLE {}.{} (id bigint, payload text) \
                     USING ao_column WITH (compresstype='zstd', compresslevel=1) \
                     DISTRIBUTED BY (id)",
                    quote_identifier(&target_schema),
                    quote_identifier(&format!("items_{table_index}")),
                ))
                .await?;
        }
        let rows = (0..rows_per_table)
            .map(|index| Bytes::from(format!("{}\t{}\n", index + 1, "x".repeat(64))))
            .collect::<Vec<_>>();
        let total_rows = rows_per_table
            .checked_mul(table_count)
            .ok_or_else(|| io::Error::other("parallel COPY row count overflowed"))?;

        let parallelism_levels = [1, 2, 4]
            .into_iter()
            .filter(|parallelism| *parallelism <= table_count)
            .collect::<Vec<_>>();
        let mut measurements = parallelism_levels
            .iter()
            .map(|parallelism| (*parallelism, Vec::with_capacity(samples)))
            .collect::<BTreeMap<_, _>>();
        for sample in 0..samples {
            let levels = if sample.is_multiple_of(2) {
                parallelism_levels.to_vec()
            } else {
                parallelism_levels.iter().rev().copied().collect::<Vec<_>>()
            };
            for parallelism in levels {
                truncate_parallel_tables(&coordinator, &target_schema, table_count).await?;
                let started = Instant::now();
                for first_table in (0..table_count).step_by(parallelism) {
                    let last_table = (first_table + parallelism).min(table_count);
                    let copies = (first_table..last_table).map(|table_index| {
                        copy_parallel_table(
                            &workers[table_index - first_table],
                            &target_schema,
                            table_index,
                            &rows,
                        )
                    });
                    try_join_all(copies).await?;
                }
                measurements
                    .get_mut(&parallelism)
                    .expect("the level was initialized")
                    .push(started.elapsed().as_secs_f64());
            }
        }
        for (parallelism, mut measured) in measurements {
            measured.sort_by(f64::total_cmp);
            let median_seconds = measured[measured.len() / 2];
            println!(
                "PG2CB_PARALLEL_COPY_BENCH_RESULT {}",
                json!({
                    "access_method": "ao_column",
                    "tables": table_count,
                    "rows_per_table": rows_per_table,
                    "total_rows": total_rows,
                    "connections": parallelism,
                    "samples": samples,
                    "median_seconds": median_seconds,
                    "rows_per_second": total_rows as f64 / median_seconds,
                })
            );
        }
        Ok::<_, Box<dyn Error>>(())
    }
    .await;

    let _ = coordinator
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&target_schema)
        ))
        .await;
    coordinator_task.abort();
    for task in worker_tasks {
        task.abort();
    }
    result
}

async fn log_connection<S, T>(connection: tokio_postgres::Connection<S, T>)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    T: tokio_postgres::tls::TlsStream + Unpin,
{
    if let Err(error) = connection.await {
        eprintln!("parallel COPY benchmark connection ended: {error}");
    }
}

fn benchmark_usize(name: &str, default: usize) -> Result<usize, Box<dyn Error>> {
    match std::env::var(name) {
        Ok(value) => Ok(value.parse()?),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

async fn truncate_parallel_tables(
    client: &Client,
    target_schema: &str,
    table_count: usize,
) -> Result<(), tokio_postgres::Error> {
    let tables = (0..table_count)
        .map(|table_index| {
            format!(
                "{}.{}",
                quote_identifier(target_schema),
                quote_identifier(&format!("items_{table_index}"))
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    client.batch_execute(&format!("TRUNCATE {tables}")).await
}

async fn copy_parallel_table(
    client: &Client,
    target_schema: &str,
    table_index: usize,
    rows: &[Bytes],
) -> Result<(), tokio_postgres::Error> {
    let sink = client
        .copy_in(&format!(
            "COPY {}.{} (id, payload) FROM STDIN",
            quote_identifier(target_schema),
            quote_identifier(&format!("items_{table_index}"))
        ))
        .await?;
    let mut sink = std::pin::pin!(sink);
    for row in rows {
        sink.send(row.clone()).await?;
    }
    sink.finish().await?;
    Ok(())
}

struct FailAfterAtomicCommit;

impl LedgeredCommitObserver for FailAfterAtomicCommit {
    fn observe(&self, phase: LedgeredCommitPhase, kind: LedgeredCommitKind) -> Result<(), String> {
        if phase == LedgeredCommitPhase::AfterCommit && kind == LedgeredCommitKind::AtomicBatch {
            Err("injected lost atomic batch commit response".to_owned())
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_typed_apply_move_and_checkpoint() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_it_{suffix}");
    let pipeline_id = PipelineId::new();
    let result = run_test(&mut client, &target_schema, pipeline_id).await;
    cleanup(&client, &target_schema, pipeline_id).await;
    connection_task.abort();
    result
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_aoco_supports_current_state_dml_and_types() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });
    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_storage_{suffix}");
    let result = async {
        migrate_target_database(&mut client).await?;
        client
            .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(&target_schema)))
            .await?;
        let source = source_schema();
        let target = QualifiedName::new(&target_schema, "items_aoco")?;
        let plan = plan_create_table_with_storage(&source, target.clone(), TargetStorage::AoColumn)?;
        client.batch_execute(&plan.create_sql).await?;
        assert_eq!(
            load_relation_storage(&client, &target).await?.as_deref(),
            Some("ao_column")
        );
        let relation = format!("{}.items_aoco", quote_identifier(&target_schema));
        client
            .batch_execute(&format!(
                "INSERT INTO {relation} (id, payload, quantity) VALUES (1, 'initial', 1); \
                 UPDATE {relation} SET payload = 'updated', quantity = 3 WHERE id = 1; \
                 DELETE FROM {relation} WHERE id = 1;"
            ))
            .await?;
        let count: i64 = client
            .query_one(&format!("SELECT count(*) FROM {relation}"), &[])
            .await?
            .try_get(0)?;
        assert_eq!(count, 0);
        let type_contract = format!(
            "{}.aoco_type_contract",
            quote_identifier(&target_schema)
        );
        client
            .batch_execute(&format!(
                "CREATE TABLE {type_contract} (\
                    id bigint PRIMARY KEY, flag boolean NOT NULL, amount numeric(12,2), \
                    occurred_at timestamp(6) with time zone, payload jsonb, labels text[], \
                    binary_value bytea, network inet, token uuid\
                 ) USING ao_column WITH (compresstype='zstd', compresslevel=1) DISTRIBUTED BY (id); \
                 INSERT INTO {type_contract} VALUES (\
                    1, true, 123.45, '2026-07-22 12:34:56.123456+08', \
                    '{{\"state\":\"new\"}}', ARRAY['a', 'b'], '\\x00ff', '10.0.0.1/24', \
                    '550e8400-e29b-41d4-a716-446655440000'\
                 ); \
                 UPDATE {type_contract} SET payload = '{{\"state\":\"updated\"}}', amount = 234.56 WHERE id = 1;"
            ))
            .await?;
        let typed_row = client
            .query_one(
                &format!(
                    "SELECT flag, amount::text, payload->>'state', labels, encode(binary_value, 'hex'), network::text, token::text FROM {type_contract} WHERE id = 1"
                ),
                &[],
            )
            .await?;
        assert!(typed_row.try_get::<_, bool>(0)?);
        assert_eq!(typed_row.try_get::<_, String>(1)?, "234.56");
        assert_eq!(typed_row.try_get::<_, String>(2)?, "updated");
        assert_eq!(typed_row.try_get::<_, Vec<String>>(3)?, vec!["a", "b"]);
        assert_eq!(typed_row.try_get::<_, String>(4)?, "00ff");
        assert_eq!(typed_row.try_get::<_, String>(5)?, "10.0.0.1/24");
        assert_eq!(
            typed_row.try_get::<_, String>(6)?,
            "550e8400-e29b-41d4-a716-446655440000"
        );
        Ok::<_, Box<dyn Error>>(())
    }
    .await;
    cleanup(&client, &target_schema, PipelineId::new()).await;
    connection_task.abort();
    result
}

#[tokio::test]
#[ignore = "experimental: requires a PAX-enabled Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_pax_experimental_smoke() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("experimental PAX connection ended: {error}");
        }
    });
    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_pax_{suffix}");
    let result = async {
        let pax_available: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_am WHERE amtype = 't' AND amname = 'pax')",
                &[],
            )
            .await?
            .try_get(0)?;
        if !pax_available {
            return Ok::<_, Box<dyn Error>>(());
        }
        client
            .batch_execute(&format!("CREATE SCHEMA {}", quote_identifier(&target_schema)))
            .await?;
        let target = QualifiedName::new(&target_schema, "items")?;
        let plan = plan_create_table_with_storage(
            &source_schema(),
            target.clone(),
            TargetStorage::PaxExperimental,
        )?;
        client.batch_execute(&plan.create_sql).await?;
        assert_eq!(
            load_relation_storage(&client, &target).await?.as_deref(),
            Some("pax")
        );
        let relation = format!("{}.items", quote_identifier(&target_schema));
        client
            .batch_execute(&format!(
                "INSERT INTO {relation} VALUES (1, 'initial', 1); \
                 UPDATE {relation} SET payload = 'updated', quantity = 2 WHERE id = 1; \
                 DELETE FROM {relation} WHERE id = 1;"
            ))
            .await?;
        Ok(())
    }
    .await;
    cleanup(&client, &target_schema, PipelineId::new()).await;
    connection_task.abort();
    result
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_snapshot_group_activation_and_retry() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_snapshot_it_{suffix}");
    let pipeline_id = PipelineId::new();
    let result = run_snapshot_activation_test(&mut client, &target_schema, pipeline_id).await;
    cleanup(&client, &target_schema, pipeline_id).await;
    connection_task.abort();
    result
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_snapshot_paging_with_resume() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_paging_it_{suffix}");
    let pipeline_id = PipelineId::new();
    let result = run_snapshot_paging_test(&mut client, &target_schema, pipeline_id).await;
    cleanup(&client, &target_schema, pipeline_id).await;
    connection_task.abort();
    result
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_interrupted_table_snapshot_resets_to_fresh_start()
-> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });

    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_reset_it_{suffix}");
    let pipeline_id = PipelineId::new();
    let result =
        run_interrupted_table_snapshot_reset_test(&mut client, &target_schema, pipeline_id).await;
    cleanup(&client, &target_schema, pipeline_id).await;
    connection_task.abort();
    result
}

async fn run_interrupted_table_snapshot_reset_test(
    client: &mut Client,
    target_schema: &str,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    migrate_target_database(client).await?;
    let original_fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 20,
    };
    activate_pipeline_fence(client, original_fence).await?;

    let mut reloaded_schema = source_schema();
    reloaded_schema.generation = 2;
    let plan = plan_snapshot_target(
        &reloaded_schema,
        QualifiedName::new(target_schema, "items")?,
        QualifiedName::new(target_schema, "items__reload_shadow")?,
    )?;
    let snapshot_group_id = Uuid::now_v7();
    let request = SnapshotActivationRequest {
        fence: original_fence,
        snapshot_group_id,
        tables: vec![plan.activation_table("sha256:reload-generation-2")],
        initial_checkpoints: vec![NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id,
                topology_generation: 1,
                node_id: 0,
            },
            system_identifier: 1_234,
            timeline: 1,
            slot_name: "pg2cb_reload_snapshot_slot".to_owned(),
            applied_lsn: PgLsn::new(0x2000),
        }],
    };
    begin_snapshot_group(client, &request).await?;
    let ownership = SnapshotOwnership {
        fence: original_fence,
        snapshot_group_id,
        schema_fingerprint: "sha256:reload-generation-2".to_owned(),
    };
    let mut loader = begin_snapshot_pages(client, plan.clone(), &ownership).await?;
    let partial_page =
        futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from("1\tpartial\t1\n"))]);
    loader
        .apply_page(client, vec!["1".to_owned()], false, partial_page)
        .await?;

    let event_id = Uuid::now_v7();
    client
        .execute(
            "INSERT INTO pg2cb_meta.table_schema_transitions (
                event_id, pipeline_id, topology_generation, source_lsn, source_xid,
                source_relation_id, action, plan, barrier_lsn, active_table_generation,
                pending_table_generation, snapshot_group_id, state, fencing_token
             ) VALUES ($1, $2, $3, $4::text::pg_lsn, $5, $6, 'reload', $7,
                       $4::text::pg_lsn, 1, 2, $8, 'snapshotting', $9)",
            &[
                &event_id,
                &pipeline_id.as_uuid(),
                &1_i64,
                &PgLsn::new(0x1800).to_string(),
                &7_i64,
                &i64::from(reloaded_schema.relation_id),
                &serde_json::json!({"action": "reload"}),
                &snapshot_group_id,
                &original_fence.fencing_token,
            ],
        )
        .await?;

    let current_fence = PipelineFence {
        fencing_token: 21,
        ..original_fence
    };
    activate_pipeline_fence(client, current_fence).await?;
    assert!(matches!(
        reset_interrupted_table_snapshot_group(client, original_fence, snapshot_group_id).await,
        Err(SnapshotTargetError::Checkpoint(
            cloudberry_etl_target_cloudberry::checkpoint::CheckpointError::StaleFence { .. }
        ))
    ));
    let recovered = load_snapshot_group_manifest(client, current_fence, snapshot_group_id).await?;
    assert_eq!(recovered.status, SnapshotGroupStatus::Loading);
    assert_eq!(recovered.request.fence, original_fence);
    assert_eq!(
        recovered.request.initial_checkpoints[0].applied_lsn,
        PgLsn::new(0x2000)
    );
    assert!(matches!(
        adopt_table_snapshot_replay_group(client, current_fence, snapshot_group_id).await,
        Err(SnapshotTargetError::SnapshotGroupTransitionMismatch(group))
            if group == snapshot_group_id
    ));
    assert!(
        cleanup_stale_snapshot_groups(client, current_fence)
            .await?
            .is_empty(),
        "generic stale cleanup must preserve transition-owned groups"
    );

    for invalid_state in ["catching_up", "cutover_pending"] {
        client
            .execute(
                "UPDATE pg2cb_meta.table_schema_transitions SET state = $3
                  WHERE pipeline_id = $1 AND snapshot_group_id = $2",
                &[&pipeline_id.as_uuid(), &snapshot_group_id, &invalid_state],
            )
            .await?;
        assert!(matches!(
            reset_interrupted_table_snapshot_group(client, current_fence, snapshot_group_id).await,
            Err(SnapshotTargetError::SnapshotGroupTransitionMismatch(group))
                if group == snapshot_group_id
        ));
    }
    client
        .execute(
            "UPDATE pg2cb_meta.table_schema_transitions
                SET state = 'snapshotting', pending_table_generation = 3
              WHERE pipeline_id = $1 AND snapshot_group_id = $2",
            &[&pipeline_id.as_uuid(), &snapshot_group_id],
        )
        .await?;
    assert!(matches!(
        reset_interrupted_table_snapshot_group(client, current_fence, snapshot_group_id).await,
        Err(SnapshotTargetError::SnapshotGroupTransitionMismatch(group))
            if group == snapshot_group_id
    ));
    client
        .execute(
            "UPDATE pg2cb_meta.table_schema_transitions SET pending_table_generation = 2
              WHERE pipeline_id = $1 AND snapshot_group_id = $2",
            &[&pipeline_id.as_uuid(), &snapshot_group_id],
        )
        .await?;

    let outcome =
        reset_interrupted_table_snapshot_group(client, current_fence, snapshot_group_id).await?;
    assert_eq!(outcome.dropped_shadows, vec![plan.shadow.target.clone()]);
    let shadow_exists: bool = client
        .query_one(
            "SELECT to_regclass(format('%I.%I', $1::text, $2::text)) IS NOT NULL",
            &[&plan.shadow.target.schema, &plan.shadow.target.name],
        )
        .await?
        .try_get(0)?;
    assert!(!shadow_exists);
    for table in [
        "snapshot_groups",
        "snapshot_group_tables",
        "snapshot_group_nodes",
        "snapshot_table_progress",
    ] {
        let remaining: i64 = client
            .query_one(
                &format!("SELECT count(*) FROM pg2cb_meta.{table} WHERE snapshot_group_id = $1"),
                &[&snapshot_group_id],
            )
            .await?
            .try_get(0)?;
        assert_eq!(remaining, 0, "{table} must be removed atomically");
    }
    let transition = client
        .query_one(
            "SELECT state, snapshot_group_id, fencing_token, failure_reason
               FROM pg2cb_meta.table_schema_transitions
              WHERE pipeline_id = $1 AND source_lsn = $2::text::pg_lsn",
            &[&pipeline_id.as_uuid(), &PgLsn::new(0x1800).to_string()],
        )
        .await?;
    assert_eq!(transition.try_get::<_, String>("state")?, "pending");
    assert_eq!(
        transition.try_get::<_, Option<Uuid>>("snapshot_group_id")?,
        None
    );
    assert_eq!(transition.try_get::<_, i64>("fencing_token")?, 21);
    assert_eq!(
        transition.try_get::<_, Option<String>>("failure_reason")?,
        None
    );

    let unrelated_group_id = Uuid::now_v7();
    let unrelated_request = SnapshotActivationRequest {
        snapshot_group_id: unrelated_group_id,
        fence: current_fence,
        tables: request.tables.clone(),
        initial_checkpoints: request.initial_checkpoints.clone(),
    };
    begin_snapshot_group(client, &unrelated_request).await?;
    assert!(matches!(
        reset_interrupted_table_snapshot_group(client, current_fence, unrelated_group_id).await,
        Err(SnapshotTargetError::SnapshotGroupNotOwnedByTableTransition(group))
            if group == unrelated_group_id
    ));
    cleanup_loading_snapshot_group(
        client,
        SnapshotGroupCleanupRequest {
            current_fence,
            group_fence: current_fence,
            snapshot_group_id: unrelated_group_id,
        },
    )
    .await?;

    let completed_group_id = Uuid::now_v7();
    let mut completed_request = request.clone();
    completed_request.fence = current_fence;
    completed_request.snapshot_group_id = completed_group_id;
    completed_request.initial_checkpoints[0].applied_lsn = PgLsn::new(0x3000);
    begin_snapshot_group(client, &completed_request).await?;
    let completed_ownership = SnapshotOwnership {
        fence: current_fence,
        snapshot_group_id: completed_group_id,
        schema_fingerprint: "sha256:reload-generation-2".to_owned(),
    };
    let mut completed_loader =
        begin_snapshot_pages(client, plan.clone(), &completed_ownership).await?;
    let complete_page =
        futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from("2\tcomplete\t2\n"))]);
    completed_loader
        .apply_page(client, vec!["2".to_owned()], true, complete_page)
        .await?;
    client
        .execute(
            "INSERT INTO pg2cb_meta.table_schema_transitions (
                event_id, pipeline_id, topology_generation, source_lsn, source_xid,
                source_relation_id, action, plan, barrier_lsn, active_table_generation,
                pending_table_generation, snapshot_group_id, state, fencing_token
             ) VALUES ($1, $2, 1, $3::text::pg_lsn, 8, $4, 'reload', $5,
                       $3::text::pg_lsn, 1, 2, $6, 'catching_up', $7)",
            &[
                &Uuid::now_v7(),
                &pipeline_id.as_uuid(),
                &PgLsn::new(0x1900).to_string(),
                &i64::from(reloaded_schema.relation_id),
                &serde_json::json!({"action": "reload"}),
                &completed_group_id,
                &current_fence.fencing_token,
            ],
        )
        .await?;
    let replay_fence = PipelineFence {
        fencing_token: 22,
        ..current_fence
    };
    activate_pipeline_fence(client, replay_fence).await?;
    let adopted =
        adopt_table_snapshot_replay_group(client, replay_fence, completed_group_id).await?;
    assert_eq!(adopted.manifest.status, SnapshotGroupStatus::Loading);
    assert_eq!(adopted.manifest.request.fence, replay_fence);
    assert_eq!(
        adopted.transition_state,
        cloudberry_etl_target_cloudberry::table_transition::TableTransitionState::CatchingUp
    );
    assert_eq!(
        adopted.manifest.request.initial_checkpoints[0].applied_lsn,
        PgLsn::new(0x3000)
    );

    client
        .execute(
            "UPDATE pg2cb_meta.table_schema_transitions SET state = 'cutover_pending'
              WHERE pipeline_id = $1 AND snapshot_group_id = $2",
            &[&pipeline_id.as_uuid(), &completed_group_id],
        )
        .await?;
    activate_table_snapshot_group(client, &adopted.manifest.request).await?;
    let post_cutover_fence = PipelineFence {
        fencing_token: 23,
        ..replay_fence
    };
    activate_pipeline_fence(client, post_cutover_fence).await?;
    let adopted_active =
        adopt_table_snapshot_replay_group(client, post_cutover_fence, completed_group_id).await?;
    assert_eq!(adopted_active.manifest.status, SnapshotGroupStatus::Active);
    assert_eq!(adopted_active.manifest.request.fence, post_cutover_fence);
    Ok(())
}

async fn run_snapshot_paging_test(
    client: &mut Client,
    target_schema: &str,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    migrate_target_database(client).await?;
    let fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 10,
    };
    activate_pipeline_fence(client, fence).await?;

    // Plan a single table with composite PK (tenant_id, item_id)
    let source_schema = TableSchema {
        relation_id: 200,
        generation: 1,
        name: QualifiedName::new("public", "paged_items").unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            column(1, "tenant_id", PgTypeKind::Text, false, Some(1)),
            column(2, "item_id", PgTypeKind::Int8, false, Some(2)),
            column(3, "payload", PgTypeKind::Text, true, None),
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    };
    let target_name = QualifiedName::new(target_schema, "paged_items").unwrap();
    let shadow_name = QualifiedName::new(target_schema, "shadow_paged_items").unwrap();
    let plan = plan_snapshot_target(&source_schema, target_name, shadow_name)?;
    let snapshot_group_id = Uuid::now_v7();
    let ownership = SnapshotOwnership {
        fence,
        snapshot_group_id,
        schema_fingerprint: "sha256:paging-test".to_owned(),
    };

    // Register the snapshot group
    let request = SnapshotActivationRequest {
        snapshot_group_id,
        fence,
        tables: vec![plan.activation_table("sha256:paging-test".to_owned())],
        initial_checkpoints: vec![NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: 0,
            },
            system_identifier: 1_234,
            timeline: 1,
            slot_name: "pg2cb_paging_slot".to_owned(),
            applied_lsn: PgLsn::new(100),
        }],
    };
    assert_eq!(
        begin_snapshot_group(client, &request).await?,
        SnapshotGroupRegistrationDisposition::Registered
    );

    // Open the paged loader
    let mut loader = begin_snapshot_pages(client, plan.clone(), &ownership).await?;
    assert!(!loader.is_completed());
    assert!(loader.cursor().is_empty(), "cursor starts empty");

    // Page 1: tenant-a, 1-2, 2 rows
    let page1 = "tenant-a\t1\tpayload-1\ntenant-a\t2\tpayload-2\n";
    let stream = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(page1))]);
    let outcome = loader
        .apply_page(
            client,
            vec!["tenant-a".to_owned(), "2".to_owned()],
            false,
            stream,
        )
        .await?;
    assert!(
        matches!(outcome, SnapshotPageApplyOutcome::Applied(_)),
        "page1 applied: {outcome:?}"
    );
    assert_eq!(loader.cursor(), &["tenant-a", "2"], "cursor advanced");

    // Page 2: tenant-b, 10, 1 row
    let page2 = "tenant-b\t10\tpayload-10\n";
    let stream2 = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(page2))]);
    let outcome2 = loader
        .apply_page(
            client,
            vec!["tenant-b".to_owned(), "10".to_owned()],
            false,
            stream2,
        )
        .await?;
    assert!(
        matches!(outcome2, SnapshotPageApplyOutcome::Applied(_)),
        "page2 applied: {outcome2:?}"
    );
    assert_eq!(loader.cursor(), &["tenant-b", "10"], "cursor advanced");

    // Final empty tail: completed=true, cursor unchanged
    let empty_stream = futures::stream::empty::<Result<Bytes, std::io::Error>>();
    let outcome_tail = loader
        .apply_page(
            client,
            vec!["tenant-b".to_owned(), "10".to_owned()],
            true,
            empty_stream,
        )
        .await?;
    match outcome_tail {
        SnapshotPageApplyOutcome::Applied(progress) => {
            assert!(progress.completed, "tail marks completed");
            assert_eq!(progress.rows_copied, 3, "total rows");
            assert_eq!(progress.pages_copied, 3, "three pages");
        }
        _ => panic!("expected Applied for tail: {outcome_tail:?}"),
    }
    assert!(loader.is_completed(), "loader marks completed");

    // Activate the group
    let activation = SnapshotActivationRequest {
        snapshot_group_id,
        fence,
        tables: vec![plan.activation_table("sha256:paging-test".to_owned())],
        initial_checkpoints: request.initial_checkpoints.clone(),
    };
    let outcome = activate_snapshot_group(client, &activation).await?;
    assert_eq!(
        outcome.disposition,
        SnapshotActivationDisposition::Activated
    );

    // A table-local reload uses its own completed group but must not advance the node checkpoint.
    let mut reload_schema = source_schema.clone();
    reload_schema.generation = 2;
    let reload_plan = plan_snapshot_target(
        &reload_schema,
        QualifiedName::new(target_schema, "paged_items")?,
        QualifiedName::new(target_schema, "shadow_paged_items_reload")?,
    )?;
    let reload_group_id = Uuid::now_v7();
    let reload_fingerprint = "sha256:paging-test-reload";
    let mut reload_checkpoint = request.initial_checkpoints[0].clone();
    reload_checkpoint.applied_lsn = PgLsn::new(200);
    let reload_request = SnapshotActivationRequest {
        snapshot_group_id: reload_group_id,
        fence,
        tables: vec![reload_plan.activation_table(reload_fingerprint)],
        initial_checkpoints: vec![reload_checkpoint],
    };
    begin_snapshot_group(client, &reload_request).await?;
    let reload_ownership = SnapshotOwnership {
        fence,
        snapshot_group_id: reload_group_id,
        schema_fingerprint: reload_fingerprint.to_owned(),
    };
    let mut reload = begin_snapshot_apply(client, reload_plan, &reload_ownership).await?;
    let reload_stream = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(
        "tenant-z\t99\treloaded\n",
    ))]);
    assert_eq!(reload.copy_from_stream(reload_stream).await?, 1);
    reload.commit().await?;
    let reload_outcome = activate_table_snapshot_group(client, &reload_request).await?;
    assert_eq!(
        reload_outcome.disposition,
        SnapshotActivationDisposition::Activated
    );
    assert_eq!(reload_outcome.quarantined.len(), 1);
    assert_eq!(
        activate_table_snapshot_group(client, &reload_request)
            .await?
            .disposition,
        SnapshotActivationDisposition::AlreadyActive
    );
    let stored_checkpoint = load_node_checkpoint(client, request.initial_checkpoints[0].key)
        .await?
        .expect("initial activation checkpoint exists");
    assert_eq!(stored_checkpoint.checkpoint.applied_lsn, PgLsn::new(100));
    let active = validate_active_tables(
        client,
        fence,
        &[ActiveTableRequirement {
            target: QualifiedName::new(target_schema, "paged_items")?,
            source_relation_id: source_schema.relation_id,
            schema_fingerprint: reload_fingerprint.to_owned(),
        }],
    )
    .await?;
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].table_generation, 2);

    Ok(())
}

async fn run_snapshot_activation_test(
    client: &mut Client,
    target_schema: &str,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    migrate_target_database(client).await?;
    let first_fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 9,
    };
    activate_pipeline_fence(client, first_fence).await?;

    let old_plans = snapshot_plans(target_schema, 100, 1, "old");
    let first_request =
        activation_request(first_fence, &old_plans, "old", [(0, 50_u64), (1, 60_u64)]);
    assert_eq!(
        begin_snapshot_group(client, &first_request).await?,
        SnapshotGroupRegistrationDisposition::Registered
    );
    assert_eq!(
        begin_snapshot_group(client, &first_request).await?,
        SnapshotGroupRegistrationDisposition::AlreadyRegistered
    );
    let mut changed_registration = first_request.clone();
    changed_registration.initial_checkpoints[0].applied_lsn = PgLsn::new(51);
    assert!(matches!(
        begin_snapshot_group(client, &changed_registration).await,
        Err(SnapshotTargetError::SnapshotGroupManifestMismatch { group, .. })
            if group == first_request.snapshot_group_id
    ));
    for (index, plan) in old_plans.iter().enumerate() {
        load_snapshot_shadow(
            client,
            plan,
            SnapshotOwnership {
                fence: first_fence,
                snapshot_group_id: first_request.snapshot_group_id,
                schema_fingerprint: format!("sha256:old-{index}"),
            },
            format!("1\tnew-{index}\n"),
        )
        .await?;
    }

    assert_manifest_mismatch_is_read_only(client, target_schema, &first_request).await?;

    let mut other_group = first_request.clone();
    other_group.snapshot_group_id = Uuid::now_v7();
    assert_eq!(
        begin_snapshot_group(client, &other_group).await?,
        SnapshotGroupRegistrationDisposition::Registered
    );
    let other_ownership = SnapshotOwnership {
        fence: first_fence,
        snapshot_group_id: other_group.snapshot_group_id,
        schema_fingerprint: "sha256:old-0".to_owned(),
    };
    assert!(matches!(
        begin_snapshot_apply(client, old_plans[0].clone(), &other_ownership).await,
        Err(SnapshotTargetError::ShadowSnapshotGroupMismatch {
            expected,
            actual: Some(actual),
            ..
        }) if expected == other_group.snapshot_group_id
            && actual == first_request.snapshot_group_id
    ));

    // A committed shadow is rebuilt because schema identity does not identify an exported snapshot.
    let rebuild_ownership = SnapshotOwnership {
        fence: first_fence,
        snapshot_group_id: first_request.snapshot_group_id,
        schema_fingerprint: "sha256:old-0".to_owned(),
    };
    let mut rebuilt =
        begin_snapshot_apply(client, old_plans[0].clone(), &rebuild_ownership).await?;
    assert_eq!(rebuilt.mode(), SnapshotApplyMode::Copy);
    let stream = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from("1\tnew-0\n"))]);
    assert_eq!(rebuilt.copy_from_stream(stream).await?, 1);
    assert_eq!(
        rebuilt.commit().await?,
        SnapshotApplyOutcome::Copied { rows: 1 }
    );

    let first = activate_snapshot_group(client, &first_request).await?;
    assert_eq!(first.disposition, SnapshotActivationDisposition::Activated);
    assert!(first.quarantined.is_empty());

    let second_fence = PipelineFence {
        pipeline_id,
        topology_generation: 2,
        fencing_token: 10,
    };
    activate_pipeline_fence(client, second_fence).await?;
    // A source table can be dropped and recreated under the same mapped name. The replacement
    // snapshot must retain target ownership while moving to the new source relation identity.
    let new_plans = snapshot_plans(target_schema, 10_100, 2, "new");
    let active_new_plans = &new_plans[..1];
    let second_request = activation_request(
        second_fence,
        active_new_plans,
        "new",
        [(0, 100_u64), (1, 120_u64)],
    );
    assert_eq!(
        begin_snapshot_group(client, &second_request).await?,
        SnapshotGroupRegistrationDisposition::Registered
    );
    for (index, plan) in active_new_plans.iter().enumerate() {
        load_snapshot_shadow(
            client,
            plan,
            SnapshotOwnership {
                fence: second_fence,
                snapshot_group_id: second_request.snapshot_group_id,
                schema_fingerprint: format!("sha256:new-{index}"),
            },
            format!("2\tdone-{index}\n"),
        )
        .await?;
    }

    client
        .batch_execute(&format!(
            "CREATE TABLE {}.unmanaged_keep (id bigint PRIMARY KEY) USING heap DISTRIBUTED BY (id); INSERT INTO {}.unmanaged_keep VALUES (7)",
            quote_identifier(target_schema),
            quote_identifier(target_schema),
        ))
        .await?;

    let second = activate_snapshot_group(client, &second_request).await?;
    assert_eq!(second.disposition, SnapshotActivationDisposition::Activated);
    assert_eq!(second.quarantined.len(), 2);

    let value: String = client
        .query_one(
            &format!(
                "SELECT status::text FROM {}.items_0 WHERE id = 2",
                quote_identifier(target_schema)
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(value, "done-0");

    let original_stale_exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = 'items_1')",
            &[&target_schema],
        )
        .await?
        .try_get(0)?;
    assert!(!original_stale_exists);
    let unmanaged_value: i64 = client
        .query_one(
            &format!(
                "SELECT id FROM {}.unmanaged_keep",
                quote_identifier(target_schema)
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(unmanaged_value, 7);

    let reconciliation = client
        .query(
            "SELECT original_table, quarantine_table, reason FROM pg2cb_meta.snapshot_reconciliation_log WHERE snapshot_group_id = $1 ORDER BY original_table",
            &[&second_request.snapshot_group_id],
        )
        .await?;
    assert_eq!(reconciliation.len(), 2);
    assert_eq!(
        reconciliation[0].try_get::<_, String>("original_table")?,
        "items_0"
    );
    assert_eq!(
        reconciliation[0].try_get::<_, String>("reason")?,
        "replaced"
    );
    assert_eq!(
        reconciliation[1].try_get::<_, String>("original_table")?,
        "items_1"
    );
    assert_eq!(reconciliation[1].try_get::<_, String>("reason")?, "stale");
    for row in reconciliation {
        let quarantine: String = row.try_get("quarantine_table")?;
        let old_id: i64 = client
            .query_one(
                &format!(
                    "SELECT id FROM {}.{}",
                    quote_identifier(target_schema),
                    quote_identifier(&quarantine)
                ),
                &[],
            )
            .await?
            .try_get(0)?;
        assert_eq!(old_id, 1);
    }

    let managed_type_count: i64 = client
        .query_one(
            "SELECT count(*) FROM pg2cb_meta.managed_types WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await?
        .try_get(0)?;
    assert_eq!(
        managed_type_count, 1,
        "the enum must be shared by both tables"
    );

    let retry = activate_snapshot_group(client, &second_request).await?;
    assert_eq!(
        retry.disposition,
        SnapshotActivationDisposition::AlreadyActive
    );
    assert!(retry.quarantined.is_empty());
    assert_eq!(
        begin_snapshot_group(client, &second_request).await?,
        SnapshotGroupRegistrationDisposition::AlreadyActive
    );

    let adopted_fence = PipelineFence {
        fencing_token: second_fence.fencing_token + 1,
        ..second_fence
    };
    activate_pipeline_fence(client, adopted_fence).await?;
    assert_eq!(
        validate_active_snapshot_group(client, adopted_fence, &second_request.tables).await?,
        second_request.snapshot_group_id
    );
    let mut incomplete_active_tables = second_request.tables.clone();
    incomplete_active_tables[0]
        .schema_fingerprint
        .push_str("-changed");
    assert!(matches!(
        validate_active_snapshot_group(client, adopted_fence, &incomplete_active_tables).await,
        Err(SnapshotTargetError::SnapshotGroupManifestMismatch { group, .. })
            if group == second_request.snapshot_group_id
    ));
    for checkpoint in &second_request.initial_checkpoints {
        let stored = load_node_checkpoint(client, checkpoint.key)
            .await?
            .expect("activation checkpoint must exist");
        assert_eq!(stored.checkpoint.applied_lsn, checkpoint.applied_lsn);
    }
    Ok(())
}

async fn assert_manifest_mismatch_is_read_only(
    client: &mut Client,
    target_schema: &str,
    request: &SnapshotActivationRequest,
) -> Result<(), Box<dyn Error>> {
    let mut variants = Vec::new();

    let mut omitted_table = request.clone();
    omitted_table.tables.pop();
    variants.push(omitted_table);

    let mut extra_table = request.clone();
    let mut table = extra_table.tables[0].clone();
    table.target = QualifiedName::new(target_schema, "manifest_extra")?;
    table.shadow = QualifiedName::new(target_schema, "manifest_extra_shadow")?;
    table.source_relation_id += 10_000;
    extra_table.tables.push(table);
    variants.push(extra_table);

    let mut omitted_node = request.clone();
    omitted_node.initial_checkpoints.pop();
    variants.push(omitted_node);

    let mut extra_node = request.clone();
    let mut node = extra_node.initial_checkpoints[0].clone();
    node.key.node_id = 99;
    node.system_identifier += 99;
    node.slot_name = "pg2cb_snapshot_slot_99".to_owned();
    extra_node.initial_checkpoints.push(node);
    variants.push(extra_node);

    for variant in variants {
        assert!(matches!(
            activate_snapshot_group(client, &variant).await,
            Err(SnapshotTargetError::SnapshotGroupManifestMismatch { group, .. })
                if group == request.snapshot_group_id
        ));
    }

    let state: String = client
        .query_one(
            "SELECT state FROM pg2cb_meta.snapshot_groups WHERE snapshot_group_id = $1",
            &[&request.snapshot_group_id],
        )
        .await?
        .try_get(0)?;
    assert_eq!(state, "loading");
    let target_count: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname IN ('items_0', 'items_1')",
            &[&target_schema],
        )
        .await?
        .try_get(0)?;
    assert_eq!(target_count, 0);
    let checkpoint_count: i64 = client
        .query_one(
            "SELECT count(*) FROM pg2cb_meta.node_checkpoints WHERE pipeline_id = $1 AND topology_generation = $2",
            &[&request.fence.pipeline_id.as_uuid(), &4_i64],
        )
        .await?
        .try_get(0)?;
    assert_eq!(checkpoint_count, 0);
    Ok(())
}

async fn load_snapshot_shadow(
    client: &mut Client,
    plan: &SnapshotTargetPlan,
    ownership: SnapshotOwnership,
    row: String,
) -> Result<(), Box<dyn Error>> {
    let mut apply = begin_snapshot_apply(client, plan.clone(), &ownership).await?;
    assert_eq!(apply.mode(), SnapshotApplyMode::Copy);
    let stream = futures::stream::iter([Ok::<_, std::io::Error>(Bytes::from(row))]);
    assert_eq!(apply.copy_from_stream(stream).await?, 1);
    assert_eq!(
        apply.commit().await?,
        SnapshotApplyOutcome::Copied { rows: 1 }
    );
    Ok(())
}

fn snapshot_plans(
    target_schema: &str,
    relation_id_base: u32,
    generation: u64,
    shadow_suffix: &str,
) -> Vec<SnapshotTargetPlan> {
    (0..2)
        .map(|index| {
            plan_snapshot_target(
                &snapshot_source_schema(relation_id_base + index, generation, index),
                QualifiedName::new(target_schema, format!("items_{index}")).unwrap(),
                QualifiedName::new(
                    target_schema,
                    format!("items_{index}__{shadow_suffix}_shadow"),
                )
                .unwrap(),
            )
            .unwrap()
        })
        .collect()
}

fn activation_request(
    fence: PipelineFence,
    plans: &[SnapshotTargetPlan],
    fingerprint: &str,
    nodes: [(i32, u64); 2],
) -> SnapshotActivationRequest {
    SnapshotActivationRequest {
        fence,
        snapshot_group_id: Uuid::now_v7(),
        tables: plans
            .iter()
            .enumerate()
            .map(|(index, plan)| plan.activation_table(format!("sha256:{fingerprint}-{index}")))
            .collect(),
        initial_checkpoints: nodes
            .into_iter()
            .map(|(node_id, lsn)| NodeCheckpoint {
                key: CheckpointKey {
                    pipeline_id: fence.pipeline_id,
                    topology_generation: fence.topology_generation,
                    node_id,
                },
                system_identifier: 1_000 + u64::try_from(node_id).unwrap(),
                timeline: 1,
                slot_name: format!("pg2cb_snapshot_slot_{node_id}"),
                applied_lsn: PgLsn::new(lsn),
            })
            .collect(),
    }
}

fn snapshot_source_schema(relation_id: u32, generation: u64, index: u32) -> TableSchema {
    TableSchema {
        relation_id,
        generation,
        name: QualifiedName::new("public", format!("snapshot_items_{index}")).unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            column(1, "id", PgTypeKind::Int8, false, Some(1)),
            ColumnSchema {
                attnum: 2,
                name: "status".to_owned(),
                data_type: PgType {
                    oid: 90_001,
                    name: QualifiedName::new("source_types", "snapshot_status").unwrap(),
                    kind: PgTypeKind::Enum {
                        labels: vec![
                            "new-0".to_owned(),
                            "new-1".to_owned(),
                            "done-0".to_owned(),
                            "done-1".to_owned(),
                        ],
                    },
                },
                nullable: false,
                primary_key_ordinal: None,
                generated: GeneratedColumn::None,
                identity: IdentityColumn::None,
                collation: None,
            },
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

async fn run_test(
    client: &mut Client,
    target_schema: &str,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    let version: String = client
        .query_one("SELECT version()", &[])
        .await?
        .try_get(0)?;
    assert!(
        version.to_ascii_lowercase().contains("cloudberry"),
        "server is not Apache Cloudberry: {version}"
    );
    assert!(
        version.contains("2.1.0"),
        "expected Cloudberry 2.1.0: {version}"
    );

    migrate_target_database(client).await?;
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {}",
            quote_identifier(target_schema)
        ))
        .await?;

    let source = source_schema();
    let target = QualifiedName::new(target_schema, "items")?;
    let plan = plan_apply_with_storage(
        &source,
        target,
        &format!("stage_{}", &target_schema[9..]),
        TargetStorage::AoColumn,
    )?;
    for prerequisite in &plan.table.prerequisites {
        client.batch_execute(&prerequisite.create_sql).await?;
    }
    client.batch_execute(&plan.table.create_sql).await?;
    assert_eq!(
        load_relation_storage(client, &plan.table.target).await?,
        Some("ao_column".to_owned())
    );

    let fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 1,
    };
    activate_pipeline_fence(client, fence).await?;
    register_managed_apply_table(client, fence, &source, &plan.table.target).await?;

    let initial = request(
        fence,
        &plan,
        100,
        StagingRow {
            operation: StageOperation::Upsert,
            cells: vec![text("1"), text("before"), text("10")],
            old_key: None,
        },
    );
    let outcome = execute_apply(client, &initial).await?;
    assert_eq!(outcome.checkpoint, AdvanceOutcome::Inserted);
    assert_eq!(outcome.inserted_rows, 1);

    let replay = execute_apply(client, &initial).await?;
    assert_eq!(replay.checkpoint, AdvanceOutcome::Unchanged);
    assert_eq!(
        row(client, target_schema, 1).await?,
        Some(("before".into(), 10))
    );

    let atomic_insert = request(
        fence,
        &plan,
        105,
        StagingRow {
            operation: StageOperation::Insert,
            cells: vec![text("9999"), text("atomic"), text("20")],
            old_key: None,
        },
    );
    assert!(matches!(
        execute_atomic_apply_observed(client, &atomic_insert, &FailAfterAtomicCommit).await,
        Err(ApplyError::CommitObserver(message))
            if message.contains("lost atomic batch commit response")
    ));
    assert!(matches!(
        execute_atomic_apply(client, &atomic_insert).await?,
        AtomicApplyOutcome::AlreadyCheckpointed {
            applied_lsn
        } if applied_lsn == PgLsn::new(105)
    ));
    assert_eq!(
        row(client, target_schema, 9999).await?,
        Some(("atomic".into(), 20))
    );

    let chunk = request(
        fence,
        &plan,
        110,
        StagingRow {
            operation: StageOperation::Upsert,
            cells: vec![text("1"), text("chunk"), text("11")],
            old_key: None,
        },
    );
    let manifest = TransactionChunkManifest {
        key: TransactionChunkKey {
            pipeline_id,
            topology_generation: 1,
            node_id: 0,
            end_lsn: chunk.checkpoint.applied_lsn,
        },
        system_identifier: chunk.checkpoint.system_identifier,
        timeline: chunk.checkpoint.timeline,
        slot_name: chunk.checkpoint.slot_name.clone(),
        xid: 110,
        manifest_version: 1,
        record_count: 1,
        manifest_digest: [0x11; 32],
    };
    let data_chunk = LedgeredDataChunkRequest {
        fence,
        manifest: manifest.clone(),
        chunk: DataChunkIdentity {
            start_seq: 0,
            end_seq: 1,
            digest: [0x22; 32],
        },
        tables: chunk.tables.clone(),
    };
    client
        .execute(
            "UPDATE pg2cb_meta.managed_tables SET schema_fingerprint = 'sha256:drifted' WHERE target_schema = $1 AND target_table = $2",
            &[&target_schema, &"items"],
        )
        .await?;
    assert!(matches!(
        execute_ledgered_data_chunk(client, &data_chunk).await,
        Err(ApplyError::ManagedTable(
            ManagedTableError::SchemaFingerprintMismatch(_)
        ))
    ));
    assert_eq!(
        row(client, target_schema, 1).await?,
        Some(("before".into(), 10))
    );
    assert_eq!(ledger_counts(client, manifest.key).await?, (0, 0));
    assert_eq!(
        load_node_checkpoint(client, chunk.checkpoint.key)
            .await?
            .expect("failed identity guard must preserve the previous checkpoint")
            .checkpoint
            .applied_lsn,
        PgLsn::new(105)
    );
    client
        .execute(
            "UPDATE pg2cb_meta.managed_tables SET schema_fingerprint = $3 WHERE target_schema = $1 AND target_table = $2",
            &[&target_schema, &"items", &APPLY_SCHEMA_FINGERPRINT],
        )
        .await?;
    assert!(matches!(
        execute_ledgered_data_chunk(client, &data_chunk).await?,
        LedgeredDataChunkOutcome::Completed {
            next_seq: 1,
            disposition: DataChunkDisposition::Applied { .. },
            checkpoint: AdvanceOutcome::Advanced { previous_lsn }
        } if previous_lsn == PgLsn::new(105)
    ));
    assert_eq!(
        row(client, target_schema, 1).await?,
        Some(("chunk".into(), 11))
    );
    assert!(matches!(
        execute_ledgered_data_chunk(client, &data_chunk).await?,
        LedgeredDataChunkOutcome::AlreadyCheckpointed { applied_lsn }
            if applied_lsn == manifest.key.end_lsn
    ));
    let checkpoint = load_node_checkpoint(client, chunk.checkpoint.key)
        .await?
        .expect("final chunk must publish its checkpoint");
    assert_eq!(checkpoint.checkpoint.applied_lsn, PgLsn::new(110));
    assert_eq!(ledger_counts(client, manifest.key).await?, (0, 0));

    // A lost COMMIT response can replay this request after the receipts were retired. The target
    // checkpoint must short-circuit before staging or user-table DML executes.
    client
        .execute(
            &format!(
                "UPDATE {}.items SET payload = 'checkpoint_sentinel', quantity = 99 WHERE id = 1",
                quote_identifier(target_schema)
            ),
            &[],
        )
        .await?;
    assert!(matches!(
        execute_ledgered_data_chunk(client, &data_chunk).await?,
        LedgeredDataChunkOutcome::AlreadyCheckpointed { applied_lsn }
            if applied_lsn == manifest.key.end_lsn
    ));
    assert_eq!(
        row(client, target_schema, 1).await?,
        Some(("checkpoint_sentinel".into(), 99))
    );
    assert_eq!(ledger_counts(client, manifest.key).await?, (0, 0));
    client
        .execute(
            &format!(
                "UPDATE {}.items SET payload = 'chunk', quantity = 11 WHERE id = 1",
                quote_identifier(target_schema)
            ),
            &[],
        )
        .await?;

    let moved = request(
        fence,
        &plan,
        120,
        StagingRow {
            operation: StageOperation::Move,
            cells: vec![text("2"), Cell::UnchangedToast, text("20")],
            old_key: Some(vec![text("1")]),
        },
    );
    let outcome = execute_apply(client, &moved).await?;
    assert_eq!(
        outcome.checkpoint,
        AdvanceOutcome::Advanced {
            previous_lsn: PgLsn::new(110)
        }
    );
    assert_eq!(row(client, target_schema, 1).await?, None);
    assert_eq!(
        row(client, target_schema, 2).await?,
        Some(("chunk".into(), 20))
    );

    let deleted = request(
        fence,
        &plan,
        130,
        StagingRow {
            operation: StageOperation::Delete,
            cells: vec![text("2"), Cell::UnchangedToast, Cell::UnchangedToast],
            old_key: None,
        },
    );
    execute_apply(client, &deleted).await?;
    assert_eq!(row(client, target_schema, 2).await?, None);

    let graph_seed = request_rows(
        fence,
        &plan,
        140,
        vec![
            StagingRow {
                operation: StageOperation::Upsert,
                cells: vec![text("1"), text("one"), text("10")],
                old_key: None,
            },
            StagingRow {
                operation: StageOperation::Upsert,
                cells: vec![text("2"), text("two"), text("20")],
                old_key: None,
            },
        ],
    );
    execute_apply(client, &graph_seed).await?;

    let duplicate_destination = request_rows(
        fence,
        &plan,
        145,
        vec![
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("3"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("1")]),
            },
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("3"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("2")]),
            },
        ],
    );
    assert!(matches!(
        execute_apply(client, &duplicate_destination).await,
        Err(ApplyError::InvalidBatch(_))
    ));
    let duplicate_origin = request_rows(
        fence,
        &plan,
        145,
        vec![
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("3"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("1")]),
            },
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("4"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("1")]),
            },
        ],
    );
    assert!(matches!(
        execute_apply(client, &duplicate_origin).await,
        Err(ApplyError::InvalidBatch(_))
    ));

    let chain = request_rows(
        fence,
        &plan,
        150,
        vec![
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("3"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("2")]),
            },
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("2"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("1")]),
            },
        ],
    );
    let outcome = execute_apply(client, &chain).await?;
    assert_eq!(outcome.moved_rows, 2);
    assert_eq!(row(client, target_schema, 1).await?, None);
    assert_eq!(
        row(client, target_schema, 2).await?,
        Some(("one".into(), 10))
    );
    assert_eq!(
        row(client, target_schema, 3).await?,
        Some(("two".into(), 20))
    );

    let swap = request_rows(
        fence,
        &plan,
        160,
        vec![
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("3"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("2")]),
            },
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("2"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("3")]),
            },
        ],
    );
    execute_apply(client, &swap).await?;
    assert_eq!(
        row(client, target_schema, 2).await?,
        Some(("two".into(), 20))
    );
    assert_eq!(
        row(client, target_schema, 3).await?,
        Some(("one".into(), 10))
    );

    let delete_then_move = request_rows(
        fence,
        &plan,
        170,
        vec![
            StagingRow {
                operation: StageOperation::Delete,
                cells: vec![text("2"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: None,
            },
            StagingRow {
                operation: StageOperation::Move,
                cells: vec![text("2"), Cell::UnchangedToast, Cell::UnchangedToast],
                old_key: Some(vec![text("3")]),
            },
        ],
    );
    execute_apply(client, &delete_then_move).await?;
    assert_eq!(
        row(client, target_schema, 2).await?,
        Some(("one".into(), 10))
    );
    assert_eq!(row(client, target_schema, 3).await?, None);

    let checkpoint = load_node_checkpoint(
        client,
        CheckpointKey {
            pipeline_id,
            topology_generation: 1,
            node_id: 0,
        },
    )
    .await?
    .expect("checkpoint must exist");
    assert_eq!(checkpoint.checkpoint.applied_lsn, PgLsn::new(170));
    assert_eq!(checkpoint.fencing_token, fence.fencing_token);
    Ok(())
}

fn request(
    fence: PipelineFence,
    plan: &cloudberry_etl_target_cloudberry::apply::ApplyPlan,
    lsn: u64,
    row: StagingRow,
) -> ApplyRequest {
    request_rows(fence, plan, lsn, vec![row])
}

fn request_rows(
    fence: PipelineFence,
    plan: &cloudberry_etl_target_cloudberry::apply::ApplyPlan,
    lsn: u64,
    rows: Vec<StagingRow>,
) -> ApplyRequest {
    ApplyRequest {
        fence,
        checkpoint: NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: 0,
            },
            system_identifier: u64::MAX,
            timeline: 1,
            slot_name: "pg2cb_it_slot".into(),
            applied_lsn: PgLsn::new(lsn),
        },
        tables: vec![TableApplyBatch {
            identity: Arc::new(TableApplyIdentity {
                target: plan.table.target.clone(),
                source_relation_id: 42,
                table_generation: fence.topology_generation,
                schema_fingerprint: APPLY_SCHEMA_FINGERPRINT.to_owned(),
            }),
            plan: Arc::new(plan.clone()),
            rows,
        }],
    }
}

async fn register_managed_apply_table(
    client: &Client,
    fence: PipelineFence,
    source: &TableSchema,
    target: &QualifiedName,
) -> Result<(), tokio_postgres::Error> {
    let relation_oid: i64 = client
        .query_one(
            "SELECT c.oid::bigint FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p')",
            &[&target.schema, &target.name],
        )
        .await?
        .get(0);
    let pipeline_id = fence.pipeline_id.as_uuid();
    let source_relation_id = i64::from(source.relation_id);
    let table_generation = i64::try_from(fence.topology_generation)
        .expect("integration topology generation fits target bigint");
    client
        .execute(
            "INSERT INTO pg2cb_meta.managed_tables (target_schema, target_table, pipeline_id, relation_oid, source_relation_id, table_generation, schema_fingerprint, state, fencing_token) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8)",
            &[
                &target.schema,
                &target.name,
                &pipeline_id,
                &relation_oid,
                &source_relation_id,
                &table_generation,
                &APPLY_SCHEMA_FINGERPRINT,
                &fence.fencing_token,
            ],
        )
        .await?;
    Ok(())
}

async fn row(
    client: &Client,
    target_schema: &str,
    id: i64,
) -> Result<Option<(String, i32)>, tokio_postgres::Error> {
    client
        .query_opt(
            &format!(
                "SELECT payload, quantity FROM {}.items WHERE id = $1",
                quote_identifier(target_schema)
            ),
            &[&id],
        )
        .await?
        .map(|row| Ok((row.try_get(0)?, row.try_get(1)?)))
        .transpose()
}

async fn ledger_counts(
    client: &Client,
    key: TransactionChunkKey,
) -> Result<(i64, i64), tokio_postgres::Error> {
    let pipeline_id = key.pipeline_id.as_uuid();
    let generation = i64::try_from(key.topology_generation).expect("test generation fits bigint");
    let end_lsn = key.end_lsn.to_string();
    let parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)] =
        &[&pipeline_id, &generation, &key.node_id, &end_lsn];
    let progress = client
        .query_one(
            "SELECT count(*) FROM pg2cb_meta.transaction_chunk_progress WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3 AND end_lsn = $4::text::pg_lsn",
            parameters,
        )
        .await?
        .get(0);
    let chunks = client
        .query_one(
            "SELECT count(*) FROM pg2cb_meta.transaction_committed_chunks WHERE pipeline_id = $1 AND topology_generation = $2 AND node_id = $3 AND end_lsn = $4::text::pg_lsn",
            parameters,
        )
        .await?
        .get(0);
    Ok((progress, chunks))
}

fn source_schema() -> TableSchema {
    TableSchema {
        relation_id: 42,
        generation: 1,
        name: QualifiedName::new("public", "items").expect("static identifiers are valid"),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            column(1, "id", PgTypeKind::Int8, false, Some(1)),
            column(2, "payload", PgTypeKind::Text, true, None),
            column(3, "quantity", PgTypeKind::Int4, false, None),
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

fn column(
    attnum: i16,
    name: &str,
    kind: PgTypeKind,
    nullable: bool,
    primary_key_ordinal: Option<u16>,
) -> ColumnSchema {
    let (type_oid, type_name) = match kind {
        PgTypeKind::Int8 => (20, "int8"),
        PgTypeKind::Int4 => (23, "int4"),
        PgTypeKind::Text => (25, "text"),
        _ => unreachable!("integration helper only uses three built-in types"),
    };
    let stable_text_key = primary_key_ordinal.is_some() && kind == PgTypeKind::Text;
    ColumnSchema {
        attnum,
        name: name.into(),
        data_type: PgType {
            oid: type_oid,
            name: QualifiedName::new("pg_catalog", type_name)
                .expect("static identifiers are valid"),
            kind,
        },
        nullable,
        primary_key_ordinal,
        generated: GeneratedColumn::None,
        identity: IdentityColumn::None,
        collation: stable_text_key
            .then(|| QualifiedName::new("pg_catalog", "C").expect("static collation is valid")),
    }
}

fn text(value: &'static str) -> Cell {
    Cell::Text(Bytes::from_static(value.as_bytes()))
}

async fn cleanup(client: &Client, target_schema: &str, pipeline_id: PipelineId) {
    let id = pipeline_id.as_uuid();
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.table_schema_transitions WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.schema_events WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let group_ids = client
        .query(
            "SELECT snapshot_group_id FROM pg2cb_meta.snapshot_groups WHERE pipeline_id = $1",
            &[&id],
        )
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|row| row.get::<_, Uuid>(0))
        .collect::<Vec<_>>();
    for group_id in group_ids {
        let _ = client
            .execute(
                "DELETE FROM pg2cb_meta.snapshot_table_progress WHERE snapshot_group_id = $1",
                &[&group_id],
            )
            .await;
        let _ = client
            .execute(
                "DELETE FROM pg2cb_meta.snapshot_reconciliation_log WHERE snapshot_group_id = $1",
                &[&group_id],
            )
            .await;
        let _ = client
            .execute(
                "DELETE FROM pg2cb_meta.snapshot_group_tables WHERE snapshot_group_id = $1",
                &[&group_id],
            )
            .await;
        let _ = client
            .execute(
                "DELETE FROM pg2cb_meta.snapshot_group_nodes WHERE snapshot_group_id = $1",
                &[&group_id],
            )
            .await;
        let _ = client
            .execute(
                "DELETE FROM pg2cb_meta.snapshot_groups WHERE snapshot_group_id = $1",
                &[&group_id],
            )
            .await;
    }
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.managed_tables WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.managed_types WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.node_checkpoints WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(target_schema)
        ))
        .await;
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
