//! Opt-in Apache Cloudberry 2.1 integration coverage.

use std::{error::Error, sync::Arc};

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
        ApplyError, ApplyRequest, LedgeredDataChunkOutcome, LedgeredDataChunkRequest,
        StageOperation, StagingRow, TableApplyBatch, execute_apply, execute_ledgered_completion,
        execute_ledgered_data_chunk, plan_apply,
    },
    checkpoint::{
        AdvanceOutcome, CheckpointKey, NodeCheckpoint, PipelineFence, activate_pipeline_fence,
        load_node_checkpoint,
    },
    chunk::{
        DataChunkIdentity, TransactionChunkKey, TransactionChunkManifest,
        prepare_transaction_completion,
    },
    migration::migrate_target_database,
    snapshot::{
        SnapshotActivationDisposition, SnapshotActivationRequest, SnapshotApplyMode,
        SnapshotApplyOutcome, SnapshotGroupRegistrationDisposition, SnapshotOwnership,
        SnapshotTargetError, SnapshotTargetPlan, activate_snapshot_group, begin_snapshot_apply,
        begin_snapshot_group, plan_snapshot_target, validate_active_snapshot_group,
    },
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

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

    let old_plans = snapshot_plans(target_schema, 1, "old");
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
        Err(SnapshotTargetError::SnapshotGroupManifestMismatch(group))
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
    let new_plans = snapshot_plans(target_schema, 2, "new");
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
        Err(SnapshotTargetError::SnapshotGroupManifestMismatch(group))
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
            Err(SnapshotTargetError::SnapshotGroupManifestMismatch(group))
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
    generation: u64,
    shadow_suffix: &str,
) -> Vec<SnapshotTargetPlan> {
    (0..2)
        .map(|index| {
            plan_snapshot_target(
                &snapshot_source_schema(100 + index, generation, index),
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
    let plan = plan_apply(&source, target, &format!("stage_{}", &target_schema[9..]))?;
    for prerequisite in &plan.table.prerequisites {
        client.batch_execute(&prerequisite.create_sql).await?;
    }
    client.batch_execute(&plan.table.create_sql).await?;

    let fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 1,
    };
    activate_pipeline_fence(client, fence).await?;

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
    assert!(matches!(
        execute_ledgered_data_chunk(client, &data_chunk).await?,
        LedgeredDataChunkOutcome::Applied { next_seq: 1, .. }
    ));
    assert_eq!(
        row(client, target_schema, 1).await?,
        Some(("chunk".into(), 11))
    );
    assert!(matches!(
        execute_ledgered_data_chunk(client, &data_chunk).await?,
        LedgeredDataChunkOutcome::AlreadyCommitted { next_seq: 1 }
    ));
    let checkpoint = load_node_checkpoint(client, chunk.checkpoint.key)
        .await?
        .expect("ledgered data chunk must not remove the existing checkpoint");
    assert_eq!(checkpoint.checkpoint.applied_lsn, PgLsn::new(100));

    let transaction = client.transaction().await?;
    let completion = prepare_transaction_completion(&transaction, fence, &manifest).await?;
    let outcome = execute_ledgered_completion(transaction, completion, &chunk.checkpoint).await?;
    assert_eq!(
        outcome,
        AdvanceOutcome::Advanced {
            previous_lsn: PgLsn::new(100)
        }
    );
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
            plan: Arc::new(plan.clone()),
            rows,
        }],
    }
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
    let type_name = match kind {
        PgTypeKind::Int8 => "int8",
        PgTypeKind::Int4 => "int4",
        PgTypeKind::Text => "text",
        _ => unreachable!("integration helper only uses three built-in types"),
    };
    ColumnSchema {
        attnum,
        name: name.into(),
        data_type: PgType {
            oid: 0,
            name: QualifiedName::new("pg_catalog", type_name)
                .expect("static identifiers are valid"),
            kind,
        },
        nullable,
        primary_key_ordinal,
        generated: GeneratedColumn::None,
        identity: IdentityColumn::None,
        collation: None,
    }
}

fn text(value: &'static str) -> Cell {
    Cell::Text(Bytes::from_static(value.as_bytes()))
}

async fn cleanup(client: &Client, target_schema: &str, pipeline_id: PipelineId) {
    let id = pipeline_id.as_uuid();
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
