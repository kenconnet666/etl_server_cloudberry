//! Opt-in durable reconciliation state coverage against Apache Cloudberry 2.1.

use std::{
    error::Error,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use cloudberry_etl_core::{
    id::PipelineId,
    lsn::PgLsn,
    schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
        ReplicaIdentity, TableKind, TableSchema,
    },
};
use cloudberry_etl_target_cloudberry::{
    checkpoint::{CheckpointKey, NodeCheckpoint, PipelineFence, activate_pipeline_fence},
    migration::migrate_target_database,
    reconciliation::{
        BeginReconciliationOutcome, ReconciliationRunIdentity, ReconciliationScanCompletion,
        ReconciliationStartupRecovery, ReconciliationState, ReconciliationStateError,
        ReconciliationStats, ReconciliationTransitionOutcome, activate_reconciliation_reload,
        begin_reconciliation, complete_reconciliation_scan, fail_reconciliation,
        load_reconciliation_startup_recovery, load_reconciliation_state,
        mark_reconciliation_scanning, supersede_reconciliation,
    },
    snapshot::{
        SnapshotActivationDisposition, SnapshotActivationRequest, SnapshotApplyOutcome,
        SnapshotOwnership, activate_snapshot_group, begin_snapshot_apply, begin_snapshot_group,
        plan_snapshot_target,
    },
};
use futures::stream;
use tokio_postgres::NoTls;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_reconciliation_state_is_fenced_and_restart_safe() -> Result<(), Box<dyn Error>>
{
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("reconciliation integration connection ended: {error}");
        }
    });

    migrate_target_database(&mut client).await?;
    let fence = PipelineFence {
        pipeline_id: PipelineId::new(),
        topology_generation: 1,
        fencing_token: 10,
    };
    activate_pipeline_fence(&client, fence).await?;
    let first = run(fence, 1, 0x1000);

    assert_eq!(
        begin_reconciliation(&mut client, &first).await?,
        BeginReconciliationOutcome::Started
    );
    assert_eq!(
        begin_reconciliation(&mut client, &first).await?,
        BeginReconciliationOutcome::AlreadyExists(ReconciliationState::Aligning)
    );
    assert!(
        load_reconciliation_state(&mut client, fence, first.source_relation_id)
            .await?
            .expect("started reconciliation")
            .was_interrupted()
    );
    let recovery = load_reconciliation_startup_recovery(&mut client, fence).await?;
    assert!(matches!(
        recovery.as_slice(),
        [ReconciliationStartupRecovery::RestartInterruptedScan(stored)]
            if stored.state == ReconciliationState::Aligning
                && stored.run.run_id == first.run_id
    ));

    assert_eq!(
        mark_reconciliation_scanning(&mut client, &first, PgLsn::new(0x0fff)).await?,
        ReconciliationTransitionOutcome::Transitioned
    );
    let mismatch = ReconciliationScanCompletion::ReloadPending {
        source: stats(100, 8_000, 0x11),
        target: stats(99, 7_920, 0x22),
    };
    assert_eq!(
        complete_reconciliation_scan(&mut client, &first, &mismatch).await?,
        ReconciliationTransitionOutcome::Transitioned
    );
    let pending = load_reconciliation_state(&mut client, fence, first.source_relation_id)
        .await?
        .expect("reload-pending reconciliation");
    assert_eq!(pending.state, ReconciliationState::ReloadPending);
    assert!(pending.last_mismatch_at.is_some());
    assert!(pending.completed_at.is_none());

    let recovery = load_reconciliation_startup_recovery(&mut client, fence).await?;
    assert_eq!(recovery.len(), 1);
    assert!(matches!(
        &recovery[0],
        ReconciliationStartupRecovery::RestartPendingReload(stored)
            if stored.run.run_id == first.run_id
    ));
    assert_eq!(
        supersede_reconciliation(&mut client, &first, "reload recovery was inspected").await?,
        ReconciliationTransitionOutcome::Transitioned
    );

    let second = run(fence, 2, 0x2000);
    assert_eq!(
        begin_reconciliation(&mut client, &second).await?,
        BeginReconciliationOutcome::Started
    );
    assert_eq!(
        mark_reconciliation_scanning(&mut client, &second, PgLsn::new(0x1fff)).await?,
        ReconciliationTransitionOutcome::Transitioned
    );
    let replacement_fence = PipelineFence {
        fencing_token: 11,
        ..fence
    };
    activate_pipeline_fence(&client, replacement_fence).await?;
    let recovery = load_reconciliation_startup_recovery(&mut client, replacement_fence).await?;
    assert_eq!(recovery.len(), 1);
    let ReconciliationStartupRecovery::RestartInterruptedScan(interrupted) = &recovery[0] else {
        panic!("scanning run must restart its scan: {:?}", recovery[0]);
    };
    assert_eq!(interrupted.state, ReconciliationState::Scanning);
    assert_eq!(interrupted.run.run_id, second.run_id);
    assert_eq!(interrupted.run.fence.fencing_token, fence.fencing_token);

    let mut adopted = second.clone();
    adopted.fence = replacement_fence;
    assert!(matches!(
        mark_reconciliation_scanning(&mut client, &adopted, PgLsn::new(0x1fff)).await,
        Err(ReconciliationStateError::FenceMismatch {
            stored: 10,
            active: 11
        })
    ));
    assert_eq!(
        supersede_reconciliation(&mut client, &adopted, "source snapshot session was lost").await?,
        ReconciliationTransitionOutcome::Transitioned
    );
    let superseded =
        load_reconciliation_state(&mut client, replacement_fence, first.source_relation_id)
            .await?
            .expect("superseded reconciliation");
    assert_eq!(superseded.state, ReconciliationState::Superseded);
    assert_eq!(
        superseded.run.fence.fencing_token,
        replacement_fence.fencing_token
    );

    let third = run(replacement_fence, 3, 0x3000);
    assert_eq!(
        begin_reconciliation(&mut client, &third).await?,
        BeginReconciliationOutcome::Started
    );
    let retry_at = SystemTime::now() + Duration::from_secs(30);
    assert_eq!(
        fail_reconciliation(&mut client, &third, "source read failed", retry_at).await?,
        ReconciliationTransitionOutcome::Transitioned
    );
    assert_eq!(
        fail_reconciliation(&mut client, &third, "source read failed", retry_at).await?,
        ReconciliationTransitionOutcome::AlreadyComplete
    );
    let failed =
        load_reconciliation_state(&mut client, replacement_fence, third.source_relation_id)
            .await?
            .expect("failed reconciliation");
    assert_eq!(failed.state, ReconciliationState::Failed);
    assert_eq!(failed.consecutive_failures, 1);
    assert_eq!(failed.failure_reason.as_deref(), Some("source read failed"));

    connection_task.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_reconciliation_reload_activation_is_atomic_and_idempotent()
-> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("reconciliation reload integration connection ended: {error}");
        }
    });

    migrate_target_database(&mut client).await?;
    let fence = PipelineFence {
        pipeline_id: PipelineId::new(),
        topology_generation: 1,
        fencing_token: 10,
    };
    activate_pipeline_fence(&client, fence).await?;
    let suffix = Uuid::new_v4().simple().to_string();
    let target = QualifiedName::new(format!("pg2cb_rec_{}", &suffix[..12]), "orders")?;
    let fingerprint = "sha256:reconciliation-orders-v1";

    let initial_schema = reconciliation_source_schema(142, 1);
    let initial_plan = plan_snapshot_target(
        &initial_schema,
        target.clone(),
        QualifiedName::new(&target.schema, "orders_initial_shadow")?,
    )?;
    let initial_request = SnapshotActivationRequest {
        fence,
        snapshot_group_id: Uuid::now_v7(),
        tables: vec![initial_plan.activation_table(fingerprint)],
        initial_checkpoints: vec![NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: 0,
            },
            system_identifier: 7_123_456_789,
            timeline: 1,
            slot_name: format!("pg2cb_initial_{}", &suffix[..16]),
            applied_lsn: PgLsn::new(0x80),
        }],
    };
    begin_snapshot_group(&mut client, &initial_request).await?;
    load_shadow(
        &mut client,
        initial_plan,
        SnapshotOwnership {
            fence,
            snapshot_group_id: initial_request.snapshot_group_id,
            schema_fingerprint: fingerprint.to_owned(),
        },
        "1\n",
    )
    .await?;
    assert_eq!(
        activate_snapshot_group(&mut client, &initial_request)
            .await?
            .disposition,
        SnapshotActivationDisposition::Activated
    );

    let target_oid_raw: i64 = client
        .query_one(
            "SELECT c.oid::bigint FROM pg_catalog.pg_class AS c \
             JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&target.schema, &target.name],
        )
        .await?
        .try_get(0)?;
    let target_relation_oid = u32::try_from(target_oid_raw)?;
    let reconciliation = ReconciliationRunIdentity {
        fence,
        source_relation_id: initial_schema.relation_id,
        target: target.clone(),
        target_relation_oid,
        table_generation: initial_schema.generation,
        schema_fingerprint: fingerprint.to_owned(),
        run_id: Uuid::now_v7(),
        source_node_id: 0,
        temporary_slot_name: format!("pg2cb_reconcile_{}", &suffix[..16]),
        source_system_identifier: 7_123_456_789,
        source_timeline: 1,
        source_snapshot_lsn: PgLsn::new(0x100),
    };
    assert_eq!(
        begin_reconciliation(&mut client, &reconciliation).await?,
        BeginReconciliationOutcome::Started
    );
    assert_eq!(
        mark_reconciliation_scanning(&mut client, &reconciliation, PgLsn::new(0xff)).await?,
        ReconciliationTransitionOutcome::Transitioned
    );
    assert_eq!(
        complete_reconciliation_scan(
            &mut client,
            &reconciliation,
            &ReconciliationScanCompletion::ReloadPending {
                source: stats(2, 16, 0x11),
                target: stats(1, 8, 0x22),
            },
        )
        .await?,
        ReconciliationTransitionOutcome::Transitioned
    );

    let reload_schema = reconciliation_source_schema(142, 2);
    let reload_plan = plan_snapshot_target(
        &reload_schema,
        target.clone(),
        QualifiedName::new(&target.schema, "orders_reconciliation_shadow")?,
    )?;
    let reload_request = SnapshotActivationRequest {
        fence,
        snapshot_group_id: Uuid::now_v7(),
        tables: vec![reload_plan.activation_table(fingerprint)],
        initial_checkpoints: vec![NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: fence.pipeline_id,
                topology_generation: fence.topology_generation,
                node_id: reconciliation.source_node_id,
            },
            system_identifier: reconciliation.source_system_identifier,
            timeline: reconciliation.source_timeline,
            slot_name: reconciliation.temporary_slot_name.clone(),
            applied_lsn: reconciliation.source_snapshot_lsn,
        }],
    };
    begin_snapshot_group(&mut client, &reload_request).await?;
    load_shadow(
        &mut client,
        reload_plan,
        SnapshotOwnership {
            fence,
            snapshot_group_id: reload_request.snapshot_group_id,
            schema_fingerprint: fingerprint.to_owned(),
        },
        "2\n",
    )
    .await?;

    let next_due_at = SystemTime::now() + Duration::from_secs(300);
    let first =
        activate_reconciliation_reload(&mut client, &reconciliation, &reload_request, next_due_at)
            .await?;
    assert_eq!(
        first.activation.disposition,
        SnapshotActivationDisposition::Activated
    );
    assert_eq!(
        first.reconciliation,
        ReconciliationTransitionOutcome::Transitioned
    );
    let reloaded = load_reconciliation_state(&mut client, fence, reconciliation.source_relation_id)
        .await?
        .expect("reloaded reconciliation");
    assert_eq!(reloaded.state, ReconciliationState::Reloaded);
    assert!(reloaded.completed_at.is_some());
    assert!(reloaded.last_consistent_at.is_some());
    assert_eq!(reloaded.consecutive_failures, 0);
    let ids = client
        .query(&format!("SELECT id FROM {target} ORDER BY id"), &[])
        .await?
        .into_iter()
        .map(|row| row.try_get::<_, i64>(0))
        .collect::<Result<Vec<_>, _>>()?;
    assert_eq!(ids, vec![2]);

    let replay =
        activate_reconciliation_reload(&mut client, &reconciliation, &reload_request, next_due_at)
            .await?;
    assert_eq!(
        replay.activation.disposition,
        SnapshotActivationDisposition::AlreadyActive
    );
    assert_eq!(
        replay.reconciliation,
        ReconciliationTransitionOutcome::AlreadyComplete
    );

    connection_task.abort();
    Ok(())
}

async fn load_shadow(
    client: &mut tokio_postgres::Client,
    plan: cloudberry_etl_target_cloudberry::snapshot::SnapshotTargetPlan,
    ownership: SnapshotOwnership,
    row: &'static str,
) -> Result<(), Box<dyn Error>> {
    let mut apply = begin_snapshot_apply(client, plan, &ownership).await?;
    let rows = apply
        .copy_from_stream(stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(
            row.as_bytes(),
        ))]))
        .await?;
    assert_eq!(rows, 1);
    assert_eq!(
        apply.commit().await?,
        SnapshotApplyOutcome::Copied { rows: 1 }
    );
    Ok(())
}

fn reconciliation_source_schema(relation_id: u32, generation: u64) -> TableSchema {
    TableSchema {
        relation_id,
        generation,
        name: QualifiedName::new("public", "source_orders").unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![ColumnSchema {
            attnum: 1,
            name: "id".to_owned(),
            data_type: PgType {
                oid: 20,
                name: QualifiedName::new("pg_catalog", "int8").unwrap(),
                kind: PgTypeKind::Int8,
            },
            nullable: false,
            primary_key_ordinal: Some(1),
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

fn run(fence: PipelineFence, ordinal: u128, snapshot_lsn: u64) -> ReconciliationRunIdentity {
    ReconciliationRunIdentity {
        fence,
        source_relation_id: 42,
        target: QualifiedName::new("public", "reconciled_orders").unwrap(),
        target_relation_oid: 17_001,
        table_generation: 3,
        schema_fingerprint: "sha256:reconciled-orders-v3".to_owned(),
        run_id: Uuid::from_u128(ordinal),
        source_node_id: 0,
        temporary_slot_name: format!("pg2cb_reconcile_{ordinal}"),
        source_system_identifier: 7_123_456_789,
        source_timeline: 1,
        source_snapshot_lsn: PgLsn::new(snapshot_lsn),
    }
}

fn stats(rows: u64, bytes: u64, digest_byte: u8) -> ReconciliationStats {
    ReconciliationStats {
        rows,
        bytes,
        digest: [digest_byte; 64],
    }
}
