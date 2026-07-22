//! Opt-in Apache Cloudberry 2.1 coverage for schema-event and table-transition ledgers (V8/V9).
//!
//! Run explicitly with a disposable Cloudberry 2.1 instance:
//! `PG2CB_TEST_TARGET_DSN=postgres://... cargo test -p cloudberry-etl-target-cloudberry --test schema_event_cloudberry21 -- --ignored --nocapture`

use std::error::Error;

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use cloudberry_etl_target_cloudberry::{
    checkpoint::{PipelineFence, activate_pipeline_fence},
    migration::migrate_target_database,
    schema_event::{
        RecordOutcome, SchemaEventRecord, SchemaEventState, advance_schema_event_state,
        fail_schema_event_and_block_transitions, list_unfinished_schema_events, load_schema_event,
        record_schema_event,
    },
    table_transition::{
        TableTransitionAction, TableTransitionKey, TableTransitionRecord,
        TableTransitionRecordOutcome, TableTransitionState, advance_table_transition_state,
        begin_table_snapshot_transition, list_unfinished_table_transitions, load_table_transition,
        record_table_transition,
    },
};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn schema_event_ledger_records_and_transitions() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });

    let pipeline_id = PipelineId::new();
    let result = run_ledger_test(&mut client, pipeline_id).await;
    cleanup(&client, pipeline_id).await;
    connection_task.abort();
    result
}

async fn run_ledger_test(
    client: &mut Client,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    migrate_target_database(client).await?;
    let fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 5,
    };
    activate_pipeline_fence(client, fence).await?;

    let event_id = Uuid::now_v7();
    let record = SchemaEventRecord {
        event_id,
        fence,
        source_lsn: PgLsn::new(0x1000),
        source_xid: 4242,
        command_tag: "ALTER TABLE".to_owned(),
        schema_fingerprint: "fp-1".to_owned(),
        transitions: serde_json::json!([
            {"relation_id": 100, "type": "add_column", "column": "note"}
        ]),
    };

    // First insert records the event.
    assert_eq!(
        record_schema_event(client, &record).await?,
        RecordOutcome::Inserted
    );
    // Replayed WAL (same source identity) is idempotent.
    assert_eq!(
        record_schema_event(client, &record).await?,
        RecordOutcome::AlreadyRecorded
    );
    let mut conflicting = record.clone();
    conflicting.transitions = serde_json::json!([{"relation_id": 100, "type": "drop_column"}]);
    assert!(
        record_schema_event(client, &conflicting).await.is_err(),
        "same source identity with different payload must fail closed"
    );

    // Loadable with the persisted payload intact.
    let loaded = load_schema_event(client, pipeline_id, PgLsn::new(0x1000), 4242)
        .await?
        .expect("event exists");
    assert_eq!(loaded.event_id, event_id);
    assert_eq!(loaded.state, SchemaEventState::Pending);
    assert_eq!(loaded.command_tag, "ALTER TABLE");
    assert_eq!(loaded.source_xid, 4242);
    assert_eq!(loaded.transitions[0]["column"], "note");

    // Appears in the unfinished list.
    let pending = list_unfinished_schema_events(client, fence).await?;
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].event_id, event_id);

    // A newer lease first activates the target pipeline fence, then adopts unfinished rows.
    let newer_fence = PipelineFence {
        fencing_token: 6,
        ..fence
    };
    activate_pipeline_fence(client, newer_fence).await?;
    assert!(
        list_unfinished_schema_events(client, fence).await.is_err(),
        "the stale owner must not read/adopt transition work"
    );
    let adopted = list_unfinished_schema_events(client, newer_fence).await?;
    assert_eq!(adopted.len(), 1);
    assert_eq!(adopted[0].fencing_token, newer_fence.fencing_token);

    let table_key = TableTransitionKey {
        pipeline_id,
        source_lsn: PgLsn::new(0x1000),
        source_xid: 4242,
        source_relation_id: 100,
    };
    let snapshot_group_id = Uuid::now_v7();
    let table_record = TableTransitionRecord {
        event_id,
        fence: newer_fence,
        source_lsn: table_key.source_lsn,
        source_xid: table_key.source_xid,
        source_relation_id: table_key.source_relation_id,
        action: TableTransitionAction::Reload,
        plan: serde_json::json!({"action": "reload", "after": {"relation_id": 100}}),
        barrier_lsn: table_key.source_lsn,
        active_table_generation: Some(1),
        pending_table_generation: Some(2),
        snapshot_group_id: None,
    };
    assert_eq!(
        record_table_transition(client, &table_record).await?,
        TableTransitionRecordOutcome::Inserted
    );
    assert_eq!(
        record_table_transition(client, &table_record).await?,
        TableTransitionRecordOutcome::AlreadyRecorded
    );
    let loaded_table = load_table_transition(client, table_key)
        .await?
        .expect("table transition exists");
    assert_eq!(loaded_table.plan, table_record.plan);
    assert_eq!(loaded_table.state, TableTransitionState::Pending);
    let unfinished_tables = list_unfinished_table_transitions(client, newer_fence).await?;
    assert_eq!(unfinished_tables.len(), 1);
    assert_eq!(unfinished_tables[0].key, table_key);

    let snapshotting = begin_table_snapshot_transition(
        client,
        newer_fence,
        table_key,
        TableTransitionState::Pending,
        snapshot_group_id,
    )
    .await?;
    assert_eq!(snapshotting.state, TableTransitionState::Snapshotting);
    assert_eq!(snapshotting.snapshot_group_id, Some(snapshot_group_id));
    let replayed_snapshotting = begin_table_snapshot_transition(
        client,
        newer_fence,
        table_key,
        TableTransitionState::Pending,
        snapshot_group_id,
    )
    .await?;
    assert_eq!(replayed_snapshotting, snapshotting);
    advance_table_transition_state(
        client,
        newer_fence,
        table_key,
        TableTransitionState::Snapshotting,
        TableTransitionState::CatchingUp,
        None,
    )
    .await?;
    advance_table_transition_state(
        client,
        newer_fence,
        table_key,
        TableTransitionState::CatchingUp,
        TableTransitionState::CutoverPending,
        None,
    )
    .await?;
    advance_table_transition_state(
        client,
        newer_fence,
        table_key,
        TableTransitionState::CutoverPending,
        TableTransitionState::Completed,
        None,
    )
    .await?;
    assert!(
        list_unfinished_table_transitions(client, newer_fence)
            .await?
            .is_empty()
    );

    // Drive the state machine: pending -> in_transition -> completed.
    assert!(
        advance_schema_event_state(
            client,
            fence,
            PgLsn::new(0x1000),
            4242,
            SchemaEventState::Pending,
            SchemaEventState::InTransition,
            None,
        )
        .await
        .is_err(),
        "the stale owner must not advance an adopted event"
    );
    advance_schema_event_state(
        client,
        newer_fence,
        PgLsn::new(0x1000),
        4242,
        SchemaEventState::Pending,
        SchemaEventState::InTransition,
        None,
    )
    .await?;

    // A stale expected-from no longer matches, so the guarded update fails.
    let stale = advance_schema_event_state(
        client,
        newer_fence,
        PgLsn::new(0x1000),
        4242,
        SchemaEventState::Pending,
        SchemaEventState::Completed,
        None,
    )
    .await;
    assert!(stale.is_err(), "stale expected-from must not update");

    advance_schema_event_state(
        client,
        newer_fence,
        PgLsn::new(0x1000),
        4242,
        SchemaEventState::InTransition,
        SchemaEventState::Completed,
        None,
    )
    .await?;

    // Completed events drop out of the unfinished list.
    let after = list_unfinished_schema_events(client, newer_fence).await?;
    assert!(
        after.is_empty(),
        "completed event must not remain unfinished"
    );

    let completed = load_schema_event(client, pipeline_id, PgLsn::new(0x1000), 4242)
        .await?
        .expect("event exists");
    assert_eq!(completed.state, SchemaEventState::Completed);

    // A second event that fails carries its reason.
    let failed_id = Uuid::now_v7();
    let failed = SchemaEventRecord {
        event_id: failed_id,
        fence: newer_fence,
        source_lsn: PgLsn::new(0x2000),
        source_xid: 4243,
        command_tag: "ALTER TABLE".to_owned(),
        schema_fingerprint: "fp-2".to_owned(),
        transitions: serde_json::json!([]),
    };
    assert_eq!(
        record_schema_event(client, &failed).await?,
        RecordOutcome::Inserted
    );
    let failed_table_key = TableTransitionKey {
        pipeline_id,
        source_lsn: failed.source_lsn,
        source_xid: failed.source_xid,
        source_relation_id: 101,
    };
    record_table_transition(
        client,
        &TableTransitionRecord {
            event_id: failed_id,
            fence: newer_fence,
            source_lsn: failed.source_lsn,
            source_xid: failed.source_xid,
            source_relation_id: failed_table_key.source_relation_id,
            action: TableTransitionAction::Reload,
            plan: serde_json::json!({"action": "reload"}),
            barrier_lsn: failed.source_lsn,
            active_table_generation: Some(1),
            pending_table_generation: Some(2),
            snapshot_group_id: None,
        },
    )
    .await?;
    fail_schema_event_and_block_transitions(
        client,
        newer_fence,
        failed.source_lsn,
        failed.source_xid,
        "narrowing type change is not online-safe",
    )
    .await?;
    let failed_row = load_schema_event(client, pipeline_id, PgLsn::new(0x2000), 4243)
        .await?
        .expect("failed event exists");
    assert_eq!(failed_row.state, SchemaEventState::Failed);
    assert_eq!(
        failed_row.failure_reason.as_deref(),
        Some("narrowing type change is not online-safe")
    );
    let failed_table = load_table_transition(client, failed_table_key)
        .await?
        .expect("failed table transition exists");
    assert_eq!(failed_table.state, TableTransitionState::Blocked);
    assert_eq!(
        failed_table.failure_reason.as_deref(),
        failed_row.failure_reason.as_deref()
    );

    Ok(())
}

async fn cleanup(client: &Client, pipeline_id: PipelineId) {
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
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
}
