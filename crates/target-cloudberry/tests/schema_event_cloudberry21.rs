//! Opt-in Apache Cloudberry 2.1 coverage for the schema-event ledger (migration V8).
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
        list_unfinished_schema_events, load_schema_event, record_schema_event,
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

    // Drive the state machine: pending -> in_transition -> completed.
    advance_schema_event_state(
        client,
        pipeline_id,
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
        pipeline_id,
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
        pipeline_id,
        PgLsn::new(0x1000),
        4242,
        SchemaEventState::InTransition,
        SchemaEventState::Completed,
        None,
    )
    .await?;

    // Completed events drop out of the unfinished list.
    let after = list_unfinished_schema_events(client, fence).await?;
    assert!(after.is_empty(), "completed event must not remain unfinished");

    let completed = load_schema_event(client, pipeline_id, PgLsn::new(0x1000), 4242)
        .await?
        .expect("event exists");
    assert_eq!(completed.state, SchemaEventState::Completed);

    // A second event that fails carries its reason.
    let failed_id = Uuid::now_v7();
    let failed = SchemaEventRecord {
        event_id: failed_id,
        fence,
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
    advance_schema_event_state(
        client,
        pipeline_id,
        PgLsn::new(0x2000),
        4243,
        SchemaEventState::Pending,
        SchemaEventState::Failed,
        Some("narrowing type change is not online-safe"),
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

    Ok(())
}

async fn cleanup(client: &Client, pipeline_id: PipelineId) {
    let id = pipeline_id.as_uuid();
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
