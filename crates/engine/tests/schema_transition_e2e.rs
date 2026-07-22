//! Real PG18 -> Cloudberry 2.1 coverage for transaction-scoped schema event preparation.

use std::{collections::HashSet, error::Error, panic::AssertUnwindSafe};

use cloudberry_etl_core::{
    change::{
        DdlMessage, SourcePosition, SourceTransaction, TableTransition, TransactionChange,
        TransitionKind,
    },
    id::PipelineId,
    lsn::PgLsn,
};
use cloudberry_etl_engine::schema_transition::{
    CatalogMismatchKind, SchemaAction, plan_schema_transaction, prepare_schema_capability_event,
    prepare_schema_event,
};
use cloudberry_etl_source_postgres::{
    catalog::CatalogOptions,
    ddl::{DdlInstallSpec, ensure_ddl_capture, load_current_relation_schemas},
    wal::CommittedTransaction,
};
use cloudberry_etl_target_cloudberry::{
    checkpoint::{PipelineFence, activate_pipeline_fence},
    migration::migrate_target_database,
    schema_event::{RecordOutcome, load_schema_event},
    table_transition::{TableTransitionAction, TableTransitionKey, load_table_transition},
};
use futures::FutureExt;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires real PG18 and Cloudberry 2.1 DSNs"]
async fn schema_event_preparation_is_catalog_checked_and_fenced() -> Result<(), Box<dyn Error>> {
    let source_dsn = std::env::var("PG2CB_TEST_SOURCE_DSN")?;
    let target_dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (source, source_connection) = tokio_postgres::connect(&source_dsn, NoTls).await?;
    let source_task = tokio::spawn(async move {
        if let Err(error) = source_connection.await {
            eprintln!("schema transition source connection ended: {error}");
        }
    });
    let (mut target, target_connection) = tokio_postgres::connect(&target_dsn, NoTls).await?;
    let target_task = tokio::spawn(async move {
        if let Err(error) = target_connection.await {
            eprintln!("schema transition target connection ended: {error}");
        }
    });
    let pipeline_id = PipelineId::new();
    let schema = format!("pg2cb_schema_it_{}", Uuid::now_v7().simple());
    let result = AssertUnwindSafe(run_test(&source, &mut target, pipeline_id, &schema))
        .catch_unwind()
        .await;
    cleanup(&source, &target, pipeline_id, &schema).await;
    source_task.abort();
    target_task.abort();
    match result {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

async fn run_test(
    source: &Client,
    target: &mut Client,
    pipeline_id: PipelineId,
    schema: &str,
) -> Result<(), Box<dyn Error>> {
    let install = DdlInstallSpec::default();
    ensure_ddl_capture(source, &install).await?;
    migrate_target_database(target).await?;
    let fence = PipelineFence {
        pipeline_id,
        topology_generation: 1,
        fencing_token: 1,
    };
    activate_pipeline_fence(target, fence).await?;

    let schema_sql = quote_identifier(schema);
    source
        .batch_execute(&format!(
            "CREATE SCHEMA {schema_sql};
             CREATE TABLE {schema_sql}.items (id bigint PRIMARY KEY, payload text NOT NULL)"
        ))
        .await?;
    let relation_id = u32::try_from(
        source
            .query_one(
                "SELECT $1::text::regclass::oid::int8",
                &[&format!("{schema}.items")],
            )
            .await?
            .try_get::<_, i64>(0)?,
    )?;
    let current =
        load_current_relation_schemas(source, &install.metadata_schema, &[relation_id]).await?;
    let captured = current
        .get(&relation_id)
        .and_then(Option::as_ref)
        .expect("new source table has a typed catalog state")
        .clone();
    let transaction = CommittedTransaction::from_memory(
        SourceTransaction {
            xid: 77,
            commit_time: chrono::Utc::now(),
            final_position: SourcePosition {
                node_id: 0,
                system_identifier: 99,
                timeline: 1,
                lsn: PgLsn::new(0x1010),
            },
            changes: vec![TransactionChange::Ddl(DdlMessage {
                version: 2,
                command_tag: "CREATE TABLE".to_owned(),
                relation_ids: vec![relation_id],
                affected_schemas: vec![schema.to_owned()],
                schema_fingerprint: captured.fingerprint.clone(),
                transitions: vec![TableTransition {
                    relation_id,
                    before_generation: None,
                    after_generation: None,
                    before_fingerprint: None,
                    after_fingerprint: Some(captured.fingerprint.clone()),
                    after_schema: Some(captured.schema),
                    kind: TransitionKind::AddTable,
                }],
            })],
        },
        PgLsn::new(0x1000),
    );

    let plan = plan_schema_transaction(&transaction)?.expect("CREATE TABLE has a schema plan");
    let catalog_options = CatalogOptions {
        metadata_schema: install.metadata_schema.clone(),
        include_schemas: Some(HashSet::from([schema.to_owned()])),
        exclude_schemas: HashSet::new(),
        include_partitions: false,
    };
    let prepared = prepare_schema_capability_event(
        source,
        target,
        &catalog_options,
        fence,
        plan.clone(),
        &Default::default(),
        &Default::default(),
    )
    .await?;
    assert!(prepared.event.catalog_validation.is_exact_match());
    assert_eq!(prepared.event.record_outcome, RecordOutcome::Inserted);
    assert!(matches!(
        prepared.capability.actions.get(&relation_id),
        Some(SchemaAction::Add { .. })
    ));
    let table_key = TableTransitionKey {
        pipeline_id,
        source_lsn: PgLsn::new(0x1010),
        source_xid: 77,
        source_relation_id: relation_id,
    };
    let table_transition = load_table_transition(target, table_key)
        .await?
        .expect("bound table action is persisted with the event");
    assert_eq!(table_transition.event_id, prepared.event.event_id);
    assert_eq!(table_transition.action, TableTransitionAction::Add);
    assert_eq!(table_transition.active_table_generation, None);
    assert_eq!(table_transition.pending_table_generation, Some(1));
    let persisted_action: SchemaAction = serde_json::from_value(table_transition.plan)?;
    assert!(matches!(persisted_action, SchemaAction::Add { .. }));

    let replay = prepare_schema_capability_event(
        source,
        target,
        &catalog_options,
        fence,
        plan,
        &Default::default(),
        &Default::default(),
    )
    .await?;
    assert_eq!(replay.event.record_outcome, RecordOutcome::AlreadyRecorded);
    assert_eq!(
        replay.event.plan.payload_fingerprint,
        prepared.event.plan.payload_fingerprint
    );

    source
        .batch_execute(&format!(
            "ALTER TABLE {schema_sql}.items ADD COLUMN later text"
        ))
        .await?;
    let advanced = prepare_schema_event(
        source,
        target,
        &install.metadata_schema,
        fence,
        &transaction,
    )
    .await?
    .expect("old event remains durable after rapid later DDL");
    assert!(!advanced.catalog_validation.is_exact_match());
    assert_eq!(
        advanced.catalog_validation.mismatches[0].kind,
        CatalogMismatchKind::DifferentPresentState
    );
    assert_eq!(advanced.record_outcome, RecordOutcome::AlreadyRecorded);

    let newer_fence = PipelineFence {
        fencing_token: 2,
        ..fence
    };
    activate_pipeline_fence(target, newer_fence).await?;
    let adopted = prepare_schema_event(
        source,
        target,
        &install.metadata_schema,
        newer_fence,
        &transaction,
    )
    .await?
    .expect("new owner replays the pending event");
    assert_eq!(adopted.record_outcome, RecordOutcome::Adopted);
    let stored = load_schema_event(target, pipeline_id, PgLsn::new(0x1010), 77)
        .await?
        .expect("schema event is persisted");
    assert_eq!(stored.fencing_token, newer_fence.fencing_token);
    assert_eq!(stored.transitions, adopted.plan.ledger_payload()?);
    Ok(())
}

async fn cleanup(source: &Client, target: &Client, pipeline_id: PipelineId, schema: &str) {
    let _ = source
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(schema)
        ))
        .await;
    let id = pipeline_id.as_uuid();
    let _ = target
        .execute(
            "DELETE FROM pg2cb_meta.table_schema_transitions WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = target
        .execute(
            "DELETE FROM pg2cb_meta.schema_events WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = target
        .execute(
            "DELETE FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
