//! Opt-in control-store coverage against PostgreSQL 18.
//!
//! Run with a disposable control database:
//! `PG2CB_TEST_CONTROL_DSN=postgres://... cargo test -p cloudberry-etl-metadata --test control_store_pg18 -- --ignored`

use std::error::Error;
use std::time::Duration;

use chrono::Utc;
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
};
use cloudberry_etl_metadata::{
    crypto::EncryptedSecret,
    migration::migrate_control_database,
    model::{PipelineDefinition, SourceProfile, TargetProfile},
    store::{ControlStore, PostgresControlStore, StoreError},
};
use serde_json::json;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires PostgreSQL 18 and PG2CB_TEST_CONTROL_DSN"]
async fn desired_state_and_rebuild_have_independent_revisions() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_CONTROL_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("control-store test connection ended: {error}");
        }
    });
    let (audit_client, audit_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let audit_task = tokio::spawn(async move {
        if let Err(error) = audit_connection.await {
            eprintln!("control-store audit connection ended: {error}");
        }
    });

    migrate_control_database(&mut client).await?;
    let ids = TestIds::new();
    let store = PostgresControlStore::new(client);
    let result = run_test(&store, &audit_client, &ids).await;
    cleanup(&audit_client, &ids).await;
    connection_task.abort();
    audit_task.abort();
    result
}

struct TestIds {
    source: SourceId,
    target: TargetId,
    pipeline: PipelineId,
    suffix: String,
}

impl TestIds {
    fn new() -> Self {
        Self {
            source: SourceId::new(),
            target: TargetId::new(),
            pipeline: PipelineId::new(),
            suffix: Uuid::now_v7().simple().to_string(),
        }
    }
}

async fn run_test(
    store: &PostgresControlStore,
    audit_client: &Client,
    ids: &TestIds,
) -> Result<(), Box<dyn Error>> {
    let now = Utc::now();
    store
        .put_source(&SourceProfile {
            id: ids.source,
            name: format!("source_{}", ids.suffix),
            prefix: SourcePrefix::new(format!("s{}", &ids.suffix[..8]))?,
            database_name: "source".into(),
            topology: SourceTopology::Standalone,
            encrypted_dsn: encrypted_placeholder(),
            settings: json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
        })
        .await?;
    store
        .put_target(&TargetProfile {
            id: ids.target,
            name: format!("target_{}", ids.suffix),
            database_name: "target".into(),
            encrypted_dsn: encrypted_placeholder(),
            settings: json!({}),
            enabled: true,
            created_at: now,
            updated_at: now,
        })
        .await?;

    let settings = json!({"batch": {"max_rows": 100}});
    let pipeline = PipelineDefinition {
        id: ids.pipeline,
        name: format!("pipeline_{}", ids.suffix),
        source_id: ids.source,
        target_id: ids.target,
        desired_running: false,
        config_revision: 1,
        snapshot_generation: 1,
        settings: settings.clone(),
        created_at: now,
        updated_at: now,
    };
    store.put_pipeline(&pipeline).await?;

    let first_holder = Uuid::new_v4();
    let first_lease = store
        .try_acquire_lease(ids.pipeline, first_holder, Duration::from_secs(30))
        .await?
        .expect("first holder acquires lease");
    store.release_lease(&first_lease).await?;

    let second_holder = Uuid::new_v4();
    let second_lease = store
        .try_acquire_lease(ids.pipeline, second_holder, Duration::from_secs(30))
        .await?
        .expect("released lease can be acquired");
    assert!(
        second_lease.fencing_token > first_lease.fencing_token,
        "fencing token must remain monotonic across release/reacquire"
    );
    assert!(
        store
            .renew_lease(&first_lease, Duration::from_secs(30))
            .await?
            .is_none(),
        "stale holder must not renew a newer lease"
    );
    store.release_lease(&first_lease).await?;
    let renewed = store
        .renew_lease(&second_lease, Duration::from_secs(30))
        .await?
        .expect("stale release must not affect the current holder");
    assert_eq!(renewed.fencing_token, second_lease.fencing_token);
    store.release_lease(&second_lease).await?;

    let started = store
        .set_pipeline_desired_running(ids.pipeline, true)
        .await?
        .expect("pipeline exists");
    assert!(started.desired_running);
    assert_eq!(started.config_revision, 1);
    assert_eq!(started.snapshot_generation, 1);
    assert_eq!(started.settings, settings);

    let paused = store
        .set_pipeline_desired_running(ids.pipeline, false)
        .await?
        .expect("pipeline exists");
    assert!(!paused.desired_running);
    assert_eq!(paused.config_revision, 1);
    assert_eq!(paused.snapshot_generation, 1);

    let rebuild = store
        .request_pipeline_rebuild(ids.pipeline)
        .await?
        .expect("pipeline exists");
    assert_eq!(rebuild.pipeline.config_revision, 1);
    assert_eq!(rebuild.pipeline.snapshot_generation, 2);
    assert_eq!(rebuild.pipeline.settings, settings);

    let operation = audit_client
        .query_one(
            "SELECT pipeline_id,operation_type,state,detail FROM cloudberry_etl_control.operations WHERE id=$1",
            &[&rebuild.operation_id.as_uuid()],
        )
        .await?;
    assert_eq!(
        operation.get::<_, Uuid>("pipeline_id"),
        ids.pipeline.as_uuid()
    );
    assert_eq!(operation.get::<_, &str>("operation_type"), "rebuild");
    assert_eq!(operation.get::<_, &str>("state"), "requested");
    assert_eq!(
        operation.get::<_, serde_json::Value>("detail")["snapshot_generation"],
        2
    );

    let audit_count: i64 = audit_client
        .query_one(
            "SELECT count(*) FROM cloudberry_etl_control.audit_log WHERE action='pipeline.rebuild_requested' AND object_id=$1",
            &[&ids.pipeline.to_string()],
        )
        .await?
        .get(0);
    assert_eq!(audit_count, 1);

    let second_rebuild = store
        .request_pipeline_rebuild(ids.pipeline)
        .await?
        .expect("pipeline exists");
    assert_eq!(second_rebuild.pipeline.config_revision, 1);
    assert_eq!(second_rebuild.pipeline.snapshot_generation, 3);
    assert_ne!(second_rebuild.operation_id, rebuild.operation_id);

    assert_eq!(store.complete_pipeline_rebuilds(ids.pipeline, 2).await?, 1);
    let first_state: String = audit_client
        .query_one(
            "SELECT state FROM cloudberry_etl_control.operations WHERE id=$1",
            &[&rebuild.operation_id.as_uuid()],
        )
        .await?
        .get(0);
    let second_state: String = audit_client
        .query_one(
            "SELECT state FROM cloudberry_etl_control.operations WHERE id=$1",
            &[&second_rebuild.operation_id.as_uuid()],
        )
        .await?
        .get(0);
    assert_eq!(first_state, "completed");
    assert_eq!(second_state, "requested");
    assert_eq!(store.complete_pipeline_rebuilds(ids.pipeline, 3).await?, 1);
    assert_eq!(store.complete_pipeline_rebuilds(ids.pipeline, 3).await?, 0);
    let operations = store.list_operations().await?;
    let persisted = operations
        .iter()
        .filter(|operation| operation.pipeline_id == Some(ids.pipeline))
        .collect::<Vec<_>>();
    assert_eq!(persisted.len(), 2);
    assert!(
        persisted
            .iter()
            .all(|operation| operation.state == "completed")
    );

    store
        .set_pipeline_desired_running(ids.pipeline, true)
        .await?
        .expect("pipeline exists");
    let mut config_update = second_rebuild.pipeline;
    config_update.config_revision += 1;
    config_update.snapshot_generation += 1;
    config_update.settings = json!({"batch": {"max_rows": 200}});
    store.put_pipeline(&config_update).await?;
    let stored = store
        .list_pipelines()
        .await?
        .into_iter()
        .find(|pipeline| pipeline.id == ids.pipeline)
        .expect("pipeline exists");
    assert!(
        stored.desired_running,
        "config writes preserve desired state"
    );
    assert_eq!(stored.config_revision, 2);
    assert_eq!(stored.snapshot_generation, 4);

    let mut invalid_config_update = stored;
    invalid_config_update.config_revision += 1;
    assert!(matches!(
        store.put_pipeline(&invalid_config_update).await,
        Err(StoreError::StaleRevision)
    ));
    Ok(())
}

fn encrypted_placeholder() -> EncryptedSecret {
    EncryptedSecret {
        key_version: 1,
        nonce: vec![0; 24],
        ciphertext: vec![0; 16],
    }
}

async fn cleanup(client: &Client, ids: &TestIds) {
    if let Err(error) = client
        .execute(
            "DELETE FROM cloudberry_etl_control.operations WHERE pipeline_id=$1",
            &[&ids.pipeline.as_uuid()],
        )
        .await
    {
        eprintln!("failed to remove control-store test operations: {error}");
    }
    if let Err(error) = client
        .execute(
            "DELETE FROM cloudberry_etl_control.pipeline_leases WHERE pipeline_id=$1",
            &[&ids.pipeline.as_uuid()],
        )
        .await
    {
        eprintln!("failed to remove control-store test lease: {error}");
    }
    if let Err(error) = client
        .execute(
            "DELETE FROM cloudberry_etl_control.audit_log WHERE action IN ('pipeline.rebuild_requested','pipeline.rebuild_completed') AND object_id=$1",
            &[&ids.pipeline.to_string()],
        )
        .await
    {
        eprintln!("failed to remove control-store test audit rows: {error}");
    }
    if let Err(error) = client
        .execute(
            "DELETE FROM cloudberry_etl_control.pipelines WHERE id=$1",
            &[&ids.pipeline.as_uuid()],
        )
        .await
    {
        eprintln!("failed to remove control-store test pipeline: {error}");
    }
    for (table, id) in [
        ("sources", ids.source.as_uuid()),
        ("targets", ids.target.as_uuid()),
    ] {
        let statement = format!("DELETE FROM cloudberry_etl_control.{table} WHERE id=$1");
        if let Err(error) = client.execute(&statement, &[&id]).await {
            eprintln!("failed to remove control-store test {table} row: {error}");
        }
    }
}
