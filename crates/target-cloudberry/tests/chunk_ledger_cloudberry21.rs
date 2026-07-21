//! Opt-in durable chunk ledger coverage against Apache Cloudberry 2.1.

use std::error::Error;

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use cloudberry_etl_target_cloudberry::{
    apply::{ApplyError, execute_ledgered_completion, execute_register_manifest},
    checkpoint::{
        AdvanceOutcome, CheckpointError, CheckpointKey, NodeCheckpoint, PipelineFence,
        activate_pipeline_fence, load_node_checkpoint,
    },
    chunk::{
        ChunkLedgerError, DataChunkIdentity, PrepareDataChunkOutcome, ProgressRegistration,
        TransactionChunkKey, TransactionChunkManifest, prepare_data_chunk,
        prepare_transaction_completion, record_data_chunk,
    },
    migration::migrate_target_database,
};
use tokio_postgres::{Client, NoTls};

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_chunk_ledger_replays_and_completes_exactly() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test Cloudberry connection ended: {error}");
        }
    });

    migrate_target_database(&mut client).await?;
    let fence = PipelineFence {
        pipeline_id: PipelineId::new(),
        topology_generation: 1,
        fencing_token: 1,
    };
    activate_pipeline_fence(&client, fence).await?;
    let manifest = manifest(fence);

    let first = chunk(0, 2, 0x11);
    let transaction = client.transaction().await?;
    let PrepareDataChunkOutcome::Apply(prepared) =
        prepare_data_chunk(&transaction, fence, &manifest, first).await?
    else {
        panic!("new first chunk must be ready to apply");
    };
    record_data_chunk(&transaction, prepared).await?;
    transaction.commit().await?;

    let transaction = client.transaction().await?;
    assert!(matches!(
        prepare_data_chunk(&transaction, fence, &manifest, first).await?,
        PrepareDataChunkOutcome::AlreadyCommitted { next_seq: 2 }
    ));
    transaction.rollback().await?;

    let transaction = client.transaction().await?;
    assert!(matches!(
        prepare_data_chunk(&transaction, fence, &manifest, chunk(3, 4, 0x33)).await,
        Err(ChunkLedgerError::SequenceGap {
            next_seq: 2,
            start_seq: 3
        })
    ));
    transaction.rollback().await?;

    let transaction = client.transaction().await?;
    assert!(matches!(
        prepare_transaction_completion(&transaction, fence, &manifest).await,
        Err(ChunkLedgerError::IncompleteTransaction {
            next_seq: 2,
            record_count: 4
        })
    ));
    transaction.rollback().await?;

    let transaction = client.transaction().await?;
    let PrepareDataChunkOutcome::Apply(prepared) =
        prepare_data_chunk(&transaction, fence, &manifest, chunk(2, 4, 0x22)).await?
    else {
        panic!("contiguous second chunk must be ready to apply");
    };
    record_data_chunk(&transaction, prepared).await?;
    transaction.commit().await?;
    assert_eq!(ledger_counts(&client, manifest.key).await?, (1, 2));

    let first_checkpoint = checkpoint(&manifest);
    let transaction = client.transaction().await?;
    let completion = prepare_transaction_completion(&transaction, fence, &manifest).await?;
    assert_eq!(
        execute_ledgered_completion(transaction, completion, &first_checkpoint).await?,
        AdvanceOutcome::Inserted
    );

    let stored = load_node_checkpoint(&client, first_checkpoint.key)
        .await?
        .expect("completion must atomically publish the checkpoint");
    assert_eq!(stored.checkpoint, first_checkpoint);
    assert_eq!(ledger_counts(&client, manifest.key).await?, (0, 0));

    // Simulate a COMMIT response lost after checkpoint publication and ledger retirement. The
    // checkpoint is the recovery authority, so replay cannot recreate progress or execute DML.
    assert_eq!(
        execute_register_manifest(&mut client, fence, &manifest).await?,
        ProgressRegistration::AlreadyCheckpointed {
            applied_lsn: manifest.key.end_lsn
        }
    );
    let transaction = client.transaction().await?;
    assert!(matches!(
        prepare_data_chunk(&transaction, fence, &manifest, first).await?,
        PrepareDataChunkOutcome::AlreadyCheckpointed { applied_lsn }
            if applied_lsn == manifest.key.end_lsn
    ));
    transaction.rollback().await?;
    assert_eq!(ledger_counts(&client, manifest.key).await?, (0, 0));

    let mut wrong_identity = manifest.clone();
    wrong_identity.timeline += 1;
    assert!(matches!(
        execute_register_manifest(&mut client, fence, &wrong_identity).await,
        Err(ApplyError::ChunkLedger(ChunkLedgerError::Checkpoint(
            CheckpointError::SourceIdentityChanged
        )))
    ));

    let mut empty_manifest = manifest.clone();
    empty_manifest.key.end_lsn = PgLsn::new(manifest.key.end_lsn.as_u64() + 1);
    empty_manifest.xid = 7;
    empty_manifest.record_count = 0;
    empty_manifest.manifest_digest = [0xE0; 32];
    assert_eq!(
        execute_register_manifest(&mut client, fence, &empty_manifest).await?,
        ProgressRegistration::Registered
    );
    assert_eq!(
        execute_register_manifest(&mut client, fence, &empty_manifest).await?,
        ProgressRegistration::Existing { next_seq: 0 }
    );
    let empty_checkpoint = checkpoint(&empty_manifest);
    let transaction = client.transaction().await?;
    let completion = prepare_transaction_completion(&transaction, fence, &empty_manifest).await?;
    assert_eq!(
        execute_ledgered_completion(transaction, completion, &empty_checkpoint).await?,
        AdvanceOutcome::Advanced {
            previous_lsn: manifest.key.end_lsn
        }
    );
    assert_eq!(ledger_counts(&client, empty_manifest.key).await?, (0, 0));
    assert_eq!(
        execute_register_manifest(&mut client, fence, &empty_manifest).await?,
        ProgressRegistration::AlreadyCheckpointed {
            applied_lsn: empty_manifest.key.end_lsn
        }
    );

    cleanup(&client, fence.pipeline_id).await;
    connection_task.abort();
    Ok(())
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

fn manifest(fence: PipelineFence) -> TransactionChunkManifest {
    TransactionChunkManifest {
        key: TransactionChunkKey {
            pipeline_id: fence.pipeline_id,
            topology_generation: fence.topology_generation,
            node_id: 0,
            end_lsn: PgLsn::new(0xAA55),
        },
        system_identifier: u64::MAX,
        timeline: 1,
        slot_name: "pg2cb_chunk_it_slot".to_owned(),
        xid: u32::MAX,
        manifest_version: 1,
        record_count: 4,
        manifest_digest: [0xA5; 32],
    }
}

fn checkpoint(manifest: &TransactionChunkManifest) -> NodeCheckpoint {
    NodeCheckpoint {
        key: CheckpointKey {
            pipeline_id: manifest.key.pipeline_id,
            topology_generation: manifest.key.topology_generation,
            node_id: manifest.key.node_id,
        },
        system_identifier: manifest.system_identifier,
        timeline: manifest.timeline,
        slot_name: manifest.slot_name.clone(),
        applied_lsn: manifest.key.end_lsn,
    }
}

fn chunk(start_seq: u64, end_seq: u64, byte: u8) -> DataChunkIdentity {
    DataChunkIdentity {
        start_seq,
        end_seq,
        digest: [byte; 32],
    }
}

async fn cleanup(client: &Client, pipeline_id: PipelineId) {
    let id = pipeline_id.as_uuid();
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.transaction_committed_chunks WHERE pipeline_id = $1",
            &[&id],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.transaction_chunk_progress WHERE pipeline_id = $1",
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
}
