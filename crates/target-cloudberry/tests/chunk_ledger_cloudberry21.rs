//! Opt-in durable chunk ledger coverage against Apache Cloudberry 2.1.

use std::{error::Error, sync::Arc};

use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};
use cloudberry_etl_target_cloudberry::{
    apply::{
        ApplyError, DataChunkDisposition, LedgeredDataChunkOutcome, LedgeredDataChunkRequest,
        LedgeredEmptyTransactionOutcome, execute_ledgered_data_chunk,
        execute_ledgered_empty_transaction, execute_register_manifest,
    },
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
use tokio::sync::Barrier;
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

    // A restart may use different chunk limits. Although the requested 0..1 range does not match
    // the historical 0..2 receipt, durable progress already covers the manifest, so this same
    // target transaction must publish the checkpoint and retire the old ledger.
    let first_checkpoint = checkpoint(&manifest);
    let resumed = LedgeredDataChunkRequest {
        fence,
        manifest: manifest.clone(),
        chunk: chunk(0, 1, 0x44),
        tables: Vec::new(),
    };
    assert_eq!(
        execute_ledgered_data_chunk(&mut client, &resumed).await?,
        LedgeredDataChunkOutcome::Completed {
            next_seq: manifest.record_count,
            disposition: DataChunkDisposition::ResumeAt,
            checkpoint: AdvanceOutcome::Inserted,
        }
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
    let empty_checkpoint = checkpoint(&empty_manifest);
    assert_eq!(
        execute_ledgered_empty_transaction(&mut client, fence, &empty_manifest).await?,
        LedgeredEmptyTransactionOutcome::Completed {
            checkpoint: AdvanceOutcome::Advanced {
                previous_lsn: manifest.key.end_lsn
            }
        }
    );
    assert_eq!(
        load_node_checkpoint(&client, empty_checkpoint.key)
            .await?
            .expect("empty transaction must publish its checkpoint")
            .checkpoint,
        empty_checkpoint
    );
    assert_eq!(ledger_counts(&client, empty_manifest.key).await?, (0, 0));
    assert_eq!(
        execute_ledgered_empty_transaction(&mut client, fence, &empty_manifest).await?,
        LedgeredEmptyTransactionOutcome::AlreadyCheckpointed {
            applied_lsn: empty_manifest.key.end_lsn
        }
    );

    // Recover a final receipt that committed before the old separate completion transaction.
    let mut exact_manifest = manifest.clone();
    exact_manifest.key.end_lsn = PgLsn::new(manifest.key.end_lsn.as_u64() + 2);
    exact_manifest.xid = 8;
    exact_manifest.record_count = 1;
    exact_manifest.manifest_digest = [0xE1; 32];
    let exact = chunk(0, 1, 0x51);
    let transaction = client.transaction().await?;
    let PrepareDataChunkOutcome::Apply(prepared) =
        prepare_data_chunk(&transaction, fence, &exact_manifest, exact).await?
    else {
        panic!("new exact chunk must be ready to apply");
    };
    record_data_chunk(&transaction, prepared).await?;
    transaction.commit().await?;
    assert_eq!(
        execute_ledgered_data_chunk(
            &mut client,
            &LedgeredDataChunkRequest {
                fence,
                manifest: exact_manifest.clone(),
                chunk: exact,
                tables: Vec::new(),
            }
        )
        .await?,
        LedgeredDataChunkOutcome::Completed {
            next_seq: 1,
            disposition: DataChunkDisposition::AlreadyCommitted,
            checkpoint: AdvanceOutcome::Advanced {
                previous_lsn: empty_manifest.key.end_lsn
            },
        }
    );
    assert_eq!(ledger_counts(&client, exact_manifest.key).await?, (0, 0));

    // The normal multi-chunk path uses one target transaction per chunk. The final call includes
    // its DML receipt, checkpoint advancement, and retirement.
    let mut multi_manifest = manifest.clone();
    multi_manifest.key.end_lsn = PgLsn::new(manifest.key.end_lsn.as_u64() + 3);
    multi_manifest.xid = 9;
    multi_manifest.record_count = 2;
    multi_manifest.manifest_digest = [0xE2; 32];
    let first_request = LedgeredDataChunkRequest {
        fence,
        manifest: multi_manifest.clone(),
        chunk: chunk(0, 1, 0x61),
        tables: Vec::new(),
    };
    assert!(matches!(
        execute_ledgered_data_chunk(&mut client, &first_request).await?,
        LedgeredDataChunkOutcome::InProgress {
            next_seq: 1,
            disposition: DataChunkDisposition::Applied { .. }
        }
    ));
    assert_eq!(ledger_counts(&client, multi_manifest.key).await?, (1, 1));
    let final_request = LedgeredDataChunkRequest {
        fence,
        manifest: multi_manifest.clone(),
        chunk: chunk(1, 2, 0x62),
        tables: Vec::new(),
    };
    assert!(matches!(
        execute_ledgered_data_chunk(&mut client, &final_request).await?,
        LedgeredDataChunkOutcome::Completed {
            next_seq: 2,
            disposition: DataChunkDisposition::Applied { .. },
            checkpoint: AdvanceOutcome::Advanced { previous_lsn }
        } if previous_lsn == exact_manifest.key.end_lsn
    ));
    assert_eq!(ledger_counts(&client, multi_manifest.key).await?, (0, 0));

    cleanup(&client, fence.pipeline_id).await;
    connection_task.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_concurrent_final_chunk_commits_exactly_once() -> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut first_client, first_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let first_connection_task = tokio::spawn(async move {
        if let Err(error) = first_connection.await {
            eprintln!("first concurrent Cloudberry connection ended: {error}");
        }
    });
    let (mut second_client, second_connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let second_connection_task = tokio::spawn(async move {
        if let Err(error) = second_connection.await {
            eprintln!("second concurrent Cloudberry connection ended: {error}");
        }
    });

    migrate_target_database(&mut first_client).await?;
    let fence = PipelineFence {
        pipeline_id: PipelineId::new(),
        topology_generation: 1,
        fencing_token: 1,
    };
    activate_pipeline_fence(&first_client, fence).await?;
    let mut manifest = manifest(fence);
    manifest.record_count = 1;
    manifest.manifest_digest = [0xC1; 32];
    let request = LedgeredDataChunkRequest {
        fence,
        manifest: manifest.clone(),
        chunk: chunk(0, 1, 0xC2),
        tables: Vec::new(),
    };
    let barrier = Arc::new(Barrier::new(2));

    let first_barrier = Arc::clone(&barrier);
    let first_request = request.clone();
    let first = async move {
        first_barrier.wait().await;
        let outcome = execute_ledgered_data_chunk(&mut first_client, &first_request).await;
        (first_client, outcome)
    };
    let second_barrier = Arc::clone(&barrier);
    let second = async move {
        second_barrier.wait().await;
        let outcome = execute_ledgered_data_chunk(&mut second_client, &request).await;
        (second_client, outcome)
    };
    let ((verifier, first_outcome), (_second_client, second_outcome)) = tokio::join!(first, second);
    let outcomes = [first_outcome?, second_outcome?];

    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| {
                matches!(
                    outcome,
                    LedgeredDataChunkOutcome::Completed {
                        next_seq: 1,
                        disposition: DataChunkDisposition::Applied { stats },
                        checkpoint: AdvanceOutcome::Inserted,
                    } if *stats == Default::default()
                )
            })
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| {
                matches!(
                    outcome,
                    LedgeredDataChunkOutcome::AlreadyCheckpointed { applied_lsn }
                        if *applied_lsn == manifest.key.end_lsn
                )
            })
            .count(),
        1
    );
    assert_eq!(
        load_node_checkpoint(&verifier, checkpoint(&manifest).key)
            .await?
            .expect("one concurrent final chunk must publish the checkpoint")
            .checkpoint,
        checkpoint(&manifest)
    );
    assert_eq!(ledger_counts(&verifier, manifest.key).await?, (0, 0));

    cleanup(&verifier, fence.pipeline_id).await;
    first_connection_task.abort();
    second_connection_task.abort();
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
