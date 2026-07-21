//! Replication loop and recoverable pipeline state machine.

use async_trait::async_trait;
use cloudberry_etl_core::{change::SourcePosition, pipeline::PipelinePhase};
use cloudberry_etl_source_postgres::wal::CommittedTransaction;
use thiserror::Error;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::{
    batch::{BatchError, Batcher, TransactionBatch},
    telemetry::PipelineTelemetryHandle,
};

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("source stream failed: {0}")]
    Source(String),
    #[error("target apply failed: {0}")]
    Target(String),
    #[error("schema orchestration is required before replication can continue: {0}")]
    SchemaBarrier(String),
    #[error(transparent)]
    Normalize(#[from] crate::normalize::NormalizeError),
    #[error("source acknowledgement failed: {0}")]
    Acknowledge(String),
    #[error("transaction spool cleanup failed: {0}")]
    SpoolCleanup(String),
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error("invalid pipeline transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: PipelinePhase,
        to: PipelinePhase,
    },
}

#[async_trait]
pub trait TransactionSource: Send {
    async fn next_transaction(&mut self) -> Result<Option<CommittedTransaction>, PipelineError>;
    async fn acknowledge(&mut self, position: &SourcePosition) -> Result<(), PipelineError>;
}

#[async_trait]
pub trait TransactionSink: Send + Sync {
    async fn apply(&self, batch: &TransactionBatch) -> Result<(), PipelineError>;
}

#[derive(Debug)]
pub struct PipelineMachine {
    phase: PipelinePhase,
}

impl Default for PipelineMachine {
    fn default() -> Self {
        Self {
            phase: PipelinePhase::Draft,
        }
    }
}

impl PipelineMachine {
    #[must_use]
    pub const fn phase(&self) -> PipelinePhase {
        self.phase
    }

    pub fn transition(&mut self, to: PipelinePhase) -> Result<(), PipelineError> {
        let from = self.phase;
        let valid = from == to
            || matches!(
                (from, to),
                (PipelinePhase::Draft, PipelinePhase::Validating)
                    | (PipelinePhase::Validating, PipelinePhase::Snapshotting)
                    | (PipelinePhase::Validating, PipelinePhase::Running)
                    | (PipelinePhase::Snapshotting, PipelinePhase::CatchingUp)
                    | (PipelinePhase::CatchingUp, PipelinePhase::Running)
                    | (PipelinePhase::Running, PipelinePhase::Paused)
                    | (PipelinePhase::Running, PipelinePhase::Degraded)
                    | (PipelinePhase::Degraded, PipelinePhase::Running)
                    | (PipelinePhase::Degraded, PipelinePhase::Paused)
                    | (PipelinePhase::Paused, PipelinePhase::Validating)
                    | (PipelinePhase::Failed, PipelinePhase::Validating)
                    | (_, PipelinePhase::Failed)
                    | (_, PipelinePhase::Stopped)
            );
        if !valid {
            return Err(PipelineError::InvalidTransition { from, to });
        }
        self.phase = to;
        Ok(())
    }
}

pub async fn replicate(
    source: &mut dyn TransactionSource,
    sink: &dyn TransactionSink,
    batcher: Batcher,
    cancellation: CancellationToken,
) -> Result<(), PipelineError> {
    replicate_with_telemetry(source, sink, batcher, cancellation, None).await
}

/// Runs replication while reporting durable progress to the runtime registry.
pub async fn replicate_with_telemetry(
    source: &mut dyn TransactionSource,
    sink: &dyn TransactionSink,
    mut batcher: Batcher,
    cancellation: CancellationToken,
    telemetry: Option<&PipelineTelemetryHandle>,
) -> Result<(), PipelineError> {
    let timer = sleep_until(Instant::now() + batcher.max_delay());
    tokio::pin!(timer);

    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                // Cancellation is also used when the lease/fence is lost.  Do not commit the
                // buffered batch with an ownership token that may no longer be current; the
                // target checkpoint remains authoritative and the next owner can replay it.
                return Ok(());
            }
            () = &mut timer, if !batcher.is_empty() => {
                if let Some(batch) = batcher.flush()
                    && !apply_and_ack_or_cancel(
                        source,
                        sink,
                        &batch,
                        telemetry,
                        &cancellation,
                    )
                    .await?
                {
                    return Ok(());
                }
                timer.as_mut().reset(Instant::now() + batcher.max_delay());
            }
            transaction = source.next_transaction() => {
                let Some(transaction) = transaction? else {
                    if let Some(batch) = batcher.flush()
                        && !apply_and_ack_or_cancel(
                            source,
                            sink,
                            &batch,
                            telemetry,
                            &cancellation,
                        )
                        .await?
                    {
                        return Ok(());
                    }
                    return Ok(());
                };
                if let Some(telemetry) = telemetry {
                    telemetry.transaction_received(
                        transaction.final_position.lsn,
                        transaction.commit_time,
                    );
                }
                if batcher.is_empty() {
                    timer.as_mut().reset(Instant::now() + batcher.max_delay());
                }
                if let Some(batch) = batcher.push(transaction)?
                    && !apply_and_ack_or_cancel(
                        source,
                        sink,
                        &batch,
                        telemetry,
                        &cancellation,
                    )
                    .await?
                {
                    return Ok(());
                }
            }
        }
    }
}

/// Apply one batch only while ownership is still valid. Dropping an in-flight target apply
/// future rolls back its database transaction; data and the checkpoint therefore advance
/// together, or remain unchanged for replay by the next owner.
async fn apply_and_ack_or_cancel(
    source: &mut dyn TransactionSource,
    sink: &dyn TransactionSink,
    batch: &TransactionBatch,
    telemetry: Option<&PipelineTelemetryHandle>,
    cancellation: &CancellationToken,
) -> Result<bool, PipelineError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Ok(false),
        result = apply_and_ack(source, sink, batch, telemetry) => result.map(|()| true),
    }
}

async fn apply_and_ack(
    source: &mut dyn TransactionSource,
    sink: &dyn TransactionSink,
    batch: &TransactionBatch,
    telemetry: Option<&PipelineTelemetryHandle>,
) -> Result<(), PipelineError> {
    sink.apply(batch).await?;
    let final_position = &batch.final_transaction().final_position;
    if let Some(telemetry) = telemetry {
        telemetry.applied(final_position.lsn);
    }
    for transaction in batch.transactions() {
        transaction
            .cleanup_spool()
            .map_err(|error| PipelineError::SpoolCleanup(error.to_string()))?;
    }
    source.acknowledge(final_position).await?;
    if let Some(telemetry) = telemetry {
        telemetry.acknowledged(final_position.lsn);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex, time::Duration};

    use chrono::Utc;
    use cloudberry_etl_core::{change::SourcePosition, lsn::PgLsn};

    use super::*;
    use crate::batch::BatchLimits;

    struct FakeSource {
        events: VecDeque<CommittedTransaction>,
        log: std::sync::Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TransactionSource for FakeSource {
        async fn next_transaction(
            &mut self,
        ) -> Result<Option<CommittedTransaction>, PipelineError> {
            Ok(self.events.pop_front())
        }

        async fn acknowledge(&mut self, position: &SourcePosition) -> Result<(), PipelineError> {
            self.log
                .lock()
                .unwrap()
                .push(format!("ack:{}", position.lsn));
            Ok(())
        }
    }

    struct FakeSink {
        log: std::sync::Arc<Mutex<Vec<String>>>,
        fail: bool,
    }

    #[async_trait]
    impl TransactionSink for FakeSink {
        async fn apply(&self, batch: &TransactionBatch) -> Result<(), PipelineError> {
            self.log.lock().unwrap().push(format!(
                "apply:{}",
                batch.final_transaction().final_position.lsn
            ));
            if self.fail {
                Err(PipelineError::Target("injected".into()))
            } else {
                Ok(())
            }
        }
    }

    fn transaction(lsn: u64) -> CommittedTransaction {
        cloudberry_etl_core::change::SourceTransaction {
            xid: lsn as u32,
            commit_time: Utc::now(),
            final_position: SourcePosition {
                node_id: 1,
                system_identifier: 9,
                timeline: 1,
                lsn: PgLsn::new(lsn),
            },
            changes: vec![],
        }
        .into()
    }

    fn batcher() -> Batcher {
        Batcher::new(BatchLimits {
            max_rows: 100,
            max_bytes: 1,
            max_delay: Duration::from_secs(30),
        })
        .unwrap()
    }

    #[tokio::test]
    async fn acknowledges_only_after_durable_apply() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut source = FakeSource {
            events: VecDeque::from([transaction(1), transaction(2)]),
            log: std::sync::Arc::clone(&log),
        };
        let sink = FakeSink {
            log: std::sync::Arc::clone(&log),
            fail: false,
        };

        replicate(&mut source, &sink, batcher(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(*log.lock().unwrap(), ["apply:0/00000002", "ack:0/00000002"]);
    }

    #[tokio::test]
    async fn target_failure_never_acknowledges_source() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut source = FakeSource {
            events: VecDeque::from([transaction(1)]),
            log: std::sync::Arc::clone(&log),
        };
        let sink = FakeSink {
            log: std::sync::Arc::clone(&log),
            fail: true,
        };

        assert!(
            replicate(&mut source, &sink, batcher(), CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(*log.lock().unwrap(), ["apply:0/00000001"]);
    }

    #[tokio::test]
    async fn cancellation_drops_buffered_batch_without_apply_or_ack() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut source = FakeSource {
            events: VecDeque::from([transaction(1)]),
            log: std::sync::Arc::clone(&log),
        };
        let sink = FakeSink {
            log: std::sync::Arc::clone(&log),
            fail: false,
        };
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        replicate(&mut source, &sink, batcher(), cancellation)
            .await
            .unwrap();
        assert!(log.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn telemetry_records_source_apply_and_ack_progress() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut source = FakeSource {
            events: VecDeque::from([transaction(7)]),
            log: std::sync::Arc::clone(&log),
        };
        let sink = FakeSink { log, fail: false };
        let telemetry = PipelineTelemetryHandle::new(cloudberry_etl_core::id::PipelineId::new());

        replicate_with_telemetry(
            &mut source,
            &sink,
            batcher(),
            CancellationToken::new(),
            Some(&telemetry),
        )
        .await
        .unwrap();

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.source_received_lsn, Some(PgLsn::new(7)));
        assert_eq!(snapshot.source_current_lsn, Some(PgLsn::new(7)));
        assert_eq!(snapshot.target_checkpoint_lsn, Some(PgLsn::new(7)));
        assert_eq!(snapshot.estimated_byte_lag, Some(0));
        assert!(snapshot.last_transaction_at.is_some());
        assert!(snapshot.last_apply_at.is_some());
        assert!(snapshot.last_ack_at.is_some());
    }
}
