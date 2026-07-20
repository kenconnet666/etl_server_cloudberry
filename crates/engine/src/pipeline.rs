//! Replication loop and recoverable pipeline state machine.

use async_trait::async_trait;
use cloudberry_etl_core::{
    change::{SourcePosition, SourceTransaction},
    pipeline::PipelinePhase,
};
use thiserror::Error;
use tokio::time::{Instant, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::batch::{BatchError, Batcher, TransactionBatch};

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("source stream failed: {0}")]
    Source(String),
    #[error("target apply failed: {0}")]
    Target(String),
    #[error("source acknowledgement failed: {0}")]
    Acknowledge(String),
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
    async fn next_transaction(&mut self) -> Result<Option<SourceTransaction>, PipelineError>;
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
    mut batcher: Batcher,
    cancellation: CancellationToken,
) -> Result<(), PipelineError> {
    let timer = sleep_until(Instant::now() + batcher.max_delay());
    tokio::pin!(timer);

    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => {
                if let Some(batch) = batcher.flush() {
                    apply_and_ack(source, sink, &batch).await?;
                }
                return Ok(());
            }
            () = &mut timer, if !batcher.is_empty() => {
                if let Some(batch) = batcher.flush() {
                    apply_and_ack(source, sink, &batch).await?;
                }
                timer.as_mut().reset(Instant::now() + batcher.max_delay());
            }
            transaction = source.next_transaction() => {
                let Some(transaction) = transaction? else {
                    if let Some(batch) = batcher.flush() {
                        apply_and_ack(source, sink, &batch).await?;
                    }
                    return Ok(());
                };
                if batcher.is_empty() {
                    timer.as_mut().reset(Instant::now() + batcher.max_delay());
                }
                if let Some(batch) = batcher.push(transaction)? {
                    apply_and_ack(source, sink, &batch).await?;
                }
            }
        }
    }
}

async fn apply_and_ack(
    source: &mut dyn TransactionSource,
    sink: &dyn TransactionSink,
    batch: &TransactionBatch,
) -> Result<(), PipelineError> {
    sink.apply(batch).await?;
    source
        .acknowledge(&batch.final_transaction().final_position)
        .await
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, sync::Mutex, time::Duration};

    use chrono::Utc;
    use cloudberry_etl_core::{change::SourcePosition, lsn::PgLsn};

    use super::*;
    use crate::batch::BatchLimits;

    struct FakeSource {
        events: VecDeque<SourceTransaction>,
        log: std::sync::Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TransactionSource for FakeSource {
        async fn next_transaction(&mut self) -> Result<Option<SourceTransaction>, PipelineError> {
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

    fn transaction(lsn: u64) -> SourceTransaction {
        SourceTransaction {
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
            log: log.clone(),
        };
        let sink = FakeSink {
            log: log.clone(),
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
            log: log.clone(),
        };
        let sink = FakeSink {
            log: log.clone(),
            fail: true,
        };

        assert!(
            replicate(&mut source, &sink, batcher(), CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(*log.lock().unwrap(), ["apply:0/00000001"]);
    }
}
