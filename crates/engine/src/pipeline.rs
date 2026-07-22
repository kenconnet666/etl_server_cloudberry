//! Replication loop and recoverable pipeline state machine.

use std::time::Duration;

use async_trait::async_trait;
use cloudberry_etl_core::{change::SourcePosition, pipeline::PipelinePhase};
use cloudberry_etl_source_postgres::wal::CommittedTransaction;
use thiserror::Error;

use tokio::time::{Instant, MissedTickBehavior, interval_at, sleep_until};
use tokio_util::sync::CancellationToken;

use crate::{
    batch::{BatchError, Batcher, TransactionBatch},
    telemetry::PipelineTelemetryHandle,
};

/// Observable lifecycle of a one-shot replication stop boundary.
///
/// `Draining` means the source consumed the first complete transaction beyond the boundary but
/// deliberately did not deliver it. `Reached` is stronger: every delivered transaction at or
/// before the boundary has also been durably applied and acknowledged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopBoundaryStatus {
    Unset,
    Armed {
        boundary: cloudberry_etl_core::lsn::PgLsn,
    },
    Draining {
        boundary: cloudberry_etl_core::lsn::PgLsn,
    },
    Reached {
        boundary: cloudberry_etl_core::lsn::PgLsn,
        durable_lsn: cloudberry_etl_core::lsn::PgLsn,
    },
}

#[derive(Debug)]
struct StopBoundaryState {
    status: StopBoundaryStatus,
    source_attached: bool,
    delivered_lsn: Option<cloudberry_etl_core::lsn::PgLsn>,
    durable_lsn: Option<cloudberry_etl_core::lsn::PgLsn>,
}

/// Cloneable control and observation handle for a one-shot replication stop boundary.
///
/// The handle may be retained by the runtime while the pipeline owns the source. Once armed, a
/// boundary cannot be moved or cleared; construct a fresh handle for another reconciliation run.
#[derive(Debug, Clone)]
pub struct ReplicationStopBoundary {
    state: std::sync::Arc<std::sync::Mutex<StopBoundaryState>>,
}

impl Default for ReplicationStopBoundary {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicationStopBoundary {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: std::sync::Arc::new(std::sync::Mutex::new(StopBoundaryState {
                status: StopBoundaryStatus::Unset,
                source_attached: false,
                delivered_lsn: None,
                durable_lsn: None,
            })),
        }
    }

    /// Arm this handle exactly once. Repeating the same request is idempotent.
    pub fn set(&self, boundary: cloudberry_etl_core::lsn::PgLsn) -> Result<(), PipelineError> {
        if boundary == cloudberry_etl_core::lsn::PgLsn::ZERO {
            return Err(PipelineError::StopBoundary(
                "stop boundary must be greater than zero".to_owned(),
            ));
        }
        let mut state = self.lock();
        match state.status {
            StopBoundaryStatus::Unset => {}
            StopBoundaryStatus::Armed { boundary: current }
            | StopBoundaryStatus::Draining { boundary: current }
            | StopBoundaryStatus::Reached {
                boundary: current, ..
            } if current == boundary => return Ok(()),
            StopBoundaryStatus::Armed { boundary: current }
            | StopBoundaryStatus::Draining { boundary: current }
            | StopBoundaryStatus::Reached {
                boundary: current, ..
            } => {
                return Err(PipelineError::StopBoundary(format!(
                    "stop boundary is already fixed at {current}; cannot reset it to {boundary}"
                )));
            }
        }
        if let Some(progress) = state.delivered_lsn
            && boundary < progress
        {
            return Err(PipelineError::StopBoundary(format!(
                "stop boundary {boundary} precedes already delivered position {progress}"
            )));
        }
        state.status = StopBoundaryStatus::Armed { boundary };
        Ok(())
    }

    #[must_use]
    pub fn status(&self) -> StopBoundaryStatus {
        self.lock().status
    }

    pub(crate) fn attach_source(
        &self,
        durable_lsn: cloudberry_etl_core::lsn::PgLsn,
    ) -> Result<(), PipelineError> {
        let mut state = self.lock();
        if state.source_attached {
            return Err(PipelineError::StopBoundary(
                "stop boundary is already attached to a source".to_owned(),
            ));
        }
        if let StopBoundaryStatus::Armed { boundary } = state.status
            && boundary < durable_lsn
        {
            return Err(PipelineError::StopBoundary(format!(
                "stop boundary {boundary} precedes durable source position {durable_lsn}"
            )));
        }
        state.source_attached = true;
        state.delivered_lsn = Some(durable_lsn);
        state.durable_lsn = Some(durable_lsn);
        Ok(())
    }

    /// Returns true when this complete transaction is the first one beyond the boundary.
    pub(crate) fn observe_transaction(
        &self,
        transaction_lsn: cloudberry_etl_core::lsn::PgLsn,
    ) -> Result<bool, PipelineError> {
        let mut state = self.lock();
        if !state.source_attached {
            return Err(PipelineError::StopBoundary(
                "stop boundary has no attached source".to_owned(),
            ));
        }
        if transaction_lsn == cloudberry_etl_core::lsn::PgLsn::ZERO {
            return Err(PipelineError::StopBoundary(
                "transaction stop-boundary position must be greater than zero".to_owned(),
            ));
        }
        if let Some(previous) = state.delivered_lsn
            && transaction_lsn <= previous
        {
            return Err(PipelineError::StopBoundary(format!(
                "transaction stop-boundary position did not advance beyond {previous}: got {transaction_lsn}"
            )));
        }
        match state.status {
            StopBoundaryStatus::Armed { boundary } if transaction_lsn > boundary => {
                state.status = StopBoundaryStatus::Draining { boundary };
                Ok(true)
            }
            StopBoundaryStatus::Draining { .. } | StopBoundaryStatus::Reached { .. } => Ok(true),
            StopBoundaryStatus::Unset | StopBoundaryStatus::Armed { .. } => {
                state.delivered_lsn = Some(transaction_lsn);
                Ok(false)
            }
        }
    }

    pub(crate) fn record_durable(
        &self,
        durable_lsn: cloudberry_etl_core::lsn::PgLsn,
    ) -> Result<(), PipelineError> {
        let mut state = self.lock();
        let previous = state.durable_lsn.ok_or_else(|| {
            PipelineError::StopBoundary("stop boundary has no attached source".to_owned())
        })?;
        if durable_lsn < previous {
            return Err(PipelineError::StopBoundary(format!(
                "stop boundary durable position regressed from {previous} to {durable_lsn}"
            )));
        }
        if state
            .delivered_lsn
            .is_none_or(|delivered| durable_lsn > delivered)
        {
            return Err(PipelineError::StopBoundary(format!(
                "durable position {durable_lsn} was not delivered by the source"
            )));
        }
        if let StopBoundaryStatus::Armed { boundary } | StopBoundaryStatus::Draining { boundary } =
            state.status
            && durable_lsn > boundary
        {
            return Err(PipelineError::StopBoundary(format!(
                "durable position {durable_lsn} passed stop boundary {boundary}"
            )));
        }
        state.durable_lsn = Some(durable_lsn);
        Ok(())
    }

    pub(crate) fn mark_reached(&self) -> Result<(), PipelineError> {
        let mut state = self.lock();
        let StopBoundaryStatus::Draining { boundary } = state.status else {
            return Ok(());
        };
        let durable_lsn = state.durable_lsn.ok_or_else(|| {
            PipelineError::StopBoundary("stop boundary has no durable source position".to_owned())
        })?;
        if state.delivered_lsn != Some(durable_lsn) {
            return Err(PipelineError::StopBoundary(format!(
                "cannot finish stop boundary {boundary}: delivered position {:?} is not durable at {durable_lsn}",
                state.delivered_lsn
            )));
        }
        state.status = StopBoundaryStatus::Reached {
            boundary,
            durable_lsn,
        };
        Ok(())
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StopBoundaryState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaEventKey {
    pub source_lsn: cloudberry_etl_core::lsn::PgLsn,
    pub source_xid: u64,
}

const SOURCE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("source stream failed: {0}")]
    Source(String),
    #[error("target apply failed: {0}")]
    Target(String),
    #[error("schema orchestration is required before replication can continue: {reason}")]
    SchemaBarrier {
        reason: String,
        /// The source command tag that raised the barrier, when it originated
        /// from a DDL event (as opposed to TRUNCATE). Lets the runtime record a
        /// structured schema event before requesting a rebuild.
        command_tag: Option<String>,
        /// Durable target event written before this barrier was returned. The runtime uses this
        /// identity to close the current fallback as failed before requesting a rebuild.
        schema_event: Option<SchemaEventKey>,
    },
    #[error(transparent)]
    Normalize(#[from] crate::normalize::NormalizeError),
    #[error("source acknowledgement failed: {0}")]
    Acknowledge(String),
    #[error("transaction spool cleanup failed: {0}")]
    SpoolCleanup(String),
    #[error("replication stop boundary failed: {0}")]
    StopBoundary(String),
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

    /// Keep the source session alive without advancing its durable position.
    async fn heartbeat(&mut self) -> Result<(), PipelineError>;

    /// Promote an observed stop boundary after every buffered transaction is durable and ACKed.
    fn finish_stop_boundary(&mut self) -> Result<(), PipelineError> {
        Ok(())
    }
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
                    source.finish_stop_boundary()?;
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
    apply_and_ack_or_cancel_with_heartbeat(
        source,
        sink,
        batch,
        telemetry,
        cancellation,
        SOURCE_HEARTBEAT_INTERVAL,
    )
    .await
}

async fn apply_and_ack_or_cancel_with_heartbeat(
    source: &mut dyn TransactionSource,
    sink: &dyn TransactionSink,
    batch: &TransactionBatch,
    telemetry: Option<&PipelineTelemetryHandle>,
    cancellation: &CancellationToken,
    heartbeat_interval: Duration,
) -> Result<bool, PipelineError> {
    let apply = sink.apply(batch);
    tokio::pin!(apply);
    let mut heartbeat = interval_at(Instant::now() + heartbeat_interval, heartbeat_interval);
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            biased;
            () = cancellation.cancelled() => return Ok(false),
            result = &mut apply => {
                result?;
                break;
            }
            _ = heartbeat.tick() => source.heartbeat().await?,
        }
    }
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
    Ok(true)
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

        async fn heartbeat(&mut self) -> Result<(), PipelineError> {
            self.log.lock().unwrap().push("heartbeat".to_owned());
            Ok(())
        }
    }

    struct BoundarySource {
        events: VecDeque<CommittedTransaction>,
        log: std::sync::Arc<Mutex<Vec<String>>>,
        stop_boundary: ReplicationStopBoundary,
    }

    impl BoundarySource {
        fn new(
            events: impl IntoIterator<Item = CommittedTransaction>,
            durable_lsn: u64,
            boundary_lsn: u64,
            log: std::sync::Arc<Mutex<Vec<String>>>,
        ) -> (Self, ReplicationStopBoundary) {
            let stop_boundary = ReplicationStopBoundary::new();
            stop_boundary.set(PgLsn::new(boundary_lsn)).unwrap();
            stop_boundary
                .attach_source(PgLsn::new(durable_lsn))
                .unwrap();
            (
                Self {
                    events: events.into_iter().collect(),
                    log,
                    stop_boundary: stop_boundary.clone(),
                },
                stop_boundary,
            )
        }
    }

    #[async_trait]
    impl TransactionSource for BoundarySource {
        async fn next_transaction(
            &mut self,
        ) -> Result<Option<CommittedTransaction>, PipelineError> {
            let Some(transaction) = self.events.pop_front() else {
                return Ok(None);
            };
            if self
                .stop_boundary
                .observe_transaction(transaction.final_position.lsn)?
            {
                return Ok(None);
            }
            Ok(Some(transaction))
        }

        async fn acknowledge(&mut self, position: &SourcePosition) -> Result<(), PipelineError> {
            self.log
                .lock()
                .unwrap()
                .push(format!("ack:{}", position.lsn));
            self.stop_boundary.record_durable(position.lsn)
        }

        async fn heartbeat(&mut self) -> Result<(), PipelineError> {
            Ok(())
        }

        fn finish_stop_boundary(&mut self) -> Result<(), PipelineError> {
            self.stop_boundary.mark_reached()
        }
    }

    struct FakeSink {
        log: std::sync::Arc<Mutex<Vec<String>>>,
        fail: bool,
    }

    struct SlowSink {
        delay: Duration,
    }

    struct CancellingSink {
        cancellation: CancellationToken,
        log: std::sync::Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl TransactionSink for SlowSink {
        async fn apply(&self, _batch: &TransactionBatch) -> Result<(), PipelineError> {
            tokio::time::sleep(self.delay).await;
            Ok(())
        }
    }

    #[async_trait]
    impl TransactionSink for CancellingSink {
        async fn apply(&self, batch: &TransactionBatch) -> Result<(), PipelineError> {
            self.log.lock().unwrap().push(format!(
                "apply:{}",
                batch.final_transaction().final_position.lsn
            ));
            self.cancellation.cancel();
            std::future::pending().await
        }
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

    #[test]
    fn stop_boundary_is_nonzero_one_shot_and_cannot_move_behind_progress() {
        let stop_boundary = ReplicationStopBoundary::new();
        assert!(stop_boundary.set(PgLsn::ZERO).is_err());
        stop_boundary.attach_source(PgLsn::new(5)).unwrap();
        assert!(!stop_boundary.observe_transaction(PgLsn::new(7)).unwrap());
        assert!(stop_boundary.set(PgLsn::new(6)).is_err());
        stop_boundary.set(PgLsn::new(7)).unwrap();
        stop_boundary.set(PgLsn::new(7)).unwrap();
        assert!(stop_boundary.set(PgLsn::new(8)).is_err());
        assert_eq!(
            stop_boundary.status(),
            StopBoundaryStatus::Armed {
                boundary: PgLsn::new(7)
            }
        );

        let stale_boundary = ReplicationStopBoundary::new();
        stale_boundary.set(PgLsn::new(4)).unwrap();
        assert!(stale_boundary.attach_source(PgLsn::new(5)).is_err());
    }

    #[test]
    fn stop_boundary_rejects_zero_duplicate_and_regressing_transaction_positions() {
        let stop_boundary = ReplicationStopBoundary::new();
        stop_boundary.attach_source(PgLsn::ZERO).unwrap();

        assert!(stop_boundary.observe_transaction(PgLsn::ZERO).is_err());
        assert!(!stop_boundary.observe_transaction(PgLsn::new(2)).unwrap());
        assert!(stop_boundary.observe_transaction(PgLsn::new(2)).is_err());
        assert!(stop_boundary.observe_transaction(PgLsn::new(1)).is_err());
        assert!(!stop_boundary.observe_transaction(PgLsn::new(3)).unwrap());
    }

    #[tokio::test]
    async fn boundary_end_flushes_all_complete_transactions_before_reporting_reached() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let (mut source, stop_boundary) = BoundarySource::new(
            [transaction(1), transaction(2), transaction(3)],
            0,
            2,
            std::sync::Arc::clone(&log),
        );
        let sink = FakeSink {
            log: std::sync::Arc::clone(&log),
            fail: false,
        };

        replicate(&mut source, &sink, batcher(), CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(*log.lock().unwrap(), ["apply:0/00000002", "ack:0/00000002"]);
        assert_eq!(
            stop_boundary.status(),
            StopBoundaryStatus::Reached {
                boundary: PgLsn::new(2),
                durable_lsn: PgLsn::new(2),
            }
        );
    }

    #[tokio::test]
    async fn boundary_before_the_first_transaction_reports_the_existing_durable_lsn() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let (mut source, stop_boundary) =
            BoundarySource::new([transaction(3)], 1, 2, std::sync::Arc::clone(&log));
        let sink = FakeSink {
            log: std::sync::Arc::clone(&log),
            fail: false,
        };

        replicate(&mut source, &sink, batcher(), CancellationToken::new())
            .await
            .unwrap();

        assert!(log.lock().unwrap().is_empty());
        assert_eq!(
            stop_boundary.status(),
            StopBoundaryStatus::Reached {
                boundary: PgLsn::new(2),
                durable_lsn: PgLsn::new(1),
            }
        );
    }

    #[tokio::test]
    async fn target_failure_after_boundary_observation_never_reports_reached() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let (mut source, stop_boundary) = BoundarySource::new(
            [transaction(1), transaction(3)],
            0,
            2,
            std::sync::Arc::clone(&log),
        );
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
        assert_eq!(
            stop_boundary.status(),
            StopBoundaryStatus::Draining {
                boundary: PgLsn::new(2)
            }
        );
    }

    #[tokio::test]
    async fn cancellation_during_boundary_flush_never_reports_reached() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let (mut source, stop_boundary) = BoundarySource::new(
            [transaction(1), transaction(3)],
            0,
            2,
            std::sync::Arc::clone(&log),
        );
        let cancellation = CancellationToken::new();
        let sink = CancellingSink {
            cancellation: cancellation.clone(),
            log: std::sync::Arc::clone(&log),
        };

        replicate(&mut source, &sink, batcher(), cancellation)
            .await
            .unwrap();

        assert_eq!(*log.lock().unwrap(), ["apply:0/00000001"]);
        assert_eq!(
            stop_boundary.status(),
            StopBoundaryStatus::Draining {
                boundary: PgLsn::new(2)
            }
        );
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

    #[tokio::test(start_paused = true)]
    async fn long_target_apply_keeps_source_alive_without_advancing_ack() {
        let log = std::sync::Arc::new(Mutex::new(Vec::new()));
        let mut source = FakeSource {
            events: VecDeque::from([transaction(1)]),
            log: std::sync::Arc::clone(&log),
        };
        let sink = SlowSink {
            delay: Duration::from_secs(25),
        };

        replicate(&mut source, &sink, batcher(), CancellationToken::new())
            .await
            .unwrap();

        let log = log.lock().unwrap();
        assert_eq!(
            log.iter()
                .filter(|entry| entry.as_str() == "heartbeat")
                .count(),
            2
        );
        assert_eq!(log.last().map(String::as_str), Some("ack:0/00000001"));
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
