//! PostgreSQL pgoutput adapter for the engine transaction source.

use std::{collections::VecDeque, time::Duration};

use async_trait::async_trait;
use cloudberry_etl_core::{change::SourcePosition, lsn::PgLsn};
use cloudberry_etl_source_postgres::{
    SourceResult,
    spool::ResourceState,
    wal::{
        AssembledEvent, DecodedMessage, ReplicationTransport, StandbyStatus, TransactionAssembler,
    },
};
use futures::StreamExt;
use tokio::time::{Instant, sleep};

use crate::pipeline::{PipelineError, TransactionSource};
use crate::telemetry::PipelineTelemetryHandle;

#[async_trait]
trait EventTransport: Send {
    async fn next_message(&mut self) -> Option<SourceResult<DecodedMessage>>;
    async fn send_status(&mut self, status: StandbyStatus) -> SourceResult<()>;
}

#[async_trait]
impl EventTransport for ReplicationTransport {
    async fn next_message(&mut self) -> Option<SourceResult<DecodedMessage>> {
        self.next().await
    }

    async fn send_status(&mut self, status: StandbyStatus) -> SourceResult<()> {
        ReplicationTransport::send_status(self, status).await
    }
}

/// Turns decoded pgoutput messages into committed engine transactions.
///
/// Standby feedback never advances from a server keepalive. It reports only the
/// latest position that the target has durably applied and that the engine has
/// passed to `acknowledge` after a successful sink transaction.
pub struct PgOutputTransactionSource {
    transport: Box<dyn EventTransport>,
    assembler: TransactionAssembler,
    durable_applied_lsn: PgLsn,
    delivered_commits: VecDeque<PgLsn>,
    telemetry: Option<PipelineTelemetryHandle>,
    pending_message: Option<DecodedMessage>,
    resource_retry_delay: Duration,
    resource_heartbeat_interval: Duration,
    next_resource_heartbeat: Option<Instant>,
    forced_resource_wait: Option<ResourceState>,
    resource_wait_active: bool,
    #[cfg(test)]
    injected_actual_capacity_failures: usize,
}

const RESOURCE_RETRY_DELAY: Duration = Duration::from_millis(250);
const RESOURCE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);

impl PgOutputTransactionSource {
    #[must_use]
    pub fn new(
        transport: ReplicationTransport,
        assembler: TransactionAssembler,
        durable_applied_lsn: PgLsn,
    ) -> Self {
        Self::from_transport(transport, assembler, durable_applied_lsn)
    }

    #[must_use]
    pub fn new_with_telemetry(
        transport: ReplicationTransport,
        assembler: TransactionAssembler,
        durable_applied_lsn: PgLsn,
        telemetry: PipelineTelemetryHandle,
    ) -> Self {
        Self::from_transport_with_telemetry(
            transport,
            assembler,
            durable_applied_lsn,
            Some(telemetry),
        )
    }

    #[must_use]
    pub const fn durable_applied_lsn(&self) -> PgLsn {
        self.durable_applied_lsn
    }

    fn from_transport(
        transport: impl EventTransport + 'static,
        assembler: TransactionAssembler,
        durable_applied_lsn: PgLsn,
    ) -> Self {
        Self::from_transport_with_telemetry(transport, assembler, durable_applied_lsn, None)
    }

    fn from_transport_with_telemetry(
        transport: impl EventTransport + 'static,
        assembler: TransactionAssembler,
        durable_applied_lsn: PgLsn,
        telemetry: Option<PipelineTelemetryHandle>,
    ) -> Self {
        Self {
            transport: Box::new(transport),
            assembler,
            durable_applied_lsn,
            delivered_commits: VecDeque::new(),
            telemetry,
            pending_message: None,
            resource_retry_delay: RESOURCE_RETRY_DELAY,
            resource_heartbeat_interval: RESOURCE_HEARTBEAT_INTERVAL,
            next_resource_heartbeat: None,
            forced_resource_wait: None,
            resource_wait_active: false,
            #[cfg(test)]
            injected_actual_capacity_failures: 0,
        }
    }

    async fn send_durable_status(&mut self) -> Result<(), PipelineError> {
        let lsn = self.durable_applied_lsn;
        self.transport
            .send_status(StandbyStatus {
                write_lsn: lsn,
                flush_lsn: lsn,
                apply_lsn: lsn,
                reply_requested: false,
            })
            .await
            .map_err(|error| PipelineError::Acknowledge(error.to_string()))
    }

    async fn wait_for_spool_capacity(&mut self) -> Result<bool, PipelineError> {
        loop {
            let state = match self.forced_resource_wait.take() {
                Some(state) => Some(state),
                None => {
                    let message = self
                        .pending_message
                        .as_ref()
                        .expect("resource preflight requires a pending decoded message");
                    self.assembler
                        .spool_resource_state(message)
                        .map_err(|error| PipelineError::Source(error.to_string()))?
                }
            };
            let Some(state) = state else {
                return Ok(false);
            };
            if let Some(telemetry) = &self.telemetry {
                telemetry.spool_usage_observed(state.used_bytes());
            }
            match state {
                ResourceState::Ready { .. } => {
                    return Ok(true);
                }
                ResourceState::Wait {
                    used_bytes,
                    disk_high_water_bytes,
                    free_bytes,
                    minimum_free_disk_bytes,
                } => {
                    self.resource_wait_active = true;
                    if let Some(telemetry) = &self.telemetry {
                        telemetry.mark_resource_wait(format!(
                            "spool capacity wait: used={used_bytes}, high={disk_high_water_bytes}, free={free_bytes:?}, minimum={minimum_free_disk_bytes}"
                        ));
                    }
                }
            }

            let now = Instant::now();
            let heartbeat_at = *self
                .next_resource_heartbeat
                .get_or_insert(now + self.resource_heartbeat_interval);
            sleep(
                self.resource_retry_delay
                    .min(heartbeat_at.saturating_duration_since(now)),
            )
            .await;
            if Instant::now() >= heartbeat_at {
                self.send_durable_status().await?;
                self.next_resource_heartbeat =
                    Some(Instant::now() + self.resource_heartbeat_interval);
            }
        }
    }

    #[cfg(test)]
    fn with_resource_wait_timing(mut self, retry: Duration, heartbeat: Duration) -> Self {
        self.resource_retry_delay = retry;
        self.resource_heartbeat_interval = heartbeat;
        self
    }

    #[cfg(test)]
    fn with_injected_actual_capacity_failures(mut self, failures: usize) -> Self {
        self.injected_actual_capacity_failures = failures;
        self
    }

    fn clear_resource_wait_after_write(&mut self) {
        if self.resource_wait_active {
            self.resource_wait_active = false;
            self.next_resource_heartbeat = None;
            if let Some(telemetry) = &self.telemetry {
                telemetry.clear_resource_wait();
            }
        }
    }

    fn validate_acknowledgement(&self, position: &SourcePosition) -> Result<(), PipelineError> {
        let identity = self.assembler.identity();
        if position.node_id != identity.node_id
            || position.system_identifier != identity.system_identifier
            || position.timeline != identity.timeline
        {
            return Err(PipelineError::Acknowledge(format!(
                "position identity does not match source node {}",
                identity.node_id
            )));
        }
        if position.lsn < self.durable_applied_lsn {
            return Err(PipelineError::Acknowledge(format!(
                "durable position regressed from {} to {}",
                self.durable_applied_lsn, position.lsn
            )));
        }
        if !self.delivered_commits.contains(&position.lsn) {
            return Err(PipelineError::Acknowledge(format!(
                "position {} was not emitted as a committed transaction",
                position.lsn
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl TransactionSource for PgOutputTransactionSource {
    async fn next_transaction(
        &mut self,
    ) -> Result<Option<cloudberry_etl_source_postgres::wal::CommittedTransaction>, PipelineError>
    {
        loop {
            if self.pending_message.is_none() {
                let Some(message) = self.transport.next_message().await else {
                    self.assembler
                        .finish()
                        .map_err(|error| PipelineError::Source(error.to_string()))?;
                    return Ok(None);
                };
                self.pending_message =
                    Some(message.map_err(|error| PipelineError::Source(error.to_string()))?);
            }
            let spool_write = self.wait_for_spool_capacity().await?;
            let message = self
                .pending_message
                .take()
                .expect("capacity wait retains the decoded message");
            let retry_message = spool_write.then(|| message.clone());
            #[cfg(test)]
            if spool_write && self.injected_actual_capacity_failures > 0 {
                self.injected_actual_capacity_failures -= 1;
                self.pending_message = retry_message;
                self.forced_resource_wait = Some(ResourceState::Wait {
                    used_bytes: 0,
                    disk_high_water_bytes: 1,
                    free_bytes: Some(0),
                    minimum_free_disk_bytes: 1,
                });
                continue;
            }
            let assembled = match self.assembler.push(message) {
                Ok(assembled) => assembled,
                Err(error) => {
                    if let Some(wait) = self.assembler.take_resource_wait() {
                        self.pending_message = Some(
                            retry_message
                                .expect("capacity failure must follow a spool-write preflight"),
                        );
                        self.forced_resource_wait = Some(wait);
                        continue;
                    }
                    return Err(PipelineError::Source(error.to_string()));
                }
            };
            self.clear_resource_wait_after_write();
            match assembled {
                Some(AssembledEvent::Transaction(committed)) => {
                    self.delivered_commits
                        .push_back(committed.transaction.final_position.lsn);
                    return Ok(Some(*committed));
                }
                Some(AssembledEvent::Keepalive {
                    wal_end,
                    reply_requested,
                    ..
                }) => {
                    if let Some(telemetry) = &self.telemetry {
                        telemetry.source_received(wal_end);
                    }
                    if reply_requested {
                        self.send_durable_status().await?;
                    }
                }
                None => {}
            }
        }
    }

    async fn acknowledge(&mut self, position: &SourcePosition) -> Result<(), PipelineError> {
        self.validate_acknowledgement(position)?;
        self.durable_applied_lsn = position.lsn;
        while self
            .delivered_commits
            .front()
            .is_some_and(|lsn| *lsn <= position.lsn)
        {
            self.delivered_commits.pop_front();
        }
        self.send_durable_status().await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        fs,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use chrono::{TimeZone, Utc};
    use cloudberry_etl_core::{
        change::{RowChange, TableChange, TransactionChange, Tuple},
        id::PipelineId,
    };
    use cloudberry_etl_source_postgres::{
        SourceError,
        spool::{SpoolIdentity, SpoolJournal, SpoolLimits},
        wal::{RelationEvent, SourceNodeIdentity, TransactionLimits, WireReplicaIdentityKind},
    };

    use super::*;
    use crate::telemetry::PipelineRuntimeState;

    struct FakeTransport {
        messages: VecDeque<SourceResult<DecodedMessage>>,
        statuses: Arc<Mutex<Vec<StandbyStatus>>>,
    }

    #[async_trait]
    impl EventTransport for FakeTransport {
        async fn next_message(&mut self) -> Option<SourceResult<DecodedMessage>> {
            self.messages.pop_front()
        }

        async fn send_status(&mut self, status: StandbyStatus) -> SourceResult<()> {
            self.statuses.lock().unwrap().push(status);
            Ok(())
        }
    }

    fn identity() -> SourceNodeIdentity {
        SourceNodeIdentity {
            node_id: 3,
            system_identifier: 99,
            timeline: 2,
        }
    }

    fn assembler() -> TransactionAssembler {
        TransactionAssembler::with_limits(identity(), TransactionLimits::default()).unwrap()
    }

    fn timestamp(seconds: i64) -> chrono::DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).single().unwrap()
    }

    fn keepalive(wal_end: u64, reply_requested: bool) -> DecodedMessage {
        DecodedMessage::Keepalive {
            wal_end: PgLsn::new(wal_end),
            timestamp_micros: 1,
            reply_requested,
        }
    }

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("pg2cb-source-wait-{}", uuid::Uuid::new_v4()))
    }

    fn change() -> TransactionChange {
        TransactionChange::Row(TableChange {
            relation_id: 42,
            generation: 1,
            change: RowChange::Insert {
                new: Tuple { cells: Vec::new() },
            },
        })
    }

    #[tokio::test]
    async fn keepalive_and_ack_report_only_the_latest_durable_position() {
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let transport = FakeTransport {
            messages: VecDeque::from([
                Ok(keepalive(1_000, true)),
                Ok(DecodedMessage::Begin {
                    final_lsn: PgLsn::new(10),
                    timestamp: timestamp(10),
                    xid: 7,
                }),
                Ok(DecodedMessage::Commit {
                    commit_lsn: PgLsn::new(10),
                    end_lsn: PgLsn::new(11),
                    timestamp: timestamp(11),
                    flags: 0,
                }),
                Ok(keepalive(2_000, false)),
                Ok(keepalive(3_000, true)),
            ]),
            statuses: Arc::clone(&statuses),
        };
        let mut source =
            PgOutputTransactionSource::from_transport(transport, assembler(), PgLsn::new(5));

        let transaction = source.next_transaction().await.unwrap().unwrap();
        assert_eq!(transaction.final_position.lsn, PgLsn::new(11));
        source
            .acknowledge(&transaction.final_position)
            .await
            .unwrap();
        assert!(source.next_transaction().await.unwrap().is_none());

        let statuses = statuses.lock().unwrap();
        assert_eq!(statuses.len(), 3);
        for (status, expected) in statuses.iter().zip([5, 11, 11]) {
            assert_eq!(status.write_lsn, PgLsn::new(expected));
            assert_eq!(status.flush_lsn, PgLsn::new(expected));
            assert_eq!(status.apply_lsn, PgLsn::new(expected));
            assert!(!status.reply_requested);
        }
    }

    #[tokio::test]
    async fn rejects_acknowledgement_regression_and_wrong_identity() {
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let transport = FakeTransport {
            messages: VecDeque::new(),
            statuses: Arc::clone(&statuses),
        };
        let mut source =
            PgOutputTransactionSource::from_transport(transport, assembler(), PgLsn::new(10));
        let mut position = identity().position(PgLsn::new(9));
        assert!(matches!(
            source.acknowledge(&position).await,
            Err(PipelineError::Acknowledge(_))
        ));
        position.lsn = PgLsn::new(11);
        position.timeline = 3;
        assert!(matches!(
            source.acknowledge(&position).await,
            Err(PipelineError::Acknowledge(_))
        ));
        assert!(statuses.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn capacity_wait_retains_message_heartbeats_and_resumes_without_ack() {
        let root = temp_root();
        let limits = SpoolLimits {
            memory_high_water_bytes: 1,
            segment_target_bytes: 128,
            disk_high_water_bytes: 512,
            minimum_free_disk_bytes: 1,
        };
        let journal = SpoolJournal::open(
            &root,
            SpoolIdentity {
                pipeline_id: PipelineId::new(),
                topology_generation: 1,
                node_id: identity().node_id,
                system_identifier: identity().system_identifier,
                timeline: identity().timeline,
            },
            limits,
        )
        .unwrap();
        let mut old_writer = journal.begin(1, PgLsn::new(1)).unwrap();
        for _ in 0..64 {
            old_writer.append(&change()).unwrap();
        }
        let old = old_writer.finish(PgLsn::new(2), PgLsn::new(3)).unwrap();
        assert!(journal.used_bytes().unwrap() > limits.disk_high_water_bytes);

        let assembler = TransactionAssembler::with_spool(
            identity(),
            TransactionLimits {
                max_changes: 1,
                max_bytes: usize::MAX,
            },
            journal,
        )
        .unwrap();
        let statuses = Arc::new(Mutex::new(Vec::new()));
        let transport = FakeTransport {
            messages: VecDeque::from([
                Ok(DecodedMessage::Relation(RelationEvent {
                    relation_id: 42,
                    namespace: "public".to_owned(),
                    name: "items".to_owned(),
                    generation: 1,
                    replica_identity: WireReplicaIdentityKind::Default,
                    columns: Vec::new(),
                })),
                Ok(DecodedMessage::Begin {
                    final_lsn: PgLsn::new(10),
                    timestamp: timestamp(10),
                    xid: 7,
                }),
                Ok(DecodedMessage::Insert {
                    relation_id: 42,
                    generation: 1,
                    new: Tuple { cells: Vec::new() },
                }),
                Ok(DecodedMessage::Insert {
                    relation_id: 42,
                    generation: 1,
                    new: Tuple { cells: Vec::new() },
                }),
                Ok(DecodedMessage::Commit {
                    commit_lsn: PgLsn::new(10),
                    end_lsn: PgLsn::new(11),
                    timestamp: timestamp(11),
                    flags: 0,
                }),
            ]),
            statuses: Arc::clone(&statuses),
        };
        let telemetry = PipelineTelemetryHandle::new(PipelineId::new());
        telemetry.mark_running();
        let mut source = PgOutputTransactionSource::from_transport_with_telemetry(
            transport,
            assembler,
            PgLsn::new(5),
            Some(telemetry.clone()),
        )
        .with_resource_wait_timing(Duration::from_secs(1), Duration::from_secs(10))
        .with_injected_actual_capacity_failures(12);

        assert!(
            tokio::time::timeout(Duration::from_secs(25), source.next_transaction())
                .await
                .is_err()
        );
        assert_eq!(
            telemetry.snapshot().state,
            PipelineRuntimeState::ResourceWait
        );
        assert!(statuses.lock().unwrap().len() >= 2);
        assert!(statuses.lock().unwrap().iter().all(|status| {
            status.write_lsn == PgLsn::new(5)
                && status.flush_lsn == PgLsn::new(5)
                && status.apply_lsn == PgLsn::new(5)
        }));

        let heartbeats_before_retry = statuses.lock().unwrap().len();
        old.remove().unwrap();
        let committed = tokio::time::timeout(Duration::from_secs(20), source.next_transaction())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(committed.final_position.lsn, PgLsn::new(11));
        assert_eq!(telemetry.snapshot().state, PipelineRuntimeState::Running);
        assert!(statuses.lock().unwrap().len() > heartbeats_before_retry);
        assert!(statuses.lock().unwrap().iter().all(|status| {
            status.write_lsn == PgLsn::new(5)
                && status.flush_lsn == PgLsn::new(5)
                && status.apply_lsn == PgLsn::new(5)
        }));
        committed.cleanup_spool().unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn reports_an_open_transaction_when_the_stream_ends() {
        let transport = FakeTransport {
            messages: VecDeque::from([Ok(DecodedMessage::Begin {
                final_lsn: PgLsn::new(10),
                timestamp: timestamp(10),
                xid: 7,
            })]),
            statuses: Arc::new(Mutex::new(Vec::new())),
        };
        let mut source =
            PgOutputTransactionSource::from_transport(transport, assembler(), PgLsn::new(0));
        assert!(matches!(
            source.next_transaction().await,
            Err(PipelineError::Source(_))
        ));
    }

    #[tokio::test]
    async fn maps_transport_errors_without_treating_them_as_end_of_stream() {
        let transport = FakeTransport {
            messages: VecDeque::from([Err(SourceError::ReplicationProtocol(
                "injected".to_owned(),
            ))]),
            statuses: Arc::new(Mutex::new(Vec::new())),
        };
        let mut source =
            PgOutputTransactionSource::from_transport(transport, assembler(), PgLsn::new(0));
        assert!(matches!(
            source.next_transaction().await,
            Err(PipelineError::Source(message)) if message.contains("injected")
        ));
    }
}
