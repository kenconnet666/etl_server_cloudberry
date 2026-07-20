//! PostgreSQL pgoutput adapter for the engine transaction source.

use std::collections::VecDeque;

use async_trait::async_trait;
use cloudberry_etl_core::{change::SourcePosition, lsn::PgLsn};
use cloudberry_etl_source_postgres::{
    SourceResult,
    wal::{
        AssembledEvent, DecodedMessage, ReplicationTransport, StandbyStatus, TransactionAssembler,
    },
};
use futures::StreamExt;

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
}

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
    ) -> Result<Option<cloudberry_etl_core::change::SourceTransaction>, PipelineError> {
        loop {
            let Some(message) = self.transport.next_message().await else {
                self.assembler
                    .finish()
                    .map_err(|error| PipelineError::Source(error.to_string()))?;
                return Ok(None);
            };
            let message = message.map_err(|error| PipelineError::Source(error.to_string()))?;
            match self
                .assembler
                .push(message)
                .map_err(|error| PipelineError::Source(error.to_string()))?
            {
                Some(AssembledEvent::Transaction(committed)) => {
                    self.delivered_commits
                        .push_back(committed.transaction.final_position.lsn);
                    return Ok(Some(committed.transaction));
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
        sync::{Arc, Mutex},
    };

    use chrono::{TimeZone, Utc};
    use cloudberry_etl_source_postgres::{
        SourceError,
        wal::{SourceNodeIdentity, TransactionLimits},
    };

    use super::*;

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
