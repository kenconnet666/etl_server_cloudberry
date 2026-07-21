//! Low-cardinality, in-process pipeline runtime telemetry.
//!
//! The handle is deliberately independent from a job task.  A reconciler can
//! restart a task while the same handle retains the last useful position,
//! restart count, and error for the pipeline.  No connection strings, table
//! names, or other user supplied values are used as metric labels.

use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, Utc};
use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn, pipeline::PipelinePhase};
use serde::Serialize;

/// Coarse task lifecycle state, distinct from the replication phase.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelineRuntimeState {
    Starting,
    Running,
    ResourceWait,
    Stopped,
    Failed,
    Degraded,
}

impl PipelineRuntimeState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::ResourceWait => "resource_wait",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Degraded => "degraded",
        }
    }
}

/// Serializable snapshot exposed by the API and used to render metrics.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineRuntimeSnapshot {
    pub pipeline_id: PipelineId,
    pub phase: PipelinePhase,
    pub state: PipelineRuntimeState,
    pub source_received_lsn: Option<PgLsn>,
    pub source_current_lsn: Option<PgLsn>,
    pub target_checkpoint_lsn: Option<PgLsn>,
    /// LSN distance, which is a useful byte-oriented estimate of WAL backlog.
    pub estimated_byte_lag: Option<u64>,
    /// Current durable transaction spool usage, when a runtime reports it.
    pub spool_bytes: Option<u64>,
    /// Human-readable reason for a recoverable resource wait. Never used as a metric label.
    pub resource_wait_reason: Option<String>,
    pub slot_retained_wal_bytes: Option<u64>,
    pub slot_safe_wal_bytes: Option<u64>,
    pub wal_retention_warning: bool,
    pub last_transaction_at: Option<DateTime<Utc>>,
    pub last_apply_at: Option<DateTime<Utc>>,
    pub last_ack_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub stopped_at: Option<DateTime<Utc>>,
    pub restart_count: u64,
    pub last_error: Option<String>,
}

impl PipelineRuntimeSnapshot {
    #[must_use]
    pub const fn phase_name(&self) -> &'static str {
        match self.phase {
            PipelinePhase::Draft => "draft",
            PipelinePhase::Validating => "validating",
            PipelinePhase::Snapshotting => "snapshotting",
            PipelinePhase::CatchingUp => "catching_up",
            PipelinePhase::Running => "running",
            PipelinePhase::Paused => "paused",
            PipelinePhase::Degraded => "degraded",
            PipelinePhase::Failed => "failed",
            PipelinePhase::Stopped => "stopped",
        }
    }
}

#[derive(Debug)]
struct PipelineRuntime {
    snapshot: PipelineRuntimeSnapshot,
}

/// Thread-safe reporter owned by a job and its supervisor.
#[derive(Clone, Debug)]
pub struct PipelineTelemetryHandle {
    inner: Arc<Mutex<PipelineRuntime>>,
}

impl PipelineTelemetryHandle {
    #[must_use]
    pub fn new(pipeline_id: PipelineId) -> Self {
        Self {
            inner: Arc::new(Mutex::new(PipelineRuntime {
                snapshot: PipelineRuntimeSnapshot {
                    pipeline_id,
                    phase: PipelinePhase::Draft,
                    state: PipelineRuntimeState::Stopped,
                    source_received_lsn: None,
                    source_current_lsn: None,
                    target_checkpoint_lsn: None,
                    estimated_byte_lag: None,
                    spool_bytes: None,
                    resource_wait_reason: None,
                    slot_retained_wal_bytes: None,
                    slot_safe_wal_bytes: None,
                    wal_retention_warning: false,
                    last_transaction_at: None,
                    last_apply_at: None,
                    last_ack_at: None,
                    started_at: None,
                    stopped_at: None,
                    restart_count: 0,
                    last_error: None,
                },
            })),
        }
    }

    fn with_mut<R>(&self, update: impl FnOnce(&mut PipelineRuntimeSnapshot) -> R) -> R {
        let mut runtime = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let result = update(&mut runtime.snapshot);
        runtime.snapshot.estimated_byte_lag = match (
            runtime.snapshot.source_received_lsn,
            runtime.snapshot.target_checkpoint_lsn,
        ) {
            (Some(received), Some(applied)) => Some(received.saturating_sub(applied)),
            _ => None,
        };
        result
    }

    #[must_use]
    pub fn snapshot(&self) -> PipelineRuntimeSnapshot {
        self.inner
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .snapshot
            .clone()
    }

    pub fn mark_started(&self) {
        self.with_mut(|snapshot| {
            if snapshot.started_at.is_some() {
                snapshot.restart_count = snapshot.restart_count.saturating_add(1);
            }
            snapshot.started_at = Some(Utc::now());
            snapshot.stopped_at = None;
            snapshot.state = PipelineRuntimeState::Starting;
            snapshot.resource_wait_reason = None;
        });
    }

    pub fn mark_running(&self) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Running;
            snapshot.resource_wait_reason = None;
        });
    }

    /// Marks a recoverable capacity/resource wait while the job remains alive.
    /// The replication phase is intentionally retained so this cannot be
    /// mistaken for a rebuild or terminal pipeline failure.
    pub fn mark_resource_wait(&self, reason: impl Into<String>) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::ResourceWait;
            snapshot.stopped_at = None;
            snapshot.resource_wait_reason = Some(trim_message(reason.into()));
        });
    }

    /// Clears a resource wait without disturbing a terminal or stopped state.
    pub fn clear_resource_wait(&self) {
        self.with_mut(|snapshot| {
            if snapshot.state == PipelineRuntimeState::ResourceWait {
                snapshot.state = PipelineRuntimeState::Running;
                snapshot.resource_wait_reason = None;
            }
        });
    }

    pub fn mark_stopped(&self) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Stopped;
            snapshot.phase = PipelinePhase::Stopped;
            snapshot.stopped_at = Some(Utc::now());
            snapshot.resource_wait_reason = None;
        });
    }

    pub fn mark_failed(&self, error: impl Into<String>) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Failed;
            snapshot.phase = PipelinePhase::Failed;
            snapshot.stopped_at = Some(Utc::now());
            snapshot.resource_wait_reason = None;
            snapshot.last_error = Some(trim_message(error.into()));
        });
    }

    pub fn mark_degraded(&self, error: impl Into<String>) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Degraded;
            snapshot.phase = PipelinePhase::Degraded;
            snapshot.resource_wait_reason = None;
            snapshot.last_error = Some(trim_message(error.into()));
        });
    }

    pub fn set_phase(&self, phase: PipelinePhase) {
        self.with_mut(|snapshot| snapshot.phase = phase);
    }

    pub fn source_received(&self, lsn: PgLsn) {
        self.with_mut(|snapshot| {
            if snapshot
                .source_received_lsn
                .is_none_or(|previous| lsn > previous)
            {
                snapshot.source_received_lsn = Some(lsn);
            }
            if !matches!(
                snapshot.phase,
                PipelinePhase::Degraded | PipelinePhase::Failed
            ) {
                snapshot.phase = match snapshot.target_checkpoint_lsn {
                    Some(applied) if lsn > applied => PipelinePhase::CatchingUp,
                    Some(_) => PipelinePhase::Running,
                    None => snapshot.phase,
                };
            }
        });
    }

    pub fn transaction_received(&self, lsn: PgLsn, commit_time: DateTime<Utc>) {
        self.with_mut(|snapshot| {
            if snapshot
                .source_current_lsn
                .is_none_or(|previous| lsn > previous)
            {
                snapshot.source_current_lsn = Some(lsn);
            }
            if snapshot
                .source_received_lsn
                .is_none_or(|previous| lsn > previous)
            {
                snapshot.source_received_lsn = Some(lsn);
            }
            snapshot.last_transaction_at = Some(commit_time);
        });
    }

    /// Seeds the target position loaded from a durable checkpoint without
    /// pretending that an apply happened during process startup.
    pub fn checkpoint_initialized(&self, lsn: PgLsn) {
        self.with_mut(|snapshot| {
            if snapshot
                .target_checkpoint_lsn
                .is_none_or(|previous| lsn > previous)
            {
                snapshot.target_checkpoint_lsn = Some(lsn);
            }
        });
    }

    pub fn applied(&self, lsn: PgLsn) {
        self.with_mut(|snapshot| {
            if snapshot
                .target_checkpoint_lsn
                .is_none_or(|previous| lsn > previous)
            {
                snapshot.target_checkpoint_lsn = Some(lsn);
            }
            snapshot.last_apply_at = Some(Utc::now());
            if !matches!(
                snapshot.phase,
                PipelinePhase::Degraded | PipelinePhase::Failed
            ) && snapshot
                .source_received_lsn
                .is_none_or(|received| received <= lsn)
            {
                snapshot.phase = PipelinePhase::Running;
            }
        });
    }

    pub fn acknowledged(&self, lsn: PgLsn) {
        self.with_mut(|snapshot| {
            if snapshot
                .target_checkpoint_lsn
                .is_none_or(|previous| lsn > previous)
            {
                snapshot.target_checkpoint_lsn = Some(lsn);
            }
            snapshot.last_ack_at = Some(Utc::now());
        });
    }

    pub fn wal_retention_observed(
        &self,
        retained_bytes: Option<u64>,
        safe_bytes: Option<u64>,
        warning: bool,
    ) {
        self.with_mut(|snapshot| {
            snapshot.slot_retained_wal_bytes = retained_bytes;
            snapshot.slot_safe_wal_bytes = safe_bytes;
            snapshot.wal_retention_warning = warning;
        });
    }

    pub fn spool_usage_observed(&self, bytes: u64) {
        self.with_mut(|snapshot| snapshot.spool_bytes = Some(bytes));
    }

    pub fn error(&self, error: impl Into<String>) {
        self.with_mut(|snapshot| snapshot.last_error = Some(trim_message(error.into())));
    }
}

fn trim_message(mut message: String) -> String {
    const MAX_MESSAGE_BYTES: usize = 1024;
    if message.len() > MAX_MESSAGE_BYTES {
        let mut end = MAX_MESSAGE_BYTES;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        message.truncate(end);
        message.push_str("...");
    }
    message
}

#[cfg(test)]
mod tests {
    use cloudberry_etl_core::{id::PipelineId, lsn::PgLsn};

    use super::*;

    #[test]
    fn computes_monotonic_lag_and_restart_count() {
        let telemetry = PipelineTelemetryHandle::new(PipelineId::new());
        telemetry.mark_started();
        telemetry.source_received(PgLsn::new(100));
        telemetry.applied(PgLsn::new(40));
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.estimated_byte_lag, Some(60));
        telemetry.mark_started();
        assert_eq!(telemetry.snapshot().restart_count, 1);
        telemetry.source_received(PgLsn::new(20));
        assert_eq!(
            telemetry.snapshot().source_received_lsn,
            Some(PgLsn::new(100))
        );
        telemetry.wal_retention_observed(Some(80), Some(20), true);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.slot_retained_wal_bytes, Some(80));
        assert_eq!(snapshot.slot_safe_wal_bytes, Some(20));
        assert!(snapshot.wal_retention_warning);
    }

    #[test]
    fn resource_wait_is_recoverable_and_does_not_change_replication_phase() {
        let telemetry = PipelineTelemetryHandle::new(PipelineId::new());
        telemetry.mark_started();
        telemetry.set_phase(PipelinePhase::CatchingUp);
        telemetry.spool_usage_observed(4096);
        telemetry.mark_resource_wait("spool capacity exhausted");

        let waiting = telemetry.snapshot();
        assert_eq!(waiting.state, PipelineRuntimeState::ResourceWait);
        assert_eq!(waiting.phase, PipelinePhase::CatchingUp);
        assert_eq!(waiting.spool_bytes, Some(4096));
        assert_eq!(
            waiting.resource_wait_reason.as_deref(),
            Some("spool capacity exhausted")
        );
        assert!(waiting.last_error.is_none());
        assert!(waiting.stopped_at.is_none());

        telemetry.clear_resource_wait();
        let recovered = telemetry.snapshot();
        assert_eq!(recovered.state, PipelineRuntimeState::Running);
        assert_eq!(recovered.phase, PipelinePhase::CatchingUp);
        assert!(recovered.resource_wait_reason.is_none());
        assert_eq!(recovered.spool_bytes, Some(4096));
    }

    #[test]
    fn terminal_transitions_clear_resource_wait_reason() {
        let telemetry = PipelineTelemetryHandle::new(PipelineId::new());
        telemetry.mark_resource_wait("disk unavailable");
        telemetry.mark_failed("source slot lost");

        let failed = telemetry.snapshot();
        assert_eq!(failed.state, PipelineRuntimeState::Failed);
        assert!(failed.resource_wait_reason.is_none());
        assert_eq!(failed.last_error.as_deref(), Some("source slot lost"));
    }

    #[test]
    fn long_multibyte_resource_wait_reason_is_trimmed_safely() {
        let telemetry = PipelineTelemetryHandle::new(PipelineId::new());
        telemetry.mark_resource_wait("磁".repeat(400));

        let reason = telemetry
            .snapshot()
            .resource_wait_reason
            .expect("wait reason is recorded");
        assert!(reason.len() <= 1027);
        assert!(reason.ends_with("..."));
    }
}
