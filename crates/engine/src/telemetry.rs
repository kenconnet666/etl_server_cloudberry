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
        });
    }

    pub fn mark_running(&self) {
        self.with_mut(|snapshot| snapshot.state = PipelineRuntimeState::Running);
    }

    pub fn mark_stopped(&self) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Stopped;
            snapshot.phase = PipelinePhase::Stopped;
            snapshot.stopped_at = Some(Utc::now());
        });
    }

    pub fn mark_failed(&self, error: impl Into<String>) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Failed;
            snapshot.phase = PipelinePhase::Failed;
            snapshot.stopped_at = Some(Utc::now());
            snapshot.last_error = Some(trim_error(error.into()));
        });
    }

    pub fn mark_degraded(&self, error: impl Into<String>) {
        self.with_mut(|snapshot| {
            snapshot.state = PipelineRuntimeState::Degraded;
            snapshot.phase = PipelinePhase::Degraded;
            snapshot.last_error = Some(trim_error(error.into()));
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

    pub fn error(&self, error: impl Into<String>) {
        self.with_mut(|snapshot| snapshot.last_error = Some(trim_error(error.into())));
    }
}

fn trim_error(mut error: String) -> String {
    const MAX_ERROR_BYTES: usize = 1024;
    if error.len() > MAX_ERROR_BYTES {
        error.truncate(MAX_ERROR_BYTES);
        error.push_str("...");
    }
    error
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
}
