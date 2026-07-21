//! Pipeline lifecycle and task ownership.

use std::{collections::HashMap, sync::Arc, time::Duration};

use async_trait::async_trait;
use cloudberry_etl_core::id::PipelineId;
use thiserror::Error;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::telemetry::{PipelineRuntimeSnapshot, PipelineTelemetryHandle};

#[derive(Debug, Error)]
pub enum SupervisorError {
    #[error("pipeline {0} is already running")]
    AlreadyRunning(PipelineId),
    #[error("pipeline {0} is not running")]
    NotRunning(PipelineId),
    #[error("pipeline task failed: {0}")]
    Task(String),
    #[error("pipeline task panicked: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("pipeline {pipeline_id} did not stop within {timeout:?} and was aborted")]
    StopTimeout {
        pipeline_id: PipelineId,
        timeout: Duration,
    },
}

#[async_trait]
pub trait PipelineJob: Send + Sync + 'static {
    async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError>;
}

struct ManagedPipeline {
    cancellation: CancellationToken,
    handle: JoinHandle<Result<(), SupervisorError>>,
}

#[derive(Debug)]
pub struct PipelineTaskOutcome {
    pub pipeline_id: PipelineId,
    pub result: Result<(), SupervisorError>,
}

#[derive(Default)]
pub struct PipelineSupervisor {
    pipelines: Mutex<HashMap<PipelineId, ManagedPipeline>>,
    telemetry: Mutex<HashMap<PipelineId, PipelineTelemetryHandle>>,
}

impl std::fmt::Debug for PipelineSupervisor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PipelineSupervisor")
    }
}

impl PipelineSupervisor {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn start(
        &self,
        pipeline_id: PipelineId,
        job: Arc<dyn PipelineJob>,
    ) -> Result<(), SupervisorError> {
        let finished = {
            let mut pipelines = self.pipelines.lock().await;
            match pipelines.get(&pipeline_id) {
                Some(managed) if managed.handle.is_finished() => pipelines.remove(&pipeline_id),
                Some(_) => return Err(SupervisorError::AlreadyRunning(pipeline_id)),
                None => None,
            }
        };
        if let Some(managed) = finished {
            let result = join_managed(managed).await;
            self.record_finished(pipeline_id, &result, false).await;
            tracing::debug!(
                pipeline_id = %pipeline_id,
                ?result,
                "reaped finished pipeline before restart"
            );
        }
        let telemetry = self.telemetry_for(pipeline_id).await;
        let mut pipelines = self.pipelines.lock().await;
        if pipelines.contains_key(&pipeline_id) {
            return Err(SupervisorError::AlreadyRunning(pipeline_id));
        }
        telemetry.mark_started();
        let cancellation = CancellationToken::new();
        let task_cancellation = cancellation.clone();
        let handle = tokio::spawn(async move { job.run(task_cancellation).await });
        pipelines.insert(
            pipeline_id,
            ManagedPipeline {
                cancellation,
                handle,
            },
        );
        Ok(())
    }

    /// Returns the stable reporter for a pipeline.  It survives task
    /// replacement so restart counters and the last error remain visible.
    pub async fn telemetry_for(&self, pipeline_id: PipelineId) -> PipelineTelemetryHandle {
        let mut telemetry = self.telemetry.lock().await;
        telemetry
            .entry(pipeline_id)
            .or_insert_with(|| PipelineTelemetryHandle::new(pipeline_id))
            .clone()
    }

    /// Returns all known runtime snapshots.  Callers should intersect these
    /// with configured pipeline IDs before exposing them externally.
    pub async fn runtime_snapshots(&self) -> Vec<PipelineRuntimeSnapshot> {
        self.telemetry
            .lock()
            .await
            .values()
            .map(PipelineTelemetryHandle::snapshot)
            .collect()
    }

    pub async fn runtime_snapshot(
        &self,
        pipeline_id: PipelineId,
    ) -> Option<PipelineRuntimeSnapshot> {
        self.telemetry
            .lock()
            .await
            .get(&pipeline_id)
            .map(PipelineTelemetryHandle::snapshot)
    }

    pub async fn stop(&self, pipeline_id: PipelineId) -> Result<(), SupervisorError> {
        let managed = self
            .pipelines
            .lock()
            .await
            .remove(&pipeline_id)
            .ok_or(SupervisorError::NotRunning(pipeline_id))?;
        managed.cancellation.cancel();
        let result = join_managed(managed).await;
        self.record_finished(pipeline_id, &result, true).await;
        result
    }

    /// Cancel a task and forcibly abort it if cooperative cancellation does not finish in time.
    /// Awaiting the aborted handle is intentional: return guarantees the old owner is no longer
    /// executing before a reconciler releases or reacquires its lease.
    pub async fn stop_with_timeout(
        &self,
        pipeline_id: PipelineId,
        timeout: Duration,
    ) -> Result<(), SupervisorError> {
        let managed = self
            .pipelines
            .lock()
            .await
            .remove(&pipeline_id)
            .ok_or(SupervisorError::NotRunning(pipeline_id))?;
        managed.cancellation.cancel();
        let mut handle = managed.handle;
        let result = match tokio::time::timeout(timeout, &mut handle).await {
            Ok(joined) => match joined {
                Ok(task_result) => task_result,
                Err(error) => Err(SupervisorError::Join(error)),
            },
            Err(_) => {
                handle.abort();
                let _ = handle.await;
                Err(SupervisorError::StopTimeout {
                    pipeline_id,
                    timeout,
                })
            }
        };
        let timed_out = matches!(result, Err(SupervisorError::StopTimeout { .. }));
        self.record_finished(pipeline_id, &result, !timed_out).await;
        result
    }

    pub async fn stop_all(&self) -> Result<(), SupervisorError> {
        let managed: Vec<_> = self.pipelines.lock().await.drain().collect();
        for (_, pipeline) in &managed {
            pipeline.cancellation.cancel();
        }
        let mut first_error = None;
        for (pipeline_id, pipeline) in managed {
            let result = join_managed(pipeline).await;
            self.record_finished(pipeline_id, &result, true).await;
            if let Err(error) = result
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    pub async fn running(&self) -> Vec<PipelineId> {
        self.pipelines
            .lock()
            .await
            .iter()
            .filter_map(|(id, pipeline)| (!pipeline.handle.is_finished()).then_some(*id))
            .collect()
    }

    pub async fn reap_finished(&self) -> Vec<PipelineTaskOutcome> {
        let finished = {
            let mut pipelines = self.pipelines.lock().await;
            let ids: Vec<_> = pipelines
                .iter()
                .filter_map(|(id, pipeline)| pipeline.handle.is_finished().then_some(*id))
                .collect();
            ids.into_iter()
                .filter_map(|id| pipelines.remove(&id).map(|pipeline| (id, pipeline)))
                .collect::<Vec<_>>()
        };

        let mut outcomes = Vec::with_capacity(finished.len());
        for (pipeline_id, pipeline) in finished {
            let result = join_managed(pipeline).await;
            self.record_finished(pipeline_id, &result, false).await;
            outcomes.push(PipelineTaskOutcome {
                pipeline_id,
                result,
            });
        }
        outcomes
    }

    async fn record_finished(
        &self,
        pipeline_id: PipelineId,
        result: &Result<(), SupervisorError>,
        intentional_stop: bool,
    ) {
        let Some(telemetry) = self.telemetry.lock().await.get(&pipeline_id).cloned() else {
            return;
        };
        if intentional_stop {
            telemetry.mark_stopped();
            return;
        }
        match result {
            Err(error) => telemetry.mark_failed(error.to_string()),
            Ok(()) => {
                let snapshot = telemetry.snapshot();
                if !matches!(
                    snapshot.state,
                    crate::telemetry::PipelineRuntimeState::Degraded
                        | crate::telemetry::PipelineRuntimeState::Failed
                ) {
                    telemetry.mark_stopped();
                }
            }
        }
    }
}

async fn join_managed(managed: ManagedPipeline) -> Result<(), SupervisorError> {
    managed.handle.await?
}

#[cfg(test)]
mod tests {
    use std::{
        future::pending,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    use super::*;

    struct WaitJob(Arc<AtomicBool>);

    struct CompleteJob;

    struct StuckJob;

    #[async_trait]
    impl PipelineJob for WaitJob {
        async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError> {
            cancellation.cancelled().await;
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl PipelineJob for CompleteJob {
        async fn run(&self, _cancellation: CancellationToken) -> Result<(), SupervisorError> {
            Ok(())
        }
    }

    #[async_trait]
    impl PipelineJob for StuckJob {
        async fn run(&self, _cancellation: CancellationToken) -> Result<(), SupervisorError> {
            pending::<()>().await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn stops_owned_task() {
        let supervisor = PipelineSupervisor::new();
        let stopped = Arc::new(AtomicBool::new(false));
        let id = PipelineId::new();
        supervisor
            .start(id, Arc::new(WaitJob(Arc::clone(&stopped))))
            .await
            .unwrap();
        supervisor.stop(id).await.unwrap();
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn reaps_finished_task_and_allows_restart() {
        let supervisor = PipelineSupervisor::new();
        let id = PipelineId::new();
        supervisor.start(id, Arc::new(CompleteJob)).await.unwrap();

        while !supervisor.running().await.is_empty() {
            tokio::task::yield_now().await;
        }
        let outcomes = supervisor.reap_finished().await;
        assert_eq!(outcomes.len(), 1);
        assert_eq!(outcomes[0].pipeline_id, id);
        assert!(outcomes[0].result.is_ok());

        let stopped = Arc::new(AtomicBool::new(false));
        supervisor
            .start(id, Arc::new(WaitJob(Arc::clone(&stopped))))
            .await
            .unwrap();
        supervisor.stop(id).await.unwrap();
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn timeout_aborts_a_task_that_ignores_cancellation() {
        let supervisor = PipelineSupervisor::new();
        let id = PipelineId::new();
        supervisor.start(id, Arc::new(StuckJob)).await.unwrap();
        let result = supervisor
            .stop_with_timeout(id, Duration::from_millis(1))
            .await;
        assert!(matches!(
            result,
            Err(SupervisorError::StopTimeout { pipeline_id, .. }) if pipeline_id == id
        ));
        assert!(supervisor.running().await.is_empty());
    }
}
