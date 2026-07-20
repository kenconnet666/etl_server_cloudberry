//! Pipeline lifecycle and task ownership.

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use cloudberry_etl_core::id::PipelineId;
use thiserror::Error;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

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
}

#[async_trait]
pub trait PipelineJob: Send + Sync + 'static {
    async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError>;
}

struct ManagedPipeline {
    cancellation: CancellationToken,
    handle: JoinHandle<Result<(), SupervisorError>>,
}

#[derive(Default)]
pub struct PipelineSupervisor {
    pipelines: Mutex<HashMap<PipelineId, ManagedPipeline>>,
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
        let mut pipelines = self.pipelines.lock().await;
        if pipelines.contains_key(&pipeline_id) {
            return Err(SupervisorError::AlreadyRunning(pipeline_id));
        }
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

    pub async fn stop(&self, pipeline_id: PipelineId) -> Result<(), SupervisorError> {
        let managed = self
            .pipelines
            .lock()
            .await
            .remove(&pipeline_id)
            .ok_or(SupervisorError::NotRunning(pipeline_id))?;
        managed.cancellation.cancel();
        managed.handle.await??;
        Ok(())
    }

    pub async fn stop_all(&self) -> Result<(), SupervisorError> {
        let managed: Vec<_> = self
            .pipelines
            .lock()
            .await
            .drain()
            .map(|(_, value)| value)
            .collect();
        for pipeline in &managed {
            pipeline.cancellation.cancel();
        }
        for pipeline in managed {
            pipeline.handle.await??;
        }
        Ok(())
    }

    pub async fn running(&self) -> Vec<PipelineId> {
        self.pipelines.lock().await.keys().copied().collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct WaitJob(Arc<AtomicBool>);

    #[async_trait]
    impl PipelineJob for WaitJob {
        async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError> {
            cancellation.cancelled().await;
            self.0.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn stops_owned_task() {
        let supervisor = PipelineSupervisor::new();
        let stopped = Arc::new(AtomicBool::new(false));
        let id = PipelineId::new();
        supervisor
            .start(id, Arc::new(WaitJob(stopped.clone())))
            .await
            .unwrap();
        supervisor.stop(id).await.unwrap();
        assert!(stopped.load(Ordering::SeqCst));
    }
}
