//! Desired-state reconciliation for leased pipeline jobs.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use cloudberry_etl_core::id::PipelineId;
use cloudberry_etl_metadata::{
    model::{PipelineDefinition, PipelineLease},
    store::ControlStore,
};
use futures::{StreamExt, stream};
use thiserror::Error;
use tokio::{
    sync::watch,
    time::{self, Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::supervisor::{PipelineJob, PipelineSupervisor, SupervisorError};
use crate::telemetry::PipelineTelemetryHandle;

const MAX_CONCURRENT_LEASE_RENEWALS: usize = 4;

#[derive(Debug, Error)]
pub enum ReconcilerConfigError {
    #[error("reconciler poll interval must be greater than zero")]
    ZeroPollInterval,
    #[error("reconciler lease TTL must be greater than zero")]
    ZeroLeaseTtl,
    #[error("reconciler lease renewal interval must be greater than zero")]
    ZeroRenewInterval,
    #[error("lease renewal interval must not exceed one third of lease TTL")]
    RenewalTooLate,
    #[error("initial restart backoff must be greater than zero")]
    ZeroInitialBackoff,
    #[error("maximum restart backoff must be greater than or equal to the initial backoff")]
    BackoffRange,
    #[error("restart backoff reset interval must be greater than zero")]
    ZeroBackoffReset,
}

#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub poll_interval: Duration,
    pub lease_ttl: Duration,
    pub lease_renew_interval: Duration,
    pub restart_backoff_initial: Duration,
    pub restart_backoff_max: Duration,
    pub restart_backoff_reset_after: Duration,
}

impl Default for ReconcilerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            lease_ttl: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(10),
            restart_backoff_initial: Duration::from_secs(1),
            restart_backoff_max: Duration::from_secs(60),
            restart_backoff_reset_after: Duration::from_secs(300),
        }
    }
}

impl ReconcilerConfig {
    fn validate(&self) -> Result<(), ReconcilerConfigError> {
        if self.poll_interval.is_zero() {
            return Err(ReconcilerConfigError::ZeroPollInterval);
        }
        if self.lease_ttl.is_zero() {
            return Err(ReconcilerConfigError::ZeroLeaseTtl);
        }
        if self.lease_renew_interval.is_zero() {
            return Err(ReconcilerConfigError::ZeroRenewInterval);
        }
        if self.lease_renew_interval > self.lease_ttl / 3 {
            return Err(ReconcilerConfigError::RenewalTooLate);
        }
        if self.restart_backoff_initial.is_zero() {
            return Err(ReconcilerConfigError::ZeroInitialBackoff);
        }
        if self.restart_backoff_initial > self.restart_backoff_max {
            return Err(ReconcilerConfigError::BackoffRange);
        }
        if self.restart_backoff_reset_after.is_zero() {
            return Err(ReconcilerConfigError::ZeroBackoffReset);
        }
        Ok(())
    }

    fn tick_interval(&self) -> Duration {
        self.poll_interval.min(self.lease_renew_interval)
    }
}

#[derive(Debug, Error)]
#[error("pipeline job factory failed: {message}")]
pub struct JobFactoryError {
    message: String,
}

impl JobFactoryError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[async_trait]
pub trait PipelineJobFactory: Send + Sync + 'static {
    async fn create(
        &self,
        pipeline: &PipelineDefinition,
        lease: &PipelineLease,
        telemetry: PipelineTelemetryHandle,
    ) -> Result<Arc<dyn PipelineJob>, JobFactoryError>;
}

struct LeaseGuardedJob {
    inner: Arc<dyn PipelineJob>,
    lease_cancellation: CancellationToken,
}

#[async_trait]
impl PipelineJob for LeaseGuardedJob {
    async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError> {
        let inner = self.inner.run(cancellation.clone());
        tokio::pin!(inner);
        tokio::select! {
            biased;
            () = self.lease_cancellation.cancelled() => {
                cancellation.cancel();
                Err(SupervisorError::Task("pipeline lease safety deadline reached".into()))
            }
            result = &mut inner => result,
        }
    }
}

struct LeaseDeadlineGuard {
    deadline: watch::Sender<Instant>,
    job_cancellation: CancellationToken,
}

impl LeaseDeadlineGuard {
    fn new(
        pipeline_id: PipelineId,
        request_started: Instant,
        ttl: Duration,
        safety_margin: Duration,
    ) -> Self {
        let stop_at = lease_stop_at(request_started, ttl, safety_margin);
        let (deadline, receiver) = watch::channel(stop_at);
        let job_cancellation = CancellationToken::new();
        tokio::spawn(watch_lease_deadline(
            pipeline_id,
            receiver,
            job_cancellation.clone(),
        ));
        Self {
            deadline,
            job_cancellation,
        }
    }

    fn job_cancellation(&self) -> CancellationToken {
        self.job_cancellation.clone()
    }

    fn rearm(&self, request_started: Instant, ttl: Duration, safety_margin: Duration) -> bool {
        if self.expired() {
            return false;
        }
        let stop_at = lease_stop_at(request_started, ttl, safety_margin);
        self.deadline.send(stop_at).is_ok() && !self.expired()
    }

    fn expired(&self) -> bool {
        self.job_cancellation.is_cancelled() || Instant::now() >= *self.deadline.borrow()
    }

    fn stop_at(&self) -> Instant {
        *self.deadline.borrow()
    }
}

impl Drop for LeaseDeadlineGuard {
    fn drop(&mut self) {
        self.job_cancellation.cancel();
    }
}

fn lease_stop_at(request_started: Instant, ttl: Duration, safety_margin: Duration) -> Instant {
    request_started + ttl - safety_margin
}

async fn watch_lease_deadline(
    pipeline_id: PipelineId,
    mut deadline: watch::Receiver<Instant>,
    job_cancellation: CancellationToken,
) {
    loop {
        let stop_at = *deadline.borrow();
        tokio::select! {
            biased;
            changed = deadline.changed() => {
                if changed.is_err() {
                    return;
                }
            }
            () = time::sleep_until(stop_at) => {
                tracing::error!(
                    pipeline_id = %pipeline_id,
                    "pipeline reached its conservative lease safety deadline"
                );
                job_cancellation.cancel();
                return;
            }
        }
    }
}

pub struct PipelineReconciler {
    store: Arc<dyn ControlStore>,
    supervisor: Arc<PipelineSupervisor>,
    factory: Arc<dyn PipelineJobFactory>,
    config: ReconcilerConfig,
    holder_id: Uuid,
}

impl std::fmt::Debug for PipelineReconciler {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PipelineReconciler")
            .field("holder_id", &self.holder_id)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl PipelineReconciler {
    pub fn new(
        store: Arc<dyn ControlStore>,
        supervisor: Arc<PipelineSupervisor>,
        factory: Arc<dyn PipelineJobFactory>,
        config: ReconcilerConfig,
    ) -> Result<Self, ReconcilerConfigError> {
        Self::with_holder_id(store, supervisor, factory, config, Uuid::now_v7())
    }

    pub fn with_holder_id(
        store: Arc<dyn ControlStore>,
        supervisor: Arc<PipelineSupervisor>,
        factory: Arc<dyn PipelineJobFactory>,
        config: ReconcilerConfig,
        holder_id: Uuid,
    ) -> Result<Self, ReconcilerConfigError> {
        config.validate()?;
        Ok(Self {
            store,
            supervisor,
            factory,
            config,
            holder_id,
        })
    }

    /// Runs until `shutdown` is cancelled. Operational store and job errors are
    /// isolated to the affected pipeline so one bad source cannot stop control.
    pub async fn run(&self, shutdown: CancellationToken) {
        let mut runtime = RuntimeState::default();
        let mut ticker = time::interval(self.config.tick_interval());
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut next_poll = Instant::now();

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => {
                    self.shutdown(&mut runtime).await;
                    return;
                }
                _ = ticker.tick() => {
                    let now = Instant::now();
                    let poll_due = now >= next_poll;
                    if poll_due {
                        next_poll = now + self.config.poll_interval;
                    }
                    self.tick(&mut runtime, poll_due).await;
                }
            }
        }
    }

    async fn tick(&self, runtime: &mut RuntimeState, poll_due: bool) {
        self.reap_finished(runtime).await;
        self.renew_leases(runtime).await;
        if poll_due {
            self.reconcile(runtime).await;
        }
    }

    async fn reap_finished(&self, runtime: &mut RuntimeState) {
        for outcome in self.supervisor.reap_finished().await {
            let Some(owned) = runtime.owned.remove(&outcome.pipeline_id) else {
                tracing::debug!(
                    pipeline_id = %outcome.pipeline_id,
                    "reaped pipeline that was not owned by this reconciler"
                );
                continue;
            };
            let stable = Instant::now().duration_since(owned.started_at)
                >= self.config.restart_backoff_reset_after;
            self.release(&owned.lease).await;
            if stable {
                runtime.restart.remove(&outcome.pipeline_id);
            }
            runtime.record_failure(outcome.pipeline_id, Instant::now(), &self.config);
            match outcome.result {
                Ok(()) => tracing::warn!(
                    pipeline_id = %outcome.pipeline_id,
                    "pipeline job exited; scheduling restart"
                ),
                Err(error) => tracing::warn!(
                    pipeline_id = %outcome.pipeline_id,
                    %error,
                    "pipeline job failed; scheduling restart"
                ),
            }
        }
    }

    async fn renew_leases(&self, runtime: &mut RuntimeState) {
        let mut due: Vec<_> = runtime
            .owned
            .iter()
            .filter_map(|(pipeline_id, owned)| {
                (Instant::now() >= owned.next_renewal).then_some((
                    owned.deadline.stop_at(),
                    *pipeline_id,
                    owned.lease.clone(),
                ))
            })
            .collect();
        due.sort_unstable_by_key(|(stop_at, _, _)| *stop_at);
        let store = Arc::clone(&self.store);
        let lease_ttl = self.config.lease_ttl;
        let mut renewals = stream::iter(due.into_iter().map(move |(_, pipeline_id, lease)| {
            let store = Arc::clone(&store);
            async move {
                let request_started = Instant::now();
                let result = store.renew_lease(&lease, lease_ttl).await;
                (pipeline_id, lease, request_started, result)
            }
        }))
        .buffer_unordered(MAX_CONCURRENT_LEASE_RENEWALS);
        let mut lost = Vec::new();

        while let Some((pipeline_id, lease, request_started, result)) = renewals.next().await {
            match result {
                Ok(Some(renewed)) => {
                    let rearmed = runtime.owned.get_mut(&pipeline_id).is_some_and(|owned| {
                        if !owned.deadline.rearm(
                            request_started,
                            self.config.lease_ttl,
                            self.config.lease_renew_interval,
                        ) {
                            return false;
                        }
                        owned.lease = renewed;
                        owned.next_renewal = request_started + self.config.lease_renew_interval;
                        true
                    });
                    if !rearmed {
                        tracing::error!(
                            pipeline_id = %pipeline_id,
                            "lease renewal completed after the local safety deadline"
                        );
                        lost.push((pipeline_id, lease));
                    }
                }
                Ok(None) => {
                    tracing::error!(pipeline_id = %pipeline_id, "pipeline lease was lost");
                    lost.push((pipeline_id, lease));
                }
                Err(error) => {
                    tracing::error!(pipeline_id = %pipeline_id, %error, "pipeline lease renewal failed");
                    lost.push((pipeline_id, lease));
                }
            }
        }

        for (pipeline_id, lease) in lost {
            self.lease_lost(runtime, pipeline_id, lease).await;
        }
    }

    async fn lease_lost(
        &self,
        runtime: &mut RuntimeState,
        pipeline_id: PipelineId,
        lease: PipelineLease,
    ) {
        let owned = runtime.owned.remove(&pipeline_id);
        self.cancel_job(pipeline_id).await;
        self.release(&lease).await;
        if let Some(owned) = owned {
            if Instant::now().duration_since(owned.started_at)
                >= self.config.restart_backoff_reset_after
            {
                runtime.restart.remove(&pipeline_id);
            }
            runtime.record_failure(pipeline_id, Instant::now(), &self.config);
            tracing::warn!(
                pipeline_id = %pipeline_id,
                elapsed = ?Instant::now().duration_since(owned.started_at),
                "cancelled pipeline after lease loss"
            );
        }
    }

    async fn reconcile(&self, runtime: &mut RuntimeState) {
        let definitions = match self.store.list_pipelines().await {
            Ok(definitions) => definitions,
            Err(error) => {
                tracing::error!(%error, "failed to list pipelines for reconciliation");
                return;
            }
        };
        let now = Instant::now();
        let mut seen = HashSet::with_capacity(definitions.len());

        for definition in &definitions {
            seen.insert(definition.id);
            if !definition.desired_running {
                runtime.restart.remove(&definition.id);
                if runtime.owned.contains_key(&definition.id) {
                    self.stop_owned(runtime, definition.id).await;
                }
                continue;
            }

            if let Some(owned) = runtime.owned.get(&definition.id) {
                if owned.config_revision == definition.config_revision
                    && owned.snapshot_generation == definition.snapshot_generation
                {
                    continue;
                }
                tracing::info!(
                    pipeline_id = %definition.id,
                    old_revision = owned.config_revision,
                    new_revision = definition.config_revision,
                    old_snapshot_generation = owned.snapshot_generation,
                    new_snapshot_generation = definition.snapshot_generation,
                    "restarting pipeline for a new desired definition"
                );
                self.stop_owned(runtime, definition.id).await;
                runtime.restart.remove(&definition.id);
            }

            if runtime.can_start(definition.id, now) {
                self.start_pipeline(runtime, definition).await;
            }
        }

        let obsolete: Vec<_> = runtime
            .owned
            .keys()
            .filter_map(|pipeline_id| (!seen.contains(pipeline_id)).then_some(*pipeline_id))
            .collect();
        for pipeline_id in obsolete {
            self.stop_owned(runtime, pipeline_id).await;
            runtime.restart.remove(&pipeline_id);
        }
        runtime
            .restart
            .retain(|pipeline_id, _| seen.contains(pipeline_id));
    }

    async fn start_pipeline(&self, runtime: &mut RuntimeState, definition: &PipelineDefinition) {
        let acquire_started = Instant::now();
        let lease = match self
            .store
            .try_acquire_lease(definition.id, self.holder_id, self.config.lease_ttl)
            .await
        {
            Ok(Some(lease)) => lease,
            Ok(None) => return,
            Err(error) => {
                tracing::error!(pipeline_id = %definition.id, %error, "failed to acquire pipeline lease");
                runtime.record_failure(definition.id, Instant::now(), &self.config);
                return;
            }
        };
        let deadline = LeaseDeadlineGuard::new(
            definition.id,
            acquire_started,
            self.config.lease_ttl,
            self.config.lease_renew_interval,
        );
        if deadline.expired() {
            tracing::error!(
                pipeline_id = %definition.id,
                "lease acquisition completed after the local safety deadline"
            );
            self.release(&lease).await;
            runtime.record_failure(definition.id, Instant::now(), &self.config);
            return;
        }

        let telemetry = self.supervisor.telemetry_for(definition.id).await;
        let job = match self.factory.create(definition, &lease, telemetry).await {
            Ok(job) => job,
            Err(error) => {
                tracing::error!(pipeline_id = %definition.id, %error, "failed to create pipeline job");
                self.release(&lease).await;
                runtime.record_failure(definition.id, Instant::now(), &self.config);
                return;
            }
        };
        if deadline.expired() {
            tracing::error!(
                pipeline_id = %definition.id,
                "pipeline job creation exceeded the local lease safety deadline"
            );
            self.release(&lease).await;
            runtime.record_failure(definition.id, Instant::now(), &self.config);
            return;
        }
        let job: Arc<dyn PipelineJob> = Arc::new(LeaseGuardedJob {
            inner: job,
            lease_cancellation: deadline.job_cancellation(),
        });

        if let Err(error) = self.supervisor.start(definition.id, job).await {
            tracing::error!(pipeline_id = %definition.id, %error, "failed to start pipeline job");
            self.release(&lease).await;
            runtime.record_failure(definition.id, Instant::now(), &self.config);
            return;
        }

        runtime.owned.insert(
            definition.id,
            OwnedPipeline {
                lease,
                config_revision: definition.config_revision,
                snapshot_generation: definition.snapshot_generation,
                started_at: Instant::now(),
                next_renewal: acquire_started + self.config.lease_renew_interval,
                deadline,
            },
        );
        runtime.mark_started(definition.id);
        tracing::info!(pipeline_id = %definition.id, "pipeline job started");
    }

    async fn stop_owned(&self, runtime: &mut RuntimeState, pipeline_id: PipelineId) {
        let Some(owned) = runtime.owned.remove(&pipeline_id) else {
            return;
        };
        self.cancel_job(pipeline_id).await;
        self.release(&owned.lease).await;
    }

    async fn cancel_job(&self, pipeline_id: PipelineId) {
        if let Err(error) = self
            .supervisor
            .stop_with_timeout(pipeline_id, self.config.lease_renew_interval)
            .await
            && !matches!(error, SupervisorError::NotRunning(_))
        {
            tracing::warn!(pipeline_id = %pipeline_id, %error, "pipeline stop returned an error");
        }
    }

    async fn release(&self, lease: &PipelineLease) {
        if let Err(error) = self.store.release_lease(lease).await {
            tracing::warn!(pipeline_id = %lease.pipeline_id, %error, "failed to release pipeline lease");
        }
    }

    async fn shutdown(&self, runtime: &mut RuntimeState) {
        let owned: Vec<_> = runtime.owned.keys().copied().collect();
        for pipeline_id in owned {
            self.stop_owned(runtime, pipeline_id).await;
        }
        if let Err(error) = self.supervisor.stop_all().await {
            tracing::warn!(%error, "pipeline supervisor shutdown returned an error");
        }
    }
}

#[derive(Default)]
struct RuntimeState {
    owned: HashMap<PipelineId, OwnedPipeline>,
    restart: HashMap<PipelineId, RestartState>,
}

struct OwnedPipeline {
    lease: PipelineLease,
    config_revision: i64,
    snapshot_generation: i64,
    started_at: Instant,
    next_renewal: Instant,
    deadline: LeaseDeadlineGuard,
}

#[derive(Default)]
struct RestartState {
    failures: u32,
    retry_at: Option<Instant>,
}

impl RuntimeState {
    fn can_start(&self, pipeline_id: PipelineId, now: Instant) -> bool {
        self.restart
            .get(&pipeline_id)
            .and_then(|state| state.retry_at)
            .is_none_or(|retry_at| now >= retry_at)
    }

    fn mark_started(&mut self, pipeline_id: PipelineId) {
        if let Some(state) = self.restart.get_mut(&pipeline_id) {
            state.retry_at = None;
        }
    }

    fn record_failure(&mut self, pipeline_id: PipelineId, now: Instant, config: &ReconcilerConfig) {
        let state = self.restart.entry(pipeline_id).or_default();
        let shift = state.failures.min(31);
        let multiplier = 1_u32.checked_shl(shift).unwrap_or(u32::MAX);
        let delay = config
            .restart_backoff_initial
            .saturating_mul(multiplier)
            .min(config.restart_backoff_max);
        state.failures = state.failures.saturating_add(1);
        state.retry_at = Some(now + delay);
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, VecDeque},
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use chrono::Utc;
    use cloudberry_etl_core::id::{PipelineId, SourceId, TargetId};
    use cloudberry_etl_metadata::{
        model::{PipelineDefinition, PipelineLease, SourceProfile, TargetProfile},
        store::{ControlStore, StoreError},
    };
    use tokio::sync::{Mutex, Notify};

    use super::*;

    #[derive(Default)]
    struct FakeStore {
        pipelines: Mutex<Vec<PipelineDefinition>>,
        leases: Mutex<HashMap<PipelineId, PipelineLease>>,
        fence: AtomicI64,
        renew_enabled: AtomicBool,
        renew_blocked: AtomicBool,
        renew_started: Notify,
        renew_release: Notify,
        release_count: AtomicUsize,
    }

    impl FakeStore {
        fn with_pipelines(pipelines: Vec<PipelineDefinition>) -> Arc<Self> {
            Arc::new(Self {
                pipelines: Mutex::new(pipelines),
                renew_enabled: AtomicBool::new(true),
                ..Self::default()
            })
        }

        async fn set_desired(&self, pipeline_id: PipelineId, desired_running: bool) {
            if let Some(pipeline) = self
                .pipelines
                .lock()
                .await
                .iter_mut()
                .find(|pipeline| pipeline.id == pipeline_id)
            {
                pipeline.desired_running = desired_running;
            }
        }
    }

    #[async_trait]
    impl ControlStore for FakeStore {
        async fn check_readiness(&self) -> Result<(), StoreError> {
            Ok(())
        }

        async fn put_source(&self, _source: &SourceProfile) -> Result<(), StoreError> {
            Ok(())
        }

        async fn list_sources(&self) -> Result<Vec<SourceProfile>, StoreError> {
            Ok(Vec::new())
        }

        async fn put_target(&self, _target: &TargetProfile) -> Result<(), StoreError> {
            Ok(())
        }

        async fn list_targets(&self) -> Result<Vec<TargetProfile>, StoreError> {
            Ok(Vec::new())
        }

        async fn put_pipeline(&self, _pipeline: &PipelineDefinition) -> Result<(), StoreError> {
            Ok(())
        }

        async fn list_pipelines(&self) -> Result<Vec<PipelineDefinition>, StoreError> {
            Ok(self.pipelines.lock().await.clone())
        }

        async fn set_pipeline_desired_running(
            &self,
            pipeline_id: PipelineId,
            desired_running: bool,
        ) -> Result<Option<PipelineDefinition>, StoreError> {
            let mut pipelines = self.pipelines.lock().await;
            let Some(pipeline) = pipelines.iter_mut().find(|p| p.id == pipeline_id) else {
                return Ok(None);
            };
            pipeline.desired_running = desired_running;
            Ok(Some(pipeline.clone()))
        }

        async fn request_pipeline_rebuild(
            &self,
            pipeline_id: PipelineId,
        ) -> Result<Option<cloudberry_etl_metadata::model::RebuildRequest>, StoreError> {
            let mut pipelines = self.pipelines.lock().await;
            let Some(pipeline) = pipelines.iter_mut().find(|p| p.id == pipeline_id) else {
                return Ok(None);
            };
            pipeline.snapshot_generation += 1;
            Ok(Some(cloudberry_etl_metadata::model::RebuildRequest {
                pipeline: pipeline.clone(),
                operation_id: cloudberry_etl_core::id::OperationId::new(),
            }))
        }

        async fn complete_pipeline_rebuilds(
            &self,
            _pipeline_id: PipelineId,
            _snapshot_generation: i64,
        ) -> Result<u64, StoreError> {
            Ok(0)
        }

        async fn list_operations(
            &self,
        ) -> Result<Vec<cloudberry_etl_metadata::model::OperationRecord>, StoreError> {
            Ok(Vec::new())
        }

        async fn try_acquire_lease(
            &self,
            pipeline_id: PipelineId,
            holder_id: Uuid,
            ttl: Duration,
        ) -> Result<Option<PipelineLease>, StoreError> {
            let mut leases = self.leases.lock().await;
            if let Some(existing) = leases.get(&pipeline_id)
                && existing.holder_id != holder_id
            {
                return Ok(None);
            }
            let lease = PipelineLease {
                pipeline_id,
                holder_id,
                fencing_token: self.fence.fetch_add(1, Ordering::SeqCst) + 1,
                expires_at: Utc::now()
                    + chrono::Duration::from_std(ttl)
                        .unwrap_or_else(|_| chrono::Duration::seconds(1)),
            };
            leases.insert(pipeline_id, lease.clone());
            Ok(Some(lease))
        }

        async fn renew_lease(
            &self,
            lease: &PipelineLease,
            ttl: Duration,
        ) -> Result<Option<PipelineLease>, StoreError> {
            if self.renew_blocked.load(Ordering::SeqCst) {
                self.renew_started.notify_one();
                self.renew_release.notified().await;
            }
            if !self.renew_enabled.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let mut leases = self.leases.lock().await;
            let Some(current) = leases.get_mut(&lease.pipeline_id) else {
                return Ok(None);
            };
            if current.holder_id != lease.holder_id || current.fencing_token != lease.fencing_token
            {
                return Ok(None);
            }
            current.expires_at = Utc::now()
                + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(1));
            Ok(Some(current.clone()))
        }

        async fn release_lease(&self, lease: &PipelineLease) -> Result<(), StoreError> {
            let mut leases = self.leases.lock().await;
            if leases.get(&lease.pipeline_id).is_some_and(|current| {
                current.holder_id == lease.holder_id && current.fencing_token == lease.fencing_token
            }) {
                leases.remove(&lease.pipeline_id);
            }
            self.release_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone, Copy)]
    enum JobKind {
        Wait,
        Complete,
        Fail,
    }

    struct FakeJob {
        kind: JobKind,
        cancelled: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl PipelineJob for FakeJob {
        async fn run(&self, cancellation: CancellationToken) -> Result<(), SupervisorError> {
            match self.kind {
                JobKind::Wait => {
                    cancellation.cancelled().await;
                    self.cancelled.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
                JobKind::Complete => Ok(()),
                JobKind::Fail => Err(SupervisorError::Task("fake failure".into())),
            }
        }
    }

    struct SharedFactory {
        kinds: Mutex<VecDeque<JobKind>>,
        created: AtomicUsize,
        cancelled: Arc<AtomicUsize>,
    }

    impl SharedFactory {
        fn new(kinds: impl IntoIterator<Item = JobKind>) -> Arc<Self> {
            Arc::new(Self {
                kinds: Mutex::new(kinds.into_iter().collect()),
                created: AtomicUsize::new(0),
                cancelled: Arc::new(AtomicUsize::new(0)),
            })
        }
    }

    #[async_trait]
    impl PipelineJobFactory for SharedFactory {
        async fn create(
            &self,
            _pipeline: &PipelineDefinition,
            _lease: &PipelineLease,
            _telemetry: PipelineTelemetryHandle,
        ) -> Result<Arc<dyn PipelineJob>, JobFactoryError> {
            self.created.fetch_add(1, Ordering::SeqCst);
            let kind = self.kinds.lock().await.pop_front().unwrap_or(JobKind::Wait);
            Ok(Arc::new(FakeJob {
                kind,
                cancelled: Arc::clone(&self.cancelled),
            }))
        }
    }

    fn pipeline(desired_running: bool) -> PipelineDefinition {
        PipelineDefinition {
            id: PipelineId::new(),
            name: "test pipeline".into(),
            source_id: SourceId::new(),
            target_id: TargetId::new(),
            desired_running,
            config_revision: 1,
            snapshot_generation: 1,
            settings: Default::default(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn config() -> ReconcilerConfig {
        ReconcilerConfig {
            poll_interval: Duration::from_millis(10),
            lease_ttl: Duration::from_millis(100),
            lease_renew_interval: Duration::from_millis(20),
            restart_backoff_initial: Duration::from_millis(50),
            restart_backoff_max: Duration::from_millis(200),
            restart_backoff_reset_after: Duration::from_secs(10),
        }
    }

    async fn wait_for(counter: &AtomicUsize, expected: usize) {
        for _ in 0..100 {
            if counter.load(Ordering::SeqCst) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("counter did not reach {expected}");
    }

    #[tokio::test(start_paused = true)]
    async fn lease_loss_cancels_job_before_reacquisition() {
        let pipeline = pipeline(true);
        let store = FakeStore::with_pipelines(vec![pipeline]);
        let factory = SharedFactory::new([JobKind::Wait]);
        let supervisor = Arc::new(PipelineSupervisor::new());
        let reconciler = PipelineReconciler::new(
            Arc::clone(&store) as Arc<dyn ControlStore>,
            Arc::clone(&supervisor),
            Arc::clone(&factory) as Arc<dyn PipelineJobFactory>,
            config(),
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { reconciler.run(task_shutdown).await });

        wait_for(&factory.created, 1).await;
        store.renew_enabled.store(false, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(25)).await;
        wait_for(&factory.cancelled, 1).await;
        assert!(supervisor.running().await.is_empty());
        assert!(store.release_count.load(Ordering::SeqCst) >= 1);

        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_renewal_stops_the_job_before_the_lease_ttl() {
        let pipeline = pipeline(true);
        let store = FakeStore::with_pipelines(vec![pipeline]);
        let factory = SharedFactory::new([JobKind::Wait]);
        let supervisor = Arc::new(PipelineSupervisor::new());
        let reconciler = PipelineReconciler::new(
            Arc::clone(&store) as Arc<dyn ControlStore>,
            Arc::clone(&supervisor),
            Arc::clone(&factory) as Arc<dyn PipelineJobFactory>,
            config(),
        )
        .unwrap();
        let test_started = Instant::now();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { reconciler.run(task_shutdown).await });

        wait_for(&factory.created, 1).await;
        store.renew_blocked.store(true, Ordering::SeqCst);
        tokio::time::advance(Duration::from_millis(21)).await;
        store.renew_started.notified().await;

        tokio::time::advance(Duration::from_millis(58)).await;
        tokio::task::yield_now().await;
        assert_eq!(supervisor.running().await.len(), 1);

        tokio::time::advance(Duration::from_millis(2)).await;
        for _ in 0..100 {
            if supervisor.running().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(supervisor.running().await.is_empty());
        assert!(Instant::now() < test_started + config().lease_ttl);

        store.renew_blocked.store(false, Ordering::SeqCst);
        store.renew_release.notify_one();
        wait_for(&store.release_count, 1).await;
        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn pause_stops_job_and_releases_lease() {
        let pipeline = pipeline(true);
        let pipeline_id = pipeline.id;
        let store = FakeStore::with_pipelines(vec![pipeline]);
        let factory = SharedFactory::new([JobKind::Wait]);
        let supervisor = Arc::new(PipelineSupervisor::new());
        let reconciler = PipelineReconciler::new(
            Arc::clone(&store) as Arc<dyn ControlStore>,
            Arc::clone(&supervisor),
            Arc::clone(&factory) as Arc<dyn PipelineJobFactory>,
            config(),
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { reconciler.run(task_shutdown).await });

        wait_for(&factory.created, 1).await;
        store.set_desired(pipeline_id, false).await;
        tokio::time::advance(Duration::from_millis(10)).await;
        wait_for(&factory.cancelled, 1).await;
        assert!(supervisor.running().await.is_empty());
        assert!(store.release_count.load(Ordering::SeqCst) >= 1);

        shutdown.cancel();
        task.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_generation_change_restarts_running_job() {
        let pipeline = pipeline(true);
        let pipeline_id = pipeline.id;
        let store = FakeStore::with_pipelines(vec![pipeline]);
        let factory = SharedFactory::new([JobKind::Wait, JobKind::Wait]);
        let supervisor = Arc::new(PipelineSupervisor::new());
        let reconciler = PipelineReconciler::new(
            Arc::clone(&store) as Arc<dyn ControlStore>,
            Arc::clone(&supervisor),
            Arc::clone(&factory) as Arc<dyn PipelineJobFactory>,
            config(),
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { reconciler.run(task_shutdown).await });

        wait_for(&factory.created, 1).await;
        let request = store
            .request_pipeline_rebuild(pipeline_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(request.pipeline.config_revision, 1);
        assert_eq!(request.pipeline.snapshot_generation, 2);
        tokio::time::advance(Duration::from_millis(10)).await;
        wait_for(&factory.cancelled, 1).await;
        wait_for(&factory.created, 2).await;

        shutdown.cancel();
        task.await.unwrap();
        assert_eq!(factory.cancelled.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn reaps_completed_and_failed_jobs_with_exponential_backoff() {
        let store = FakeStore::with_pipelines(vec![pipeline(true)]);
        let factory = SharedFactory::new([JobKind::Complete, JobKind::Fail, JobKind::Wait]);
        let supervisor = Arc::new(PipelineSupervisor::new());
        let reconciler = PipelineReconciler::new(
            Arc::clone(&store) as Arc<dyn ControlStore>,
            Arc::clone(&supervisor),
            Arc::clone(&factory) as Arc<dyn PipelineJobFactory>,
            config(),
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { reconciler.run(task_shutdown).await });

        wait_for(&factory.created, 1).await;
        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        assert_eq!(factory.created.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(49)).await;
        tokio::task::yield_now().await;
        assert_eq!(factory.created.load(Ordering::SeqCst), 1);
        tokio::time::advance(Duration::from_millis(11)).await;
        wait_for(&factory.created, 2).await;

        tokio::time::advance(Duration::from_millis(10)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(99)).await;
        tokio::task::yield_now().await;
        assert_eq!(factory.created.load(Ordering::SeqCst), 2);
        tokio::time::advance(Duration::from_millis(11)).await;
        wait_for(&factory.created, 3).await;

        shutdown.cancel();
        task.await.unwrap();
        assert_eq!(factory.cancelled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_cancels_all_jobs_and_releases_leases() {
        let first = pipeline(true);
        let second = pipeline(true);
        let store = FakeStore::with_pipelines(vec![first, second]);
        let factory = SharedFactory::new([JobKind::Wait, JobKind::Wait]);
        let supervisor = Arc::new(PipelineSupervisor::new());
        let reconciler = PipelineReconciler::new(
            Arc::clone(&store) as Arc<dyn ControlStore>,
            Arc::clone(&supervisor),
            Arc::clone(&factory) as Arc<dyn PipelineJobFactory>,
            config(),
        )
        .unwrap();
        let shutdown = CancellationToken::new();
        let task_shutdown = shutdown.clone();
        let task = tokio::spawn(async move { reconciler.run(task_shutdown).await });

        wait_for(&factory.created, 2).await;
        shutdown.cancel();
        task.await.unwrap();
        assert_eq!(factory.cancelled.load(Ordering::SeqCst), 2);
        assert_eq!(store.release_count.load(Ordering::SeqCst), 2);
        assert!(supervisor.running().await.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn successful_renewal_rearms_the_lease_watchdog() {
        let pipeline_id = PipelineId::new();
        let ttl = Duration::from_millis(100);
        let margin = Duration::from_millis(20);
        let guard = LeaseDeadlineGuard::new(pipeline_id, Instant::now(), ttl, margin);
        let job_cancellation = guard.job_cancellation();

        tokio::time::advance(Duration::from_millis(70)).await;
        assert!(guard.rearm(Instant::now(), ttl, margin));
        tokio::time::advance(Duration::from_millis(20)).await;
        tokio::task::yield_now().await;
        assert!(!job_cancellation.is_cancelled());

        tokio::time::advance(Duration::from_millis(61)).await;
        tokio::task::yield_now().await;
        assert!(job_cancellation.is_cancelled());
    }

    #[test]
    fn rejects_unsafe_lease_configuration() {
        let mut invalid = config();
        invalid.lease_renew_interval = invalid.lease_ttl / 3 + Duration::from_millis(1);
        assert!(matches!(
            invalid.validate(),
            Err(ReconcilerConfigError::RenewalTooLate)
        ));
        invalid.lease_renew_interval = invalid.lease_ttl / 3;
        assert!(invalid.validate().is_ok());
    }
}
