//! Management API routes.

use std::{collections::HashMap, fmt::Display, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, HeaderValue, StatusCode},
    middleware,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::Utc;
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
};
use cloudberry_etl_engine::runtime::{PipelineSettings, SourceSettings, TargetSettings};
use cloudberry_etl_metadata::{
    crypto::{source_credential_aad, target_credential_aad},
    model::{PipelineDefinition, SourceProfile, TargetProfile},
    store::StoreError,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    auth::{AuthState, current_session, login, logout, require_session},
    error::ApiError,
    state::{AppState, ConnectionReport},
};
use cloudberry_etl_engine::telemetry::PipelineRuntimeSnapshot;

const READINESS_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECTION_TEST_TIMEOUT: Duration = Duration::from_secs(10);

pub fn router(state: AppState, auth: AuthState) -> Router {
    let auth_middleware = middleware::from_fn_with_state(auth.clone(), require_session);
    let authenticated_auth = Router::new()
        .route("/api/v1/auth/session", get(current_session))
        .route("/api/v1/auth/logout", post(logout))
        .route_layer(auth_middleware.clone())
        .with_state(auth.clone());
    let operational = Router::new()
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(state.clone());
    let authenticated_api = Router::new()
        .route("/api/v1/overview", get(overview))
        .route("/api/v1/operations", get(operations))
        .route("/api/v1/sources", get(list_sources).post(create_source))
        .route("/api/v1/sources/test", post(test_source))
        .route("/api/v1/targets", get(list_targets).post(create_target))
        .route("/api/v1/targets/test", post(test_target))
        .route(
            "/api/v1/pipelines",
            get(list_pipelines).post(create_pipeline),
        )
        .route("/api/v1/pipelines/{id}", get(get_pipeline))
        .route("/api/v1/pipelines/{id}/start", post(start_pipeline))
        .route("/api/v1/pipelines/{id}/pause", post(pause_pipeline))
        .route("/api/v1/pipelines/{id}/rebuild", post(rebuild_pipeline))
        .route_layer(auth_middleware)
        .with_state(state);

    Router::new()
        .route("/health/live", get(live))
        .route("/api/v1/auth/login", post(login))
        .with_state(auth)
        .merge(operational)
        .merge(authenticated_auth)
        .merge(authenticated_api)
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> StatusCode {
    match tokio::time::timeout(READINESS_TIMEOUT, state.control.check_readiness()).await {
        Ok(Ok(())) => StatusCode::NO_CONTENT,
        Ok(Err(error)) => {
            tracing::warn!(%error, "readiness check could not reach the control database");
            StatusCode::SERVICE_UNAVAILABLE
        }
        Err(_) => {
            tracing::warn!("readiness check timed out");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn metrics(State(state): State<AppState>) -> Result<impl IntoResponse, ApiError> {
    let _permit = Arc::clone(&state.metrics_gate)
        .try_acquire_owned()
        .map_err(|_| ApiError::unavailable("a metrics scrape is already in progress"))?;
    let sources = state.control.list_sources().await.map_err(store_error)?;
    let targets = state.control.list_targets().await.map_err(store_error)?;
    let pipelines = state.control.list_pipelines().await.map_err(store_error)?;
    let desired_running = pipelines
        .iter()
        .filter(|pipeline| pipeline.desired_running)
        .count();
    let running = state.supervisor.running().await.len();
    let snapshots: HashMap<_, _> = state
        .supervisor
        .runtime_snapshots()
        .await
        .into_iter()
        .map(|snapshot| (snapshot.pipeline_id, snapshot))
        .collect();
    let mut body = format!(
        "# HELP pg2cb_sources_configured Configured PostgreSQL source profiles.\n\
         # TYPE pg2cb_sources_configured gauge\n\
         pg2cb_sources_configured {}\n\
         # HELP pg2cb_targets_configured Configured Cloudberry target profiles.\n\
         # TYPE pg2cb_targets_configured gauge\n\
         pg2cb_targets_configured {}\n\
         # HELP pg2cb_pipelines_configured Configured replication pipelines.\n\
         # TYPE pg2cb_pipelines_configured gauge\n\
         pg2cb_pipelines_configured {}\n\
         # HELP pg2cb_pipelines_desired_running Pipelines whose desired state is running.\n\
         # TYPE pg2cb_pipelines_desired_running gauge\n\
         pg2cb_pipelines_desired_running {}\n\
         # HELP pg2cb_pipelines_running Pipeline jobs currently owned by this process.\n\
         # TYPE pg2cb_pipelines_running gauge\n\
         pg2cb_pipelines_running {}\n",
        sources.len(),
        targets.len(),
        pipelines.len(),
        desired_running,
        running,
    );
    body.push_str(
        "# HELP pg2cb_pipeline_runtime_state Current runtime state.\n\
         # TYPE pg2cb_pipeline_runtime_state gauge\n\
         # HELP pg2cb_pipeline_phase Current replication phase.\n\
         # TYPE pg2cb_pipeline_phase gauge\n\
         # HELP pg2cb_pipeline_source_received_lsn Source WAL position observed on the wire.\n\
         # TYPE pg2cb_pipeline_source_received_lsn gauge\n\
         # HELP pg2cb_pipeline_source_current_lsn Latest committed source WAL position.\n\
         # TYPE pg2cb_pipeline_source_current_lsn gauge\n\
         # HELP pg2cb_pipeline_target_checkpoint_lsn Target-durable checkpoint WAL position.\n\
         # TYPE pg2cb_pipeline_target_checkpoint_lsn gauge\n\
         # HELP pg2cb_pipeline_estimated_byte_lag Estimated WAL byte distance.\n\
         # TYPE pg2cb_pipeline_estimated_byte_lag gauge\n\
         # HELP pg2cb_pipeline_spool_bytes Durable transaction spool bytes currently in use.\n\
         # TYPE pg2cb_pipeline_spool_bytes gauge\n\
         # HELP pg2cb_pipeline_resource_wait Whether the pipeline is waiting for a recoverable resource.\n\
         # TYPE pg2cb_pipeline_resource_wait gauge\n\
         # HELP pg2cb_pipeline_slot_retained_wal_bytes WAL bytes retained by the source logical slot.\n\
         # TYPE pg2cb_pipeline_slot_retained_wal_bytes gauge\n\
         # HELP pg2cb_pipeline_slot_safe_wal_bytes WAL bytes remaining before PostgreSQL may invalidate the slot.\n\
         # TYPE pg2cb_pipeline_slot_safe_wal_bytes gauge\n\
         # HELP pg2cb_pipeline_wal_retention_warning Whether source WAL retention crossed a warning threshold.\n\
         # TYPE pg2cb_pipeline_wal_retention_warning gauge\n\
         # HELP pg2cb_pipeline_last_transaction_timestamp_seconds Last source transaction time.\n\
         # TYPE pg2cb_pipeline_last_transaction_timestamp_seconds gauge\n\
         # HELP pg2cb_pipeline_last_apply_timestamp_seconds Last target apply time.\n\
         # TYPE pg2cb_pipeline_last_apply_timestamp_seconds gauge\n\
         # HELP pg2cb_pipeline_last_ack_timestamp_seconds Last source acknowledgement time.\n\
         # TYPE pg2cb_pipeline_last_ack_timestamp_seconds gauge\n\
         # HELP pg2cb_pipeline_restart_total Number of restarts after the first start.\n\
         # TYPE pg2cb_pipeline_restart_total counter\n\
         # HELP pg2cb_pipeline_last_error_info Whether a runtime error is present.\n\
         # TYPE pg2cb_pipeline_last_error_info gauge\n",
    );
    for pipeline in &pipelines {
        let Some(snapshot) = snapshots.get(&pipeline.id) else {
            continue;
        };
        let pipeline_id = escape_prometheus_label(&pipeline.id.to_string());
        body.push_str(&format!(
            "pg2cb_pipeline_runtime_state{{pipeline_id=\"{pipeline_id}\",state=\"{}\"}} 1\n\
             pg2cb_pipeline_phase{{pipeline_id=\"{pipeline_id}\",phase=\"{}\"}} 1\n",
            snapshot.state.as_str(),
            snapshot.phase_name(),
        ));
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_source_received_lsn",
            &pipeline_id,
            snapshot.source_received_lsn.map(|lsn| lsn.as_u64()),
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_source_current_lsn",
            &pipeline_id,
            snapshot.source_current_lsn.map(|lsn| lsn.as_u64()),
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_target_checkpoint_lsn",
            &pipeline_id,
            snapshot.target_checkpoint_lsn.map(|lsn| lsn.as_u64()),
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_estimated_byte_lag",
            &pipeline_id,
            snapshot.estimated_byte_lag,
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_spool_bytes",
            &pipeline_id,
            snapshot.spool_bytes,
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_slot_retained_wal_bytes",
            &pipeline_id,
            snapshot.slot_retained_wal_bytes,
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_slot_safe_wal_bytes",
            &pipeline_id,
            snapshot.slot_safe_wal_bytes,
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_last_transaction_timestamp_seconds",
            &pipeline_id,
            snapshot
                .last_transaction_at
                .map(|timestamp| timestamp.timestamp()),
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_last_apply_timestamp_seconds",
            &pipeline_id,
            snapshot
                .last_apply_at
                .map(|timestamp| timestamp.timestamp()),
        );
        append_optional_metric(
            &mut body,
            "pg2cb_pipeline_last_ack_timestamp_seconds",
            &pipeline_id,
            snapshot.last_ack_at.map(|timestamp| timestamp.timestamp()),
        );
        body.push_str(&format!(
            "pg2cb_pipeline_restart_total{{pipeline_id=\"{pipeline_id}\"}} {}\n\
             pg2cb_pipeline_last_error_info{{pipeline_id=\"{pipeline_id}\"}} {}\n\
             pg2cb_pipeline_resource_wait{{pipeline_id=\"{pipeline_id}\"}} {}\n\
             pg2cb_pipeline_wal_retention_warning{{pipeline_id=\"{pipeline_id}\"}} {}\n",
            snapshot.restart_count,
            u8::from(snapshot.last_error.is_some()),
            u8::from(matches!(
                snapshot.state,
                cloudberry_etl_engine::telemetry::PipelineRuntimeState::ResourceWait
            )),
            u8::from(snapshot.wal_retention_warning),
        ));
    }
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    Ok((headers, body))
}

fn append_optional_metric<T: Display>(
    body: &mut String,
    name: &str,
    pipeline_id: &str,
    value: Option<T>,
) {
    if let Some(value) = value {
        body.push_str(&format!(
            "{name}{{pipeline_id=\"{pipeline_id}\"}} {value}\n"
        ));
    }
}

fn escape_prometheus_label(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[derive(Debug, Serialize)]
struct ListResponse<T> {
    items: Vec<T>,
}

#[derive(Debug, Serialize)]
struct OverviewResponse {
    sources: usize,
    targets: usize,
    pipelines: usize,
    running_pipelines: usize,
}

async fn overview(State(state): State<AppState>) -> Result<Json<OverviewResponse>, ApiError> {
    let sources = state.control.list_sources().await.map_err(store_error)?;
    let targets = state.control.list_targets().await.map_err(store_error)?;
    let pipelines = state.control.list_pipelines().await.map_err(store_error)?;
    Ok(Json(OverviewResponse {
        sources: sources.len(),
        targets: targets.len(),
        pipelines: pipelines.len(),
        running_pipelines: state.supervisor.running().await.len(),
    }))
}

#[derive(Debug, Serialize)]
struct OperationView {
    id: String,
    pipeline_id: Option<PipelineId>,
    operation_type: String,
    state: String,
    detail: Value,
    runtime: Option<PipelineRuntimeSnapshot>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
}

async fn operations(
    State(state): State<AppState>,
) -> Result<Json<ListResponse<OperationView>>, ApiError> {
    let mut items = state
        .control
        .list_operations()
        .await
        .map_err(store_error)?
        .into_iter()
        .map(|operation| OperationView {
            id: operation.id.to_string(),
            pipeline_id: operation.pipeline_id,
            operation_type: operation.operation_type,
            state: operation.state,
            detail: operation.detail,
            runtime: None,
            created_at: Some(operation.created_at),
            updated_at: Some(operation.updated_at),
        })
        .collect::<Vec<_>>();
    items.extend(
        state
            .supervisor
            .runtime_snapshots()
            .await
            .into_iter()
            .map(|snapshot| {
                let updated_at = snapshot
                    .last_ack_at
                    .or(snapshot.last_apply_at)
                    .or(snapshot.last_transaction_at)
                    .or(snapshot.stopped_at)
                    .or(snapshot.started_at);
                OperationView {
                    id: format!("runtime:{}", snapshot.pipeline_id),
                    pipeline_id: Some(snapshot.pipeline_id),
                    operation_type: "replication".to_owned(),
                    state: snapshot.state.as_str().to_owned(),
                    detail: serde_json::to_value(&snapshot)
                        .unwrap_or_else(|_| Value::Object(Default::default())),
                    runtime: Some(snapshot),
                    created_at: None,
                    updated_at,
                }
            }),
    );
    Ok(Json(ListResponse { items }))
}

#[derive(Deserialize)]
struct CreateSourceRequest {
    name: String,
    prefix: String,
    database_name: String,
    topology: SourceTopology,
    dsn: SecretString,
    #[serde(default = "empty_object")]
    settings: Value,
}

#[derive(Debug, Serialize)]
struct SourceView {
    id: SourceId,
    name: String,
    prefix: String,
    database_name: String,
    topology: SourceTopology,
    settings: Value,
    enabled: bool,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl From<SourceProfile> for SourceView {
    fn from(source: SourceProfile) -> Self {
        Self {
            id: source.id,
            name: source.name,
            prefix: source.prefix.as_str().to_owned(),
            database_name: source.database_name,
            topology: source.topology,
            settings: source.settings,
            enabled: source.enabled,
            created_at: source.created_at,
            updated_at: source.updated_at,
        }
    }
}

async fn list_sources(
    State(state): State<AppState>,
) -> Result<Json<ListResponse<SourceView>>, ApiError> {
    let items = state
        .control
        .list_sources()
        .await
        .map_err(store_error)?
        .into_iter()
        .map(SourceView::from)
        .collect();
    Ok(Json(ListResponse { items }))
}

async fn create_source(
    State(state): State<AppState>,
    Json(request): Json<CreateSourceRequest>,
) -> Result<(StatusCode, Json<SourceView>), ApiError> {
    validate_name(&request.name)?;
    validate_name(&request.database_name)?;
    validate_object(&request.settings)?;
    SourceSettings::parse(&request.settings)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let id = SourceId::new();
    let now = Utc::now();
    let source = SourceProfile {
        id,
        name: request.name,
        prefix: SourcePrefix::new(request.prefix)
            .map_err(|error| ApiError::bad_request(error.to_string()))?,
        database_name: request.database_name,
        topology: request.topology,
        encrypted_dsn: state
            .master_key
            .encrypt(&request.dsn, source_credential_aad(id).as_bytes(), 1)
            .map_err(|error| {
                tracing::error!(error = %error, "failed to encrypt source credential");
                ApiError::internal()
            })?,
        settings: request.settings,
        enabled: true,
        created_at: now,
        updated_at: now,
    };
    state
        .control
        .put_source(&source)
        .await
        .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(source.into())))
}

#[derive(Deserialize)]
struct CreateTargetRequest {
    name: String,
    database_name: String,
    dsn: SecretString,
    #[serde(default = "empty_object")]
    settings: Value,
}

#[derive(Debug, Serialize)]
struct TargetView {
    id: TargetId,
    name: String,
    database_name: String,
    settings: Value,
    enabled: bool,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

impl From<TargetProfile> for TargetView {
    fn from(target: TargetProfile) -> Self {
        Self {
            id: target.id,
            name: target.name,
            database_name: target.database_name,
            settings: target.settings,
            enabled: target.enabled,
            created_at: target.created_at,
            updated_at: target.updated_at,
        }
    }
}

async fn list_targets(
    State(state): State<AppState>,
) -> Result<Json<ListResponse<TargetView>>, ApiError> {
    let items = state
        .control
        .list_targets()
        .await
        .map_err(store_error)?
        .into_iter()
        .map(TargetView::from)
        .collect();
    Ok(Json(ListResponse { items }))
}

async fn create_target(
    State(state): State<AppState>,
    Json(request): Json<CreateTargetRequest>,
) -> Result<(StatusCode, Json<TargetView>), ApiError> {
    validate_name(&request.name)?;
    validate_name(&request.database_name)?;
    validate_object(&request.settings)?;
    TargetSettings::parse(&request.settings)
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let id = TargetId::new();
    let now = Utc::now();
    let target = TargetProfile {
        id,
        name: request.name,
        database_name: request.database_name,
        encrypted_dsn: state
            .master_key
            .encrypt(&request.dsn, target_credential_aad(id).as_bytes(), 1)
            .map_err(|error| {
                tracing::error!(error = %error, "failed to encrypt target credential");
                ApiError::internal()
            })?,
        settings: request.settings,
        enabled: true,
        created_at: now,
        updated_at: now,
    };
    state
        .control
        .put_target(&target)
        .await
        .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(target.into())))
}

#[derive(Deserialize)]
struct TestConnectionRequest {
    dsn: SecretString,
}

async fn test_source(
    State(state): State<AppState>,
    Json(request): Json<TestConnectionRequest>,
) -> Result<Json<ConnectionReport>, ApiError> {
    let _permit = Arc::clone(&state.connection_test_gate)
        .try_acquire_owned()
        .map_err(|_| ApiError::unavailable("connection test capacity is exhausted"))?;
    tokio::time::timeout(
        CONNECTION_TEST_TIMEOUT,
        state.connection_tester.test_source(&request.dsn),
    )
    .await
    .map_err(|_| ApiError::unavailable("source connection test timed out"))?
    .map(Json)
    .map_err(ApiError::bad_request)
}

async fn test_target(
    State(state): State<AppState>,
    Json(request): Json<TestConnectionRequest>,
) -> Result<Json<ConnectionReport>, ApiError> {
    let _permit = Arc::clone(&state.connection_test_gate)
        .try_acquire_owned()
        .map_err(|_| ApiError::unavailable("connection test capacity is exhausted"))?;
    tokio::time::timeout(
        CONNECTION_TEST_TIMEOUT,
        state.connection_tester.test_target(&request.dsn),
    )
    .await
    .map_err(|_| ApiError::unavailable("target connection test timed out"))?
    .map(Json)
    .map_err(ApiError::bad_request)
}

#[derive(Deserialize)]
struct CreatePipelineRequest {
    name: String,
    source_id: SourceId,
    target_id: TargetId,
    #[serde(default = "empty_object")]
    settings: Value,
}

#[derive(Debug, Serialize)]
struct PipelineView {
    id: PipelineId,
    name: String,
    source_id: SourceId,
    target_id: TargetId,
    desired_running: bool,
    config_revision: i64,
    snapshot_generation: i64,
    settings: Value,
    runtime_state: String,
    runtime: Option<PipelineRuntimeSnapshot>,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

fn pipeline_view(
    pipeline: PipelineDefinition,
    running: bool,
    runtime: Option<PipelineRuntimeSnapshot>,
) -> PipelineView {
    let runtime_state = runtime.as_ref().map_or_else(
        || if running { "running" } else { "stopped" }.to_owned(),
        |snapshot| snapshot.state.as_str().to_owned(),
    );
    PipelineView {
        id: pipeline.id,
        name: pipeline.name,
        source_id: pipeline.source_id,
        target_id: pipeline.target_id,
        desired_running: pipeline.desired_running,
        config_revision: pipeline.config_revision,
        snapshot_generation: pipeline.snapshot_generation,
        settings: pipeline.settings,
        runtime_state,
        runtime,
        created_at: pipeline.created_at,
        updated_at: pipeline.updated_at,
    }
}

async fn list_pipelines(
    State(state): State<AppState>,
) -> Result<Json<ListResponse<PipelineView>>, ApiError> {
    let running = state.supervisor.running().await;
    let snapshots: HashMap<_, _> = state
        .supervisor
        .runtime_snapshots()
        .await
        .into_iter()
        .map(|snapshot| (snapshot.pipeline_id, snapshot))
        .collect();
    let items = state
        .control
        .list_pipelines()
        .await
        .map_err(store_error)?
        .into_iter()
        .map(|pipeline| {
            let pipeline_id = pipeline.id;
            let is_running = running.contains(&pipeline_id);
            pipeline_view(pipeline, is_running, snapshots.get(&pipeline_id).cloned())
        })
        .collect();
    Ok(Json(ListResponse { items }))
}

async fn create_pipeline(
    State(state): State<AppState>,
    Json(request): Json<CreatePipelineRequest>,
) -> Result<(StatusCode, Json<PipelineView>), ApiError> {
    validate_name(&request.name)?;
    validate_object(&request.settings)?;
    let sources = state.control.list_sources().await.map_err(store_error)?;
    let targets = state.control.list_targets().await.map_err(store_error)?;
    let source = sources
        .iter()
        .find(|source| source.id == request.source_id)
        .ok_or_else(|| ApiError::bad_request("source_id does not exist"))?;
    if !targets.iter().any(|target| target.id == request.target_id) {
        return Err(ApiError::bad_request("target_id does not exist"));
    }
    let source_settings = SourceSettings::parse(&source.settings)
        .map_err(|error| ApiError::bad_request(format!("source settings are invalid: {error}")))?;
    PipelineSettings::parse(&request.settings)
        .and_then(|settings| settings.validate_with_source(&source_settings))
        .map_err(|error| ApiError::bad_request(error.to_string()))?;
    let now = Utc::now();
    let pipeline = PipelineDefinition {
        id: PipelineId::new(),
        name: request.name,
        source_id: request.source_id,
        target_id: request.target_id,
        desired_running: false,
        config_revision: 1,
        snapshot_generation: 1,
        settings: request.settings,
        created_at: now,
        updated_at: now,
    };
    state
        .control
        .put_pipeline(&pipeline)
        .await
        .map_err(store_error)?;
    Ok((
        StatusCode::CREATED,
        Json(pipeline_view(pipeline, false, None)),
    ))
}

async fn get_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    let pipeline = find_pipeline(&state, id).await?;
    let running = state.supervisor.running().await.contains(&id);
    let runtime = state.supervisor.runtime_snapshot(id).await;
    Ok(Json(pipeline_view(pipeline, running, runtime)))
}

async fn start_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    set_pipeline_desired_running(&state, id, true).await
}

async fn pause_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    set_pipeline_desired_running(&state, id, false).await
}

async fn rebuild_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<(HeaderMap, Json<PipelineView>), ApiError> {
    let request = state
        .control
        .request_pipeline_rebuild(id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| ApiError::not_found("pipeline"))?;
    let running = state.supervisor.running().await.contains(&id);
    let mut headers = HeaderMap::new();
    headers.insert(
        "x-operation-id",
        HeaderValue::from_str(&request.operation_id.to_string()).map_err(|error| {
            tracing::error!(%error, "failed to encode rebuild operation id");
            ApiError::internal()
        })?,
    );
    let runtime = state.supervisor.runtime_snapshot(id).await;
    Ok((
        headers,
        Json(pipeline_view(request.pipeline, running, runtime)),
    ))
}

async fn set_pipeline_desired_running(
    state: &AppState,
    id: PipelineId,
    desired_running: bool,
) -> Result<Json<PipelineView>, ApiError> {
    let pipeline = state
        .control
        .set_pipeline_desired_running(id, desired_running)
        .await
        .map_err(store_error)?
        .ok_or_else(|| ApiError::not_found("pipeline"))?;
    let running = state.supervisor.running().await.contains(&id);
    let runtime = state.supervisor.runtime_snapshot(id).await;
    Ok(Json(pipeline_view(pipeline, running, runtime)))
}

async fn find_pipeline(state: &AppState, id: PipelineId) -> Result<PipelineDefinition, ApiError> {
    state
        .control
        .list_pipelines()
        .await
        .map_err(store_error)?
        .into_iter()
        .find(|pipeline| pipeline.id == id)
        .ok_or_else(|| ApiError::not_found("pipeline"))
}

fn validate_name(value: &str) -> Result<(), ApiError> {
    if value.trim().is_empty() || value.contains('\0') || value.len() > 128 {
        Err(ApiError::bad_request(
            "name must contain 1 to 128 valid characters",
        ))
    } else {
        Ok(())
    }
}

fn validate_object(value: &Value) -> Result<(), ApiError> {
    value
        .is_object()
        .then_some(())
        .ok_or_else(|| ApiError::bad_request("settings must be a JSON object"))
}

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

fn store_error(error: StoreError) -> ApiError {
    if let StoreError::Database(database) = &error
        && database.as_db_error().is_some_and(|detail| {
            detail.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION
        })
    {
        return ApiError::conflict("an object with the same identity already exists");
    }
    tracing::error!(error = %error, "control database request failed");
    ApiError::internal()
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use async_trait::async_trait;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use cloudberry_etl_core::{id::OperationId, lsn::PgLsn, pipeline::PipelinePhase};
    use cloudberry_etl_engine::supervisor::PipelineSupervisor;
    use cloudberry_etl_metadata::{
        crypto::MasterKey,
        model::{PipelineLease, RebuildRequest},
        store::ControlStore,
    };
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use super::*;
    use crate::state::ConnectionTester;

    struct CommandStore {
        pipeline: Mutex<PipelineDefinition>,
        ready: std::sync::atomic::AtomicBool,
    }

    #[async_trait]
    impl ControlStore for CommandStore {
        async fn check_readiness(&self) -> Result<(), StoreError> {
            if self.ready.load(std::sync::atomic::Ordering::SeqCst) {
                Ok(())
            } else {
                Err(StoreError::IncompatibleSchemaVersion {
                    expected: 2,
                    actual: 1,
                })
            }
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

        async fn put_pipeline(&self, pipeline: &PipelineDefinition) -> Result<(), StoreError> {
            *self.pipeline.lock().await = pipeline.clone();
            Ok(())
        }

        async fn list_pipelines(&self) -> Result<Vec<PipelineDefinition>, StoreError> {
            Ok(vec![self.pipeline.lock().await.clone()])
        }

        async fn set_pipeline_desired_running(
            &self,
            pipeline_id: PipelineId,
            desired_running: bool,
        ) -> Result<Option<PipelineDefinition>, StoreError> {
            let mut pipeline = self.pipeline.lock().await;
            if pipeline.id != pipeline_id {
                return Ok(None);
            }
            pipeline.desired_running = desired_running;
            pipeline.updated_at = Utc::now();
            Ok(Some(pipeline.clone()))
        }

        async fn request_pipeline_rebuild(
            &self,
            pipeline_id: PipelineId,
        ) -> Result<Option<RebuildRequest>, StoreError> {
            let mut pipeline = self.pipeline.lock().await;
            if pipeline.id != pipeline_id {
                return Ok(None);
            }
            pipeline.snapshot_generation += 1;
            pipeline.updated_at = Utc::now();
            Ok(Some(RebuildRequest {
                pipeline: pipeline.clone(),
                operation_id: OperationId::new(),
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
            _pipeline_id: PipelineId,
            _holder_id: Uuid,
            _ttl: Duration,
        ) -> Result<Option<PipelineLease>, StoreError> {
            Ok(None)
        }

        async fn renew_lease(
            &self,
            _lease: &PipelineLease,
            _ttl: Duration,
        ) -> Result<Option<PipelineLease>, StoreError> {
            Ok(None)
        }

        async fn release_lease(&self, _lease: &PipelineLease) -> Result<(), StoreError> {
            Ok(())
        }
    }

    struct NoopConnectionTester;

    #[async_trait]
    impl ConnectionTester for NoopConnectionTester {
        async fn test_source(&self, _dsn: &SecretString) -> Result<ConnectionReport, String> {
            unreachable!("connection tests are not part of command route coverage")
        }

        async fn test_target(&self, _dsn: &SecretString) -> Result<ConnectionReport, String> {
            unreachable!("connection tests are not part of command route coverage")
        }
    }

    fn command_state() -> (AppState, PipelineId, Arc<CommandStore>) {
        let now = Utc::now();
        let pipeline = PipelineDefinition {
            id: PipelineId::new(),
            name: "orders".into(),
            source_id: SourceId::new(),
            target_id: TargetId::new(),
            desired_running: false,
            config_revision: 7,
            snapshot_generation: 3,
            settings: serde_json::json!({"batch": {"max_rows": 100}}),
            created_at: now,
            updated_at: now,
        };
        let id = pipeline.id;
        let key = MasterKey::from_base64(&SecretString::from(STANDARD.encode([7_u8; 32])))
            .expect("test key is valid");
        let control = Arc::new(CommandStore {
            pipeline: Mutex::new(pipeline),
            ready: std::sync::atomic::AtomicBool::new(true),
        });
        (
            AppState {
                control: Arc::<CommandStore>::clone(&control),
                master_key: Arc::new(key),
                supervisor: Arc::new(PipelineSupervisor::new()),
                connection_tester: Arc::new(NoopConnectionTester),
                metrics_gate: Arc::new(tokio::sync::Semaphore::new(1)),
                connection_test_gate: Arc::new(tokio::sync::Semaphore::new(1)),
            },
            id,
            control,
        )
    }

    #[tokio::test]
    async fn start_and_pause_only_change_desired_state() {
        let (state, id, _) = command_state();

        let Json(started) = set_pipeline_desired_running(&state, id, true)
            .await
            .expect("start succeeds");
        assert!(started.desired_running);
        assert_eq!(started.config_revision, 7);
        assert_eq!(started.snapshot_generation, 3);

        let Json(paused) = set_pipeline_desired_running(&state, id, false)
            .await
            .expect("pause succeeds");
        assert!(!paused.desired_running);
        assert_eq!(paused.config_revision, 7);
        assert_eq!(paused.snapshot_generation, 3);
    }

    #[tokio::test]
    async fn readiness_checks_control_plane_not_local_pipeline_ownership() {
        let (state, id, control) = command_state();
        assert_eq!(ready(State(state.clone())).await, StatusCode::NO_CONTENT);

        let Json(_) = set_pipeline_desired_running(&state, id, true)
            .await
            .expect("start succeeds");
        assert_eq!(ready(State(state.clone())).await, StatusCode::NO_CONTENT);

        let telemetry = state.supervisor.telemetry_for(id).await;
        telemetry.mark_started();
        assert_eq!(ready(State(state.clone())).await, StatusCode::NO_CONTENT);
        telemetry.mark_degraded("source connection lost");
        assert_eq!(ready(State(state.clone())).await, StatusCode::NO_CONTENT);

        control
            .ready
            .store(false, std::sync::atomic::Ordering::SeqCst);
        assert_eq!(ready(State(state)).await, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rejects_overlapping_metrics_scrapes_before_querying_the_store() {
        let (state, _, _) = command_state();
        let _permit = Arc::clone(&state.metrics_gate)
            .acquire_owned()
            .await
            .expect("metrics gate is open");

        let Err(error) = metrics(State(state)).await else {
            panic!("overlapping scrape was accepted");
        };
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn rejects_connection_tests_when_the_bounded_worker_set_is_busy() {
        let (state, _, _) = command_state();
        let _permit = Arc::clone(&state.connection_test_gate)
            .acquire_owned()
            .await
            .expect("connection test gate is open");
        let request = TestConnectionRequest {
            dsn: SecretString::from("postgresql://unused.invalid/test"),
        };

        let error = test_source(State(state), Json(request))
            .await
            .expect_err("excess connection test is rejected");
        assert_eq!(
            error.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn rebuild_changes_only_snapshot_generation_and_returns_operation_id() {
        let (state, id, _) = command_state();

        let (headers, Json(rebuilt)) = rebuild_pipeline(State(state), Path(id))
            .await
            .expect("rebuild succeeds");
        assert_eq!(rebuilt.config_revision, 7);
        assert_eq!(rebuilt.snapshot_generation, 4);
        assert!(rebuilt.settings.get("rebuild_requested_at").is_none());
        let operation_id = headers
            .get("x-operation-id")
            .expect("operation header is present")
            .to_str()
            .expect("operation header is ASCII");
        assert!(Uuid::parse_str(operation_id).is_ok());
    }

    #[tokio::test]
    async fn exposes_runtime_progress_without_leaking_error_text_into_metrics() {
        let (state, id, _) = command_state();
        let telemetry = state.supervisor.telemetry_for(id).await;
        telemetry.mark_started();
        telemetry.mark_running();
        telemetry.set_phase(PipelinePhase::CatchingUp);
        telemetry.source_received(PgLsn::new(200));
        telemetry.transaction_received(PgLsn::new(180), Utc::now());
        telemetry.checkpoint_initialized(PgLsn::new(100));
        telemetry.applied(PgLsn::new(150));
        telemetry.acknowledged(PgLsn::new(150));
        telemetry.error("postgresql://admin:secret@example.invalid/private_table");

        let Json(view) = get_pipeline(State(state.clone()), Path(id))
            .await
            .expect("pipeline detail succeeds");
        let runtime = view.runtime.expect("runtime is included");
        assert_eq!(runtime.pipeline_id, id);
        assert_eq!(runtime.estimated_byte_lag, Some(50));
        assert_eq!(runtime.target_checkpoint_lsn, Some(PgLsn::new(150)));

        let Json(operation_list) = operations(State(state.clone()))
            .await
            .expect("operations succeeds");
        let operation = operation_list
            .items
            .into_iter()
            .find(|operation| operation.id == format!("runtime:{id}"))
            .expect("runtime operation is included");
        assert_eq!(operation.state, "running");
        assert_eq!(
            operation
                .runtime
                .expect("operation includes telemetry")
                .estimated_byte_lag,
            Some(50)
        );

        let response = metrics(State(state))
            .await
            .expect("metrics succeeds")
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body can be read");
        let body = String::from_utf8(body.to_vec()).expect("metrics are UTF-8");
        assert!(body.contains("pg2cb_pipeline_estimated_byte_lag"));
        assert!(body.contains(&format!("pipeline_id=\"{id}\"")));
        assert!(!body.contains("secret"));
        assert!(!body.contains("private_table"));
    }

    #[tokio::test]
    async fn exposes_recoverable_resource_wait_without_metric_reason_labels() {
        let (state, id, _) = command_state();
        let telemetry = state.supervisor.telemetry_for(id).await;
        telemetry.mark_started();
        telemetry.mark_running();
        telemetry.set_phase(PipelinePhase::CatchingUp);
        telemetry.spool_usage_observed(8192);
        telemetry.mark_resource_wait("spool path C:/private/pipeline is full");

        let Json(view) = get_pipeline(State(state.clone()), Path(id))
            .await
            .expect("pipeline detail succeeds");
        let runtime = view.runtime.expect("runtime is included");
        assert_eq!(runtime.state.as_str(), "resource_wait");
        assert_eq!(runtime.phase, PipelinePhase::CatchingUp);
        assert_eq!(runtime.spool_bytes, Some(8192));
        assert_eq!(
            runtime.resource_wait_reason.as_deref(),
            Some("spool path C:/private/pipeline is full")
        );
        assert!(runtime.last_error.is_none());

        let response = metrics(State(state))
            .await
            .expect("metrics succeeds")
            .into_response();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("metrics body can be read");
        let body = String::from_utf8(body.to_vec()).expect("metrics are UTF-8");
        assert!(body.contains(&format!(
            "pg2cb_pipeline_runtime_state{{pipeline_id=\"{id}\",state=\"resource_wait\"}} 1"
        )));
        assert!(body.contains(&format!(
            "pg2cb_pipeline_resource_wait{{pipeline_id=\"{id}\"}} 1"
        )));
        assert!(body.contains(&format!(
            "pg2cb_pipeline_spool_bytes{{pipeline_id=\"{id}\"}} 8192"
        )));
        assert!(!body.contains("private"));
        assert!(!body.contains("spool path"));
    }
}
