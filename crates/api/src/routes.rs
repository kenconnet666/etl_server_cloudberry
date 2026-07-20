//! Management API routes.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    middleware,
    routing::{get, post},
};
use chrono::Utc;
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
};
use cloudberry_etl_metadata::{
    model::{PipelineDefinition, SourceProfile, TargetProfile},
    store::StoreError,
};
use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    auth::{AuthState, current_session, login, logout, require_session},
    error::ApiError,
    state::{AppState, ConnectionReport},
};

pub fn router(state: AppState, auth: AuthState) -> Router {
    let auth_middleware = middleware::from_fn_with_state(auth.clone(), require_session);
    let authenticated_auth = Router::new()
        .route("/api/v1/auth/session", get(current_session))
        .route("/api/v1/auth/logout", post(logout))
        .route_layer(auth_middleware.clone())
        .with_state(auth.clone());
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
        .merge(authenticated_auth)
        .merge(authenticated_api)
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
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
    let (sources, targets, pipelines) = tokio::try_join!(
        state.control.list_sources(),
        state.control.list_targets(),
        state.control.list_pipelines()
    )
    .map_err(store_error)?;
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
    operation_type: &'static str,
    state: &'static str,
}

async fn operations(State(state): State<AppState>) -> Json<ListResponse<OperationView>> {
    let items = state
        .supervisor
        .running()
        .await
        .into_iter()
        .map(|id| OperationView {
            id: id.to_string(),
            operation_type: "replication",
            state: "running",
        })
        .collect();
    Json(ListResponse { items })
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
            .encrypt(
                &request.dsn,
                associated_data("source", id.to_string()).as_bytes(),
                1,
            )
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
    let id = TargetId::new();
    let now = Utc::now();
    let target = TargetProfile {
        id,
        name: request.name,
        database_name: request.database_name,
        encrypted_dsn: state
            .master_key
            .encrypt(
                &request.dsn,
                associated_data("target", id.to_string()).as_bytes(),
                1,
            )
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
    state
        .connection_tester
        .test_source(&request.dsn)
        .await
        .map(Json)
        .map_err(ApiError::bad_request)
}

async fn test_target(
    State(state): State<AppState>,
    Json(request): Json<TestConnectionRequest>,
) -> Result<Json<ConnectionReport>, ApiError> {
    state
        .connection_tester
        .test_target(&request.dsn)
        .await
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
    settings: Value,
    runtime_state: &'static str,
    created_at: chrono::DateTime<Utc>,
    updated_at: chrono::DateTime<Utc>,
}

fn pipeline_view(pipeline: PipelineDefinition, running: bool) -> PipelineView {
    PipelineView {
        id: pipeline.id,
        name: pipeline.name,
        source_id: pipeline.source_id,
        target_id: pipeline.target_id,
        desired_running: pipeline.desired_running,
        config_revision: pipeline.config_revision,
        settings: pipeline.settings,
        runtime_state: if running { "running" } else { "stopped" },
        created_at: pipeline.created_at,
        updated_at: pipeline.updated_at,
    }
}

async fn list_pipelines(
    State(state): State<AppState>,
) -> Result<Json<ListResponse<PipelineView>>, ApiError> {
    let running = state.supervisor.running().await;
    let items = state
        .control
        .list_pipelines()
        .await
        .map_err(store_error)?
        .into_iter()
        .map(|pipeline| {
            let is_running = running.contains(&pipeline.id);
            pipeline_view(pipeline, is_running)
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
    if !sources.iter().any(|source| source.id == request.source_id) {
        return Err(ApiError::bad_request("source_id does not exist"));
    }
    if !targets.iter().any(|target| target.id == request.target_id) {
        return Err(ApiError::bad_request("target_id does not exist"));
    }
    let now = Utc::now();
    let pipeline = PipelineDefinition {
        id: PipelineId::new(),
        name: request.name,
        source_id: request.source_id,
        target_id: request.target_id,
        desired_running: false,
        config_revision: 1,
        settings: request.settings,
        created_at: now,
        updated_at: now,
    };
    state
        .control
        .put_pipeline(&pipeline)
        .await
        .map_err(store_error)?;
    Ok((StatusCode::CREATED, Json(pipeline_view(pipeline, false))))
}

async fn get_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    let pipeline = find_pipeline(&state, id).await?;
    let running = state.supervisor.running().await.contains(&id);
    Ok(Json(pipeline_view(pipeline, running)))
}

async fn start_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    update_pipeline_desire(&state, id, PipelineCommand::Start).await
}

async fn pause_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    update_pipeline_desire(&state, id, PipelineCommand::Pause).await
}

async fn rebuild_pipeline(
    State(state): State<AppState>,
    Path(id): Path<PipelineId>,
) -> Result<Json<PipelineView>, ApiError> {
    update_pipeline_desire(&state, id, PipelineCommand::Rebuild).await
}

enum PipelineCommand {
    Start,
    Pause,
    Rebuild,
}

async fn update_pipeline_desire(
    state: &AppState,
    id: PipelineId,
    command: PipelineCommand,
) -> Result<Json<PipelineView>, ApiError> {
    let mut pipeline = find_pipeline(state, id).await?;
    match command {
        PipelineCommand::Start => pipeline.desired_running = true,
        PipelineCommand::Pause => pipeline.desired_running = false,
        PipelineCommand::Rebuild => {
            pipeline.settings["rebuild_requested_at"] = json!(Utc::now());
        }
    }
    pipeline.config_revision += 1;
    pipeline.updated_at = Utc::now();
    state
        .control
        .put_pipeline(&pipeline)
        .await
        .map_err(store_error)?;
    let running = state.supervisor.running().await.contains(&id);
    Ok(Json(pipeline_view(pipeline, running)))
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

fn associated_data(kind: &str, id: String) -> String {
    format!("cloudberry-etl:{kind}:{id}")
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
