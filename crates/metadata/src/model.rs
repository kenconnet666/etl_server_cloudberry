//! Persistent control-plane records.

use chrono::{DateTime, Utc};
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::crypto::EncryptedSecret;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceProfile {
    pub id: SourceId,
    pub name: String,
    pub prefix: SourcePrefix,
    pub database_name: String,
    pub topology: SourceTopology,
    pub encrypted_dsn: EncryptedSecret,
    pub settings: Value,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetProfile {
    pub id: TargetId,
    pub name: String,
    pub database_name: String,
    pub encrypted_dsn: EncryptedSecret,
    pub settings: Value,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineDefinition {
    pub id: PipelineId,
    pub name: String,
    pub source_id: SourceId,
    pub target_id: TargetId,
    pub desired_running: bool,
    pub config_revision: i64,
    pub settings: Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineLease {
    pub pipeline_id: PipelineId,
    pub holder_id: Uuid,
    pub fencing_token: i64,
    pub expires_at: DateTime<Utc>,
}
