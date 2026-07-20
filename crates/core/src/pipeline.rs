use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{id::PipelineId, lsn::PgLsn};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceTopology {
    Standalone,
    PhysicalHa,
    Citus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PipelinePhase {
    Draft,
    Validating,
    Snapshotting,
    CatchingUp,
    Running,
    Paused,
    Degraded,
    Failed,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TablePhase {
    Pending,
    Snapshotting,
    CatchingUp,
    Running,
    RebuildRequired,
    Rebuilding,
    Blocked,
    Quarantined,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeCheckpoint {
    pub node_id: i32,
    pub system_identifier: u64,
    pub timeline: u32,
    pub slot_name: String,
    pub received_lsn: PgLsn,
    pub applied_lsn: PgLsn,
    pub flushed_lsn: PgLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PipelineStatus {
    pub pipeline_id: PipelineId,
    pub phase: PipelinePhase,
    pub config_revision: i64,
    pub updated_at: DateTime<Utc>,
    pub detail: Option<String>,
    pub checkpoints: Vec<NodeCheckpoint>,
}
