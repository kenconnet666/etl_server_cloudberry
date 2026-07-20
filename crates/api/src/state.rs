//! Shared API state.

use std::sync::Arc;

use async_trait::async_trait;
use cloudberry_etl_engine::supervisor::PipelineSupervisor;
use cloudberry_etl_metadata::{crypto::MasterKey, store::ControlStore};
use secrecy::SecretString;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct ConnectionReport {
    pub server_version: String,
    pub topology: String,
    pub warnings: Vec<String>,
}

#[async_trait]
pub trait ConnectionTester: Send + Sync {
    async fn test_source(&self, dsn: &SecretString) -> Result<ConnectionReport, String>;
    async fn test_target(&self, dsn: &SecretString) -> Result<ConnectionReport, String>;
}

#[derive(Clone)]
pub struct AppState {
    pub control: Arc<dyn ControlStore>,
    pub master_key: Arc<MasterKey>,
    pub supervisor: Arc<PipelineSupervisor>,
    pub connection_tester: Arc<dyn ConnectionTester>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AppState")
    }
}
