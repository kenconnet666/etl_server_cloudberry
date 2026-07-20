//! Control-plane persistence and fencing leases.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cloudberry_etl_core::{
    id::{PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
};
use thiserror::Error;
use tokio_postgres::{Client, Row};
use uuid::Uuid;

use crate::{
    crypto::EncryptedSecret,
    model::{PipelineDefinition, PipelineLease, SourceProfile, TargetProfile},
};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("control database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("invalid persisted source prefix: {0}")]
    InvalidPrefix(#[from] cloudberry_etl_core::CoreError),
    #[error("unknown source topology `{0}`")]
    InvalidTopology(String),
    #[error("lease duration is too large")]
    InvalidLeaseDuration,
    #[error("pipeline configuration revision is stale")]
    StaleRevision,
}

#[async_trait]
pub trait ControlStore: Send + Sync {
    async fn put_source(&self, source: &SourceProfile) -> Result<(), StoreError>;
    async fn list_sources(&self) -> Result<Vec<SourceProfile>, StoreError>;
    async fn put_target(&self, target: &TargetProfile) -> Result<(), StoreError>;
    async fn list_targets(&self) -> Result<Vec<TargetProfile>, StoreError>;
    async fn put_pipeline(&self, pipeline: &PipelineDefinition) -> Result<(), StoreError>;
    async fn list_pipelines(&self) -> Result<Vec<PipelineDefinition>, StoreError>;
    async fn try_acquire_lease(
        &self,
        pipeline_id: PipelineId,
        holder_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<PipelineLease>, StoreError>;
    async fn renew_lease(
        &self,
        lease: &PipelineLease,
        ttl: Duration,
    ) -> Result<Option<PipelineLease>, StoreError>;
    async fn release_lease(&self, lease: &PipelineLease) -> Result<(), StoreError>;
}

#[derive(Debug, Clone)]
pub struct PostgresControlStore {
    client: Arc<Client>,
}

impl PostgresControlStore {
    #[must_use]
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

#[async_trait]
impl ControlStore for PostgresControlStore {
    async fn put_source(&self, source: &SourceProfile) -> Result<(), StoreError> {
        self.client
            .execute(
                "INSERT INTO cloudberry_etl_control.sources (\
                    id, name, prefix, database_name, topology, dsn_key_version, dsn_nonce,\
                    dsn_ciphertext, settings, enabled, created_at, updated_at\
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)\
                 ON CONFLICT (id) DO UPDATE SET\
                    name=EXCLUDED.name, prefix=EXCLUDED.prefix, database_name=EXCLUDED.database_name,\
                    topology=EXCLUDED.topology, dsn_key_version=EXCLUDED.dsn_key_version,\
                    dsn_nonce=EXCLUDED.dsn_nonce, dsn_ciphertext=EXCLUDED.dsn_ciphertext,\
                    settings=EXCLUDED.settings, enabled=EXCLUDED.enabled, updated_at=clock_timestamp()",
                &[
                    &source.id.as_uuid(),
                    &source.name,
                    &source.prefix.as_str(),
                    &source.database_name,
                    &topology_name(source.topology),
                    &(source.encrypted_dsn.key_version as i32),
                    &source.encrypted_dsn.nonce,
                    &source.encrypted_dsn.ciphertext,
                    &source.settings,
                    &source.enabled,
                    &source.created_at,
                    &source.updated_at,
                ],
            )
            .await?;
        Ok(())
    }

    async fn list_sources(&self) -> Result<Vec<SourceProfile>, StoreError> {
        self.client
            .query(
                "SELECT id,name,prefix,database_name,topology,dsn_key_version,dsn_nonce,\
                        dsn_ciphertext,settings,enabled,created_at,updated_at\
                 FROM cloudberry_etl_control.sources ORDER BY name",
                &[],
            )
            .await?
            .into_iter()
            .map(source_from_row)
            .collect()
    }

    async fn put_target(&self, target: &TargetProfile) -> Result<(), StoreError> {
        self.client
            .execute(
                "INSERT INTO cloudberry_etl_control.targets (\
                    id,name,database_name,dsn_key_version,dsn_nonce,dsn_ciphertext,settings,enabled,created_at,updated_at\
                 ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)\
                 ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name,database_name=EXCLUDED.database_name,\
                    dsn_key_version=EXCLUDED.dsn_key_version,dsn_nonce=EXCLUDED.dsn_nonce,\
                    dsn_ciphertext=EXCLUDED.dsn_ciphertext,settings=EXCLUDED.settings,\
                    enabled=EXCLUDED.enabled,updated_at=clock_timestamp()",
                &[
                    &target.id.as_uuid(),
                    &target.name,
                    &target.database_name,
                    &(target.encrypted_dsn.key_version as i32),
                    &target.encrypted_dsn.nonce,
                    &target.encrypted_dsn.ciphertext,
                    &target.settings,
                    &target.enabled,
                    &target.created_at,
                    &target.updated_at,
                ],
            )
            .await?;
        Ok(())
    }

    async fn list_targets(&self) -> Result<Vec<TargetProfile>, StoreError> {
        Ok(self
            .client
            .query(
                "SELECT id,name,database_name,dsn_key_version,dsn_nonce,dsn_ciphertext,\
                        settings,enabled,created_at,updated_at\
                 FROM cloudberry_etl_control.targets ORDER BY name",
                &[],
            )
            .await?
            .into_iter()
            .map(target_from_row)
            .collect())
    }

    async fn put_pipeline(&self, pipeline: &PipelineDefinition) -> Result<(), StoreError> {
        let definition = serde_json::json!({
            "id": pipeline.id,
            "name": pipeline.name,
            "source_id": pipeline.source_id,
            "target_id": pipeline.target_id,
            "desired_running": pipeline.desired_running,
            "config_revision": pipeline.config_revision,
            "settings": pipeline.settings,
        });
        let affected = self
            .client
            .execute(
                "WITH changed AS (\
                    INSERT INTO cloudberry_etl_control.pipelines (\
                        id,name,source_id,target_id,desired_running,config_revision,settings,created_at,updated_at\
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)\
                    ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name,source_id=EXCLUDED.source_id,\
                        target_id=EXCLUDED.target_id,desired_running=EXCLUDED.desired_running,\
                        config_revision=EXCLUDED.config_revision,settings=EXCLUDED.settings,\
                        updated_at=clock_timestamp()\
                    WHERE cloudberry_etl_control.pipelines.config_revision < EXCLUDED.config_revision\
                    RETURNING id,config_revision\
                 )\
                 INSERT INTO cloudberry_etl_control.config_revisions (pipeline_id,revision,definition)\
                 SELECT id,config_revision,$10 FROM changed ON CONFLICT DO NOTHING",
                &[
                    &pipeline.id.as_uuid(),
                    &pipeline.name,
                    &pipeline.source_id.as_uuid(),
                    &pipeline.target_id.as_uuid(),
                    &pipeline.desired_running,
                    &pipeline.config_revision,
                    &pipeline.settings,
                    &pipeline.created_at,
                    &pipeline.updated_at,
                    &definition,
                ],
            )
            .await?;
        if affected == 0 {
            return Err(StoreError::StaleRevision);
        }
        Ok(())
    }

    async fn list_pipelines(&self) -> Result<Vec<PipelineDefinition>, StoreError> {
        Ok(self
            .client
            .query(
                "SELECT id,name,source_id,target_id,desired_running,config_revision,settings,created_at,updated_at\
                 FROM cloudberry_etl_control.pipelines ORDER BY name",
                &[],
            )
            .await?
            .into_iter()
            .map(pipeline_from_row)
            .collect())
    }

    async fn try_acquire_lease(
        &self,
        pipeline_id: PipelineId,
        holder_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<PipelineLease>, StoreError> {
        let ttl_seconds = duration_seconds(ttl)?;
        let row = self
            .client
            .query_opt(
                "INSERT INTO cloudberry_etl_control.pipeline_leases (\
                    pipeline_id,holder_id,fencing_token,expires_at\
                 ) VALUES ($1,$2,1,clock_timestamp() + $3 * interval '1 second')\
                 ON CONFLICT (pipeline_id) DO UPDATE SET\
                    holder_id=EXCLUDED.holder_id,\
                    fencing_token=cloudberry_etl_control.pipeline_leases.fencing_token + 1,\
                    expires_at=EXCLUDED.expires_at,updated_at=clock_timestamp()\
                 WHERE cloudberry_etl_control.pipeline_leases.expires_at <= clock_timestamp()\
                    OR cloudberry_etl_control.pipeline_leases.holder_id = EXCLUDED.holder_id\
                 RETURNING pipeline_id,holder_id,fencing_token,expires_at",
                &[&pipeline_id.as_uuid(), &holder_id, &ttl_seconds],
            )
            .await?;
        Ok(row.map(lease_from_row))
    }

    async fn renew_lease(
        &self,
        lease: &PipelineLease,
        ttl: Duration,
    ) -> Result<Option<PipelineLease>, StoreError> {
        let ttl_seconds = duration_seconds(ttl)?;
        let row = self
            .client
            .query_opt(
                "UPDATE cloudberry_etl_control.pipeline_leases SET\
                    expires_at=clock_timestamp() + $4 * interval '1 second',updated_at=clock_timestamp()\
                 WHERE pipeline_id=$1 AND holder_id=$2 AND fencing_token=$3\
                    AND expires_at > clock_timestamp()\
                 RETURNING pipeline_id,holder_id,fencing_token,expires_at",
                &[
                    &lease.pipeline_id.as_uuid(),
                    &lease.holder_id,
                    &lease.fencing_token,
                    &ttl_seconds,
                ],
            )
            .await?;
        Ok(row.map(lease_from_row))
    }

    async fn release_lease(&self, lease: &PipelineLease) -> Result<(), StoreError> {
        self.client
            .execute(
                "DELETE FROM cloudberry_etl_control.pipeline_leases\
                 WHERE pipeline_id=$1 AND holder_id=$2 AND fencing_token=$3",
                &[
                    &lease.pipeline_id.as_uuid(),
                    &lease.holder_id,
                    &lease.fencing_token,
                ],
            )
            .await?;
        Ok(())
    }
}

fn duration_seconds(duration: Duration) -> Result<f64, StoreError> {
    let seconds = duration.as_secs_f64();
    if seconds.is_finite() && seconds > 0.0 && seconds <= 86_400.0 {
        Ok(seconds)
    } else {
        Err(StoreError::InvalidLeaseDuration)
    }
}

fn topology_name(topology: SourceTopology) -> &'static str {
    match topology {
        SourceTopology::Standalone => "standalone",
        SourceTopology::PhysicalHa => "physical_ha",
        SourceTopology::Citus => "citus",
    }
}

fn topology_from_name(value: &str) -> Result<SourceTopology, StoreError> {
    match value {
        "standalone" => Ok(SourceTopology::Standalone),
        "physical_ha" => Ok(SourceTopology::PhysicalHa),
        "citus" => Ok(SourceTopology::Citus),
        other => Err(StoreError::InvalidTopology(other.to_owned())),
    }
}

fn source_from_row(row: Row) -> Result<SourceProfile, StoreError> {
    Ok(SourceProfile {
        id: SourceId::from_uuid(row.get("id")),
        name: row.get("name"),
        prefix: SourcePrefix::new(row.get::<_, String>("prefix"))?,
        database_name: row.get("database_name"),
        topology: topology_from_name(row.get("topology"))?,
        encrypted_dsn: EncryptedSecret {
            key_version: row.get::<_, i32>("dsn_key_version") as u32,
            nonce: row.get("dsn_nonce"),
            ciphertext: row.get("dsn_ciphertext"),
        },
        settings: row.get("settings"),
        enabled: row.get("enabled"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn target_from_row(row: Row) -> TargetProfile {
    TargetProfile {
        id: TargetId::from_uuid(row.get("id")),
        name: row.get("name"),
        database_name: row.get("database_name"),
        encrypted_dsn: EncryptedSecret {
            key_version: row.get::<_, i32>("dsn_key_version") as u32,
            nonce: row.get("dsn_nonce"),
            ciphertext: row.get("dsn_ciphertext"),
        },
        settings: row.get("settings"),
        enabled: row.get("enabled"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn pipeline_from_row(row: Row) -> PipelineDefinition {
    PipelineDefinition {
        id: PipelineId::from_uuid(row.get("id")),
        name: row.get("name"),
        source_id: SourceId::from_uuid(row.get("source_id")),
        target_id: TargetId::from_uuid(row.get("target_id")),
        desired_running: row.get("desired_running"),
        config_revision: row.get("config_revision"),
        settings: row.get("settings"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn lease_from_row(row: Row) -> PipelineLease {
    PipelineLease {
        pipeline_id: PipelineId::from_uuid(row.get("pipeline_id")),
        holder_id: row.get("holder_id"),
        fencing_token: row.get("fencing_token"),
        expires_at: row.get::<_, DateTime<Utc>>("expires_at"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_lease_duration() {
        assert!(duration_seconds(Duration::from_secs(1)).is_ok());
        assert!(duration_seconds(Duration::ZERO).is_err());
        assert!(duration_seconds(Duration::from_secs(86_401)).is_err());
    }
}
