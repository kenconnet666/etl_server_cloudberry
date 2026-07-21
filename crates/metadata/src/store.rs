//! Control-plane persistence and fencing leases.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cloudberry_etl_core::{
    id::{OperationId, PipelineId, SourceId, TargetId},
    mapping::SourcePrefix,
    pipeline::SourceTopology,
};
use deadpool_postgres::{Client as PoolClient, Pool};
use thiserror::Error;
use tokio_postgres::Row;
use uuid::Uuid;

use crate::{
    crypto::EncryptedSecret,
    migration::CONTROL_SCHEMA_VERSION,
    model::{
        OperationRecord, PipelineDefinition, PipelineLease, RebuildRequest, SourceProfile,
        TargetProfile,
    },
};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("control database operation failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("control database connection pool failed: {0}")]
    Pool(#[from] deadpool_postgres::PoolError),
    #[error("invalid persisted source prefix: {0}")]
    InvalidPrefix(#[from] cloudberry_etl_core::CoreError),
    #[error("unknown source topology `{0}`")]
    InvalidTopology(String),
    #[error("lease duration is too large")]
    InvalidLeaseDuration,
    #[error("pipeline configuration revision is stale")]
    StaleRevision,
    #[error("control database schema version {actual} is incompatible; expected {expected}")]
    IncompatibleSchemaVersion { expected: i64, actual: i64 },
    #[error("control database is read-only")]
    ReadOnly,
}

const CONTROL_SESSION_OPTIONS: &str = "-c statement_timeout=5000 \
    -c lock_timeout=2000 \
    -c idle_in_transaction_session_timeout=5000";

/// Applies bounded server-side execution time to every pooled control session.
/// Existing connection options are retained, while these final values enforce
/// the limits required by lease renewal and HTTP cancellation semantics.
pub fn configure_control_session(config: &mut tokio_postgres::Config) {
    let mut options = config.get_options().unwrap_or_default().to_owned();
    if !options.is_empty() {
        options.push(' ');
    }
    options.push_str(CONTROL_SESSION_OPTIONS);
    config.options(options);
    if config.get_application_name().is_none() {
        config.application_name("pg2cb-control");
    }
}

#[async_trait]
pub trait ControlStore: Send + Sync {
    async fn check_readiness(&self) -> Result<(), StoreError>;
    async fn put_source(&self, source: &SourceProfile) -> Result<(), StoreError>;
    async fn list_sources(&self) -> Result<Vec<SourceProfile>, StoreError>;
    async fn put_target(&self, target: &TargetProfile) -> Result<(), StoreError>;
    async fn list_targets(&self) -> Result<Vec<TargetProfile>, StoreError>;
    async fn put_pipeline(&self, pipeline: &PipelineDefinition) -> Result<(), StoreError>;
    async fn list_pipelines(&self) -> Result<Vec<PipelineDefinition>, StoreError>;
    async fn set_pipeline_desired_running(
        &self,
        pipeline_id: PipelineId,
        desired_running: bool,
    ) -> Result<Option<PipelineDefinition>, StoreError>;
    async fn request_pipeline_rebuild(
        &self,
        pipeline_id: PipelineId,
    ) -> Result<Option<RebuildRequest>, StoreError>;
    async fn complete_pipeline_rebuilds(
        &self,
        pipeline_id: PipelineId,
        snapshot_generation: i64,
    ) -> Result<u64, StoreError>;
    async fn list_operations(&self) -> Result<Vec<OperationRecord>, StoreError>;
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
    pool: Pool,
    lease_pool: Pool,
}

impl PostgresControlStore {
    #[must_use]
    pub fn new(pool: Pool) -> Self {
        Self {
            lease_pool: pool.clone(),
            pool,
        }
    }

    /// Uses a separately sized pool for lease operations so control-plane traffic cannot delay
    /// fencing renewal. Both pools must connect to the same control database and role.
    #[must_use]
    pub fn with_lease_pool(pool: Pool, lease_pool: Pool) -> Self {
        Self { pool, lease_pool }
    }

    async fn client(&self) -> Result<PoolClient, StoreError> {
        Ok(self.pool.get().await?)
    }

    async fn lease_client(&self) -> Result<PoolClient, StoreError> {
        Ok(self.lease_pool.get().await?)
    }
}

#[async_trait]
impl ControlStore for PostgresControlStore {
    async fn check_readiness(&self) -> Result<(), StoreError> {
        let client = self.client().await?;
        let row = client
            .query_one(
                "SELECT COALESCE(max(version), 0), \
                        current_setting('transaction_read_only')::boolean \
                   FROM cloudberry_etl_control.schema_migrations",
                &[],
            )
            .await?;
        let actual: i64 = row.get(0);
        if actual != CONTROL_SCHEMA_VERSION {
            return Err(StoreError::IncompatibleSchemaVersion {
                expected: CONTROL_SCHEMA_VERSION,
                actual,
            });
        }
        if row.get::<_, bool>(1) {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    async fn put_source(&self, source: &SourceProfile) -> Result<(), StoreError> {
        let client = self.client().await?;
        client
            .execute(
                r#"INSERT INTO cloudberry_etl_control.sources (
                    id, name, prefix, database_name, topology, dsn_key_version, dsn_nonce,
                    dsn_ciphertext, settings, enabled, created_at, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name, prefix=EXCLUDED.prefix, database_name=EXCLUDED.database_name,
                    topology=EXCLUDED.topology, dsn_key_version=EXCLUDED.dsn_key_version,
                    dsn_nonce=EXCLUDED.dsn_nonce, dsn_ciphertext=EXCLUDED.dsn_ciphertext,
                    settings=EXCLUDED.settings, enabled=EXCLUDED.enabled,
                    updated_at=clock_timestamp()"#,
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
        let client = self.client().await?;
        client
            .query(
                r#"SELECT id,name,prefix,database_name,topology,dsn_key_version,dsn_nonce,
                          dsn_ciphertext,settings,enabled,created_at,updated_at
                   FROM cloudberry_etl_control.sources
                   ORDER BY name"#,
                &[],
            )
            .await?
            .into_iter()
            .map(source_from_row)
            .collect()
    }

    async fn put_target(&self, target: &TargetProfile) -> Result<(), StoreError> {
        let client = self.client().await?;
        client
            .execute(
                r#"INSERT INTO cloudberry_etl_control.targets (
                    id,name,database_name,dsn_key_version,dsn_nonce,dsn_ciphertext,
                    settings,enabled,created_at,updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                ON CONFLICT (id) DO UPDATE SET
                    name=EXCLUDED.name,database_name=EXCLUDED.database_name,
                    dsn_key_version=EXCLUDED.dsn_key_version,dsn_nonce=EXCLUDED.dsn_nonce,
                    dsn_ciphertext=EXCLUDED.dsn_ciphertext,settings=EXCLUDED.settings,
                    enabled=EXCLUDED.enabled,updated_at=clock_timestamp()"#,
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
        let client = self.client().await?;
        Ok(client
            .query(
                r#"SELECT id,name,database_name,dsn_key_version,dsn_nonce,dsn_ciphertext,
                          settings,enabled,created_at,updated_at
                   FROM cloudberry_etl_control.targets
                   ORDER BY name"#,
                &[],
            )
            .await?
            .into_iter()
            .map(target_from_row)
            .collect())
    }

    async fn put_pipeline(&self, pipeline: &PipelineDefinition) -> Result<(), StoreError> {
        let client = self.client().await?;
        let definition = serde_json::json!({
            "id": pipeline.id,
            "name": pipeline.name,
            "source_id": pipeline.source_id,
            "target_id": pipeline.target_id,
            "config_revision": pipeline.config_revision,
            "snapshot_generation": pipeline.snapshot_generation,
            "settings": pipeline.settings,
        });
        let affected = client
            .execute(
                r#"WITH changed AS (
                    INSERT INTO cloudberry_etl_control.pipelines (
                        id,name,source_id,target_id,desired_running,config_revision,
                        snapshot_generation,settings,created_at,updated_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
                    ON CONFLICT (id) DO UPDATE SET
                        name=EXCLUDED.name,source_id=EXCLUDED.source_id,
                        target_id=EXCLUDED.target_id,
                        config_revision=EXCLUDED.config_revision,
                        snapshot_generation=EXCLUDED.snapshot_generation,
                        settings=EXCLUDED.settings,
                        updated_at=clock_timestamp()
                    WHERE cloudberry_etl_control.pipelines.config_revision < EXCLUDED.config_revision
                      AND cloudberry_etl_control.pipelines.snapshot_generation < EXCLUDED.snapshot_generation
                    RETURNING id,config_revision
                )
                INSERT INTO cloudberry_etl_control.config_revisions (pipeline_id,revision,definition)
                SELECT id,config_revision,$11 FROM changed
                ON CONFLICT DO NOTHING"#,
                &[
                    &pipeline.id.as_uuid(),
                    &pipeline.name,
                    &pipeline.source_id.as_uuid(),
                    &pipeline.target_id.as_uuid(),
                    &pipeline.desired_running,
                    &pipeline.config_revision,
                    &pipeline.snapshot_generation,
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
        let client = self.client().await?;
        Ok(client
            .query(
                r#"SELECT id,name,source_id,target_id,desired_running,config_revision,
                          snapshot_generation,settings,created_at,updated_at
                   FROM cloudberry_etl_control.pipelines
                   ORDER BY name"#,
                &[],
            )
            .await?
            .into_iter()
            .map(pipeline_from_row)
            .collect())
    }

    async fn set_pipeline_desired_running(
        &self,
        pipeline_id: PipelineId,
        desired_running: bool,
    ) -> Result<Option<PipelineDefinition>, StoreError> {
        let client = self.client().await?;
        let row = client
            .query_opt(
                r#"UPDATE cloudberry_etl_control.pipelines
                      SET desired_running=$2, updated_at=clock_timestamp()
                    WHERE id=$1
                RETURNING id,name,source_id,target_id,desired_running,config_revision,
                          snapshot_generation,settings,created_at,updated_at"#,
                &[&pipeline_id.as_uuid(), &desired_running],
            )
            .await?;
        Ok(row.map(pipeline_from_row))
    }

    async fn request_pipeline_rebuild(
        &self,
        pipeline_id: PipelineId,
    ) -> Result<Option<RebuildRequest>, StoreError> {
        let client = self.client().await?;
        let operation_id = OperationId::new();
        let row = client
            .query_opt(
                r#"WITH bumped AS (
                         UPDATE cloudberry_etl_control.pipelines
                            SET snapshot_generation=snapshot_generation + 1,
                                updated_at=clock_timestamp()
                          WHERE id=$1
                         RETURNING id,name,source_id,target_id,desired_running,config_revision,
                                   snapshot_generation,settings,created_at,updated_at
                     ), recorded AS (
                         INSERT INTO cloudberry_etl_control.operations
                             (id,pipeline_id,operation_type,state,detail)
                         SELECT $2,id,'rebuild','requested',
                                jsonb_build_object('snapshot_generation', snapshot_generation)
                           FROM bumped
                         RETURNING id
                     ), audited AS (
                         INSERT INTO cloudberry_etl_control.audit_log
                             (action,object_type,object_id,detail)
                         SELECT 'pipeline.rebuild_requested','pipeline',id::text,
                                jsonb_build_object(
                                    'operation_id', $2::uuid,
                                    'snapshot_generation', snapshot_generation
                                )
                           FROM bumped
                         RETURNING id
                     )
                     SELECT bumped.id,bumped.name,bumped.source_id,bumped.target_id,
                            bumped.desired_running,bumped.config_revision,
                            bumped.snapshot_generation,bumped.settings,bumped.created_at,
                            bumped.updated_at,recorded.id AS operation_id
                       FROM bumped
                       JOIN recorded ON true"#,
                &[&pipeline_id.as_uuid(), &operation_id.as_uuid()],
            )
            .await?;
        Ok(row.map(|row| {
            let operation_id = OperationId::from_uuid(row.get("operation_id"));
            RebuildRequest {
                pipeline: pipeline_from_row(row),
                operation_id,
            }
        }))
    }

    async fn complete_pipeline_rebuilds(
        &self,
        pipeline_id: PipelineId,
        snapshot_generation: i64,
    ) -> Result<u64, StoreError> {
        let client = self.client().await?;
        let row = client
            .query_one(
                r#"WITH completed AS (
                       UPDATE cloudberry_etl_control.operations
                          SET state='completed',
                              detail=detail || jsonb_build_object(
                                  'completed_snapshot_generation', $2::bigint
                              ),
                              updated_at=clock_timestamp()
                        WHERE pipeline_id=$1
                          AND operation_type='rebuild'
                          AND state='requested'
                          AND jsonb_typeof(detail->'snapshot_generation')='number'
                          AND (detail->>'snapshot_generation')::bigint <= $2
                      RETURNING id
                   ), audited AS (
                       INSERT INTO cloudberry_etl_control.audit_log
                           (action,object_type,object_id,detail)
                       SELECT 'pipeline.rebuild_completed','pipeline',$1::uuid::text,
                              jsonb_build_object(
                                  'operation_id', id,
                                  'snapshot_generation', $2::bigint
                              )
                         FROM completed
                       RETURNING id
                   )
                   SELECT count(*)::bigint FROM completed"#,
                &[&pipeline_id.as_uuid(), &snapshot_generation],
            )
            .await?;
        let completed: i64 = row.try_get(0)?;
        Ok(u64::try_from(completed).expect("PostgreSQL count cannot be negative"))
    }

    async fn list_operations(&self) -> Result<Vec<OperationRecord>, StoreError> {
        let client = self.client().await?;
        Ok(client
            .query(
                r#"SELECT id,pipeline_id,operation_type,state,detail,created_at,updated_at
                     FROM cloudberry_etl_control.operations
                    ORDER BY created_at DESC
                    LIMIT 200"#,
                &[],
            )
            .await?
            .into_iter()
            .map(operation_from_row)
            .collect())
    }

    async fn try_acquire_lease(
        &self,
        pipeline_id: PipelineId,
        holder_id: Uuid,
        ttl: Duration,
    ) -> Result<Option<PipelineLease>, StoreError> {
        let client = self.lease_client().await?;
        let ttl_seconds = duration_seconds(ttl)?;
        let row = client
            .query_opt(
                r#"INSERT INTO cloudberry_etl_control.pipeline_leases (
                    pipeline_id,holder_id,fencing_token,expires_at
                ) VALUES ($1,$2,1,clock_timestamp() + $3 * interval '1 second')
                ON CONFLICT (pipeline_id) DO UPDATE SET
                    holder_id=EXCLUDED.holder_id,
                    fencing_token=cloudberry_etl_control.pipeline_leases.fencing_token + 1,
                    expires_at=EXCLUDED.expires_at,updated_at=clock_timestamp()
                WHERE cloudberry_etl_control.pipeline_leases.expires_at <= clock_timestamp()
                   OR cloudberry_etl_control.pipeline_leases.holder_id = EXCLUDED.holder_id
                RETURNING pipeline_id,holder_id,fencing_token,expires_at"#,
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
        let client = self.lease_client().await?;
        let ttl_seconds = duration_seconds(ttl)?;
        let row = client
            .query_opt(
                r#"UPDATE cloudberry_etl_control.pipeline_leases SET
                    expires_at=clock_timestamp() + $4 * interval '1 second',
                    updated_at=clock_timestamp()
                   WHERE pipeline_id=$1 AND holder_id=$2 AND fencing_token=$3
                     AND expires_at > clock_timestamp()
                   RETURNING pipeline_id,holder_id,fencing_token,expires_at"#,
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
        let client = self.lease_client().await?;
        client
            .execute(
                r#"UPDATE cloudberry_etl_control.pipeline_leases
                      SET expires_at = clock_timestamp(),
                          updated_at = clock_timestamp()
                    WHERE pipeline_id=$1 AND holder_id=$2 AND fencing_token=$3"#,
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
        snapshot_generation: row.get("snapshot_generation"),
        settings: row.get("settings"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn operation_from_row(row: Row) -> OperationRecord {
    OperationRecord {
        id: OperationId::from_uuid(row.get("id")),
        pipeline_id: row
            .get::<_, Option<Uuid>>("pipeline_id")
            .map(PipelineId::from_uuid),
        operation_type: row.get("operation_type"),
        state: row.get("state"),
        detail: row.get("detail"),
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

    #[test]
    fn control_session_options_are_bounded_and_preserve_existing_options() {
        let mut config = tokio_postgres::Config::new();
        config.options("-c search_path=public");
        configure_control_session(&mut config);

        let options = config
            .get_options()
            .expect("session options are configured");
        assert!(options.starts_with("-c search_path=public "));
        assert!(options.contains("statement_timeout=5000"));
        assert!(options.contains("lock_timeout=2000"));
        assert!(options.contains("idle_in_transaction_session_timeout=5000"));
        assert_eq!(config.get_application_name(), Some("pg2cb-control"));
    }
}
