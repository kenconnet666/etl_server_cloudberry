//! Versioned control-database migrations.

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::Client;

const MIGRATION_LOCK_ID: i64 = 0x4350_4745_544c;

const CONTROL_V1: &str = r#"
CREATE SCHEMA IF NOT EXISTS cloudberry_etl_control;

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum bytea NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.sources (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE,
    prefix text NOT NULL UNIQUE,
    database_name text NOT NULL,
    topology text NOT NULL CHECK (topology IN ('standalone', 'physical_ha', 'citus')),
    dsn_key_version integer NOT NULL,
    dsn_nonce bytea NOT NULL,
    dsn_ciphertext bytea NOT NULL,
    settings jsonb NOT NULL DEFAULT '{}'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.targets (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE,
    database_name text NOT NULL,
    dsn_key_version integer NOT NULL,
    dsn_nonce bytea NOT NULL,
    dsn_ciphertext bytea NOT NULL,
    settings jsonb NOT NULL DEFAULT '{}'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.pipelines (
    id uuid PRIMARY KEY,
    name text NOT NULL UNIQUE,
    source_id uuid NOT NULL REFERENCES cloudberry_etl_control.sources(id),
    target_id uuid NOT NULL REFERENCES cloudberry_etl_control.targets(id),
    desired_running boolean NOT NULL DEFAULT false,
    config_revision bigint NOT NULL DEFAULT 1 CHECK (config_revision > 0),
    settings jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (source_id, target_id)
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.config_revisions (
    pipeline_id uuid NOT NULL REFERENCES cloudberry_etl_control.pipelines(id) ON DELETE CASCADE,
    revision bigint NOT NULL,
    definition jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_id, revision)
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.pipeline_leases (
    pipeline_id uuid PRIMARY KEY REFERENCES cloudberry_etl_control.pipelines(id) ON DELETE CASCADE,
    holder_id uuid NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    expires_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.operations (
    id uuid PRIMARY KEY,
    pipeline_id uuid REFERENCES cloudberry_etl_control.pipelines(id) ON DELETE SET NULL,
    operation_type text NOT NULL,
    state text NOT NULL,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS cloudberry_etl_control.audit_log (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    action text NOT NULL,
    object_type text NOT NULL,
    object_id text,
    detail jsonb NOT NULL DEFAULT '{}'::jsonb
);
"#;

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "initial_control_schema",
    sql: CONTROL_V1,
}];

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("control database migration failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("migration {version} checksum does not match the applied migration")]
    ChecksumMismatch { version: i64 },
}

pub async fn migrate_control_database(client: &mut Client) -> Result<(), MigrationError> {
    for migration in MIGRATIONS {
        let transaction = client.transaction().await?;
        transaction
            .query_one("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK_ID])
            .await?;
        transaction
            .batch_execute(
                "CREATE SCHEMA IF NOT EXISTS cloudberry_etl_control;\
                 CREATE TABLE IF NOT EXISTS cloudberry_etl_control.schema_migrations (\
                    version bigint PRIMARY KEY, name text NOT NULL, checksum bytea NOT NULL,\
                    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()\
                 );",
            )
            .await?;

        let checksum = Sha256::digest(migration.sql.as_bytes()).to_vec();
        let applied = transaction
            .query_opt(
                "SELECT checksum FROM cloudberry_etl_control.schema_migrations WHERE version = $1",
                &[&migration.version],
            )
            .await?;
        if let Some(row) = applied {
            let stored: Vec<u8> = row.get(0);
            if stored != checksum {
                return Err(MigrationError::ChecksumMismatch {
                    version: migration.version,
                });
            }
            transaction.commit().await?;
            continue;
        }

        transaction.batch_execute(migration.sql).await?;
        transaction
            .execute(
                "INSERT INTO cloudberry_etl_control.schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
                &[&migration.version, &migration.name, &checksum],
            )
            .await?;
        transaction.commit().await?;
    }
    Ok(())
}
