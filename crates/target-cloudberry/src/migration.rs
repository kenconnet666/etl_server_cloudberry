//! Target metadata migrations.

use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::Client;

pub const TARGET_METADATA_SCHEMA: &str = "pg2cb_meta";

const MIGRATION_LOCK_ID: i64 = 0x5047_3243_4254;

pub const TARGET_V1_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS pg2cb_meta;

CREATE TABLE IF NOT EXISTS pg2cb_meta.schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum bytea NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
USING heap
DISTRIBUTED BY (version);

CREATE TABLE IF NOT EXISTS pg2cb_meta.pipeline_state (
    pipeline_id uuid PRIMARY KEY,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
USING heap
DISTRIBUTED BY (pipeline_id);

CREATE TABLE IF NOT EXISTS pg2cb_meta.node_checkpoints (
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    node_id integer NOT NULL,
    system_identifier numeric(20, 0) NOT NULL CHECK (system_identifier >= 0),
    timeline bigint NOT NULL CHECK (timeline > 0 AND timeline <= 4294967295),
    slot_name text NOT NULL CHECK (slot_name <> ''),
    applied_lsn pg_lsn NOT NULL,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_id, topology_generation, node_id)
)
USING heap
DISTRIBUTED BY (pipeline_id, topology_generation, node_id);

CREATE TABLE IF NOT EXISTS pg2cb_meta.managed_tables (
    target_schema text NOT NULL,
    target_table text NOT NULL,
    pipeline_id uuid NOT NULL,
    source_relation_id bigint NOT NULL CHECK (source_relation_id > 0),
    table_generation bigint NOT NULL CHECK (table_generation >= 0),
    schema_fingerprint text NOT NULL CHECK (schema_fingerprint <> ''),
    state text NOT NULL CHECK (state IN ('shadow', 'active', 'blocked', 'quarantined')),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (target_schema, target_table)
)
USING heap
DISTRIBUTED BY (target_schema, target_table);
"#;

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[Migration {
    version: 1,
    name: "initial_target_metadata",
    sql: TARGET_V1_SQL,
}];

const BOOTSTRAP_SQL: &str = r#"
CREATE SCHEMA IF NOT EXISTS pg2cb_meta;
CREATE TABLE IF NOT EXISTS pg2cb_meta.schema_migrations (
    version bigint PRIMARY KEY,
    name text NOT NULL,
    checksum bytea NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
USING heap
DISTRIBUTED BY (version);
"#;

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("target metadata migration failed: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("target migration {version} checksum does not match the applied migration")]
    ChecksumMismatch { version: i64 },
}

/// Applies target metadata migrations under a coordinator advisory lock.
pub async fn migrate_target_database(client: &mut Client) -> Result<(), MigrationError> {
    for migration in MIGRATIONS {
        let transaction = client.transaction().await?;
        transaction
            .query_one("SELECT pg_advisory_xact_lock($1)", &[&MIGRATION_LOCK_ID])
            .await?;
        transaction.batch_execute(BOOTSTRAP_SQL).await?;

        let checksum = Sha256::digest(migration.sql.as_bytes()).to_vec();
        let applied = transaction
            .query_opt(
                "SELECT checksum FROM pg2cb_meta.schema_migrations WHERE version = $1",
                &[&migration.version],
            )
            .await?;
        if let Some(row) = applied {
            let stored: Vec<u8> = row.try_get("checksum")?;
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
                "INSERT INTO pg2cb_meta.schema_migrations (version, name, checksum) VALUES ($1, $2, $3)",
                &[&migration.version, &migration.name, &checksum],
            )
            .await?;
        transaction.commit().await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_metadata_uses_heap_and_pk_colocated_distribution() {
        assert!(TARGET_V1_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.pipeline_state"));
        assert!(TARGET_V1_SQL.contains("applied_lsn pg_lsn NOT NULL"));
        assert!(TARGET_V1_SQL.contains("system_identifier numeric(20, 0)"));
        assert!(TARGET_V1_SQL.contains("USING heap"));
        assert!(
            TARGET_V1_SQL.contains("DISTRIBUTED BY (pipeline_id, topology_generation, node_id)")
        );
    }

    #[test]
    fn migration_versions_and_checksums_are_stable() {
        assert_eq!(MIGRATIONS.len(), 1);
        assert_eq!(MIGRATIONS[0].version, 1);
        assert_eq!(
            Sha256::digest(MIGRATIONS[0].sql),
            Sha256::digest(TARGET_V1_SQL)
        );
    }
}
