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

pub const TARGET_V2_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS pg2cb_meta.managed_types (
    type_schema text NOT NULL,
    type_name text NOT NULL,
    pipeline_id uuid NOT NULL,
    definition_checksum bytea NOT NULL CHECK (octet_length(definition_checksum) = 32),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (type_schema, type_name)
)
USING heap
DISTRIBUTED BY (type_schema, type_name);
"#;

pub const TARGET_V3_SQL: &str = r#"
ALTER TABLE pg2cb_meta.managed_tables
    ADD COLUMN IF NOT EXISTS snapshot_group_id uuid;

CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_groups (
    snapshot_group_id uuid PRIMARY KEY,
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    state text NOT NULL CHECK (state IN ('loading', 'active')),
    table_count bigint NOT NULL CHECK (table_count > 0),
    node_count bigint NOT NULL CHECK (node_count > 0),
    manifest_checksum bytea NOT NULL CHECK (octet_length(manifest_checksum) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz
)
USING heap
DISTRIBUTED BY (snapshot_group_id);

CREATE INDEX IF NOT EXISTS snapshot_groups_pipeline_generation_idx
    ON pg2cb_meta.snapshot_groups (pipeline_id, topology_generation);

CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_group_tables (
    snapshot_group_id uuid NOT NULL,
    target_schema text NOT NULL,
    target_table text NOT NULL,
    shadow_schema text NOT NULL,
    shadow_table text NOT NULL,
    source_relation_id bigint NOT NULL CHECK (source_relation_id > 0),
    table_generation bigint NOT NULL CHECK (table_generation >= 0),
    schema_fingerprint text NOT NULL CHECK (schema_fingerprint <> ''),
    PRIMARY KEY (snapshot_group_id, target_schema, target_table),
    UNIQUE (snapshot_group_id, shadow_schema, shadow_table),
    UNIQUE (snapshot_group_id, source_relation_id)
)
USING heap
DISTRIBUTED BY (snapshot_group_id);

CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_group_nodes (
    snapshot_group_id uuid NOT NULL,
    node_id integer NOT NULL,
    system_identifier numeric(20, 0) NOT NULL CHECK (system_identifier >= 0),
    timeline bigint NOT NULL CHECK (timeline > 0 AND timeline <= 4294967295),
    slot_name text NOT NULL CHECK (slot_name <> ''),
    consistent_lsn pg_lsn NOT NULL,
    PRIMARY KEY (snapshot_group_id, node_id)
)
USING heap
DISTRIBUTED BY (snapshot_group_id);
"#;

pub const TARGET_V4_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_reconciliation_log (
    snapshot_group_id uuid NOT NULL,
    original_schema text NOT NULL,
    original_table text NOT NULL,
    quarantine_schema text NOT NULL,
    quarantine_table text NOT NULL,
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    source_relation_id bigint NOT NULL CHECK (source_relation_id > 0),
    previous_snapshot_group_id uuid,
    table_generation bigint NOT NULL CHECK (table_generation >= 0),
    schema_fingerprint text NOT NULL CHECK (schema_fingerprint <> ''),
    reason text NOT NULL CHECK (reason IN ('replaced', 'stale')),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (snapshot_group_id, original_schema, original_table)
)
USING heap
DISTRIBUTED BY (snapshot_group_id);
"#;

/// Metadata needed to make destructive cleanup identity-safe.
///
/// `relation_oid` is intentionally nullable for databases upgraded from V4.  Cleanup refuses
/// those legacy rows until a new, identity-bearing activation records the object.  The
/// reconciliation audit row is retained after a quarantine is dropped and records who performed
/// the purge.
pub const TARGET_V5_SQL: &str = r#"
ALTER TABLE pg2cb_meta.managed_tables
    ADD COLUMN IF NOT EXISTS relation_oid bigint;

ALTER TABLE pg2cb_meta.snapshot_reconciliation_log
    ADD COLUMN IF NOT EXISTS quarantine_relation_oid bigint,
    ADD COLUMN IF NOT EXISTS previous_fencing_token bigint,
    ADD COLUMN IF NOT EXISTS purged_at timestamptz,
    ADD COLUMN IF NOT EXISTS purged_by_fencing_token bigint;

CREATE INDEX IF NOT EXISTS managed_tables_pipeline_group_state_idx
    ON pg2cb_meta.managed_tables (pipeline_id, snapshot_group_id, state);

CREATE INDEX IF NOT EXISTS snapshot_reconciliation_gc_idx
    ON pg2cb_meta.snapshot_reconciliation_log (pipeline_id, recorded_at, snapshot_group_id);

CREATE INDEX IF NOT EXISTS snapshot_groups_loading_idx
    ON pg2cb_meta.snapshot_groups (pipeline_id, topology_generation, state, created_at);
"#;

/// Durable progress for source transactions applied in bounded target chunks.
///
/// The progress row is the immutable source transaction manifest plus the first sequence that has
/// not committed on the target.  Individual chunk identities are retained separately so a replay
/// can distinguish an exact committed chunk from a request whose boundaries or bytes changed.
pub const TARGET_V6_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS pg2cb_meta.transaction_chunk_progress (
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    node_id integer NOT NULL,
    end_lsn pg_lsn NOT NULL,
    system_identifier numeric(20, 0) NOT NULL CHECK (system_identifier >= 0),
    timeline bigint NOT NULL CHECK (timeline > 0 AND timeline <= 4294967295),
    slot_name text NOT NULL CHECK (slot_name <> ''),
    xid bigint NOT NULL CHECK (xid > 0 AND xid <= 4294967295),
    manifest_version integer NOT NULL CHECK (manifest_version > 0 AND manifest_version <= 65535),
    record_count bigint NOT NULL CHECK (record_count >= 0),
    manifest_digest bytea NOT NULL CHECK (octet_length(manifest_digest) = 32),
    next_seq bigint NOT NULL CHECK (next_seq >= 0 AND next_seq <= record_count),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_id, topology_generation, node_id, end_lsn)
)
USING heap
DISTRIBUTED BY (pipeline_id, topology_generation, node_id);

CREATE TABLE IF NOT EXISTS pg2cb_meta.transaction_committed_chunks (
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    node_id integer NOT NULL,
    end_lsn pg_lsn NOT NULL,
    start_seq bigint NOT NULL CHECK (start_seq >= 0),
    end_seq bigint NOT NULL CHECK (end_seq > start_seq),
    chunk_digest bytea NOT NULL CHECK (octet_length(chunk_digest) = 32),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (pipeline_id, topology_generation, node_id, end_lsn, start_seq)
)
USING heap
DISTRIBUTED BY (pipeline_id, topology_generation, node_id);
"#;

/// Durable per-table progress for bounded snapshot COPY pages.
///
/// The cursor contains the source primary-key values in canonical text form and is advanced in
/// the same target transaction as the corresponding shadow-table COPY.  Distribution by the
/// snapshot group keeps group-wide activation checks colocated while remaining a prefix of every
/// unique key.
pub const TARGET_V7_SQL: &str = r#"
ALTER TABLE pg2cb_meta.snapshot_groups
    ADD COLUMN IF NOT EXISTS snapshot_progress_version integer NOT NULL DEFAULT 0
        CHECK (snapshot_progress_version IN (0, 1));

CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_table_progress (
    snapshot_group_id uuid NOT NULL,
    target_schema text NOT NULL,
    target_table text NOT NULL,
    shadow_schema text NOT NULL,
    shadow_table text NOT NULL,
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    shadow_relation_oid bigint NOT NULL CHECK (shadow_relation_oid > 0),
    source_relation_id bigint NOT NULL CHECK (source_relation_id > 0),
    table_generation bigint NOT NULL CHECK (table_generation >= 0),
    schema_fingerprint text NOT NULL CHECK (schema_fingerprint <> ''),
    cursor_format_version integer NOT NULL CHECK (cursor_format_version = 1),
    primary_key_arity integer NOT NULL CHECK (primary_key_arity >= 0),
    cursor_values text[] NOT NULL DEFAULT ARRAY[]::text[]
        CHECK (array_position(cursor_values, NULL) IS NULL)
        CHECK (cardinality(cursor_values) = 0
            OR cardinality(cursor_values) = primary_key_arity),
    cursor_digest bytea NOT NULL CHECK (octet_length(cursor_digest) = 32),
    completed boolean NOT NULL DEFAULT false,
    pages_copied bigint NOT NULL DEFAULT 0 CHECK (pages_copied >= 0),
    rows_copied bigint NOT NULL DEFAULT 0 CHECK (rows_copied >= 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (snapshot_group_id, target_schema, target_table),
    UNIQUE (snapshot_group_id, shadow_relation_oid),
    CHECK ((completed AND completed_at IS NOT NULL)
        OR (NOT completed AND completed_at IS NULL))
)
USING heap
DISTRIBUTED BY (snapshot_group_id);

CREATE INDEX IF NOT EXISTS snapshot_table_progress_pipeline_generation_idx
    ON pg2cb_meta.snapshot_table_progress (
        pipeline_id, topology_generation, snapshot_group_id, completed
    );
"#;

/// Durable schema-transition ledger for Phase 2 tight DDL follow.
///
/// Each row records one source DDL event that touches a managed relation, keyed
/// by the source LSN and transaction id so replayed WAL is idempotent. The
/// engine persists an event as `pending` before starting a table transition and
/// advances it through `in_transition` to `completed`/`failed`; a restart scans
/// unfinished source-transaction rows and resumes them in source-LSN order. `transitions` holds the
/// serialized per-table before/after descriptors so recovery does not depend on
/// re-reading the source catalog after the fact. Distribution by pipeline keeps
/// a pipeline's ordered event history colocated.
pub const TARGET_V8_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS pg2cb_meta.schema_events (
    event_id uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    topology_generation bigint NOT NULL CHECK (topology_generation >= 0),
    source_lsn pg_lsn NOT NULL,
    source_xid bigint NOT NULL CHECK (source_xid >= 0),
    command_tag text NOT NULL CHECK (command_tag <> ''),
    schema_fingerprint text NOT NULL,
    transitions jsonb NOT NULL DEFAULT '[]'::jsonb,
    state text NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'in_transition', 'completed', 'failed')),
    failure_reason text,
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    emitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    processed_at timestamptz,
    -- Cloudberry requires the distribution key to be a subset of every UNIQUE/PK
    -- constraint. The real idempotency key is (pipeline_id, source_lsn, source_xid)
    -- and pipeline_id distributes a pipeline's ordered history to one segment.
    -- event_id is a UUIDv7 used only as an opaque record handle (never a lookup
    -- or FK), so it does not carry a separate UNIQUE constraint.
    PRIMARY KEY (pipeline_id, source_lsn, source_xid),
    CHECK ((state IN ('completed', 'failed')) = (processed_at IS NOT NULL)),
    CHECK ((state = 'failed') OR (failure_reason IS NULL))
)
USING heap
DISTRIBUTED BY (pipeline_id);

CREATE INDEX IF NOT EXISTS schema_events_pending_idx
    ON pg2cb_meta.schema_events (pipeline_id, topology_generation, source_lsn)
    WHERE state IN ('pending', 'in_transition');
"#;

struct Migration {
    version: i64,
    name: &'static str,
    sql: &'static str,
}

const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 1,
        name: "initial_target_metadata",
        sql: TARGET_V1_SQL,
    },
    Migration {
        version: 2,
        name: "managed_user_types",
        sql: TARGET_V2_SQL,
    },
    Migration {
        version: 3,
        name: "snapshot_group_manifests",
        sql: TARGET_V3_SQL,
    },
    Migration {
        version: 4,
        name: "snapshot_reconciliation_log",
        sql: TARGET_V4_SQL,
    },
    Migration {
        version: 5,
        name: "identity_safe_snapshot_cleanup",
        sql: TARGET_V5_SQL,
    },
    Migration {
        version: 6,
        name: "durable_transaction_chunk_ledger",
        sql: TARGET_V6_SQL,
    },
    Migration {
        version: 7,
        name: "durable_snapshot_table_progress",
        sql: TARGET_V7_SQL,
    },
    Migration {
        version: 8,
        name: "schema_event_ledger",
        sql: TARGET_V8_SQL,
    },
];

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
        assert!(TARGET_V2_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.managed_types"));
        assert!(TARGET_V2_SQL.contains(
            "definition_checksum bytea NOT NULL CHECK (octet_length(definition_checksum) = 32)"
        ));
        assert!(TARGET_V2_SQL.contains("DISTRIBUTED BY (type_schema, type_name)"));
        assert!(TARGET_V3_SQL.contains("ADD COLUMN IF NOT EXISTS snapshot_group_id uuid"));
        assert!(TARGET_V3_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_groups"));
        assert!(
            TARGET_V3_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_group_tables")
        );
        assert!(
            TARGET_V3_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_group_nodes")
        );
        assert_eq!(
            TARGET_V3_SQL
                .matches("DISTRIBUTED BY (snapshot_group_id)")
                .count(),
            3
        );
        assert!(
            TARGET_V4_SQL
                .contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_reconciliation_log")
        );
        assert!(TARGET_V4_SQL.contains("reason IN ('replaced', 'stale')"));
        assert!(TARGET_V4_SQL.contains("DISTRIBUTED BY (snapshot_group_id)"));
        assert!(TARGET_V5_SQL.contains("ADD COLUMN IF NOT EXISTS relation_oid bigint"));
        assert!(TARGET_V5_SQL.contains("ADD COLUMN IF NOT EXISTS purged_at timestamptz"));
        assert!(TARGET_V5_SQL.contains("quarantine_relation_oid bigint"));
        assert!(TARGET_V5_SQL.contains("previous_fencing_token bigint"));
        assert!(TARGET_V5_SQL.contains("purged_by_fencing_token bigint"));
        assert!(TARGET_V5_SQL.contains("snapshot_reconciliation_gc_idx"));
        assert!(
            TARGET_V6_SQL
                .contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.transaction_chunk_progress")
        );
        assert!(
            TARGET_V6_SQL
                .contains("PRIMARY KEY (pipeline_id, topology_generation, node_id, end_lsn)")
        );
        assert!(TARGET_V6_SQL.contains("system_identifier numeric(20, 0)"));
        assert!(TARGET_V6_SQL.contains("manifest_version integer NOT NULL"));
        assert!(TARGET_V6_SQL.contains("record_count bigint NOT NULL"));
        assert!(
            TARGET_V6_SQL.contains(
                "manifest_digest bytea NOT NULL CHECK (octet_length(manifest_digest) = 32)"
            )
        );
        assert!(
            TARGET_V6_SQL
                .contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.transaction_committed_chunks")
        );
        assert!(
            TARGET_V6_SQL
                .contains("chunk_digest bytea NOT NULL CHECK (octet_length(chunk_digest) = 32)")
        );
        assert_eq!(
            TARGET_V6_SQL
                .matches("DISTRIBUTED BY (pipeline_id, topology_generation, node_id)")
                .count(),
            2
        );
        assert!(
            TARGET_V7_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.snapshot_table_progress")
        );
        assert!(TARGET_V7_SQL.contains(
            "ADD COLUMN IF NOT EXISTS snapshot_progress_version integer NOT NULL DEFAULT 0"
        ));
        assert!(
            TARGET_V7_SQL.contains("PRIMARY KEY (snapshot_group_id, target_schema, target_table)")
        );
        assert!(TARGET_V7_SQL.contains("shadow_schema text NOT NULL"));
        assert!(TARGET_V7_SQL.contains("shadow_table text NOT NULL"));
        assert!(TARGET_V7_SQL.contains("shadow_relation_oid bigint NOT NULL"));
        assert!(TARGET_V7_SQL.contains("UNIQUE (snapshot_group_id, shadow_relation_oid)"));
        assert!(TARGET_V7_SQL.contains("cursor_format_version integer NOT NULL"));
        assert!(TARGET_V7_SQL.contains("primary_key_arity integer NOT NULL"));
        assert!(TARGET_V7_SQL.contains("cursor_values text[] NOT NULL"));
        assert!(TARGET_V7_SQL.contains("cursor_digest bytea NOT NULL"));
        assert!(TARGET_V7_SQL.contains("completed boolean NOT NULL DEFAULT false"));
        assert!(TARGET_V7_SQL.contains("pages_copied bigint NOT NULL DEFAULT 0"));
        assert!(TARGET_V7_SQL.contains("rows_copied bigint NOT NULL DEFAULT 0"));
        assert!(TARGET_V7_SQL.contains("DISTRIBUTED BY (snapshot_group_id)"));
    }

    #[test]
    fn migration_versions_and_checksums_are_stable() {
        assert_eq!(MIGRATIONS.len(), 8);
        assert_eq!(MIGRATIONS[0].version, 1);
        assert_eq!(MIGRATIONS[1].version, 2);
        assert_eq!(MIGRATIONS[2].version, 3);
        assert_eq!(MIGRATIONS[3].version, 4);
        assert_eq!(MIGRATIONS[4].version, 5);
        assert_eq!(MIGRATIONS[5].version, 6);
        assert_eq!(MIGRATIONS[6].version, 7);
        assert_eq!(MIGRATIONS[7].version, 8);
        assert_eq!(
            Sha256::digest(MIGRATIONS[0].sql),
            Sha256::digest(TARGET_V1_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[1].sql),
            Sha256::digest(TARGET_V2_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[2].sql),
            Sha256::digest(TARGET_V3_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[3].sql),
            Sha256::digest(TARGET_V4_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[4].sql),
            Sha256::digest(TARGET_V5_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[5].sql),
            Sha256::digest(TARGET_V6_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[6].sql),
            Sha256::digest(TARGET_V7_SQL)
        );
        assert_eq!(
            Sha256::digest(MIGRATIONS[7].sql),
            Sha256::digest(TARGET_V8_SQL)
        );
    }

    #[test]
    fn v8_defines_the_schema_event_ledger() {
        assert!(TARGET_V8_SQL.contains("CREATE TABLE IF NOT EXISTS pg2cb_meta.schema_events"));
        assert!(TARGET_V8_SQL.contains("event_id uuid NOT NULL"));
        assert!(TARGET_V8_SQL.contains("source_lsn pg_lsn NOT NULL"));
        assert!(TARGET_V8_SQL.contains("source_xid bigint NOT NULL"));
        assert!(TARGET_V8_SQL.contains("transitions jsonb NOT NULL"));
        assert!(TARGET_V8_SQL.contains("state text NOT NULL DEFAULT 'pending'"));
        assert!(TARGET_V8_SQL.contains("PRIMARY KEY (pipeline_id, source_lsn, source_xid)"));
        // No standalone UNIQUE(event_id): Cloudberry requires the distribution key
        // to be a subset of every unique constraint, and event_id is not a lookup key.
        assert!(!TARGET_V8_SQL.contains("UNIQUE (event_id)"));
        assert!(TARGET_V8_SQL.contains("DISTRIBUTED BY (pipeline_id)"));
        assert!(TARGET_V8_SQL.contains("schema_events_pending_idx"));
    }
}
