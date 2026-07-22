//! Explicit publication and logical-slot lifecycle management.

use cloudberry_etl_core::schema::QualifiedName;
use tokio_postgres::Client;

use crate::{
    SourceError, SourceResult,
    sql::{quote_identifier, quote_literal, quote_qualified},
};

#[derive(Debug, Clone)]
pub struct PublicationSpec {
    pub name: String,
    pub tables: Vec<QualifiedName>,
    pub publish_via_partition_root: bool,
}

impl PublicationSpec {
    pub fn new(name: impl Into<String>, tables: Vec<QualifiedName>) -> SourceResult<Self> {
        let name = name.into();
        validate_name(&name)?;
        let mut seen = std::collections::HashSet::with_capacity(tables.len());
        for table in &tables {
            let key = format!("{}\0{}", table.schema, table.name);
            if !seen.insert(key) {
                return Err(SourceError::contract(format!(
                    "publication table {} is listed more than once",
                    table
                )));
            }
        }
        Ok(Self {
            name,
            tables,
            publish_via_partition_root: false,
        })
    }

    pub fn create_sql(&self) -> SourceResult<String> {
        let tables = self
            .tables
            .iter()
            .map(|table| quote_qualified(&table.schema, &table.name))
            .collect::<SourceResult<Vec<_>>>()?
            .join(", ");
        let name = quote_identifier(&self.name)?;
        let membership = if tables.is_empty() {
            String::new()
        } else {
            format!(" FOR TABLE {tables}")
        };
        Ok(format!(
            "CREATE PUBLICATION {name}{membership} WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = {}, publish_generated_columns = 'stored')",
            if self.publish_via_partition_root {
                "true"
            } else {
                "false"
            }
        ))
    }

    pub fn alter_tables_sql(&self) -> SourceResult<String> {
        if self.tables.is_empty() {
            return Err(SourceError::contract(
                "empty publication membership must be applied by dropping the current tables",
            ));
        }
        let tables = self
            .tables
            .iter()
            .map(|table| quote_qualified(&table.schema, &table.name))
            .collect::<SourceResult<Vec<_>>>()?
            .join(", ");
        Ok(format!(
            "ALTER PUBLICATION {} SET TABLE {}",
            quote_identifier(&self.name)?,
            tables
        ))
    }

    fn drop_tables_sql(&self, tables: &[QualifiedName]) -> SourceResult<String> {
        if tables.is_empty() {
            return Err(SourceError::contract(
                "publication DROP TABLE requires at least one current table",
            ));
        }
        let tables = tables
            .iter()
            .map(|table| quote_qualified(&table.schema, &table.name))
            .collect::<SourceResult<Vec<_>>>()?
            .join(", ");
        Ok(format!(
            "ALTER PUBLICATION {} DROP TABLE {tables}",
            quote_identifier(&self.name)?
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicationState {
    pub owner: String,
    pub all_tables: bool,
    pub publish_insert: bool,
    pub publish_update: bool,
    pub publish_delete: bool,
    pub publish_truncate: bool,
    pub publish_via_partition_root: bool,
    pub publish_generated_columns_stored: bool,
    pub tables: Vec<QualifiedName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSlotState {
    pub name: String,
    pub plugin: String,
    pub database: Option<String>,
    pub temporary: bool,
    pub active: bool,
    pub confirmed_flush_lsn: Option<String>,
    pub restart_lsn: Option<String>,
    /// Bytes of WAL retained by this slot, measured from `restart_lsn` to the current WAL head.
    /// The value is `None` when the slot has no restart position yet.
    pub retained_wal_bytes: Option<u64>,
    /// PostgreSQL 18's remaining WAL budget before this slot can be invalidated. It is `None`
    /// when `max_slot_wal_keep_size` is unlimited or the slot is already lost.
    pub safe_wal_size: Option<u64>,
    pub wal_status: Option<String>,
    pub invalidation_reason: Option<String>,
    pub two_phase: bool,
    pub failover: bool,
    pub synced: bool,
}

/// Capabilities intentionally exposed by the fork-backed pgoutput transport.
///
/// The pinned fork decodes protocol version 1 only. Streaming and two-phase messages are
/// therefore disabled/rejected rather than silently skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationCapabilities {
    pub proto_version: u8,
    pub messages: bool,
    pub streaming: bool,
    pub two_phase: bool,
}

impl Default for ReplicationCapabilities {
    fn default() -> Self {
        Self {
            proto_version: 1,
            messages: true,
            streaming: false,
            two_phase: false,
        }
    }
}

impl ReplicationCapabilities {
    pub fn validate(self) -> SourceResult<()> {
        if self.proto_version != 1 {
            return Err(SourceError::unsupported(format!(
                "pgoutput protocol version {} is not supported by the pinned decoder",
                self.proto_version
            )));
        }
        if !self.messages {
            return Err(SourceError::contract(
                "pgoutput messages must be enabled for transactional DDL notices",
            ));
        }
        if self.streaming || self.two_phase {
            return Err(SourceError::unsupported(
                "streaming/two-phase logical replication is unavailable; pause and rebuild instead",
            ));
        }
        Ok(())
    }
}

/// Create or reconcile one explicit publication. Existing unmanaged publications are rejected.
pub async fn ensure_publication(
    client: &Client,
    spec: &PublicationSpec,
    expected_owner: Option<&str>,
) -> SourceResult<PublicationState> {
    let existing = inspect_publication(client, &spec.name).await?;
    if let Some(state) = existing.as_ref() {
        if state.all_tables {
            return Err(SourceError::contract(format!(
                "publication {} is not an explicit allow-list",
                spec.name
            )));
        }
        if let Some(owner) = expected_owner
            && state.owner != owner
        {
            return Err(SourceError::contract(format!(
                "publication {} is owned by {}, expected {owner}",
                spec.name, state.owner
            )));
        }
        if !(state.publish_insert
            && state.publish_update
            && state.publish_delete
            && state.publish_truncate)
        {
            return Err(SourceError::contract(format!(
                "publication {} does not publish the required row operations",
                spec.name
            )));
        }
        if state.publish_via_partition_root != spec.publish_via_partition_root {
            return Err(SourceError::contract(format!(
                "publication {} has publish_via_partition_root={}, expected {}",
                spec.name, state.publish_via_partition_root, spec.publish_via_partition_root
            )));
        }
        if !state.publish_generated_columns_stored {
            execute_internal_ddl(
                client,
                &format!(
                    "ALTER PUBLICATION {} SET (publish_generated_columns = 'stored')",
                    quote_identifier(&spec.name)?
                ),
            )
            .await?;
        }
        // Avoid producing catalog WAL and DDL-capture noise on every reconnect. Reconcile only
        // when membership actually changed; this still removes stale tables explicitly.
        if !membership_matches(&state.tables, &spec.tables) {
            let ddl = if spec.tables.is_empty() {
                spec.drop_tables_sql(&state.tables)?
            } else {
                spec.alter_tables_sql()?
            };
            execute_internal_ddl(client, &ddl).await?;
        }
    } else {
        execute_internal_ddl(client, &spec.create_sql()?).await?;
    }
    let state = inspect_publication(client, &spec.name)
        .await?
        .ok_or_else(|| SourceError::contract("publication disappeared after creation"))?;
    validate_publication_state(&state, spec, expected_owner)?;
    Ok(state)
}

/// Validate the managed publication without changing it. This is used while resuming from an
/// existing checkpoint: silently repairing membership there could skip WAL that was omitted while
/// an external operator had removed a table from the publication.
pub async fn validate_publication(
    client: &Client,
    spec: &PublicationSpec,
    expected_owner: Option<&str>,
) -> SourceResult<PublicationState> {
    let state = inspect_publication(client, &spec.name)
        .await?
        .ok_or_else(|| SourceError::contract(format!("publication {} is missing", spec.name)))?;
    validate_publication_state(&state, spec, expected_owner)?;
    Ok(state)
}

async fn execute_internal_ddl(client: &Client, ddl: &str) -> SourceResult<()> {
    client.batch_execute("BEGIN").await?;
    let result = async {
        client
            .batch_execute("SELECT set_config('pg2cb.internal_ddl', 'on', true)")
            .await?;
        client.batch_execute(ddl).await?;
        Ok::<(), SourceError>(())
    }
    .await;
    if let Err(error) = result {
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(error);
    }
    if let Err(error) = client.batch_execute("COMMIT").await {
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(error.into());
    }
    Ok(())
}

pub async fn inspect_publication(
    client: &Client,
    name: &str,
) -> SourceResult<Option<PublicationState>> {
    validate_name(name)?;
    let row = client
        .query_opt(
            "SELECT p.pubowner::regrole::text,
                    p.puballtables, p.pubinsert, p.pubupdate, p.pubdelete, p.pubtruncate,
                    p.pubviaroot, p.pubgencols = 's'
               FROM pg_publication p
              WHERE p.pubname = $1
              ",
            &[&name],
        )
        .await?;
    let Some(row) = row else { return Ok(None) };
    let tables = client
        .query(
            "SELECT n.nspname, c.relname
               FROM pg_publication p
               JOIN pg_publication_rel pr ON pr.prpubid = p.oid
               JOIN pg_class c ON c.oid = pr.prrelid
               JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE p.pubname = $1
              ORDER BY n.nspname, c.relname",
            &[&name],
        )
        .await?
        .into_iter()
        .map(|table| {
            QualifiedName::new(
                table.try_get::<_, String>(0)?,
                table.try_get::<_, String>(1)?,
            )
            .map_err(|error| SourceError::contract(error.to_string()))
        })
        .collect::<SourceResult<Vec<_>>>()?;
    Ok(Some(PublicationState {
        owner: row.try_get(0)?,
        all_tables: row.try_get(1)?,
        publish_insert: row.try_get(2)?,
        publish_update: row.try_get(3)?,
        publish_delete: row.try_get(4)?,
        publish_truncate: row.try_get(5)?,
        publish_via_partition_root: row.try_get(6)?,
        publish_generated_columns_stored: row.try_get(7)?,
        tables,
    }))
}

pub async fn create_logical_slot(
    client: &Client,
    slot_name: &str,
    plugin: &str,
) -> SourceResult<Option<String>> {
    validate_name(slot_name)?;
    if plugin.is_empty() || plugin.contains('\0') || plugin.contains('\'') {
        return Err(SourceError::contract(
            "invalid logical decoding plugin name",
        ));
    }
    let row = client
        .query_one(
            "SELECT lsn::text FROM pg_create_logical_replication_slot($1, $2)",
            &[&slot_name, &plugin],
        )
        .await?;
    Ok(row.try_get(0)?)
}

pub async fn drop_logical_slot(client: &Client, slot_name: &str) -> SourceResult<()> {
    validate_name(slot_name)?;
    client
        .execute("SELECT pg_drop_replication_slot($1)", &[&slot_name])
        .await?;
    Ok(())
}

pub async fn inspect_logical_slot(
    client: &Client,
    slot_name: &str,
) -> SourceResult<Option<LogicalSlotState>> {
    validate_name(slot_name)?;
    let row = client
        .query_opt(
            "SELECT slot_name, plugin, database, temporary, active,
                    confirmed_flush_lsn::text, restart_lsn::text,
                    pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::text,
                    safe_wal_size::text,
                    wal_status::text, invalidation_reason::text,
                    two_phase, failover, synced
               FROM pg_replication_slots
              WHERE slot_name = $1 AND slot_type = 'logical'",
            &[&slot_name],
        )
        .await?;
    row.map(|row| {
        Ok(LogicalSlotState {
            name: row.try_get(0)?,
            plugin: row.try_get(1)?,
            database: row.try_get(2)?,
            temporary: row.try_get(3)?,
            active: row.try_get(4)?,
            confirmed_flush_lsn: row.try_get(5)?,
            restart_lsn: row.try_get(6)?,
            retained_wal_bytes: parse_optional_nonnegative_u64(row.try_get(7)?, "retained WAL")?,
            safe_wal_size: parse_optional_nonnegative_u64(row.try_get(8)?, "safe WAL size")?,
            wal_status: row.try_get(9)?,
            invalidation_reason: row.try_get(10)?,
            two_phase: row.try_get(11)?,
            failover: row.try_get(12)?,
            synced: row.try_get(13)?,
        })
    })
    .transpose()
}

fn parse_optional_nonnegative_u64(value: Option<String>, label: &str) -> SourceResult<Option<u64>> {
    value
        .map(|value| {
            value.parse::<u64>().map_err(|_| {
                SourceError::contract(format!(
                    "{label} value `{value}` is not a non-negative integer"
                ))
            })
        })
        .transpose()
}

/// Build a replication command for the fork transport. The option values are quoted literals;
/// identifiers are never interpolated without `quote_identifier`.
pub fn start_replication_sql(
    slot_name: &str,
    start_lsn: &str,
    publication_name: &str,
) -> SourceResult<String> {
    start_replication_sql_with_capabilities(
        slot_name,
        start_lsn,
        publication_name,
        ReplicationCapabilities::default(),
    )
}

pub fn start_replication_sql_with_capabilities(
    slot_name: &str,
    start_lsn: &str,
    publication_name: &str,
    capabilities: ReplicationCapabilities,
) -> SourceResult<String> {
    validate_name(slot_name)?;
    validate_name(publication_name)?;
    validate_lsn(start_lsn)?;
    capabilities.validate()?;
    Ok(format!(
        "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" {}, \"publication_names\" {}, \"messages\" {})",
        quote_identifier(slot_name)?,
        start_lsn,
        quote_literal(&capabilities.proto_version.to_string())?,
        quote_literal(publication_name)?,
        quote_literal(if capabilities.messages {
            "true"
        } else {
            "false"
        })?
    ))
}

fn validate_publication_state(
    state: &PublicationState,
    spec: &PublicationSpec,
    expected_owner: Option<&str>,
) -> SourceResult<()> {
    if state.all_tables {
        return Err(SourceError::contract(format!(
            "publication {} unexpectedly includes all tables",
            spec.name
        )));
    }
    if let Some(owner) = expected_owner
        && state.owner != owner
    {
        return Err(SourceError::contract(format!(
            "publication {} is owned by {}, expected {owner}",
            spec.name, state.owner
        )));
    }
    if !(state.publish_insert
        && state.publish_update
        && state.publish_delete
        && state.publish_truncate)
    {
        return Err(SourceError::contract(format!(
            "publication {} does not publish the required row operations",
            spec.name
        )));
    }
    if state.publish_via_partition_root != spec.publish_via_partition_root {
        return Err(SourceError::contract(format!(
            "publication {} has publish_via_partition_root={}, expected {}",
            spec.name, state.publish_via_partition_root, spec.publish_via_partition_root
        )));
    }
    if !state.publish_generated_columns_stored {
        return Err(SourceError::contract(format!(
            "publication {} does not publish stored generated columns",
            spec.name
        )));
    }
    if !membership_matches(&state.tables, &spec.tables) {
        return Err(SourceError::contract(format!(
            "publication {} table allow-list differs from configuration",
            spec.name
        )));
    }
    Ok(())
}

fn membership_matches(actual: &[QualifiedName], expected: &[QualifiedName]) -> bool {
    let mut actual = actual.to_vec();
    let mut expected = expected.to_vec();
    actual.sort_by_key(ToString::to_string);
    expected.sort_by_key(ToString::to_string);
    actual == expected
}

fn validate_name(name: &str) -> SourceResult<()> {
    if name.is_empty() || name.contains('\0') || name.len() > 63 {
        return Err(SourceError::InvalidIdentifier(name.to_owned()));
    }
    Ok(())
}

fn validate_lsn(value: &str) -> SourceResult<()> {
    let (high, low) = value
        .split_once('/')
        .ok_or_else(|| SourceError::InvalidLsn(value.to_owned()))?;
    if high.is_empty() || low.is_empty() || low.len() > 8 {
        return Err(SourceError::InvalidLsn(value.to_owned()));
    }
    u32::from_str_radix(high, 16)
        .and_then(|_| u32::from_str_radix(low, 16).map(|_| ()))
        .map_err(|_| SourceError::InvalidLsn(value.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table(schema: &str, name: &str) -> QualifiedName {
        QualifiedName::new(schema, name).unwrap()
    }

    #[test]
    fn publication_sql_is_explicit_and_escaped() {
        let spec = PublicationSpec::new("pub\"x", vec![table("s", "t\"x")]).unwrap();
        let sql = spec.create_sql().unwrap();
        assert!(sql.contains("FOR TABLE"));
        assert!(!sql.contains("FOR ALL TABLES"));
        assert!(sql.contains("\"pub\"\"x\""));
        assert!(sql.contains("\"t\"\"x\""));
        assert!(sql.contains("publish_generated_columns = 'stored'"));
    }

    #[test]
    fn empty_publication_keeps_ddl_messages_available_after_the_last_table_drop() {
        let spec = PublicationSpec::new("managed", Vec::new()).unwrap();
        let sql = spec.create_sql().unwrap();
        assert!(!sql.contains("FOR TABLE"));
        assert!(sql.contains("CREATE PUBLICATION \"managed\" WITH"));
        let drop_sql = spec.drop_tables_sql(&[table("public", "items")]).unwrap();
        assert_eq!(
            drop_sql,
            "ALTER PUBLICATION \"managed\" DROP TABLE \"public\".\"items\""
        );
    }

    #[test]
    fn publication_state_validation_is_fail_closed() {
        let spec = PublicationSpec::new("managed", vec![table("public", "items")]).unwrap();
        let mut state = PublicationState {
            owner: "etl".to_owned(),
            all_tables: false,
            publish_insert: true,
            publish_update: true,
            publish_delete: true,
            publish_truncate: true,
            publish_via_partition_root: false,
            publish_generated_columns_stored: true,
            tables: spec.tables.clone(),
        };
        validate_publication_state(&state, &spec, Some("etl")).unwrap();

        state.publish_generated_columns_stored = false;
        assert!(validate_publication_state(&state, &spec, Some("etl")).is_err());
        state.publish_generated_columns_stored = true;
        state.publish_delete = false;
        assert!(validate_publication_state(&state, &spec, Some("etl")).is_err());
        state.publish_delete = true;
        state.owner = "other".to_owned();
        assert!(validate_publication_state(&state, &spec, Some("etl")).is_err());
    }

    #[test]
    fn replication_command_rejects_injection() {
        assert!(start_replication_sql("slot", "0/0;DROP", "pub").is_err());
        let sql = start_replication_sql("slot", "0/0", "pub'name").unwrap();
        assert!(sql.contains("'pub''name'"));
        assert!(sql.contains("\"messages\" 'true'"));
        assert!(
            start_replication_sql_with_capabilities(
                "slot",
                "0/0",
                "pub",
                ReplicationCapabilities {
                    streaming: true,
                    ..ReplicationCapabilities::default()
                }
            )
            .is_err()
        );
        assert!(
            ReplicationCapabilities {
                proto_version: 2,
                ..ReplicationCapabilities::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn parses_slot_wal_sizes_without_signed_truncation() {
        assert_eq!(
            parse_optional_nonnegative_u64(Some(u64::MAX.to_string()), "size").unwrap(),
            Some(u64::MAX)
        );
        assert_eq!(parse_optional_nonnegative_u64(None, "size").unwrap(), None);
        assert!(parse_optional_nonnegative_u64(Some("-1".to_owned()), "size").is_err());
        assert!(parse_optional_nonnegative_u64(Some("1.5".to_owned()), "size").is_err());
    }
}
