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
        if tables.is_empty() {
            return Err(SourceError::contract(
                "a publication must contain at least one allow-listed table",
            ));
        }
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
        Ok(format!(
            "CREATE PUBLICATION {name} FOR TABLE {tables} WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = {})",
            if self.publish_via_partition_root {
                "true"
            } else {
                "false"
            }
        ))
    }

    pub fn alter_tables_sql(&self) -> SourceResult<String> {
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
    pub tables: Vec<QualifiedName>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalSlotState {
    pub name: String,
    pub plugin: String,
    pub active: bool,
    pub confirmed_flush_lsn: Option<String>,
    pub restart_lsn: Option<String>,
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
        // Reconcile table membership explicitly. This also removes stale tables after a config
        // change, while never widening scope implicitly.
        client.batch_execute(&spec.alter_tables_sql()?).await?;
    } else {
        client.batch_execute(&spec.create_sql()?).await?;
    }
    let state = inspect_publication(client, &spec.name)
        .await?
        .ok_or_else(|| SourceError::contract("publication disappeared after creation"))?;
    validate_membership(&state, spec)?;
    Ok(state)
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
                    p.pubviaroot
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
            "SELECT slot_name, plugin, active, confirmed_flush_lsn::text, restart_lsn::text
               FROM pg_replication_slots
              WHERE slot_name = $1 AND slot_type = 'logical'",
            &[&slot_name],
        )
        .await?;
    row.map(|row| {
        Ok(LogicalSlotState {
            name: row.try_get(0)?,
            plugin: row.try_get(1)?,
            active: row.try_get(2)?,
            confirmed_flush_lsn: row.try_get(3)?,
            restart_lsn: row.try_get(4)?,
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
    validate_name(slot_name)?;
    validate_name(publication_name)?;
    validate_lsn(start_lsn)?;
    Ok(format!(
        "START_REPLICATION SLOT {} LOGICAL {} (\"proto_version\" {}, \"publication_names\" {})",
        quote_identifier(slot_name)?,
        start_lsn,
        quote_literal("1")?,
        quote_literal(publication_name)?
    ))
}

fn validate_membership(state: &PublicationState, spec: &PublicationSpec) -> SourceResult<()> {
    if state.all_tables {
        return Err(SourceError::contract(format!(
            "publication {} unexpectedly includes all tables",
            spec.name
        )));
    }
    let mut expected = spec.tables.clone();
    let mut actual = state.tables.clone();
    expected.sort_by_key(ToString::to_string);
    actual.sort_by_key(ToString::to_string);
    if expected != actual {
        return Err(SourceError::contract(format!(
            "publication {} table allow-list differs from configuration",
            spec.name
        )));
    }
    Ok(())
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
    }

    #[test]
    fn replication_command_rejects_injection() {
        assert!(start_replication_sql("slot", "0/0;DROP", "pub").is_err());
        let sql = start_replication_sql("slot", "0/0", "pub'name").unwrap();
        assert!(sql.contains("'pub''name'"));
    }
}
