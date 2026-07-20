//! Citus topology discovery and per-node replication planning.
//!
//! Citus has no cluster-wide WAL order.  This module therefore exposes a topology identity and
//! one node identity per coordinator/worker; callers persist LSNs as a vector keyed by `node_id`.

use std::collections::HashMap;

use cloudberry_etl_core::schema::{TableKind, TableSchema};
use tokio_postgres::Client;

use crate::{SourceError, SourceResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitusRole {
    Coordinator,
    Worker,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CitusNode {
    pub node_id: i32,
    pub group_id: i32,
    pub host: String,
    pub port: u16,
    pub role: String,
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CitusTopology {
    pub version: String,
    pub role: CitusRole,
    pub cluster_identity: String,
    pub generation: u64,
    pub cdc_enabled: bool,
    pub nodes: Vec<CitusNode>,
}

#[derive(Debug, Clone)]
pub struct CitusOptions {
    pub expected_major: u16,
    pub expected_minor: u16,
    pub require_cdc_setting: bool,
    pub allow_validation_gated_tables: bool,
}

impl Default for CitusOptions {
    fn default() -> Self {
        Self {
            expected_major: 14,
            expected_minor: 1,
            require_cdc_setting: true,
            allow_validation_gated_tables: false,
        }
    }
}

#[derive(Debug, Clone)]
struct DistributionDescriptor {
    method: char,
    part_key: String,
    replication_model: Option<String>,
}

/// Return `None` for a normal PostgreSQL server; otherwise discover and validate Citus.
pub async fn discover(
    client: &Client,
    options: &CitusOptions,
) -> SourceResult<Option<CitusTopology>> {
    let version = match client
        .query_opt(
            "SELECT extversion::text FROM pg_extension WHERE extname = 'citus'",
            &[],
        )
        .await?
    {
        Some(row) => row.try_get::<_, String>(0)?,
        None => return Ok(None),
    };
    let (major, minor) = parse_version(&version)?;
    if major != options.expected_major || minor < options.expected_minor {
        return Err(SourceError::unsupported(format!(
            "Citus {}.{} or newer is required, got {version}",
            options.expected_major, options.expected_minor
        )));
    }

    let has_node_catalog = client
        .query_one(
            "SELECT to_regclass('pg_catalog.pg_dist_node') IS NOT NULL",
            &[],
        )
        .await?
        .try_get::<_, bool>(0)?;
    let role = if has_node_catalog {
        CitusRole::Coordinator
    } else {
        CitusRole::Worker
    };
    let cdc_enabled = client
        .query_opt(
            "SELECT current_setting('citus.enable_change_data_capture', true)",
            &[],
        )
        .await?
        .and_then(|row| row.try_get::<_, Option<String>>(0).ok().flatten())
        .is_some_and(|value| value.eq_ignore_ascii_case("on") || value == "1" || value == "true");
    if options.require_cdc_setting && !cdc_enabled {
        return Err(SourceError::unsupported(
            "citus.enable_change_data_capture must be enabled",
        ));
    }

    let nodes = if role == CitusRole::Coordinator {
        load_nodes(client).await?
    } else {
        Vec::new()
    };
    let system_identifier = client
        .query_one(
            "SELECT system_identifier::text FROM pg_control_system()",
            &[],
        )
        .await?
        .try_get::<_, String>(0)?;
    let cluster_identity = format!("citus:{version}:system:{system_identifier}");

    Ok(Some(CitusTopology {
        version,
        role,
        cluster_identity,
        generation: 1,
        cdc_enabled,
        nodes,
    }))
}

/// Discover active Citus nodes from the coordinator.  Physical standbys are intentionally not
/// returned as additional CDC owners; one logical owner exists per node group.
pub async fn load_nodes(client: &Client) -> SourceResult<Vec<CitusNode>> {
    let rows = client
        .query(
            "SELECT nodeid::int4, groupid::int4, nodename::text, nodeport::int4,
                    noderole::text, isactive
               FROM pg_dist_node
              WHERE isactive
              ORDER BY nodeid",
            &[],
        )
        .await?;
    rows.into_iter()
        .map(|row| {
            let port = row.try_get::<_, i32>(3)?;
            let port = u16::try_from(port)
                .map_err(|_| SourceError::contract(format!("invalid Citus node port {port}")))?;
            Ok(CitusNode {
                node_id: row.try_get(0)?,
                group_id: row.try_get(1)?,
                host: row.try_get(2)?,
                port,
                role: row.try_get(4)?,
                is_primary: row.try_get::<_, String>(4)?.eq_ignore_ascii_case("primary"),
            })
        })
        .collect()
}

/// Apply Citus distribution metadata to a catalog snapshot.
pub async fn apply_table_metadata(
    client: &Client,
    tables: &mut [TableSchema],
    topology: &CitusTopology,
) -> SourceResult<()> {
    apply_table_metadata_with_options(client, tables, topology, &CitusOptions::default()).await
}

/// Apply metadata with an explicit capability opt-in for validation-gated table kinds.
pub async fn apply_table_metadata_with_options(
    client: &Client,
    tables: &mut [TableSchema],
    topology: &CitusTopology,
    options: &CitusOptions,
) -> SourceResult<()> {
    if topology.role != CitusRole::Coordinator {
        return Ok(());
    }
    let rows = client
        .query(
            "SELECT logicalrelid::int8, partmethod::text, partkey::text, repmodel::text
               FROM pg_dist_partition
              WHERE logicalrelid IS NOT NULL",
            &[],
        )
        .await?;
    let mut descriptors = HashMap::with_capacity(rows.len());
    for row in rows {
        let relation_id = u32::try_from(row.try_get::<_, i64>(0)?)
            .map_err(|_| SourceError::contract("Citus logical relation OID is out of range"))?;
        let method = row
            .try_get::<_, String>(1)?
            .chars()
            .next()
            .ok_or_else(|| SourceError::contract("empty Citus partition method"))?;
        descriptors.insert(
            relation_id,
            DistributionDescriptor {
                method,
                part_key: row.try_get(2)?,
                replication_model: row.try_get(3)?,
            },
        );
    }
    for table in tables {
        let Some(descriptor) = descriptors.get(&table.relation_id) else {
            continue;
        };
        let key = parse_distribution_key(&descriptor.part_key)?;
        match descriptor.method {
            'h' => {
                table.kind = TableKind::CitusDistributed;
                table.distribution_key = key;
            }
            'n' if descriptor
                .replication_model
                .as_deref()
                .is_some_and(|value| value.eq_ignore_ascii_case("s")) =>
            {
                // Reference tables have replicated placements and are validation-gated by the
                // product contract. Keep their identity explicit for the engine.
                if !options.allow_validation_gated_tables {
                    return Err(SourceError::unsupported(format!(
                        "Citus reference table {} is validation-gated",
                        table.name
                    )));
                }
                table.kind = TableKind::CitusReference;
                table.distribution_key = key;
            }
            'n' => {
                if !options.allow_validation_gated_tables {
                    return Err(SourceError::unsupported(format!(
                        "Citus coordinator-local table {} is validation-gated",
                        table.name
                    )));
                }
                table.kind = TableKind::CitusLocal;
                table.distribution_key = key;
            }
            method => {
                return Err(SourceError::unsupported(format!(
                    "Citus distribution method `{method}` for {} is not supported",
                    table.name
                )));
            }
        }
    }
    Ok(())
}

pub fn parse_distribution_key(part_key: &str) -> SourceResult<Vec<i16>> {
    // pg_node_tree contains `:varattno N` for each distribution column.  We parse only this
    // stable catalog field and reject expressions that do not expose a plain Var node.
    let mut result = Vec::new();
    let mut remainder = part_key;
    while let Some(index) = remainder.find(":varattno ") {
        remainder = &remainder[index + ":varattno ".len()..];
        let digits = remainder
            .split(|character: char| !character.is_ascii_digit() && character != '-')
            .next()
            .unwrap_or_default();
        if digits.is_empty() {
            return Err(SourceError::unsupported(format!(
                "cannot parse Citus distribution expression `{part_key}`"
            )));
        }
        let attnum = digits.parse::<i16>().map_err(|_| {
            SourceError::unsupported(format!("invalid Citus distribution attribute `{digits}`"))
        })?;
        if attnum <= 0 {
            return Err(SourceError::unsupported(format!(
                "system attribute {attnum} cannot be a distribution key"
            )));
        }
        if result.contains(&attnum) {
            return Err(SourceError::unsupported(format!(
                "duplicate Citus distribution attribute {attnum}"
            )));
        }
        result.push(attnum);
        remainder = &remainder[digits.len()..];
    }
    if result.is_empty() {
        return Err(SourceError::unsupported(format!(
            "Citus distribution expression has no plain column: `{part_key}`"
        )));
    }
    Ok(result)
}

fn parse_version(version: &str) -> SourceResult<(u16, u16)> {
    let mut parts = version.split('.');
    let major = parts
        .next()
        .ok_or_else(|| SourceError::contract(format!("invalid Citus version `{version}`")))?
        .parse()
        .map_err(|_| SourceError::contract(format!("invalid Citus version `{version}`")))?;
    let minor = parts
        .next()
        .unwrap_or("0")
        .split('-')
        .next()
        .unwrap_or("0")
        .parse()
        .map_err(|_| SourceError::contract(format!("invalid Citus version `{version}`")))?;
    Ok((major, minor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_distribution_var_nodes() {
        assert_eq!(
            parse_distribution_key("{VAR :varattno 2} {VAR :varattno 1}").unwrap(),
            vec![1, 2]
        );
    }

    #[test]
    fn rejects_expression_without_var() {
        assert!(parse_distribution_key("{FUNC :funcid 10}").is_err());
    }

    #[test]
    fn parses_version_with_suffix() {
        assert_eq!(parse_version("14.1.0").unwrap(), (14, 1));
        assert_eq!(parse_version("14.2-1").unwrap(), (14, 2));
    }
}
