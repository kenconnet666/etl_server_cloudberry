//! Citus topology discovery and per-node replication planning.
//!
//! Citus has no cluster-wide WAL order. This module therefore exposes a topology identity and one
//! stable lane per node group; callers persist LSNs as a vector keyed by `groupid`, never by the
//! replaceable physical `nodeid`.

use std::collections::{HashMap, HashSet};

use cloudberry_etl_core::schema::{TableKind, TableSchema};
use tokio_postgres::Client;

use crate::{SourceError, SourceResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CitusRole {
    Coordinator,
    Worker,
}

/// Stable logical replication lane identity backed by `pg_dist_node.groupid`.
///
/// A failover or node replacement may allocate a new physical `nodeid` while retaining the same
/// group. Persisting this value keeps the checkpoint vector attached to the logical shard owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CitusLaneId(i32);

impl CitusLaneId {
    pub fn new(value: i32) -> SourceResult<Self> {
        if value < 0 {
            return Err(SourceError::contract(format!(
                "Citus groupid must be nonnegative, got {value}"
            )));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub const fn get(self) -> i32 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CitusNode {
    /// Physical catalog identity. This is diagnostic only and must not key durable state.
    pub physical_node_id: i32,
    pub lane_id: CitusLaneId,
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
    part_key: Option<String>,
    replication_model: Option<String>,
    relation_kind: String,
    access_method: Option<String>,
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
    validate_version(&version, options)?;

    // Workers also expose pg_dist_node, so catalog presence cannot distinguish their role.
    let is_coordinator = client
        .query_one("SELECT pg_catalog.citus_is_coordinator()", &[])
        .await?
        .try_get::<_, bool>(0)?;
    let role = if is_coordinator {
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
                AND noderole = 'primary'
              ORDER BY groupid, nodeid",
            &[],
        )
        .await?;
    let nodes = rows
        .into_iter()
        .map(|row| {
            let physical_node_id = row.try_get::<_, i32>(0)?;
            if physical_node_id <= 0 {
                return Err(SourceError::contract(format!(
                    "Citus nodeid must be positive, got {physical_node_id}"
                )));
            }
            let port = row.try_get::<_, i32>(3)?;
            let port = u16::try_from(port)
                .map_err(|_| SourceError::contract(format!("invalid Citus node port {port}")))?;
            let host = row.try_get::<_, String>(2)?;
            if host.trim().is_empty() || host.contains('\0') {
                return Err(SourceError::contract("Citus node host is invalid"));
            }
            Ok(CitusNode {
                physical_node_id,
                lane_id: CitusLaneId::new(row.try_get(1)?)?,
                host,
                port,
                role: row.try_get(4)?,
                is_primary: row.try_get::<_, String>(4)?.eq_ignore_ascii_case("primary"),
            })
        })
        .collect::<SourceResult<Vec<_>>>()?;
    validate_nodes(&nodes)?;
    Ok(nodes)
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
            "SELECT partition.logicalrelid::int8, partition.partmethod::text,
                    partition.partkey::text, partition.repmodel::text,
                    relation.relkind::text, access_method.amname::text
               FROM pg_dist_partition AS partition
               JOIN pg_class AS relation ON relation.oid = partition.logicalrelid
               LEFT JOIN pg_am AS access_method ON access_method.oid = relation.relam
              WHERE partition.logicalrelid IS NOT NULL",
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
        let previous = descriptors.insert(
            relation_id,
            DistributionDescriptor {
                method,
                part_key: row.try_get(2)?,
                replication_model: row.try_get(3)?,
                relation_kind: row.try_get(4)?,
                access_method: row.try_get(5)?,
            },
        );
        if previous.is_some() {
            return Err(SourceError::contract(format!(
                "Citus distribution catalog contains duplicate relation {relation_id}"
            )));
        }
    }
    for table in tables {
        let descriptor = descriptors.get(&table.relation_id).ok_or_else(|| {
            SourceError::unsupported(format!(
                "Citus table {} is not tracked by pg_dist_partition",
                table.name
            ))
        })?;
        if descriptor.relation_kind != "r" {
            return Err(SourceError::unsupported(format!(
                "Citus table {} uses relation kind `{}`; only ordinary heap tables are supported",
                table.name, descriptor.relation_kind
            )));
        }
        if descriptor.access_method.as_deref() != Some("heap") {
            let access_method = descriptor.access_method.as_deref().unwrap_or("<none>");
            return Err(SourceError::unsupported(format!(
                "Citus table {} uses access method `{access_method}`; only heap source tables are supported",
                table.name
            )));
        }
        let key = match descriptor.part_key.as_deref() {
            Some(value) => parse_distribution_key(value)?,
            None => Vec::new(),
        };
        match descriptor.method {
            'h' => {
                if key.is_empty() {
                    return Err(SourceError::unsupported(format!(
                        "Citus hash-distributed table {} has no distribution key",
                        table.name
                    )));
                }
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

fn validate_version(version: &str, options: &CitusOptions) -> SourceResult<()> {
    let (major, minor) = parse_version(version)?;
    if major != options.expected_major || minor != options.expected_minor {
        return Err(SourceError::unsupported(format!(
            "Citus {}.{}.x is required, got {version}",
            options.expected_major, options.expected_minor
        )));
    }
    Ok(())
}

fn validate_nodes(nodes: &[CitusNode]) -> SourceResult<()> {
    let mut lanes = HashSet::with_capacity(nodes.len());
    let mut physical_nodes = HashSet::with_capacity(nodes.len());
    for node in nodes {
        if !lanes.insert(node.lane_id) {
            return Err(SourceError::contract(format!(
                "Citus group {} has more than one active primary",
                node.lane_id.get()
            )));
        }
        if !physical_nodes.insert(node.physical_node_id) {
            return Err(SourceError::contract(format!(
                "Citus physical node {} appears more than once",
                node.physical_node_id
            )));
        }
        if !node.is_primary || !node.role.eq_ignore_ascii_case("primary") {
            return Err(SourceError::contract(format!(
                "Citus lane {} is not backed by an active primary",
                node.lane_id.get()
            )));
        }
    }
    if !lanes.contains(&CitusLaneId(0)) {
        return Err(SourceError::contract(
            "Citus topology does not contain coordinator group 0",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_plain_distribution_var_nodes() {
        assert_eq!(
            parse_distribution_key("{VAR :varattno 2} {VAR :varattno 1}").unwrap(),
            vec![2, 1]
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

    #[test]
    fn production_version_gate_accepts_only_the_configured_minor_line() {
        let options = CitusOptions::default();
        assert!(validate_version("14.1.0", &options).is_ok());
        assert!(validate_version("14.1-3", &options).is_ok());
        assert!(validate_version("14.0.9", &options).is_err());
        assert!(validate_version("14.2.0", &options).is_err());
        assert!(validate_version("15.1.0", &options).is_err());
    }

    #[test]
    fn lane_identity_is_groupid_and_duplicate_primaries_fail_closed() {
        let node = |physical_node_id, lane| CitusNode {
            physical_node_id,
            lane_id: CitusLaneId::new(lane).unwrap(),
            host: format!("node-{physical_node_id}"),
            port: 5432,
            role: "primary".to_owned(),
            is_primary: true,
        };
        let original = vec![node(10, 0), node(20, 1)];
        assert!(validate_nodes(&original).is_ok());

        let replacement = vec![node(10, 0), node(99, 1)];
        assert_eq!(
            original[1].lane_id, replacement[1].lane_id,
            "physical node replacement must preserve the durable lane identity"
        );
        assert!(validate_nodes(&replacement).is_ok());
        assert!(validate_nodes(&[node(10, 0), node(20, 1), node(30, 1)]).is_err());
        assert!(validate_nodes(&[node(20, 1)]).is_err());
    }
}
