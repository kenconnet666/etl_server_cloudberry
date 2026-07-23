//! Opt-in PostgreSQL 18 + Citus 14.1 topology and catalog coverage.
//!
//! This test is read-only. It verifies Citus discovery and catalog metadata but deliberately does
//! not claim to test publications, logical slots, or cluster-wide CDC ordering.

use std::collections::HashSet;

use cloudberry_etl_core::schema::{QualifiedName, TableKind};
use cloudberry_etl_source_postgres::{
    SourceError, SourceResult,
    catalog::{
        CatalogOptions, PreflightOptions, PreflightReport, load_tables_with_citus, preflight,
    },
    citus::{CitusOptions, CitusRole, CitusTopology, discover},
};
use tokio::task::JoinHandle;
use tokio_postgres::{Client, NoTls};

const COORDINATOR_DSN_ENV: &str = "PG2CB_TEST_CITUS_COORDINATOR_DSN";
const WORKER1_DSN_ENV: &str = "PG2CB_TEST_CITUS_WORKER1_DSN";
const WORKER2_DSN_ENV: &str = "PG2CB_TEST_CITUS_WORKER2_DSN";

struct NodeConnection {
    client: Client,
    connection_task: JoinHandle<()>,
}

impl NodeConnection {
    async fn from_env(variable: &'static str) -> SourceResult<Self> {
        let dsn = std::env::var(variable).map_err(|_| {
            SourceError::Contract(format!(
                "{variable} is required for the ignored Citus integration test"
            ))
        })?;
        let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                eprintln!("{variable} PostgreSQL connection ended: {error}");
            }
        });
        Ok(Self {
            client,
            connection_task,
        })
    }
}

impl Drop for NodeConnection {
    fn drop(&mut self) {
        self.connection_task.abort();
    }
}

#[tokio::test]
#[ignore = "requires the pinned Citus integration cluster and three PG2CB_TEST_CITUS_*_DSN variables"]
async fn citus14_pg18_topology_and_distributed_catalog() -> SourceResult<()> {
    let coordinator = NodeConnection::from_env(COORDINATOR_DSN_ENV).await?;
    let worker1 = NodeConnection::from_env(WORKER1_DSN_ENV).await?;
    let worker2 = NodeConnection::from_env(WORKER2_DSN_ENV).await?;

    let (coordinator_preflight, coordinator_topology) =
        inspect_node("coordinator", &coordinator.client, CitusRole::Coordinator).await?;
    let (worker1_preflight, worker1_topology) =
        inspect_node("worker1", &worker1.client, CitusRole::Worker).await?;
    let (worker2_preflight, worker2_topology) =
        inspect_node("worker2", &worker2.client, CitusRole::Worker).await?;

    let system_identifiers = HashSet::from([
        coordinator_preflight.identity.system_identifier,
        worker1_preflight.identity.system_identifier,
        worker2_preflight.identity.system_identifier,
    ]);
    assert_eq!(
        system_identifiers.len(),
        3,
        "each physical Citus node must have a distinct system identifier"
    );

    assert!(worker1_topology.nodes.is_empty());
    assert!(worker2_topology.nodes.is_empty());
    assert_eq!(
        coordinator_topology
            .nodes
            .iter()
            .map(|node| (node.lane_id.get(), node.host.as_str(), node.port))
            .collect::<Vec<_>>(),
        vec![
            (0, "coordinator", 5432),
            (1, "worker1", 5432),
            (2, "worker2", 5432),
        ]
    );
    assert!(
        coordinator_topology
            .nodes
            .iter()
            .all(|node| node.is_primary && node.role == "primary")
    );

    let tables = load_tables_with_citus(
        &coordinator.client,
        &CatalogOptions {
            include_schemas: Some(HashSet::from(["integration".to_owned()])),
            ..CatalogOptions::default()
        },
        &coordinator_topology,
    )
    .await?;
    let accounts_name = QualifiedName::new("integration", "accounts")
        .map_err(|error| SourceError::Contract(error.to_string()))?;
    let accounts = tables
        .iter()
        .find(|table| table.name == accounts_name)
        .ok_or_else(|| SourceError::Contract("integration.accounts was not discovered".into()))?;

    assert_eq!(accounts.kind, TableKind::CitusDistributed);
    assert_eq!(accounts.distribution_key, vec![1]);
    let distribution_columns = accounts
        .distribution_key
        .iter()
        .map(|attnum| {
            accounts
                .columns
                .iter()
                .find(|column| column.attnum == *attnum)
                .map(|column| column.name.as_str())
                .expect("distribution attnum must reference a catalog column")
        })
        .collect::<Vec<_>>();
    assert_eq!(distribution_columns, vec!["tenant_id"]);
    assert_eq!(
        accounts
            .primary_key()
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["tenant_id", "id"]
    );
    Ok(())
}

async fn inspect_node(
    label: &str,
    client: &Client,
    expected_role: CitusRole,
) -> SourceResult<(PreflightReport, CitusTopology)> {
    let preflight = preflight(client, &PreflightOptions::default()).await?;
    assert_eq!(
        preflight.identity.server_version_num / 10_000,
        18,
        "{label}"
    );
    assert_eq!(preflight.identity.database, "source", "{label}");
    assert!(!preflight.identity.in_recovery, "{label}");
    assert!(preflight.identity.system_identifier > 0, "{label}");
    assert!(preflight.identity.timeline > 0, "{label}");
    assert_eq!(
        preflight.wal_level.to_ascii_lowercase(),
        "logical",
        "{label}"
    );
    assert!(
        preflight.max_replication_slots >= 32,
        "{label}: max_replication_slots={}",
        preflight.max_replication_slots
    );
    assert!(
        preflight.max_wal_senders >= 32,
        "{label}: max_wal_senders={}",
        preflight.max_wal_senders
    );
    let preflight_citus_version = preflight.citus_version.as_deref().unwrap_or_default();
    assert!(
        preflight_citus_version.starts_with("Citus 14.1."),
        "{label}: citus_version()={preflight_citus_version:?}"
    );

    let topology = discover(client, &CitusOptions::default())
        .await?
        .ok_or_else(|| SourceError::Contract(format!("{label} did not report Citus")))?;
    assert_eq!(topology.role, expected_role, "{label}");
    assert!(topology.cdc_enabled, "{label}");
    assert!(
        topology.version.starts_with("14.1"),
        "{label}: extension version={:?}",
        topology.version
    );
    assert_eq!(
        topology.cluster_identity,
        format!(
            "citus:{}:system:{}",
            topology.version, preflight.identity.system_identifier
        ),
        "{label}"
    );
    Ok((preflight, topology))
}
