//! Connection setup owned by the PostgreSQL source adapter.

use replication_postgres::{Client, Config, config::ReplicationMode};
use replication_postgres_native_tls::MakeTlsConnector;
use tokio_postgres::Client as SqlClient;

use crate::{SourceError, SourceResult};

/// Startup settings that make pgoutput text and target text input independent of role defaults.
/// Keep these aligned with `snapshot::SnapshotSettings`.
pub const CANONICAL_TEXT_STARTUP_OPTIONS: &str = "-c client_encoding=UTF8 -c DateStyle=ISO,YMD -c IntervalStyle=postgres -c TimeZone=UTC -c extra_float_digits=3 -c bytea_output=hex";

/// Append the canonical profile after caller options so correctness-critical settings win when
/// a DSN or role supplies a conflicting default. Other caller options remain intact.
#[must_use]
pub fn canonical_startup_options(existing: Option<&str>) -> String {
    existing.map_or_else(
        || CANONICAL_TEXT_STARTUP_OPTIONS.to_owned(),
        |existing| format!("{existing} {CANONICAL_TEXT_STARTUP_OPTIONS}"),
    )
}

/// Verify that the source is PostgreSQL 18.x.
///
/// This service only supports PostgreSQL 18 as the source. Other versions (17, 19, etc.)
/// are explicitly rejected to maintain a single, well-tested compatibility matrix.
pub async fn verify_pg18_version(client: &SqlClient) -> SourceResult<()> {
    let row = client
        .query_one("SELECT version()", &[])
        .await
        .map_err(|error| SourceError::contract(format!("failed to query version: {error}")))?;

    let version_string: String = row.get(0);

    // PostgreSQL 18.x version string format: "PostgreSQL 18.x on ..."
    if !version_string.starts_with("PostgreSQL 18.") {
        return Err(SourceError::contract(format!(
            "This service only supports PostgreSQL 18.x source. Found: {version_string}"
        )));
    }

    Ok(())
}

/// Connect the forked client used exclusively for replication protocol traffic.
///
/// Keeping this connector beside the fork prevents its `tokio-postgres` ABI from leaking into
/// normal SQL paths. Logical replication mode is applied here so the configured source DSN can
/// also be used by ordinary SQL clients. The DSN controls `sslmode`; TLS certificate validation
/// remains enabled by `native-tls` whenever TLS is negotiated.
pub async fn connect_replication(dsn: &str) -> SourceResult<Client> {
    let mut config: Config = dsn.parse()?;
    if matches!(
        config.get_replication_mode(),
        Some(ReplicationMode::Physical)
    ) {
        return Err(SourceError::contract(
            "physical replication mode cannot be used by the PostgreSQL source adapter",
        ));
    }
    // Source DSNs are shared with ordinary SQL clients in configuration.  Set logical mode here
    // rather than requiring operators to maintain a second DSN containing
    // `replication=database`.
    config.replication_mode(ReplicationMode::Logical);
    let options = canonical_startup_options(config.get_options());
    config.options(options);
    let tls = native_tls::TlsConnector::builder().build()?;
    let (client, connection) = config.connect(MakeTlsConnector::new(tls)).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source replication connection closed");
        }
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_options_override_conflicting_role_or_dsn_defaults() {
        let options = canonical_startup_options(Some(
            "-c statement_timeout=30000 -c DateStyle=SQL,DMY -c TimeZone=Asia/Shanghai",
        ));
        assert!(options.starts_with("-c statement_timeout=30000"));
        assert!(options.ends_with(CANONICAL_TEXT_STARTUP_OPTIONS));
        assert!(options.contains("-c DateStyle=ISO,YMD"));
        assert!(options.contains("-c TimeZone=UTC"));
    }
}
