//! Connection setup owned by the PostgreSQL source adapter.

use replication_postgres::{Client, Config, config::ReplicationMode};
use replication_postgres_native_tls::MakeTlsConnector;

use crate::{SourceError, SourceResult};

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
    let tls = native_tls::TlsConnector::builder().build()?;
    let (client, connection) = config.connect(MakeTlsConnector::new(tls)).await?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "source replication connection closed");
        }
    });
    Ok(client)
}
