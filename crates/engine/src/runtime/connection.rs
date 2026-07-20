//! Official PostgreSQL SQL connections used by source catalog/snapshot and Cloudberry apply.
//!
//! Logical replication uses the separately pinned fork in `source-postgres`. Keeping the normal
//! SQL connector here prevents driver ABI types from crossing that protocol boundary.

use std::fmt;

use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use secrecy::{ExposeSecret, SecretString};
use thiserror::Error;
use tokio_postgres::{Client, Config};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndpointRole {
    Source,
    Target,
}

impl fmt::Display for EndpointRole {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Source => "source PostgreSQL",
            Self::Target => "target Cloudberry",
        })
    }
}

#[derive(Debug, Error)]
pub enum SqlConnectError {
    #[error("invalid {endpoint} connection string")]
    InvalidDsn { endpoint: EndpointRole },
    #[error("failed to initialize TLS for {endpoint}: {source}")]
    Tls {
        endpoint: EndpointRole,
        #[source]
        source: native_tls::Error,
    },
    #[error("failed to connect to {endpoint}: {source}")]
    Connect {
        endpoint: EndpointRole,
        #[source]
        source: tokio_postgres::Error,
    },
}

/// Connect with the official `tokio-postgres` client and drive the connection in a scoped task.
///
/// The DSN is parsed directly from `SecretString` and is never formatted into logs or errors.
pub async fn connect_sql(
    dsn: &SecretString,
    endpoint: EndpointRole,
) -> Result<Client, SqlConnectError> {
    let config: Config = dsn
        .expose_secret()
        .parse()
        .map_err(|_| SqlConnectError::InvalidDsn { endpoint })?;
    let connector = TlsConnector::builder()
        .build()
        .map_err(|source| SqlConnectError::Tls { endpoint, source })?;
    let (client, connection) = config
        .connect(MakeTlsConnector::new(connector))
        .await
        .map_err(|source| SqlConnectError::Connect { endpoint, source })?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%endpoint, %error, "database connection closed");
        }
    });
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn invalid_dsn_never_appears_in_errors() {
        let marker = "never-log-this-password";
        let dsn = SecretString::from(format!("not a dsn {marker}\0"));
        let error = connect_sql(&dsn, EndpointRole::Source).await.unwrap_err();
        assert!(matches!(error, SqlConnectError::InvalidDsn { .. }));
        assert!(!error.to_string().contains(marker));
        assert!(!format!("{error:?}").contains(marker));
    }
}
