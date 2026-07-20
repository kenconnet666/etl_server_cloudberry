use thiserror::Error;

/// Errors raised by source inspection, snapshotting, and replication transport.
#[derive(Debug, Error)]
pub enum SourceError {
    #[error("PostgreSQL query failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("replication query failed: {0}")]
    ReplicationPostgres(#[from] replication_postgres::Error),
    #[error("TLS initialization failed: {0}")]
    Tls(#[from] native_tls::Error),
    #[error("replication protocol error: {0}")]
    ReplicationProtocol(String),
    #[error("invalid replication slot snapshot response: {0}")]
    InvalidSlotSnapshotResponse(String),
    #[error("snapshot readers are not ready: {ready} of {expected} registered")]
    SnapshotReadersPending { ready: usize, expected: usize },
    #[error("snapshot reader registration is already complete ({expected} readers)")]
    SnapshotReadersComplete { expected: usize },
    #[error("snapshot slot guard has already been released")]
    SnapshotGuardReleased,
    #[error("invalid source contract: {0}")]
    Contract(String),
    #[error("unsupported source: {0}")]
    Unsupported(String),
    #[error("invalid SQL identifier `{0}`")]
    InvalidIdentifier(String),
    #[error("invalid LSN `{0}`")]
    InvalidLsn(String),
    #[error("COPY stream failed: {0}")]
    Copy(String),
    #[error("DDL installer error: {0}")]
    Ddl(String),
    #[error("JSON encoding failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub type SourceResult<T> = Result<T, SourceError>;

impl SourceError {
    pub(crate) fn contract(message: impl Into<String>) -> Self {
        Self::Contract(message.into())
    }

    pub(crate) fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }
}
