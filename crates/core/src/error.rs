use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("invalid PostgreSQL LSN `{0}`")]
    InvalidLsn(String),
    #[error("invalid SQL identifier: {0}")]
    InvalidIdentifier(String),
    #[error("invalid source prefix `{0}`; use lowercase ASCII letters, digits, and underscores")]
    InvalidPrefix(String),
    #[error("table {0} has no supported primary key")]
    MissingPrimaryKey(String),
    #[error("table {table} is unsupported: {reason}")]
    UnsupportedTable { table: String, reason: String },
    #[error("row for relation {relation_id} has {actual} values, expected {expected}")]
    InvalidTupleArity {
        relation_id: u32,
        expected: usize,
        actual: usize,
    },
}
