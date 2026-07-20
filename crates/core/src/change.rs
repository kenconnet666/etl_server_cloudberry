use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{CoreError, CoreResult, lsn::PgLsn, schema::TableSchema};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum Cell {
    Null,
    UnchangedToast,
    Text(Bytes),
    Binary(Bytes),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tuple {
    pub cells: Vec<Cell>,
}

impl Tuple {
    pub fn validate(&self, schema: &TableSchema) -> CoreResult<()> {
        if self.cells.len() != schema.columns.len() {
            return Err(CoreError::InvalidTupleArity {
                relation_id: schema.relation_id,
                expected: schema.columns.len(),
                actual: self.cells.len(),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum RowChange {
    Insert { new: Tuple },
    Update { old_key: Option<Tuple>, new: Tuple },
    Delete { old_key: Tuple },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableChange {
    pub relation_id: u32,
    pub generation: u64,
    pub change: RowChange,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePosition {
    pub node_id: i32,
    pub system_identifier: u64,
    pub timeline: u32,
    pub lsn: PgLsn,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceTransaction {
    pub xid: u32,
    pub commit_time: DateTime<Utc>,
    pub final_position: SourcePosition,
    pub changes: Vec<TransactionChange>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DdlMessage {
    pub version: u16,
    pub command_tag: String,
    pub relation_ids: Vec<u32>,
    #[serde(default)]
    pub affected_schemas: Vec<String>,
    pub schema_fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum TransactionChange {
    Row(TableChange),
    Truncate {
        relation_ids: Vec<u32>,
        cascade: bool,
        restart_identity: bool,
    },
    Ddl(DdlMessage),
}

impl TransactionChange {
    #[must_use]
    pub fn requires_generation_barrier(&self) -> bool {
        matches!(self, Self::Truncate { .. } | Self::Ddl(_))
    }
}
