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

/// How a source DDL command affects logical row replication.
///
/// The classification is a strict allow-list: only commands whose effect on the
/// mirrored row stream is provably empty are `Irrelevant`. Everything else is
/// `SchemaSensitive` and keeps the conservative barrier behaviour, so an unknown
/// or newly introduced command tag always fails closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DdlReplicationImpact {
    /// The command cannot change a managed relation's column set, types, primary
    /// key, or row contents (e.g. index maintenance, comments, privileges,
    /// statistics). It never requires a schema transition.
    Irrelevant,
    /// The command may change a mirrored relation's schema or data shape and must
    /// be evaluated against the managed scope before rows can keep applying.
    SchemaSensitive,
}

impl DdlMessage {
    /// Classify the command's effect on logical row replication.
    ///
    /// Matching is on the leading words of the PostgreSQL command tag. Only tags
    /// proven harmless to the mirrored row stream are `Irrelevant`; any tag not
    /// on the allow-list is `SchemaSensitive` and stays fail-closed.
    #[must_use]
    pub fn replication_impact(&self) -> DdlReplicationImpact {
        if is_replication_irrelevant_tag(&self.command_tag) {
            DdlReplicationImpact::Irrelevant
        } else {
            DdlReplicationImpact::SchemaSensitive
        }
    }
}

/// Command tags whose effect on the mirrored row stream is provably empty.
///
/// These never alter a managed relation's column set, types, primary key, or row
/// contents, so they cannot desynchronise the target and never need a barrier:
/// index maintenance, comments, privileges, ownership, statistics targets, and
/// planner statistics collection. Deliberately conservative — anything that can
/// touch column shape, constraints enforced on data, or table identity is
/// excluded and therefore stays `SchemaSensitive`.
fn is_replication_irrelevant_tag(command_tag: &str) -> bool {
    const IRRELEVANT_PREFIXES: &[&str] = &[
        "CREATE INDEX",
        "DROP INDEX",
        "ALTER INDEX",
        "COMMENT",
        "GRANT",
        "REVOKE",
        "ANALYZE",
        "VACUUM",
        "REINDEX",
        "CREATE STATISTICS",
        "DROP STATISTICS",
        "ALTER STATISTICS",
        "SECURITY LABEL",
    ];
    let normalized = command_tag.trim();
    IRRELEVANT_PREFIXES
        .iter()
        .any(|prefix| tag_has_prefix(normalized, prefix))
}

/// True when `tag` equals `prefix` or continues with a space, so `CREATE INDEX`
/// matches `CREATE INDEX` and `CREATE INDEX CONCURRENTLY` but not a hypothetical
/// `CREATE INDEXER`.
fn tag_has_prefix(tag: &str, prefix: &str) -> bool {
    tag.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(' '))
}

#[cfg(test)]
mod ddl_impact_tests {
    use super::{DdlMessage, DdlReplicationImpact};

    fn ddl(command_tag: &str) -> DdlMessage {
        DdlMessage {
            version: 1,
            command_tag: command_tag.to_owned(),
            relation_ids: Vec::new(),
            affected_schemas: vec!["public".to_owned()],
            schema_fingerprint: "fp".to_owned(),
        }
    }

    #[test]
    fn index_privilege_and_statistics_commands_are_irrelevant() {
        for tag in [
            "CREATE INDEX",
            "CREATE INDEX CONCURRENTLY",
            "DROP INDEX",
            "ALTER INDEX",
            "COMMENT",
            "GRANT",
            "REVOKE",
            "ANALYZE",
            "VACUUM",
            "REINDEX",
            "CREATE STATISTICS",
            "SECURITY LABEL",
        ] {
            assert_eq!(
                ddl(tag).replication_impact(),
                DdlReplicationImpact::Irrelevant,
                "{tag} must be replication-irrelevant"
            );
        }
    }

    #[test]
    fn schema_changing_and_unknown_commands_stay_sensitive() {
        for tag in [
            "ALTER TABLE",
            "CREATE TABLE",
            "DROP TABLE",
            "ALTER TYPE",
            "ALTER PUBLICATION",
            "TRUNCATE",
            "CREATE INDEXER", // must not match the CREATE INDEX prefix
            "",
        ] {
            assert_eq!(
                ddl(tag).replication_impact(),
                DdlReplicationImpact::SchemaSensitive,
                "{tag} must stay schema-sensitive (fail-closed)"
            );
        }
    }

    #[test]
    fn leading_and_trailing_whitespace_is_tolerated() {
        assert_eq!(
            ddl("  CREATE INDEX  ").replication_impact(),
            DdlReplicationImpact::Irrelevant
        );
    }
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
