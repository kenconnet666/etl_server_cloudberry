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
    /// Per-relation structural changes (v2 envelope). Absent or empty for a v1
    /// message, in which case the engine has no structured transition to act on
    /// and treats the DDL conservatively.
    #[serde(default)]
    pub transitions: Vec<TableTransition>,
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

    /// Whether this event carries a non-empty v2 transition set in which every
    /// transition is online-safe. Only then can the engine follow the DDL with
    /// table transitions instead of a full rebuild; a v1 message (no
    /// transitions) or any unsafe/unknown transition returns false.
    #[must_use]
    pub fn all_transitions_online_safe(&self) -> bool {
        !self.transitions.is_empty()
            && self
                .transitions
                .iter()
                .all(|transition| transition.kind.is_online_safe())
    }
}

/// One managed relation's structural change described by a DDL event.
///
/// Emitted in the v2 DDL envelope so the engine can decide, per table, whether
/// the change is an online-safe transition (whitelisted) or must fall back to a
/// shadow rebuild. `before_*`/`after_*` fingerprints let a consumer detect an
/// identity change without re-reading the source catalog; `kind` records the
/// classified operation when the source could determine it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableTransition {
    pub relation_id: u32,
    /// Table generation before the DDL, if the relation already existed.
    pub before_generation: Option<u64>,
    /// Table generation after the DDL, if the relation still exists.
    pub after_generation: Option<u64>,
    /// Schema fingerprint before the DDL (None for a newly created table).
    pub before_fingerprint: Option<String>,
    /// Schema fingerprint after the DDL (None for a dropped table).
    pub after_fingerprint: Option<String>,
    pub kind: TransitionKind,
}

/// Classified DDL operation for one relation. `Unknown` is the conservative
/// default when the source cannot prove which online-safe category applies, and
/// it forces the fail-closed rebuild path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransitionKind {
    /// A column was added. `nullable_or_defaulted` is true when the add cannot
    /// rewrite existing rows unsafely (nullable, or NOT NULL with a constant
    /// default), which is the online-safe case.
    AddColumn {
        name: String,
        nullable_or_defaulted: bool,
    },
    /// A column was dropped.
    DropColumn { name: String },
    /// A column was renamed.
    RenameColumn { from: String, to: String },
    /// A column type changed. `widening` is true for a proven-compatible change
    /// (e.g. int4 -> int8, varchar(n) -> varchar(m>n) or text).
    AlterColumnType { name: String, widening: bool },
    /// A new table entered the managed scope.
    AddTable,
    /// A managed table was dropped and should be quarantined.
    DropTable,
    /// The source could not classify the change; treat as unsafe (rebuild).
    Unknown,
}

impl TransitionKind {
    /// Whether this classified operation is on the online-safe whitelist and can
    /// be applied as a table transition rather than a full rebuild.
    ///
    /// Deliberately conservative: only additive/rename/widening column changes,
    /// new tables, and drops (via quarantine) are online-safe. A narrowing type
    /// change, a NOT NULL add without a default, or an `Unknown` classification
    /// is not.
    #[must_use]
    pub const fn is_online_safe(&self) -> bool {
        match self {
            Self::AddColumn {
                nullable_or_defaulted,
                ..
            } => *nullable_or_defaulted,
            Self::AlterColumnType { widening, .. } => *widening,
            Self::DropColumn { .. }
            | Self::RenameColumn { .. }
            | Self::AddTable
            | Self::DropTable => true,
            Self::Unknown => false,
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
            transitions: Vec::new(),
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

    #[test]
    fn transition_kind_online_safety_is_conservative() {
        use super::TransitionKind;
        assert!(TransitionKind::AddColumn {
            name: "c".to_owned(),
            nullable_or_defaulted: true
        }
        .is_online_safe());
        assert!(!TransitionKind::AddColumn {
            name: "c".to_owned(),
            nullable_or_defaulted: false
        }
        .is_online_safe());
        assert!(TransitionKind::AlterColumnType {
            name: "c".to_owned(),
            widening: true
        }
        .is_online_safe());
        assert!(!TransitionKind::AlterColumnType {
            name: "c".to_owned(),
            widening: false
        }
        .is_online_safe());
        assert!(TransitionKind::DropColumn { name: "c".to_owned() }.is_online_safe());
        assert!(TransitionKind::RenameColumn {
            from: "a".to_owned(),
            to: "b".to_owned()
        }
        .is_online_safe());
        assert!(TransitionKind::AddTable.is_online_safe());
        assert!(TransitionKind::DropTable.is_online_safe());
        assert!(!TransitionKind::Unknown.is_online_safe());
    }

    #[test]
    fn all_transitions_online_safe_requires_nonempty_and_all_safe() {
        use super::{TableTransition, TransitionKind};
        // v1 message: no transitions -> not online-followable.
        assert!(!ddl("ALTER TABLE").all_transitions_online_safe());

        let safe = TableTransition {
            relation_id: 1,
            before_generation: Some(1),
            after_generation: Some(2),
            before_fingerprint: Some("a".to_owned()),
            after_fingerprint: Some("b".to_owned()),
            kind: TransitionKind::AddColumn {
                name: "note".to_owned(),
                nullable_or_defaulted: true,
            },
        };
        let unsafe_one = TableTransition {
            kind: TransitionKind::Unknown,
            ..safe.clone()
        };

        let mut message = ddl("ALTER TABLE");
        message.transitions = vec![safe.clone()];
        assert!(message.all_transitions_online_safe());

        message.transitions = vec![safe, unsafe_one];
        assert!(!message.all_transitions_online_safe());
    }

    #[test]
    fn v1_ddl_json_without_transitions_deserializes() {
        // A v1 payload has no `transitions` field; serde default must fill it.
        let json = r#"{
            "version": 1,
            "command_tag": "ALTER TABLE",
            "relation_ids": [42],
            "affected_schemas": ["public"],
            "schema_fingerprint": "fp"
        }"#;
        let message: DdlMessage = serde_json::from_str(json).unwrap();
        assert!(message.transitions.is_empty());
        assert_eq!(message.command_tag, "ALTER TABLE");
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
