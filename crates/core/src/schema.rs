use std::fmt;

use serde::{Deserialize, Deserializer, Serialize};

use crate::{CoreError, CoreResult};

/// PostgreSQL stores at most `NAMEDATALEN - 1` bytes for an identifier. Cloudberry inherits the
/// same limit. Counting UTF-8 bytes here prevents the server from silently truncating a name.
pub const POSTGRES_IDENTIFIER_MAX_BYTES: usize = 63;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct QualifiedName {
    pub schema: String,
    pub name: String,
}

impl QualifiedName {
    pub fn new(schema: impl Into<String>, name: impl Into<String>) -> CoreResult<Self> {
        let value = Self {
            schema: schema.into(),
            name: name.into(),
        };
        validate_identifier(&value.schema)?;
        validate_identifier(&value.name)?;
        Ok(value)
    }
}

impl<'de> Deserialize<'de> for QualifiedName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Fields {
            schema: String,
            name: String,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.schema, fields.name).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for QualifiedName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}", self.schema, self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PgType {
    pub oid: u32,
    pub name: QualifiedName,
    pub kind: PgTypeKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum PgTypeKind {
    Bool,
    Int2,
    Int4,
    Int8,
    Numeric {
        precision: Option<u16>,
        scale: Option<i16>,
    },
    Float4,
    Float8,
    Text,
    VarChar {
        length: Option<u32>,
    },
    Char {
        length: Option<u32>,
    },
    Bytea,
    Date,
    Time {
        precision: Option<u8>,
        with_time_zone: bool,
    },
    Timestamp {
        precision: Option<u8>,
        with_time_zone: bool,
    },
    Interval {
        precision: Option<u8>,
    },
    Uuid,
    Json,
    Jsonb,
    Inet,
    Cidr,
    MacAddr,
    MacAddr8,
    Bit {
        length: Option<u32>,
        varying: bool,
    },
    Xml,
    Enum {
        labels: Vec<String>,
    },
    Domain {
        base: Box<PgType>,
        constraints: Vec<String>,
    },
    Array {
        element: Box<PgType>,
    },
    Unsupported {
        reason: String,
    },
}

impl PgTypeKind {
    #[must_use]
    pub fn is_supported(&self) -> bool {
        match self {
            Self::Unsupported { .. } => false,
            Self::Array { element } => element.kind.is_supported(),
            Self::Domain { base, .. } => base.kind.is_supported(),
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GeneratedColumn {
    None,
    Stored,
    Virtual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityColumn {
    None,
    Always,
    ByDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub attnum: i16,
    pub name: String,
    pub data_type: PgType,
    pub nullable: bool,
    pub primary_key_ordinal: Option<u16>,
    pub generated: GeneratedColumn,
    pub identity: IdentityColumn,
    pub collation: Option<QualifiedName>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableKind {
    Ordinary,
    Partitioned,
    CitusDistributed,
    CitusReference,
    CitusLocal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaIdentity {
    Default,
    Index,
    Full,
    Nothing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableSchema {
    pub relation_id: u32,
    pub generation: u64,
    pub name: QualifiedName,
    pub kind: TableKind,
    pub replica_identity: ReplicaIdentity,
    pub columns: Vec<ColumnSchema>,
    pub distribution_key: Vec<i16>,
    pub partition_key: Vec<i16>,
}

impl TableSchema {
    pub fn validate_supported(&self) -> CoreResult<()> {
        let primary_key = self.primary_key();
        if primary_key.is_empty() {
            return Err(CoreError::MissingPrimaryKey(self.name.to_string()));
        }
        if self.replica_identity != ReplicaIdentity::Default {
            return Err(self.unsupported("REPLICA IDENTITY must be DEFAULT"));
        }
        if let Some(column) = self.columns.iter().find(|column| {
            !column.data_type.kind.is_supported() || column.generated == GeneratedColumn::Virtual
        }) {
            return Err(self.unsupported(format!(
                "column {} uses an unsupported type or virtual generation",
                column.name
            )));
        }
        for column in &primary_key {
            self.validate_primary_key_column(column)?;
        }
        if self.kind == TableKind::CitusDistributed
            && !self
                .distribution_key
                .iter()
                .all(|attnum| primary_key.iter().any(|column| column.attnum == *attnum))
        {
            return Err(self.unsupported("Citus primary key must contain the distribution key"));
        }
        Ok(())
    }

    fn validate_primary_key_column(&self, column: &ColumnSchema) -> CoreResult<()> {
        let expected_builtin = match &column.data_type.kind {
            PgTypeKind::Int2 => Some(("int2", 21)),
            PgTypeKind::Int4 => Some(("int4", 23)),
            PgTypeKind::Int8 => Some(("int8", 20)),
            PgTypeKind::Uuid => Some(("uuid", 2950)),
            PgTypeKind::Text => Some(("text", 25)),
            PgTypeKind::VarChar { .. } => Some(("varchar", 1043)),
            _ => None,
        };
        let Some((expected_name, expected_oid)) = expected_builtin else {
            return Err(self.unsupported(format!(
                "primary-key column {} uses {}; production key folding currently supports only pg_catalog.int2, pg_catalog.int4, pg_catalog.int8, pg_catalog.uuid, and pg_catalog.text/varchar with an explicit C or POSIX collation",
                column.name, column.data_type.name
            )));
        };
        if column.data_type.name.schema != "pg_catalog"
            || column.data_type.name.name != expected_name
            || column.data_type.oid != expected_oid
        {
            return Err(self.unsupported(format!(
                "primary-key column {} has type identity {} (OID {}), expected pg_catalog.{expected_name} (OID {expected_oid})",
                column.name, column.data_type.name, column.data_type.oid
            )));
        }
        if matches!(
            &column.data_type.kind,
            PgTypeKind::Text | PgTypeKind::VarChar { .. }
        ) && !matches!(
            column.collation.as_ref(),
            Some(collation)
                if collation.schema == "pg_catalog"
                    && matches!(collation.name.as_str(), "C" | "POSIX")
        ) {
            let collation = column
                .collation
                .as_ref()
                .map_or_else(|| "<none>".to_owned(), ToString::to_string);
            return Err(self.unsupported(format!(
                "primary-key column {} uses collation {collation}; text/varchar keys require explicit pg_catalog.C or pg_catalog.POSIX",
                column.name
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn primary_key(&self) -> Vec<&ColumnSchema> {
        let mut columns: Vec<_> = self
            .columns
            .iter()
            .filter(|column| column.primary_key_ordinal.is_some())
            .collect();
        columns.sort_by_key(|column| column.primary_key_ordinal);
        columns
    }

    fn unsupported(&self, reason: impl Into<String>) -> CoreError {
        CoreError::UnsupportedTable {
            table: self.name.to_string(),
            reason: reason.into(),
        }
    }
}

pub fn validate_identifier(value: &str) -> CoreResult<()> {
    if value.is_empty() || value.contains('\0') || value.len() > POSTGRES_IDENTIFIER_MAX_BYTES {
        return Err(CoreError::InvalidIdentifier(value.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_type() -> PgType {
        PgType {
            oid: 23,
            name: QualifiedName::new("pg_catalog", "int4").unwrap(),
            kind: PgTypeKind::Int4,
        }
    }

    fn table_with_key(data_type: PgType, collation: Option<QualifiedName>) -> TableSchema {
        TableSchema {
            relation_id: 1,
            generation: 1,
            name: QualifiedName::new("public", "key_contract").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![ColumnSchema {
                attnum: 1,
                name: "id".into(),
                data_type,
                nullable: false,
                primary_key_ordinal: Some(1),
                generated: GeneratedColumn::None,
                identity: IdentityColumn::None,
                collation,
            }],
            distribution_key: vec![],
            partition_key: vec![],
        }
    }

    #[test]
    fn primary_key_is_returned_in_index_order() {
        let table = TableSchema {
            relation_id: 1,
            generation: 1,
            name: QualifiedName::new("public", "orders").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                ColumnSchema {
                    attnum: 1,
                    name: "tenant_id".into(),
                    data_type: int_type(),
                    nullable: false,
                    primary_key_ordinal: Some(2),
                    generated: GeneratedColumn::None,
                    identity: IdentityColumn::None,
                    collation: None,
                },
                ColumnSchema {
                    attnum: 2,
                    name: "id".into(),
                    data_type: int_type(),
                    nullable: false,
                    primary_key_ordinal: Some(1),
                    generated: GeneratedColumn::None,
                    identity: IdentityColumn::None,
                    collation: None,
                },
            ],
            distribution_key: vec![],
            partition_key: vec![],
        };

        let names: Vec<_> = table
            .primary_key()
            .into_iter()
            .map(|column| column.name.as_str())
            .collect();
        assert_eq!(names, ["id", "tenant_id"]);
    }

    #[test]
    fn primary_key_contract_accepts_only_canonical_builtin_keys() {
        for (oid, name, kind) in [
            (21, "int2", PgTypeKind::Int2),
            (23, "int4", PgTypeKind::Int4),
            (20, "int8", PgTypeKind::Int8),
            (2950, "uuid", PgTypeKind::Uuid),
        ] {
            let data_type = PgType {
                oid,
                name: QualifiedName::new("pg_catalog", name).unwrap(),
                kind,
            };
            table_with_key(data_type, None)
                .validate_supported()
                .unwrap();
        }

        for (oid, name, kind) in [
            (25, "text", PgTypeKind::Text),
            (1043, "varchar", PgTypeKind::VarChar { length: Some(64) }),
        ] {
            for collation in ["C", "POSIX"] {
                let data_type = PgType {
                    oid,
                    name: QualifiedName::new("pg_catalog", name).unwrap(),
                    kind: kind.clone(),
                };
                table_with_key(
                    data_type,
                    Some(QualifiedName::new("pg_catalog", collation).unwrap()),
                )
                .validate_supported()
                .unwrap();
            }
        }
    }

    #[test]
    fn primary_key_contract_rejects_noncanonical_types_collations_and_identities() {
        let numeric = PgType {
            oid: 1700,
            name: QualifiedName::new("pg_catalog", "numeric").unwrap(),
            kind: PgTypeKind::Numeric {
                precision: Some(12),
                scale: Some(2),
            },
        };
        let error = table_with_key(numeric, None)
            .validate_supported()
            .unwrap_err()
            .to_string();
        assert!(error.contains("production key folding"));

        let text = PgType {
            oid: 25,
            name: QualifiedName::new("pg_catalog", "text").unwrap(),
            kind: PgTypeKind::Text,
        };
        let error = table_with_key(
            text,
            Some(QualifiedName::new("pg_catalog", "default").unwrap()),
        )
        .validate_supported()
        .unwrap_err()
        .to_string();
        assert!(error.contains("require explicit pg_catalog.C or pg_catalog.POSIX"));

        let disguised = PgType {
            oid: 90_001,
            name: QualifiedName::new("application", "int8").unwrap(),
            kind: PgTypeKind::Int8,
        };
        let error = table_with_key(disguised, None)
            .validate_supported()
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected pg_catalog.int8"));

        let wrong_oid = PgType {
            oid: 90_001,
            name: QualifiedName::new("pg_catalog", "int8").unwrap(),
            kind: PgTypeKind::Int8,
        };
        let error = table_with_key(wrong_oid, None)
            .validate_supported()
            .unwrap_err()
            .to_string();
        assert!(error.contains("expected pg_catalog.int8 (OID 20)"));
    }

    #[test]
    fn identifiers_are_limited_by_utf8_bytes() {
        let ascii_limit = "a".repeat(POSTGRES_IDENTIFIER_MAX_BYTES);
        let utf8_limit = "界".repeat(POSTGRES_IDENTIFIER_MAX_BYTES / 3);

        assert!(validate_identifier(&ascii_limit).is_ok());
        assert!(validate_identifier(&utf8_limit).is_ok());
        assert!(validate_identifier(&format!("{ascii_limit}a")).is_err());
        assert!(validate_identifier(&format!("{utf8_limit}界")).is_err());
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("invalid\0name").is_err());
    }

    #[test]
    fn qualified_name_deserialization_preserves_identifier_invariants() {
        let valid = serde_json::json!({
            "schema": "界".repeat(21),
            "name": "a".repeat(63),
        });
        assert!(serde_json::from_value::<QualifiedName>(valid).is_ok());

        let truncated_by_postgres = serde_json::json!({
            "schema": "public",
            "name": "界".repeat(22),
        });
        assert!(serde_json::from_value::<QualifiedName>(truncated_by_postgres).is_err());
    }
}
