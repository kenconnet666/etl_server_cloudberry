use std::fmt;

use serde::{Deserialize, Serialize};

use crate::{CoreError, CoreResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
    if value.is_empty() || value.contains('\0') {
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
}
