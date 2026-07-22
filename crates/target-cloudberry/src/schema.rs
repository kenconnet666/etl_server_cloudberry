//! Source-schema to Cloudberry DDL translation.

use std::collections::HashSet;

use cloudberry_etl_core::{
    CoreError,
    schema::{GeneratedColumn, PgType, PgTypeKind, QualifiedName, TableKind, TableSchema},
};
use thiserror::Error;

use crate::sql::{SqlRenderError, quote_identifier, quote_literal, quote_qualified_name};
use crate::storage::TargetStorage;

const MAX_NUMERIC_PRECISION: u16 = 1_000;
const MAX_CHARACTER_LENGTH: u32 = 10_485_760;
const MAX_BIT_LENGTH: u32 = 83_886_080;
const MAX_DATETIME_PRECISION: u8 = 6;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    #[error(transparent)]
    Core(#[from] CoreError),
    #[error(transparent)]
    Sql(#[from] SqlRenderError),
    #[error("type {data_type} is not supported by the Cloudberry 2.1 contract: {reason}")]
    UnsupportedType { data_type: String, reason: String },
    #[error("invalid typmod for {data_type}: {reason}")]
    InvalidTypmod {
        data_type: &'static str,
        reason: String,
    },
    #[error("table kind {0:?} has not been unlocked for Cloudberry apply")]
    UnsupportedTableKind(TableKind),
    #[error("table has duplicate column name `{0}`")]
    DuplicateColumn(String),
    #[error("table has duplicate or zero primary-key ordinal {0}")]
    InvalidPrimaryKeyOrdinal(u16),
    #[error("conflicting definitions map to target type {0}")]
    ConflictingUserType(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserTypeDefinition {
    /// Ordered labels of a PostgreSQL enum.  Keeping the labels structured lets the target
    /// reconciler apply safe `ADD VALUE`/`RENAME VALUE` changes without parsing SQL text.
    Enum { labels: Vec<String> },
    /// The already-rendered base type of a constraint-free domain.
    Domain { base_sql: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserTypePlan {
    pub name: QualifiedName,
    pub create_sql: String,
    pub definition: UserTypeDefinition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TypePlan {
    pub sql: String,
    pub prerequisites: Vec<UserTypePlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnPlan {
    pub source_attnum: i16,
    pub name: String,
    pub type_sql: String,
    pub nullable: bool,
    pub primary_key_ordinal: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTablePlan {
    pub target: QualifiedName,
    pub storage: TargetStorage,
    pub columns: Vec<ColumnPlan>,
    pub primary_key: Vec<String>,
    pub distribution_key: Vec<String>,
    pub prerequisites: Vec<UserTypePlan>,
    pub create_sql: String,
}

/// Maps a source PostgreSQL type to an exact Cloudberry 2.1 DDL type.
///
/// User-defined enum and constraint-free domain types are recreated in the
/// mapped target schema and returned as prerequisite DDL.
pub fn plan_type(data_type: &PgType, target_type_schema: &str) -> Result<TypePlan, SchemaError> {
    let mut prerequisites = Vec::new();
    let sql = plan_type_inner(data_type, target_type_schema, &mut prerequisites)?;
    Ok(TypePlan { sql, prerequisites })
}

/// Builds a business table whose distribution key is the complete source PK.
pub fn plan_create_table(
    source: &TableSchema,
    target: QualifiedName,
) -> Result<CreateTablePlan, SchemaError> {
    plan_create_table_with_storage(source, target, TargetStorage::default())
}

/// Builds a business table with an explicit physical storage profile.
pub fn plan_create_table_with_storage(
    source: &TableSchema,
    target: QualifiedName,
    storage: TargetStorage,
) -> Result<CreateTablePlan, SchemaError> {
    source.validate_supported()?;
    match source.kind {
        TableKind::Ordinary | TableKind::CitusDistributed => {}
        other => return Err(SchemaError::UnsupportedTableKind(other)),
    }

    let mut names = HashSet::new();
    let mut pk_ordinals = HashSet::new();
    let mut columns = Vec::with_capacity(source.columns.len());
    let mut prerequisites = Vec::new();

    for column in &source.columns {
        if !names.insert(column.name.clone()) {
            return Err(SchemaError::DuplicateColumn(column.name.clone()));
        }
        if let Some(ordinal) = column.primary_key_ordinal
            && (ordinal == 0 || !pk_ordinals.insert(ordinal))
        {
            return Err(SchemaError::InvalidPrimaryKeyOrdinal(ordinal));
        }
        if column.generated == GeneratedColumn::Virtual {
            return Err(unsupported_type(
                &column.data_type,
                "virtual generated columns cannot be materialized from pgoutput",
            ));
        }

        let mapped = plan_type(&column.data_type, &target.schema)?;
        for prerequisite in mapped.prerequisites {
            let key = prerequisite.name.to_string();
            if let Some(existing) = prerequisites
                .iter()
                .find(|existing: &&UserTypePlan| existing.name.to_string() == key)
            {
                if existing != &prerequisite {
                    return Err(SchemaError::ConflictingUserType(key));
                }
            } else {
                prerequisites.push(prerequisite);
            }
        }

        let collation = match &column.collation {
            Some(source_collation) => {
                let target_collation = if source_collation.schema == "pg_catalog" {
                    source_collation.clone()
                } else {
                    QualifiedName::new(&target.schema, &source_collation.name)?
                };
                format!(" COLLATE {}", quote_qualified_name(&target_collation)?)
            }
            None => String::new(),
        };
        columns.push(ColumnPlan {
            source_attnum: column.attnum,
            name: column.name.clone(),
            type_sql: format!("{}{}", mapped.sql, collation),
            nullable: column.nullable,
            primary_key_ordinal: column.primary_key_ordinal,
        });
    }

    let primary_key: Vec<_> = source
        .primary_key()
        .into_iter()
        .map(|column| column.name.clone())
        .collect();
    let create_sql = render_create_table(&target, &columns, &primary_key, storage)?;
    Ok(CreateTablePlan {
        target,
        storage,
        columns,
        distribution_key: primary_key.clone(),
        primary_key,
        prerequisites,
        create_sql,
    })
}

fn render_create_table(
    target: &QualifiedName,
    columns: &[ColumnPlan],
    primary_key: &[String],
    storage: TargetStorage,
) -> Result<String, SchemaError> {
    let mut definitions = Vec::with_capacity(columns.len() + 1);
    for column in columns {
        let nullability = if column.nullable { "" } else { " NOT NULL" };
        definitions.push(format!(
            "    {} {}{}",
            quote_identifier(&column.name)?,
            column.type_sql,
            nullability
        ));
    }
    let quoted_key = quote_identifier_list(primary_key)?;
    definitions.push(format!("    PRIMARY KEY ({quoted_key})"));

    Ok(format!(
        "CREATE TABLE {} (\n{}\n)\n{}\nDISTRIBUTED BY ({})",
        quote_qualified_name(target)?,
        definitions.join(",\n"),
        storage.create_clause(),
        quoted_key
    ))
}

pub(crate) fn quote_identifier_list(values: &[String]) -> Result<String, SqlRenderError> {
    values
        .iter()
        .map(|value| quote_identifier(value))
        .collect::<Result<Vec<_>, _>>()
        .map(|values| values.join(", "))
}

fn plan_type_inner(
    data_type: &PgType,
    target_type_schema: &str,
    prerequisites: &mut Vec<UserTypePlan>,
) -> Result<String, SchemaError> {
    let sql = match &data_type.kind {
        PgTypeKind::Bool => "boolean".to_owned(),
        PgTypeKind::Int2 => "smallint".to_owned(),
        PgTypeKind::Int4 => "integer".to_owned(),
        PgTypeKind::Int8 => "bigint".to_owned(),
        PgTypeKind::Numeric { precision, scale } => numeric_sql(*precision, *scale)?,
        PgTypeKind::Float4 => "real".to_owned(),
        PgTypeKind::Float8 => "double precision".to_owned(),
        PgTypeKind::Text => "text".to_owned(),
        PgTypeKind::VarChar { length } => character_sql("character varying", *length)?,
        PgTypeKind::Char { length } => character_sql("character", *length)?,
        PgTypeKind::Bytea => "bytea".to_owned(),
        PgTypeKind::Date => "date".to_owned(),
        PgTypeKind::Time {
            precision,
            with_time_zone,
        } => datetime_sql("time", *precision, *with_time_zone)?,
        PgTypeKind::Timestamp {
            precision,
            with_time_zone,
        } => datetime_sql("timestamp", *precision, *with_time_zone)?,
        PgTypeKind::Interval { precision } => {
            let suffix = precision_suffix("interval", *precision)?;
            format!("interval{suffix}")
        }
        PgTypeKind::Uuid => "uuid".to_owned(),
        PgTypeKind::Json => "json".to_owned(),
        PgTypeKind::Jsonb => "jsonb".to_owned(),
        PgTypeKind::Inet => "inet".to_owned(),
        PgTypeKind::Cidr => "cidr".to_owned(),
        PgTypeKind::MacAddr => "macaddr".to_owned(),
        PgTypeKind::MacAddr8 => "macaddr8".to_owned(),
        PgTypeKind::Bit { length, varying } => bit_sql(*length, *varying)?,
        PgTypeKind::Xml => {
            return Err(unsupported_type(
                data_type,
                "XML is outside the verified current-state type contract",
            ));
        }
        PgTypeKind::Enum { labels } => {
            if labels.is_empty() {
                return Err(unsupported_type(data_type, "enum has no labels"));
            }
            let mut unique = HashSet::new();
            if labels.iter().any(|label| !unique.insert(label)) {
                return Err(unsupported_type(data_type, "enum has duplicate labels"));
            }
            let target_name = QualifiedName::new(target_type_schema, &data_type.name.name)?;
            let rendered_labels = labels
                .iter()
                .map(|label| quote_literal(label))
                .collect::<Result<Vec<_>, _>>()?
                .join(", ");
            prerequisites.push(UserTypePlan {
                name: target_name.clone(),
                create_sql: format!(
                    "CREATE TYPE {} AS ENUM ({rendered_labels})",
                    quote_qualified_name(&target_name)?
                ),
                definition: UserTypeDefinition::Enum {
                    labels: labels.clone(),
                },
            });
            quote_qualified_name(&target_name)?
        }
        PgTypeKind::Domain { base, constraints } => {
            if !constraints.is_empty() {
                return Err(unsupported_type(
                    data_type,
                    "raw domain constraints are not safe to replay",
                ));
            }
            let base_sql = plan_type_inner(base, target_type_schema, prerequisites)?;
            let target_name = QualifiedName::new(target_type_schema, &data_type.name.name)?;
            prerequisites.push(UserTypePlan {
                name: target_name.clone(),
                create_sql: format!(
                    "CREATE DOMAIN {} AS {base_sql}",
                    quote_qualified_name(&target_name)?
                ),
                definition: UserTypeDefinition::Domain { base_sql },
            });
            quote_qualified_name(&target_name)?
        }
        PgTypeKind::Array { element } => {
            format!(
                "{}[]",
                plan_type_inner(element, target_type_schema, prerequisites)?
            )
        }
        PgTypeKind::Unsupported { reason } => {
            return Err(unsupported_type(data_type, reason));
        }
    };
    Ok(sql)
}

fn numeric_sql(precision: Option<u16>, scale: Option<i16>) -> Result<String, SchemaError> {
    match (precision, scale) {
        (None, None) => Ok("numeric".to_owned()),
        (Some(precision), None) => {
            validate_precision(precision)?;
            Ok(format!("numeric({precision})"))
        }
        (Some(precision), Some(scale)) => {
            validate_precision(precision)?;
            if scale < 0 {
                return Err(invalid_typmod(
                    "numeric",
                    "negative scale is a PostgreSQL 15+ feature absent from the PG14-based target",
                ));
            }
            if u16::try_from(scale).expect("non-negative i16 fits u16") > precision {
                return Err(invalid_typmod(
                    "numeric",
                    "scale must not exceed precision on Cloudberry 2.1",
                ));
            }
            Ok(format!("numeric({precision},{scale})"))
        }
        (None, Some(_)) => Err(invalid_typmod(
            "numeric",
            "scale cannot be specified without precision",
        )),
    }
}

fn validate_precision(precision: u16) -> Result<(), SchemaError> {
    if (1..=MAX_NUMERIC_PRECISION).contains(&precision) {
        Ok(())
    } else {
        Err(invalid_typmod(
            "numeric",
            format!("precision must be between 1 and {MAX_NUMERIC_PRECISION}"),
        ))
    }
}

fn character_sql(data_type: &'static str, length: Option<u32>) -> Result<String, SchemaError> {
    match length {
        None => Ok(data_type.to_owned()),
        Some(length) if (1..=MAX_CHARACTER_LENGTH).contains(&length) => {
            Ok(format!("{data_type}({length})"))
        }
        Some(_) => Err(invalid_typmod(
            data_type,
            format!("length must be between 1 and {MAX_CHARACTER_LENGTH}"),
        )),
    }
}

fn datetime_sql(
    data_type: &'static str,
    precision: Option<u8>,
    with_time_zone: bool,
) -> Result<String, SchemaError> {
    let precision = precision_suffix(data_type, precision)?;
    let zone = if with_time_zone {
        " with time zone"
    } else {
        " without time zone"
    };
    Ok(format!("{data_type}{precision}{zone}"))
}

fn precision_suffix(data_type: &'static str, precision: Option<u8>) -> Result<String, SchemaError> {
    match precision {
        None => Ok(String::new()),
        Some(precision) if precision <= MAX_DATETIME_PRECISION => Ok(format!("({precision})")),
        Some(_) => Err(invalid_typmod(
            data_type,
            format!("precision must be at most {MAX_DATETIME_PRECISION}"),
        )),
    }
}

fn bit_sql(length: Option<u32>, varying: bool) -> Result<String, SchemaError> {
    let name = if varying { "bit varying" } else { "bit" };
    match length {
        None => Ok(name.to_owned()),
        Some(length) if (1..=MAX_BIT_LENGTH).contains(&length) => Ok(format!("{name}({length})")),
        Some(_) => Err(invalid_typmod(
            name,
            format!("length must be between 1 and {MAX_BIT_LENGTH}"),
        )),
    }
}

fn invalid_typmod(data_type: &'static str, reason: impl Into<String>) -> SchemaError {
    SchemaError::InvalidTypmod {
        data_type,
        reason: reason.into(),
    }
}

fn unsupported_type(data_type: &PgType, reason: impl Into<String>) -> SchemaError {
    SchemaError::UnsupportedType {
        data_type: data_type.name.to_string(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use cloudberry_etl_core::schema::{ColumnSchema, IdentityColumn, ReplicaIdentity};

    use super::*;

    fn pg_type(oid: u32, name: &str, kind: PgTypeKind) -> PgType {
        PgType {
            oid,
            name: QualifiedName::new("pg_catalog", name).unwrap(),
            kind,
        }
    }

    fn column(attnum: i16, name: &str, kind: PgTypeKind, pk: Option<u16>) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: pg_type(attnum as u32, name, kind),
            nullable: pk.is_none(),
            primary_key_ordinal: pk,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn table(columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            relation_id: 42,
            generation: 1,
            name: QualifiedName::new("public", "source_orders").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns,
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    #[test]
    fn maps_cloudberry_21_builtin_types_without_text_fallback() {
        let cases = [
            (PgTypeKind::Bool, "boolean"),
            (PgTypeKind::Int4, "integer"),
            (
                PgTypeKind::Numeric {
                    precision: Some(20),
                    scale: Some(4),
                },
                "numeric(20,4)",
            ),
            (
                PgTypeKind::Timestamp {
                    precision: Some(3),
                    with_time_zone: true,
                },
                "timestamp(3) with time zone",
            ),
            (
                PgTypeKind::Bit {
                    length: Some(8),
                    varying: true,
                },
                "bit varying(8)",
            ),
        ];

        for (kind, expected) in cases {
            let mapped = plan_type(&pg_type(1, "test", kind), "target").unwrap();
            assert_eq!(mapped.sql, expected);
            assert!(mapped.prerequisites.is_empty());
        }
    }

    #[test]
    fn rejects_pg18_typmods_missing_from_the_pg14_based_target() {
        let result = plan_type(
            &pg_type(
                1,
                "numeric",
                PgTypeKind::Numeric {
                    precision: Some(10),
                    scale: Some(-2),
                },
            ),
            "target",
        );
        assert!(matches!(result, Err(SchemaError::InvalidTypmod { .. })));
    }

    #[test]
    fn enum_stays_a_typed_enum_and_quotes_labels() {
        let data_type = PgType {
            oid: 9_001,
            name: QualifiedName::new("source", "order_status").unwrap(),
            kind: PgTypeKind::Enum {
                labels: vec!["new".into(), "customer's".into()],
            },
        };
        let plan = plan_type(&data_type, "tenant").unwrap();
        assert_eq!(plan.sql, r#""tenant"."order_status""#);
        assert_eq!(
            plan.prerequisites[0].create_sql,
            r#"CREATE TYPE "tenant"."order_status" AS ENUM (E'new', E'customer''s')"#
        );
        assert_eq!(
            plan.prerequisites[0].definition,
            UserTypeDefinition::Enum {
                labels: vec!["new".into(), "customer's".into()]
            }
        );
    }

    #[test]
    fn builds_aoco_table_distributed_by_the_complete_pk() {
        let source = table(vec![
            column(1, "tenant_id", PgTypeKind::Int4, Some(2)),
            column(2, "payload", PgTypeKind::VarChar { length: Some(64) }, None),
            column(3, "id", PgTypeKind::Int8, Some(1)),
        ]);
        let plan = plan_create_table(
            &source,
            QualifiedName::new("tenant\"one", "select").unwrap(),
        )
        .unwrap();

        assert_eq!(plan.storage, TargetStorage::AoColumn);
        assert_eq!(plan.primary_key, ["id", "tenant_id"]);
        assert_eq!(plan.distribution_key, plan.primary_key);
        assert!(
            plan.create_sql
                .starts_with("CREATE TABLE \"tenant\"\"one\".\"select\"")
        );
        assert!(
            plan.create_sql
                .contains("PRIMARY KEY (\"id\", \"tenant_id\")")
        );
        assert!(
            plan.create_sql
                .contains("\nUSING ao_column WITH (compresstype='zstd', compresslevel=1)\n")
        );
        assert!(
            plan.create_sql
                .ends_with("DISTRIBUTED BY (\"id\", \"tenant_id\")")
        );
    }

    #[test]
    fn explicit_storage_profiles_render_only_the_supported_access_methods() {
        let source = table(vec![column(1, "id", PgTypeKind::Int8, Some(1))]);
        for (storage, clause) in [
            (
                TargetStorage::AoColumn,
                "USING ao_column WITH (compresstype='zstd', compresslevel=1)",
            ),
            (
                TargetStorage::PaxExperimental,
                "USING pax WITH (storage_format='porc', compresstype='zstd', compresslevel=1)",
            ),
        ] {
            let plan = plan_create_table_with_storage(
                &source,
                QualifiedName::new("target", format!("items_{storage:?}")).unwrap(),
                storage,
            )
            .unwrap();
            assert_eq!(plan.storage, storage);
            assert!(plan.create_sql.contains(clause));
        }
    }

    #[test]
    fn rejects_validation_gated_table_kinds() {
        let mut source = table(vec![column(1, "id", PgTypeKind::Int8, Some(1))]);
        source.kind = TableKind::CitusReference;
        assert_eq!(
            plan_create_table(&source, QualifiedName::new("target", "orders").unwrap()),
            Err(SchemaError::UnsupportedTableKind(TableKind::CitusReference))
        );
    }

    #[test]
    fn rejects_raw_domain_constraints_instead_of_replaying_sql() {
        let domain = PgType {
            oid: 9_002,
            name: QualifiedName::new("source", "positive_int").unwrap(),
            kind: PgTypeKind::Domain {
                base: Box::new(pg_type(23, "int4", PgTypeKind::Int4)),
                constraints: vec!["CHECK (VALUE > 0); DROP TABLE x".into()],
            },
        };
        assert!(matches!(
            plan_type(&domain, "target"),
            Err(SchemaError::UnsupportedType { .. })
        ));
    }
}
