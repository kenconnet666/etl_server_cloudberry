//! PostgreSQL catalog inspection and source-contract validation.
//!
//! The catalog layer deliberately returns `cloudberry-etl-core` types.  Driver rows and OIDs
//! stay inside this crate, so the rest of the pipeline does not depend on PostgreSQL client
//! implementation details.

use std::collections::{HashMap, HashSet};

use cloudberry_etl_core::schema::{
    ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
    ReplicaIdentity, TableKind, TableSchema,
};
use tokio_postgres::{Client, Row};

use crate::{SourceError, SourceResult, citus::CitusTopology};

const EXPECTED_MAJOR: i64 = 18;

#[derive(Debug, Clone)]
pub struct PreflightOptions {
    pub metadata_schema: String,
    pub expected_major: i64,
    pub require_logical_replication: bool,
}

impl Default for PreflightOptions {
    fn default() -> Self {
        Self {
            metadata_schema: "pg2cb_meta".to_owned(),
            expected_major: EXPECTED_MAJOR,
            require_logical_replication: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceIdentity {
    pub system_identifier: u64,
    pub timeline: u32,
    pub database: String,
    pub server_version_num: i64,
    pub server_version: String,
    pub in_recovery: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreflightReport {
    pub identity: SourceIdentity,
    pub server_encoding: String,
    pub wal_level: String,
    pub max_replication_slots: i64,
    pub max_wal_senders: i64,
    pub existing_logical_slots: i64,
    pub citus_version: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CatalogOptions {
    pub metadata_schema: String,
    pub include_schemas: Option<HashSet<String>>,
    pub exclude_schemas: HashSet<String>,
    pub include_partitions: bool,
}

impl Default for CatalogOptions {
    fn default() -> Self {
        let exclude_schemas = [
            "pg_catalog",
            "information_schema",
            "pg_toast",
            "pg_temp_1",
            "pg_toast_temp_1",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect();
        Self {
            metadata_schema: "pg2cb_meta".to_owned(),
            include_schemas: None,
            exclude_schemas,
            include_partitions: true,
        }
    }
}

#[derive(Debug, Clone)]
struct TypeDescriptor {
    oid: u32,
    schema: String,
    name: String,
    typtype: String,
    base_oid: Option<u32>,
    element_oid: Option<u32>,
    labels: Vec<String>,
    constraints: Vec<String>,
}

/// Run global source checks before creating a publication or slot.
pub async fn preflight(
    client: &Client,
    options: &PreflightOptions,
) -> SourceResult<PreflightReport> {
    let server_version_num = show_i64(client, "server_version_num").await?;
    if server_version_num / 10_000 != options.expected_major {
        return Err(SourceError::contract(format!(
            "PostgreSQL major version {} is required, got server_version_num {server_version_num}",
            options.expected_major
        )));
    }
    let server_version = show(client, "server_version").await?;
    let server_encoding = show(client, "server_encoding").await?;
    if !server_encoding.eq_ignore_ascii_case("UTF8") {
        return Err(SourceError::contract(format!(
            "server_encoding must be UTF8, got {server_encoding}"
        )));
    }
    let wal_level = show(client, "wal_level").await?;
    if options.require_logical_replication && !wal_level.eq_ignore_ascii_case("logical") {
        return Err(SourceError::contract(format!(
            "wal_level must be logical, got {wal_level}"
        )));
    }
    let max_replication_slots = show_i64(client, "max_replication_slots").await?;
    let max_wal_senders = show_i64(client, "max_wal_senders").await?;
    if options.require_logical_replication && (max_replication_slots < 1 || max_wal_senders < 1) {
        return Err(SourceError::contract(
            "max_replication_slots and max_wal_senders must both be positive",
        ));
    }
    if options.metadata_schema.is_empty() || options.metadata_schema == "pg_catalog" {
        return Err(SourceError::contract(
            "metadata schema must be a dedicated non-system schema",
        ));
    }

    let identity_row = client
        .query_one(
            "SELECT system_identifier::text, timeline_id::int8, current_database(), pg_is_in_recovery() \
             FROM pg_control_system()",
            &[],
        )
        .await?;
    let identity = SourceIdentity {
        system_identifier: parse_u64(&identity_row, 0, "system_identifier")?,
        timeline: parse_u32(&identity_row, 1, "timeline_id")?,
        database: identity_row.try_get::<_, String>(2)?,
        server_version_num,
        server_version,
        in_recovery: identity_row.try_get(3)?,
    };
    let existing_logical_slots = client
        .query_one(
            "SELECT count(*)::int8 FROM pg_replication_slots WHERE slot_type = 'logical'",
            &[],
        )
        .await?
        .try_get::<_, i64>(0)?;
    let citus_version = detect_citus(client).await?;

    Ok(PreflightReport {
        identity,
        server_encoding,
        wal_level,
        max_replication_slots,
        max_wal_senders,
        existing_logical_slots,
        citus_version,
    })
}

/// Load all eligible user tables and their strongly typed column schemas.
pub async fn load_tables(
    client: &Client,
    options: &CatalogOptions,
) -> SourceResult<Vec<TableSchema>> {
    let type_descriptors = load_type_descriptors(client).await?;
    let rows = client
        .query(
            "SELECT c.oid::int8 AS relation_id,
                    n.nspname AS schema_name,
                    c.relname AS relation_name,
                    c.relkind::text,
                    c.relispartition,
                    a.attnum::int4,
                    a.attname,
                    a.atttypid::int8 AS type_oid,
                    a.atttypmod::int8 AS type_modifier,
                    a.attnotnull,
                    a.attgenerated::text,
                    a.attidentity::text,
                    c.relreplident::text,
                    CASE WHEN i.indisprimary THEN array_position(i.indkey, a.attnum)::int4 END AS pk_ordinal,
                    coll_ns.nspname AS collation_schema,
                    coll.collname AS collation_name,
                    COALESCE((SELECT partattrs::text FROM pg_partitioned_table p WHERE p.partrelid = c.oid), '{}') AS partition_attrs
               FROM pg_class c
               JOIN pg_namespace n ON n.oid = c.relnamespace
               JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
               LEFT JOIN pg_index i ON i.indrelid = c.oid AND i.indisprimary AND i.indisvalid AND i.indisready
               LEFT JOIN pg_collation coll ON coll.oid = a.attcollation AND a.attcollation <> 0
               LEFT JOIN pg_namespace coll_ns ON coll_ns.oid = coll.collnamespace
              WHERE c.relkind IN ('r', 'p')
                AND NOT c.relpersistence = 't'
              ORDER BY c.oid, a.attnum",
            &[],
        )
        .await?;

    let mut tables: Vec<TableSchema> = Vec::new();
    for row in rows {
        let schema_name: String = row.try_get("schema_name")?;
        if !eligible_schema(&schema_name, options) {
            continue;
        }
        let relation_name: String = row.try_get("relation_name")?;
        let relation_id = parse_u32_value(row.try_get::<_, i64>("relation_id")?, "relation_id")?;
        let relation_kind: String = row.try_get("relkind")?;
        let is_partition = row.try_get::<_, bool>("relispartition")?;
        if is_partition && !options.include_partitions {
            continue;
        }
        let qualified_name = QualifiedName::new(schema_name.clone(), relation_name.clone())
            .map_err(|error| SourceError::contract(error.to_string()))?;
        let replica_identity = parse_replica_identity(
            row.try_get::<_, String>("relreplident")?
                .chars()
                .next()
                .unwrap_or('d'),
        )?;
        let type_oid = parse_u32_value(row.try_get::<_, i64>("type_oid")?, "type_oid")?;
        let descriptor = type_descriptors
            .get(&type_oid)
            .ok_or_else(|| SourceError::contract(format!("type OID {type_oid} was not found")))?;
        let data_type = resolve_type(descriptor, &type_descriptors, row.try_get("type_modifier")?)?;
        let collation = match (
            row.try_get::<_, Option<String>>("collation_schema")?,
            row.try_get::<_, Option<String>>("collation_name")?,
        ) {
            (Some(schema), Some(name)) => Some(
                QualifiedName::new(schema, name)
                    .map_err(|error| SourceError::contract(error.to_string()))?,
            ),
            _ => None,
        };
        let column = ColumnSchema {
            attnum: row.try_get::<_, i32>("attnum")? as i16,
            name: row.try_get("attname")?,
            data_type,
            nullable: !row.try_get::<_, bool>("attnotnull")?,
            primary_key_ordinal: row
                .try_get::<_, Option<i32>>("pk_ordinal")?
                .and_then(|value| u16::try_from(value).ok()),
            generated: parse_generated(row.try_get::<_, String>("attgenerated")?.as_bytes())?,
            identity: parse_identity(row.try_get::<_, String>("attidentity")?.as_bytes())?,
            collation,
        };
        let partition_key = parse_attnums(&row.try_get::<_, String>("partition_attrs")?)?;
        if let Some(table) = tables.last_mut()
            && table.relation_id == relation_id
        {
            table.columns.push(column);
            continue;
        }
        tables.push(TableSchema {
            relation_id,
            generation: 1,
            name: qualified_name,
            kind: if relation_kind == "p" {
                TableKind::Partitioned
            } else {
                TableKind::Ordinary
            },
            replica_identity,
            columns: vec![column],
            distribution_key: Vec::new(),
            partition_key,
        });
    }

    for table in &tables {
        table
            .validate_supported()
            .map_err(|error| SourceError::contract(error.to_string()))?;
    }
    Ok(tables)
}

/// Apply Citus table-kind and distribution metadata to an ordinary catalog snapshot.
pub async fn load_tables_with_citus(
    client: &Client,
    options: &CatalogOptions,
    topology: &CitusTopology,
) -> SourceResult<Vec<TableSchema>> {
    let mut tables = load_tables(client, options).await?;
    crate::citus::apply_table_metadata(client, &mut tables, topology).await?;
    for table in &tables {
        table
            .validate_supported()
            .map_err(|error| SourceError::contract(error.to_string()))?;
    }
    Ok(tables)
}

async fn load_type_descriptors(client: &Client) -> SourceResult<HashMap<u32, TypeDescriptor>> {
    let rows = client
        .query(
            "SELECT t.oid::int8, ns.nspname, t.typname, t.typtype::text, t.typcategory::text,
                    NULLIF(t.typbasetype, 0)::int8, NULLIF(t.typelem, 0)::int8,
                    COALESCE(array_agg(e.enumlabel ORDER BY e.enumsortorder)
                             FILTER (WHERE e.enumlabel IS NOT NULL), ARRAY[]::text[])
               FROM pg_type t
               JOIN pg_namespace ns ON ns.oid = t.typnamespace
               LEFT JOIN pg_enum e ON e.enumtypid = t.oid
              GROUP BY t.oid, ns.nspname, t.typname, t.typtype, t.typcategory,
                       t.typbasetype, t.typelem",
            &[],
        )
        .await?;
    let mut descriptors = HashMap::with_capacity(rows.len());
    for row in rows {
        let oid = parse_u32_value(row.try_get::<_, i64>(0)?, "type oid")?;
        let base_oid = row
            .try_get::<_, Option<i64>>(5)?
            .map(|value| parse_u32_value(value, "base type oid"))
            .transpose()?;
        let element_oid = row
            .try_get::<_, Option<i64>>(6)?
            .map(|value| parse_u32_value(value, "element type oid"))
            .transpose()?;
        let constraints = client
            .query(
                "SELECT pg_get_constraintdef(oid, true)
                   FROM pg_constraint
                  WHERE contypid = $1::oid
                  ORDER BY oid",
                &[&i64::from(oid)],
            )
            .await?
            .into_iter()
            .map(|constraint| constraint.try_get(0))
            .collect::<Result<Vec<String>, _>>()?;
        descriptors.insert(
            oid,
            TypeDescriptor {
                oid,
                schema: row.try_get(1)?,
                name: row.try_get(2)?,
                typtype: row.try_get(3)?,
                base_oid,
                element_oid,
                labels: row.try_get(7)?,
                constraints,
            },
        );
    }
    Ok(descriptors)
}

fn resolve_type(
    descriptor: &TypeDescriptor,
    all: &HashMap<u32, TypeDescriptor>,
    typmod: i64,
) -> SourceResult<PgType> {
    let name = QualifiedName::new(descriptor.schema.clone(), descriptor.name.clone())
        .map_err(|error| SourceError::contract(error.to_string()))?;
    let kind = match descriptor.typtype.as_str() {
        "e" => PgTypeKind::Enum {
            labels: descriptor.labels.clone(),
        },
        "d" => {
            let base = descriptor
                .base_oid
                .and_then(|oid| all.get(&oid))
                .ok_or_else(|| {
                    SourceError::contract(format!("domain {} has no base type", name))
                })?;
            PgTypeKind::Domain {
                base: Box::new(resolve_type(base, all, typmod)?),
                constraints: descriptor.constraints.clone(),
            }
        }
        "b" if descriptor.element_oid.is_some() => {
            let element = descriptor
                .element_oid
                .and_then(|oid| all.get(&oid))
                .ok_or_else(|| {
                    SourceError::contract(format!("array {} has no element type", name))
                })?;
            PgTypeKind::Array {
                element: Box::new(resolve_type(element, all, -1)?),
            }
        }
        _ => scalar_kind(&descriptor.name, typmod),
    };
    Ok(PgType {
        oid: descriptor.oid,
        name,
        kind,
    })
}

fn scalar_kind(name: &str, typmod: i64) -> PgTypeKind {
    match name {
        "bool" => PgTypeKind::Bool,
        "int2" => PgTypeKind::Int2,
        "int4" => PgTypeKind::Int4,
        "int8" => PgTypeKind::Int8,
        "numeric" => {
            let (precision, scale) = numeric_typmod(typmod);
            PgTypeKind::Numeric { precision, scale }
        }
        "float4" => PgTypeKind::Float4,
        "float8" => PgTypeKind::Float8,
        "text" => PgTypeKind::Text,
        "varchar" => PgTypeKind::VarChar {
            length: typmod_length(typmod),
        },
        "bpchar" => PgTypeKind::Char {
            length: typmod_length(typmod),
        },
        "bytea" => PgTypeKind::Bytea,
        "date" => PgTypeKind::Date,
        "time" => PgTypeKind::Time {
            precision: time_precision(typmod),
            with_time_zone: false,
        },
        "timetz" => PgTypeKind::Time {
            precision: time_precision(typmod),
            with_time_zone: true,
        },
        "timestamp" => PgTypeKind::Timestamp {
            precision: time_precision(typmod),
            with_time_zone: false,
        },
        "timestamptz" => PgTypeKind::Timestamp {
            precision: time_precision(typmod),
            with_time_zone: true,
        },
        "interval" => PgTypeKind::Interval {
            precision: time_precision(typmod),
        },
        "uuid" => PgTypeKind::Uuid,
        "json" => PgTypeKind::Json,
        "jsonb" => PgTypeKind::Jsonb,
        "inet" => PgTypeKind::Inet,
        "cidr" => PgTypeKind::Cidr,
        "macaddr" => PgTypeKind::MacAddr,
        "macaddr8" => PgTypeKind::MacAddr8,
        "bit" => PgTypeKind::Bit {
            length: typmod_length(typmod),
            varying: false,
        },
        "varbit" => PgTypeKind::Bit {
            length: typmod_length(typmod),
            varying: true,
        },
        "xml" => PgTypeKind::Xml,
        _ => PgTypeKind::Unsupported {
            reason: format!("unregistered PostgreSQL type {name}"),
        },
    }
}

fn numeric_typmod(typmod: i64) -> (Option<u16>, Option<i16>) {
    if typmod < 4 {
        return (None, None);
    }
    let value = typmod - 4;
    let precision = u16::try_from((value >> 16) & 0xffff).ok();
    let scale = i16::try_from(value & 0xffff).ok();
    (precision, scale)
}

fn typmod_length(typmod: i64) -> Option<u32> {
    (typmod >= 4)
        .then(|| u32::try_from(typmod - 4).ok())
        .flatten()
}

fn time_precision(typmod: i64) -> Option<u8> {
    (typmod >= 0).then(|| u8::try_from(typmod).ok()).flatten()
}

fn eligible_schema(schema: &str, options: &CatalogOptions) -> bool {
    if schema == options.metadata_schema || options.exclude_schemas.contains(schema) {
        return false;
    }
    options
        .include_schemas
        .as_ref()
        .is_none_or(|schemas| schemas.contains(schema))
}

fn parse_replica_identity(value: char) -> SourceResult<ReplicaIdentity> {
    match value {
        'd' => Ok(ReplicaIdentity::Default),
        'i' => Ok(ReplicaIdentity::Index),
        'f' => Ok(ReplicaIdentity::Full),
        'n' => Ok(ReplicaIdentity::Nothing),
        other => Err(SourceError::contract(format!(
            "unknown replica identity `{other}`"
        ))),
    }
}

fn parse_generated(value: &[u8]) -> SourceResult<GeneratedColumn> {
    match value.first().copied().unwrap_or(0) {
        0 => Ok(GeneratedColumn::None),
        b's' => Ok(GeneratedColumn::Stored),
        b'v' => Ok(GeneratedColumn::Virtual),
        value => Err(SourceError::contract(format!(
            "unknown generated marker {value}"
        ))),
    }
}

fn parse_identity(value: &[u8]) -> SourceResult<IdentityColumn> {
    match value.first().copied().unwrap_or(0) {
        0 => Ok(IdentityColumn::None),
        b'a' => Ok(IdentityColumn::Always),
        b'd' => Ok(IdentityColumn::ByDefault),
        value => Err(SourceError::contract(format!(
            "unknown identity marker {value}"
        ))),
    }
}

pub(crate) fn parse_attnums(value: &str) -> SourceResult<Vec<i16>> {
    let trimmed = value.trim();
    if trimmed == "{}" || trimmed.is_empty() {
        return Ok(Vec::new());
    }
    let inner = trimmed
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .ok_or_else(|| SourceError::contract(format!("invalid int2vector `{value}`")))?;
    inner
        .split(',')
        .filter(|item| !item.trim().is_empty())
        .map(|item| {
            let value = item
                .trim()
                .parse::<i16>()
                .map_err(|_| SourceError::contract(format!("invalid attribute number `{item}`")))?;
            if value <= 0 {
                return Err(SourceError::unsupported(format!(
                    "partition key expression `{item}` is not a plain column"
                )));
            }
            Ok(value)
        })
        .collect()
}

async fn detect_citus(client: &Client) -> SourceResult<Option<String>> {
    let row = client
        .query_one("SELECT to_regproc('citus_version') IS NOT NULL", &[])
        .await?;
    if !row.try_get::<_, bool>(0)? {
        return Ok(None);
    }
    Ok(Some(
        client
            .query_one("SELECT citus_version()::text", &[])
            .await?
            .try_get(0)?,
    ))
}

async fn show(client: &Client, parameter: &str) -> SourceResult<String> {
    // Parameter names are compile-time constants at every call site.
    let row = client.query_one(&format!("SHOW {parameter}"), &[]).await?;
    Ok(row.try_get(0)?)
}

async fn show_i64(client: &Client, parameter: &str) -> SourceResult<i64> {
    show(client, parameter)
        .await?
        .parse::<i64>()
        .map_err(|_| SourceError::contract(format!("SHOW {parameter} is not an integer")))
}

fn parse_u64(row: &Row, index: usize, name: &str) -> SourceResult<u64> {
    let value: String = row.try_get(index)?;
    value
        .parse()
        .map_err(|_| SourceError::contract(format!("{name} is not an unsigned integer")))
}

fn parse_u32(row: &Row, index: usize, name: &str) -> SourceResult<u32> {
    let value: i64 = row.try_get(index)?;
    parse_u32_value(value, name)
}

fn parse_u32_value(value: i64, name: &str) -> SourceResult<u32> {
    u32::try_from(value).map_err(|_| SourceError::contract(format!("{name} is out of range")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_partition_attribute_vectors() {
        assert_eq!(parse_attnums("{1, 3, -2}").unwrap(), vec![1, 3, -2]);
        assert!(parse_attnums("1,2").is_err());
    }

    #[test]
    fn maps_replica_identity_strictly() {
        assert_eq!(
            parse_replica_identity('d').unwrap(),
            ReplicaIdentity::Default
        );
        assert!(parse_replica_identity('x').is_err());
    }

    #[test]
    fn parses_typmods() {
        assert_eq!(typmod_length(14), Some(10));
        assert_eq!(typmod_length(-1), None);
        assert_eq!(numeric_typmod(-1), (None, None));
        assert_eq!(numeric_typmod(4 + (12 << 16) + 2), (Some(12), Some(2)));
    }

    #[test]
    fn unknown_types_fail_closed() {
        assert!(!scalar_kind("postgis_geometry", -1).is_supported());
    }
}
