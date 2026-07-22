//! Strongly typed runtime settings.
//!
//! The control store intentionally keeps profile and pipeline settings as JSON.  This module is
//! the boundary at which that untrusted JSON becomes an operational contract.  Defaults are
//! conservative and validation is kept here so callers do not have to duplicate checks before
//! starting a pipeline.

use std::{collections::HashSet, fmt, time::Duration};

use cloudberry_etl_core::{
    id::PipelineId,
    schema::{QualifiedName, validate_identifier},
};
pub use cloudberry_etl_target_cloudberry::migration::TARGET_METADATA_SCHEMA;
pub use cloudberry_etl_target_cloudberry::storage::TargetStorage;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use thiserror::Error;
use uuid::Uuid;

const POSTGRES_IDENTIFIER_MAX_BYTES: usize = 63;
const DEFAULT_SOURCE_METADATA_SCHEMA: &str = "pg2cb_meta";

const DEFAULT_BATCH_MAX_ROWS: usize = 100_000;
const DEFAULT_BATCH_MAX_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_BATCH_MAX_DELAY_MS: u64 = 250;
const DEFAULT_TRANSACTION_MEMORY_HIGH_WATER_CHANGES: usize = 100_000;
const DEFAULT_TRANSACTION_MEMORY_HIGH_WATER_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_TRANSACTION_SEGMENT_TARGET_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_TRANSACTION_DISK_HIGH_WATER_BYTES: u64 = 256 * 1024 * 1024 * 1024;
const DEFAULT_TRANSACTION_MINIMUM_FREE_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_WAL_CHECK_INTERVAL_SECONDS: u64 = 10;
const DEFAULT_WAL_WARNING_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_WAL_REBUILD_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const DEFAULT_WAL_MINIMUM_SAFE_BYTES: u64 = 1024 * 1024 * 1024;
const DEFAULT_QUARANTINE_RETENTION_DAYS: u32 = 30;
const DEFAULT_QUARANTINE_GC_MAX_TABLES: u32 = 100;
const DEFAULT_RECONCILIATION_INTERVAL_SECONDS: u64 = 300;
const DEFAULT_RECONCILIATION_RETRY_SECONDS: u64 = 30;
const DEFAULT_RECONCILIATION_BOUNDARY_TIMEOUT_SECONDS: u64 = 300;
const DEFAULT_RECONCILIATION_SCAN_TIMEOUT_SECONDS: u64 = 900;
const DEFAULT_RECONCILIATION_MAX_LAG_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_RECONCILIATION_MAX_ROW_BYTES: usize = 64 * 1024 * 1024;

/// Hard limits prevent a malformed control record from allocating unbounded memory or delaying
/// progress indefinitely.  The limits are deliberately public so operators and tests can use the
/// same contract as the parser.
pub const MAX_BATCH_ROWS: usize = 1_000_000;
pub const MAX_BATCH_BYTES: usize = 1 << 30;
pub const MAX_BATCH_DELAY_MS: u64 = 60_000;
pub const MAX_TRANSACTION_CHANGES: usize = 10_000_000;
pub const MAX_TRANSACTION_BYTES: usize = 1 << 36;
pub const MAX_TRANSACTION_DISK_BYTES: u64 = 1 << 50;
pub const MAX_WAL_CHECK_INTERVAL_SECONDS: u64 = 3600;
pub const MAX_WAL_RETENTION_BYTES: u64 = 1 << 50;
pub const MAX_QUARANTINE_RETENTION_DAYS: u32 = 3650;
pub const MAX_QUARANTINE_GC_TABLES: u32 = 1000;
pub const MAX_RECONCILIATION_INTERVAL_SECONDS: u64 = 7 * 24 * 60 * 60;
pub const MAX_RECONCILIATION_RETRY_SECONDS: u64 = 24 * 60 * 60;
pub const MAX_RECONCILIATION_BOUNDARY_TIMEOUT_SECONDS: u64 = 24 * 60 * 60;
pub const MAX_RECONCILIATION_SCAN_TIMEOUT_SECONDS: u64 = 24 * 60 * 60;
pub const MAX_RECONCILIATION_LAG_BYTES: u64 = 1 << 50;
pub const MAX_RECONCILIATION_ROW_BYTES: usize = 1 << 30;

const SYSTEM_SCHEMAS: &[&str] = &[
    "pg_catalog",
    "information_schema",
    "pg_toast",
    "pg_temp",
    "pg_toast_temp",
];

/// A non-secret connection summary retained by the UI.  Passwords and DSNs are intentionally not
/// represented here; they are stored in the encrypted profile field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionSettings {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub tls_mode: TlsMode,
}

/// TLS modes accepted by libpq-style PostgreSQL connection strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TlsMode {
    Disable,
    Allow,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

/// Source profile settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SourceSettings {
    pub connection: Option<ConnectionSettings>,
    /// `None` means every non-system, non-metadata schema.
    pub include_schemas: Option<Vec<String>>,
    pub exclude_schemas: Vec<String>,
    pub metadata_schema: String,
    pub transaction: TransactionSettings,
    pub wal_retention: WalRetentionSettings,
}

impl Default for SourceSettings {
    fn default() -> Self {
        Self {
            connection: None,
            include_schemas: None,
            exclude_schemas: default_excluded_schemas(),
            metadata_schema: DEFAULT_SOURCE_METADATA_SCHEMA.to_owned(),
            transaction: TransactionSettings::default(),
            wal_retention: WalRetentionSettings::default(),
        }
    }
}

impl SourceSettings {
    /// Parse and validate a JSON value from the control store.
    pub fn parse(value: &Value) -> Result<Self, SettingsError> {
        parse_value("source", value)
    }

    /// Alias useful at call sites that receive an owned JSON value.
    pub fn from_value(value: Value) -> Result<Self, SettingsError> {
        Self::parse(&value)
    }

    pub fn validate(&self) -> Result<(), SettingsError> {
        if let Some(connection) = &self.connection {
            connection.validate("connection")?;
        }
        validate_schema_name("metadata_schema", &self.metadata_schema)?;
        if is_system_schema(&self.metadata_schema) {
            return invalid("metadata_schema", "must be a dedicated non-system schema");
        }
        validate_schema_list("include_schemas", self.include_schemas.as_deref())?;
        validate_schema_list("exclude_schemas", Some(&self.exclude_schemas))?;
        validate_no_duplicates("exclude_schemas", &self.exclude_schemas)?;
        if let Some(include) = &self.include_schemas {
            if include.is_empty() {
                return invalid(
                    "include_schemas",
                    "must contain at least one schema when set",
                );
            }
            validate_no_duplicates("include_schemas", include)?;
            let excluded: HashSet<&str> = self.exclude_schemas.iter().map(String::as_str).collect();
            if let Some(schema) = include
                .iter()
                .find(|schema| excluded.contains(schema.as_str()))
            {
                return invalid(
                    format!("include_schemas[{schema}]"),
                    "schema is also listed in exclude_schemas",
                );
            }
            if let Some(schema) = include
                .iter()
                .find(|schema| is_system_schema(schema) || schema.as_str() == self.metadata_schema)
            {
                return invalid(
                    format!("include_schemas[{schema}]"),
                    "system and metadata schemas are never eligible",
                );
            }
        }
        self.transaction.validate("transaction")?;
        self.wal_retention.validate("wal_retention")
    }

    /// Whether a catalog schema is eligible under this source contract.
    #[must_use]
    pub fn includes_schema(&self, schema: &str) -> bool {
        !is_system_schema(schema)
            && schema != self.metadata_schema
            && !self
                .exclude_schemas
                .iter()
                .any(|excluded| excluded == schema)
            && self
                .include_schemas
                .as_ref()
                .is_none_or(|included| included.iter().any(|candidate| candidate == schema))
    }
}

/// Target profile settings.  The target metadata namespace is fixed by the target adapter and is
/// therefore not configurable through this structure.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TargetSettings {
    pub connection: Option<ConnectionSettings>,
    pub default_table_storage: TargetStorage,
    pub quarantine_retention_days: u32,
    pub quarantine_gc_max_tables: u32,
}

impl Default for TargetSettings {
    fn default() -> Self {
        Self {
            connection: None,
            default_table_storage: TargetStorage::AoColumn,
            quarantine_retention_days: DEFAULT_QUARANTINE_RETENTION_DAYS,
            quarantine_gc_max_tables: DEFAULT_QUARANTINE_GC_MAX_TABLES,
        }
    }
}

impl TargetSettings {
    pub fn parse(value: &Value) -> Result<Self, SettingsError> {
        parse_value("target", value)
    }

    pub fn from_value(value: Value) -> Result<Self, SettingsError> {
        Self::parse(&value)
    }

    pub fn validate(&self) -> Result<(), SettingsError> {
        if let Some(connection) = &self.connection {
            connection.validate("connection")?;
        }
        if !(1..=MAX_QUARANTINE_RETENTION_DAYS).contains(&self.quarantine_retention_days) {
            return invalid(
                "quarantine_retention_days",
                format!("must be between 1 and {MAX_QUARANTINE_RETENTION_DAYS}"),
            );
        }
        if !(1..=MAX_QUARANTINE_GC_TABLES).contains(&self.quarantine_gc_max_tables) {
            return invalid(
                "quarantine_gc_max_tables",
                format!("must be between 1 and {MAX_QUARANTINE_GC_TABLES}"),
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn quarantine_retention(&self) -> Duration {
        Duration::from_secs(u64::from(self.quarantine_retention_days) * 24 * 60 * 60)
    }
}

/// Per-pipeline batching and source transaction limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct PipelineSettings {
    pub batch: BatchSettings,
    pub reconciliation: ReconciliationSettings,
    pub table_mappings: Vec<TableMapping>,
}

impl PipelineSettings {
    pub fn parse(value: &Value) -> Result<Self, SettingsError> {
        parse_value("pipeline", value)
    }

    pub fn from_value(value: Value) -> Result<Self, SettingsError> {
        Self::parse(&value)
    }

    pub fn validate(&self) -> Result<(), SettingsError> {
        self.batch.validate("batch")?;
        self.reconciliation.validate("reconciliation")?;
        let mut sources = HashSet::with_capacity(self.table_mappings.len());
        let mut targets = HashSet::with_capacity(self.table_mappings.len());
        for (index, mapping) in self.table_mappings.iter().enumerate() {
            let path = format!("table_mappings[{index}]");
            validate_qualified_name(&format!("{path}.source"), &mapping.source)?;
            validate_qualified_name(&format!("{path}.target"), &mapping.target)?;
            if is_reserved_target_schema(&mapping.target.schema) {
                return invalid(
                    format!("{path}.target.schema"),
                    "targets may not use a system or reserved metadata schema",
                );
            }
            if !sources.insert(mapping.source.clone()) {
                return invalid(
                    format!("{path}.source"),
                    "source relation is mapped more than once",
                );
            }
            if !targets.insert(mapping.target.clone()) {
                return invalid(
                    format!("{path}.target"),
                    "target relation is mapped more than once",
                );
            }
        }
        Ok(())
    }

    /// Validate mappings against the source selection rules as well as the standalone pipeline
    /// rules.  This catches an explicit mapping into a schema that the source would never scan.
    pub fn validate_with_source(&self, source: &SourceSettings) -> Result<(), SettingsError> {
        self.validate()?;
        source.validate()?;
        for (index, mapping) in self.table_mappings.iter().enumerate() {
            if !source.includes_schema(&mapping.source.schema) {
                return invalid(
                    format!("table_mappings[{index}].source.schema"),
                    "source schema is excluded by source settings",
                );
            }
        }
        Ok(())
    }

    /// Return an explicit target for a source relation, if one was configured.
    #[must_use]
    pub fn target_for(&self, source: &QualifiedName) -> Option<&QualifiedName> {
        self.table_mappings
            .iter()
            .find(|mapping| mapping.source == *source)
            .map(|mapping| &mapping.target)
    }
}

/// Limits used when forming target batches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchSettings {
    pub max_rows: usize,
    pub max_bytes: usize,
    /// Maximum age of a non-empty batch, in milliseconds.
    pub max_delay_ms: u64,
}

impl Default for BatchSettings {
    fn default() -> Self {
        Self {
            max_rows: DEFAULT_BATCH_MAX_ROWS,
            max_bytes: DEFAULT_BATCH_MAX_BYTES,
            max_delay_ms: DEFAULT_BATCH_MAX_DELAY_MS,
        }
    }
}

impl BatchSettings {
    pub fn validate(&self, path: &str) -> Result<(), SettingsError> {
        if !(1..=MAX_BATCH_ROWS).contains(&self.max_rows) {
            return invalid(
                format!("{path}.max_rows"),
                format!("must be between 1 and {MAX_BATCH_ROWS}"),
            );
        }
        if !(1..=MAX_BATCH_BYTES).contains(&self.max_bytes) {
            return invalid(
                format!("{path}.max_bytes"),
                format!("must be between 1 and {MAX_BATCH_BYTES}"),
            );
        }
        if !(1..=MAX_BATCH_DELAY_MS).contains(&self.max_delay_ms) {
            return invalid(
                format!("{path}.max_delay_ms"),
                format!("must be between 1 and {MAX_BATCH_DELAY_MS} milliseconds"),
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn max_delay(&self) -> Duration {
        Duration::from_millis(self.max_delay_ms)
    }
}

/// Resource watermarks used while assembling source transactions.
///
/// These values never define a supported transaction size. Crossing the memory watermark spills
/// to disk; crossing a disk watermark pauses WAL reads and acknowledgements until space returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TransactionSettings {
    #[serde(alias = "max_changes")]
    pub memory_high_water_changes: usize,
    #[serde(alias = "max_bytes")]
    pub memory_high_water_bytes: usize,
    pub segment_target_bytes: usize,
    pub disk_high_water_bytes: u64,
    pub minimum_free_disk_bytes: u64,
}

/// Periodic whole-table consistency verification.
///
/// A run establishes an exact source WAL boundary, catches the target up to that boundary, and
/// compares constant-memory canonical multiset digests. A mismatch schedules the existing
/// table-local shadow reload path; reconciliation never edits an active AOCO table in place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ReconciliationSettings {
    pub enabled: bool,
    pub interval_seconds: u64,
    pub retry_seconds: u64,
    pub boundary_timeout_seconds: u64,
    pub scan_timeout_seconds: u64,
    pub max_lag_bytes: u64,
    /// Maximum decoded size of one COPY text record. This bounds parser memory, not table size.
    pub max_row_bytes: usize,
}

impl Default for ReconciliationSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_seconds: DEFAULT_RECONCILIATION_INTERVAL_SECONDS,
            retry_seconds: DEFAULT_RECONCILIATION_RETRY_SECONDS,
            boundary_timeout_seconds: DEFAULT_RECONCILIATION_BOUNDARY_TIMEOUT_SECONDS,
            scan_timeout_seconds: DEFAULT_RECONCILIATION_SCAN_TIMEOUT_SECONDS,
            max_lag_bytes: DEFAULT_RECONCILIATION_MAX_LAG_BYTES,
            max_row_bytes: DEFAULT_RECONCILIATION_MAX_ROW_BYTES,
        }
    }
}

impl ReconciliationSettings {
    pub fn validate(&self, path: &str) -> Result<(), SettingsError> {
        validate_bounded_u64(
            path,
            "interval_seconds",
            self.interval_seconds,
            MAX_RECONCILIATION_INTERVAL_SECONDS,
        )?;
        validate_bounded_u64(
            path,
            "retry_seconds",
            self.retry_seconds,
            MAX_RECONCILIATION_RETRY_SECONDS,
        )?;
        validate_bounded_u64(
            path,
            "boundary_timeout_seconds",
            self.boundary_timeout_seconds,
            MAX_RECONCILIATION_BOUNDARY_TIMEOUT_SECONDS,
        )?;
        validate_bounded_u64(
            path,
            "scan_timeout_seconds",
            self.scan_timeout_seconds,
            MAX_RECONCILIATION_SCAN_TIMEOUT_SECONDS,
        )?;
        validate_bounded_u64(
            path,
            "max_lag_bytes",
            self.max_lag_bytes,
            MAX_RECONCILIATION_LAG_BYTES,
        )?;
        if !(1..=MAX_RECONCILIATION_ROW_BYTES).contains(&self.max_row_bytes) {
            return invalid(
                format!("{path}.max_row_bytes"),
                format!("must be between 1 and {MAX_RECONCILIATION_ROW_BYTES}"),
            );
        }
        Ok(())
    }

    #[must_use]
    pub const fn interval(self) -> Duration {
        Duration::from_secs(self.interval_seconds)
    }

    #[must_use]
    pub const fn retry_delay(self) -> Duration {
        Duration::from_secs(self.retry_seconds)
    }

    #[must_use]
    pub const fn boundary_timeout(self) -> Duration {
        Duration::from_secs(self.boundary_timeout_seconds)
    }

    #[must_use]
    pub const fn scan_timeout(self) -> Duration {
        Duration::from_secs(self.scan_timeout_seconds)
    }
}

impl Default for TransactionSettings {
    fn default() -> Self {
        Self {
            memory_high_water_changes: DEFAULT_TRANSACTION_MEMORY_HIGH_WATER_CHANGES,
            memory_high_water_bytes: DEFAULT_TRANSACTION_MEMORY_HIGH_WATER_BYTES,
            segment_target_bytes: DEFAULT_TRANSACTION_SEGMENT_TARGET_BYTES,
            disk_high_water_bytes: DEFAULT_TRANSACTION_DISK_HIGH_WATER_BYTES,
            minimum_free_disk_bytes: DEFAULT_TRANSACTION_MINIMUM_FREE_DISK_BYTES,
        }
    }
}

impl TransactionSettings {
    pub fn validate(&self, path: &str) -> Result<(), SettingsError> {
        if !(1..=MAX_TRANSACTION_CHANGES).contains(&self.memory_high_water_changes) {
            return invalid(
                format!("{path}.memory_high_water_changes"),
                format!("must be between 1 and {MAX_TRANSACTION_CHANGES}"),
            );
        }
        if !(1..=MAX_TRANSACTION_BYTES).contains(&self.memory_high_water_bytes) {
            return invalid(
                format!("{path}.memory_high_water_bytes"),
                format!("must be between 1 and {MAX_TRANSACTION_BYTES}"),
            );
        }
        if !(1..=MAX_TRANSACTION_BYTES).contains(&self.segment_target_bytes) {
            return invalid(
                format!("{path}.segment_target_bytes"),
                format!("must be between 1 and {MAX_TRANSACTION_BYTES}"),
            );
        }
        if !(1..=MAX_TRANSACTION_DISK_BYTES).contains(&self.disk_high_water_bytes) {
            return invalid(
                format!("{path}.disk_high_water_bytes"),
                format!("must be between 1 and {MAX_TRANSACTION_DISK_BYTES}"),
            );
        }
        if self.minimum_free_disk_bytes >= self.disk_high_water_bytes {
            return invalid(
                format!("{path}.minimum_free_disk_bytes"),
                "must be below disk_high_water_bytes",
            );
        }
        if self.segment_target_bytes as u64 > self.disk_high_water_bytes {
            return invalid(
                format!("{path}.segment_target_bytes"),
                "must not exceed disk_high_water_bytes",
            );
        }
        Ok(())
    }
}

/// Source-disk protection for one logical slot. The absolute limits work even when PostgreSQL's
/// `max_slot_wal_keep_size` is unlimited; `minimum_safe_bytes` additionally reacts to PG18's
/// remaining `safe_wal_size` when a finite server-side cap is configured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WalRetentionSettings {
    pub check_interval_seconds: u64,
    pub warning_bytes: u64,
    pub rebuild_bytes: u64,
    pub minimum_safe_bytes: u64,
}

impl Default for WalRetentionSettings {
    fn default() -> Self {
        Self {
            check_interval_seconds: DEFAULT_WAL_CHECK_INTERVAL_SECONDS,
            warning_bytes: DEFAULT_WAL_WARNING_BYTES,
            rebuild_bytes: DEFAULT_WAL_REBUILD_BYTES,
            minimum_safe_bytes: DEFAULT_WAL_MINIMUM_SAFE_BYTES,
        }
    }
}

impl WalRetentionSettings {
    pub fn validate(&self, path: &str) -> Result<(), SettingsError> {
        if !(1..=MAX_WAL_CHECK_INTERVAL_SECONDS).contains(&self.check_interval_seconds) {
            return invalid(
                format!("{path}.check_interval_seconds"),
                format!("must be between 1 and {MAX_WAL_CHECK_INTERVAL_SECONDS}"),
            );
        }
        for (name, value) in [
            ("warning_bytes", self.warning_bytes),
            ("rebuild_bytes", self.rebuild_bytes),
            ("minimum_safe_bytes", self.minimum_safe_bytes),
        ] {
            if !(1..=MAX_WAL_RETENTION_BYTES).contains(&value) {
                return invalid(
                    format!("{path}.{name}"),
                    format!("must be between 1 and {MAX_WAL_RETENTION_BYTES}"),
                );
            }
        }
        if self.warning_bytes >= self.rebuild_bytes {
            return invalid(
                format!("{path}.warning_bytes"),
                "must be smaller than rebuild_bytes",
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn check_interval(&self) -> Duration {
        Duration::from_secs(self.check_interval_seconds)
    }
}

/// One explicit source relation to target relation mapping.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableMapping {
    #[serde(deserialize_with = "deserialize_qualified_name")]
    pub source: QualifiedName,
    #[serde(deserialize_with = "deserialize_qualified_name")]
    pub target: QualifiedName,
    /// Overrides the target profile default for this business table.
    #[serde(default)]
    pub storage: Option<TargetStorage>,
}

/// Names owned by one replication reader.  Both names are valid PostgreSQL identifiers and remain
/// stable across restarts for the same pipeline UUID and source node id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationNames {
    pub publication: String,
    pub slot: String,
}

impl ReplicationNames {
    #[must_use]
    pub fn for_pipeline(pipeline_id: PipelineId, node_id: i32) -> Self {
        Self::for_uuid(pipeline_id.as_uuid(), node_id)
    }

    #[must_use]
    pub fn for_uuid(pipeline_id: Uuid, node_id: i32) -> Self {
        let uuid = pipeline_id.simple();
        // Encoding the signed node id as u32 is injective and avoids '-' in an identifier.
        let node = node_id as u32;
        let node = format!("{node:08x}");
        let publication = format!("pg2cb_pub_{uuid}_{node}");
        let slot = format!("pg2cb_slot_{uuid}_{node}");
        debug_assert!(is_generated_identifier(&publication));
        debug_assert!(is_generated_identifier(&slot));
        Self { publication, slot }
    }
}

/// Convenience wrapper for callers that work with the domain `PipelineId`.
#[must_use]
pub fn replication_names(pipeline_id: PipelineId, node_id: i32) -> ReplicationNames {
    ReplicationNames::for_pipeline(pipeline_id, node_id)
}

/// Convenience wrapper for callers that already have a UUID.
#[must_use]
pub fn replication_names_for_uuid(pipeline_id: Uuid, node_id: i32) -> ReplicationNames {
    ReplicationNames::for_uuid(pipeline_id, node_id)
}

/// Errors returned while decoding or validating a settings value.
#[derive(Debug, Error)]
pub enum SettingsError {
    #[error("{section} settings must be a JSON object")]
    NotObject { section: &'static str },
    #[error("invalid {section} settings: {source}")]
    Decode {
        section: &'static str,
        #[source]
        source: serde_json::Error,
    },
    #[error("invalid settings field `{path}`: {reason}")]
    Invalid { path: String, reason: String },
}

fn parse_value<T>(section: &'static str, value: &Value) -> Result<T, SettingsError>
where
    T: DeserializeOwned + ValidateSettings,
{
    if !value.is_object() {
        return Err(SettingsError::NotObject { section });
    }
    let settings: T = serde_json::from_value(value.clone())
        .map_err(|source| SettingsError::Decode { section, source })?;
    settings.validate_settings()?;
    Ok(settings)
}

trait ValidateSettings {
    fn validate_settings(&self) -> Result<(), SettingsError>;
}

impl ValidateSettings for SourceSettings {
    fn validate_settings(&self) -> Result<(), SettingsError> {
        self.validate()
    }
}

impl ValidateSettings for TargetSettings {
    fn validate_settings(&self) -> Result<(), SettingsError> {
        self.validate()
    }
}

impl ValidateSettings for PipelineSettings {
    fn validate_settings(&self) -> Result<(), SettingsError> {
        self.validate()
    }
}

impl ConnectionSettings {
    fn validate(&self, path: &str) -> Result<(), SettingsError> {
        if self.host.trim().is_empty() || self.host.contains('\0') {
            return invalid(format!("{path}.host"), "must not be empty or contain NUL");
        }
        if self.port == 0 {
            return invalid(format!("{path}.port"), "must be between 1 and 65535");
        }
        if self.username.trim().is_empty() || self.username.contains('\0') {
            return invalid(
                format!("{path}.username"),
                "must not be empty or contain NUL",
            );
        }
        Ok(())
    }
}

fn default_excluded_schemas() -> Vec<String> {
    SYSTEM_SCHEMAS
        .iter()
        .map(|schema| (*schema).to_owned())
        .chain(std::iter::once(DEFAULT_SOURCE_METADATA_SCHEMA.to_owned()))
        .collect()
}

fn validate_schema_list(path: &str, schemas: Option<&[String]>) -> Result<(), SettingsError> {
    if let Some(schemas) = schemas {
        for (index, schema) in schemas.iter().enumerate() {
            validate_schema_name(format!("{path}[{index}]"), schema)?;
        }
    }
    Ok(())
}

fn validate_no_duplicates(path: &str, values: &[String]) -> Result<(), SettingsError> {
    let mut seen = HashSet::with_capacity(values.len());
    for (index, value) in values.iter().enumerate() {
        if !seen.insert(value) {
            return invalid(
                format!("{path}[{index}]"),
                format!("duplicate schema `{value}`"),
            );
        }
    }
    Ok(())
}

fn validate_schema_name(path: impl Into<String>, value: &str) -> Result<(), SettingsError> {
    let path = path.into();
    validate_identifier(value).map_err(|_| SettingsError::Invalid {
        path: path.clone(),
        reason: "must not be empty or contain NUL".to_owned(),
    })?;
    if value.len() > POSTGRES_IDENTIFIER_MAX_BYTES {
        return invalid(
            path,
            format!("must be at most {POSTGRES_IDENTIFIER_MAX_BYTES} UTF-8 bytes"),
        );
    }
    Ok(())
}

fn validate_qualified_name(path: &str, value: &QualifiedName) -> Result<(), SettingsError> {
    validate_schema_name(format!("{path}.schema"), &value.schema)?;
    validate_schema_name(format!("{path}.name"), &value.name)
}

fn is_system_schema(schema: &str) -> bool {
    SYSTEM_SCHEMAS.contains(&schema)
        || schema.starts_with("pg_toast_")
        || schema.starts_with("pg_temp_")
}

fn is_reserved_target_schema(schema: &str) -> bool {
    is_system_schema(schema) || schema == TARGET_METADATA_SCHEMA
}

fn is_generated_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= POSTGRES_IDENTIFIER_MAX_BYTES
        && value.as_bytes()[0].is_ascii_lowercase()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn deserialize_qualified_name<'de, D>(deserializer: D) -> Result<QualifiedName, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct RawQualifiedName {
        schema: String,
        name: String,
    }

    let raw = RawQualifiedName::deserialize(deserializer)?;
    QualifiedName::new(raw.schema, raw.name).map_err(serde::de::Error::custom)
}

fn invalid<T>(path: impl Into<String>, reason: impl Into<String>) -> Result<T, SettingsError> {
    Err(SettingsError::Invalid {
        path: path.into(),
        reason: reason.into(),
    })
}

fn validate_bounded_u64(
    path: &str,
    field: &str,
    value: u64,
    maximum: u64,
) -> Result<(), SettingsError> {
    if (1..=maximum).contains(&value) {
        Ok(())
    } else {
        invalid(
            format!("{path}.{field}"),
            format!("must be between 1 and {maximum}"),
        )
    }
}

impl fmt::Display for ReplicationNames {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "publication={}, slot={}",
            self.publication, self.slot
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn defaults_scan_all_business_schemas_and_exclude_system_metadata() {
        let settings = SourceSettings::parse(&json!({})).unwrap();
        assert!(settings.include_schemas.is_none());
        assert!(!settings.includes_schema("pg_catalog"));
        assert!(!settings.includes_schema("pg_temp_42"));
        assert!(!settings.includes_schema("pg2cb_meta"));
        assert!(settings.includes_schema("orders"));
    }

    #[test]
    fn source_settings_reject_unknown_and_ambiguous_schema_selection() {
        let unknown = SourceSettings::parse(&json!({"unexpected": true}));
        assert!(matches!(unknown, Err(SettingsError::Decode { .. })));

        let overlap = SourceSettings::parse(&json!({
            "include_schemas": ["public"],
            "exclude_schemas": ["public"]
        }));
        assert!(overlap.is_err());
    }

    #[test]
    fn limits_are_strictly_bounded() {
        let invalid = PipelineSettings::parse(&json!({
            "batch": {"max_rows": 0},
            "table_mappings": []
        }));
        assert!(invalid.is_err());

        let invalid_reconciliation = PipelineSettings::parse(&json!({
            "reconciliation": {"interval_seconds": 0},
            "table_mappings": []
        }));
        assert!(invalid_reconciliation.is_err());
        let oversized_reconciliation_row = PipelineSettings::parse(&json!({
            "reconciliation": {"max_row_bytes": MAX_RECONCILIATION_ROW_BYTES + 1},
            "table_mappings": []
        }));
        assert!(oversized_reconciliation_row.is_err());

        let too_large = SourceSettings::parse(&json!({
            "transaction": {"max_bytes": MAX_TRANSACTION_BYTES + 1}
        }));
        assert!(too_large.is_err());

        let reversed_wal_limits = SourceSettings::parse(&json!({
            "wal_retention": {
                "warning_bytes": 1024,
                "rebuild_bytes": 1024
            }
        }));
        assert!(reversed_wal_limits.is_err());

        let wal = SourceSettings::parse(&json!({})).unwrap().wal_retention;
        assert!(wal.warning_bytes < wal.rebuild_bytes);
        assert!(!wal.check_interval().is_zero());

        let target = TargetSettings::parse(&json!({})).unwrap();
        assert_eq!(target.default_table_storage, TargetStorage::AoColumn);
        assert_eq!(target.quarantine_retention_days, 30);
        assert!(!target.quarantine_retention().is_zero());
        assert!(TargetSettings::parse(&json!({"quarantine_retention_days": 0})).is_err());
        assert_eq!(
            TargetSettings::parse(&json!({"default_table_storage": "pax_experimental"}))
                .unwrap()
                .default_table_storage,
            TargetStorage::PaxExperimental
        );
        assert!(TargetSettings::parse(&json!({"default_table_storage": "heap"})).is_err());
        assert!(TargetSettings::parse(&json!({"default_table_storage": "pax"})).is_err());

        let reconciliation = PipelineSettings::parse(&json!({})).unwrap().reconciliation;
        assert!(reconciliation.enabled);
        assert_eq!(
            reconciliation.interval(),
            Duration::from_secs(DEFAULT_RECONCILIATION_INTERVAL_SECONDS)
        );
        assert!(!reconciliation.retry_delay().is_zero());
        assert!(!reconciliation.boundary_timeout().is_zero());
        assert!(!reconciliation.scan_timeout().is_zero());
    }

    #[test]
    fn mappings_must_be_a_bijection_and_cannot_use_target_metadata() {
        let duplicate_source = json!({
            "table_mappings": [
                {"source": {"schema": "public", "name": "a"}, "target": {"schema": "x", "name": "a"}},
                {"source": {"schema": "public", "name": "a"}, "target": {"schema": "y", "name": "a"}}
            ]
        });
        assert!(PipelineSettings::parse(&duplicate_source).is_err());

        let reserved = json!({
            "table_mappings": [{
                "source": {"schema": "public", "name": "a"},
                "target": {"schema": TARGET_METADATA_SCHEMA, "name": "a"}
            }]
        });
        assert!(PipelineSettings::parse(&reserved).is_err());
    }

    #[test]
    fn mapping_nested_objects_are_strict_and_identifiers_are_bounded() {
        let unknown = PipelineSettings::parse(&json!({
            "table_mappings": [{
                "source": {"schema": "public", "name": "a", "extra": true},
                "target": {"schema": "x", "name": "a"}
            }]
        }));
        assert!(unknown.is_err());

        let long_name = "x".repeat(64);
        let invalid = PipelineSettings::parse(&json!({
            "table_mappings": [{
                "source": {"schema": "public", "name": long_name},
                "target": {"schema": "x", "name": "a"}
            }]
        }));
        assert!(invalid.is_err());
    }

    #[test]
    fn generated_names_are_stable_valid_and_short() {
        let pipeline = Uuid::parse_str("123e4567-e89b-12d3-a456-426614174000").unwrap();
        let first = ReplicationNames::for_uuid(pipeline, -7);
        let second = ReplicationNames::for_uuid(pipeline, -7);
        assert_eq!(first, second);
        assert!(is_generated_identifier(&first.publication));
        assert!(is_generated_identifier(&first.slot));
        assert!(first.publication.len() <= POSTGRES_IDENTIFIER_MAX_BYTES);
        assert!(first.slot.len() <= POSTGRES_IDENTIFIER_MAX_BYTES);
        assert_ne!(first, ReplicationNames::for_uuid(pipeline, 7));
    }

    #[test]
    fn connection_summary_matches_ui_contract() {
        let source = SourceSettings::parse(&json!({
            "connection": {
                "host": "pg.internal",
                "port": 5432,
                "username": "reader",
                "tls_mode": "verify-full"
            }
        }))
        .unwrap();
        assert_eq!(source.connection.unwrap().tls_mode, TlsMode::VerifyFull);
    }
}
