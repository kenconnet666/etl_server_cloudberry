//! Validation-gated Cloudberry COPY reader for whole-table reconciliation.
//!
//! A reader owns one repeatable-read target transaction from fence acquisition through COPY. It
//! never advances reconciliation state, apply checkpoints, or business rows.

use std::{
    collections::HashSet,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use cloudberry_etl_core::{
    id::PipelineId,
    schema::{ColumnSchema, PgTypeKind, QualifiedName, TableSchema},
};
use futures::Stream;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio_postgres::{Client, CopyOutStream, IsolationLevel, Row, Transaction};

use crate::{
    checkpoint::{
        CheckpointError, CheckpointKey, StoredNodeCheckpoint, load_node_checkpoint_locked,
    },
    managed::{ManagedTableError, TableApplyIdentity, lock_active_apply_table},
    reconciliation::{
        ReconciliationRunIdentity, ReconciliationState, ReconciliationStateError,
        StoredReconciliationState, load_reconciliation_state_in_transaction,
    },
    schema::{
        CreateTablePlan, SchemaError, UserTypeDefinition, UserTypePlan,
        plan_create_table_with_storage, plan_type,
    },
    sql::{SqlRenderError, quote_identifier, quote_qualified_name},
};

const MAX_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

const LOAD_RELATION_SQL: &str = r#"
SELECT c.oid::bigint AS relation_oid,
       c.relkind::text AS relation_kind,
       c.relpersistence::text AS persistence,
       c.relispartition,
       c.relrowsecurity,
       c.relforcerowsecurity,
       am.amname AS access_method
FROM pg_catalog.pg_class AS c
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN pg_catalog.pg_am AS am ON am.oid = c.relam
WHERE n.nspname = $1 AND c.relname = $2
"#;

const LOAD_COLUMNS_SQL: &str = r#"
SELECT a.attnum::integer AS attnum,
       a.attname,
       pg_catalog.format_type(a.atttypid, a.atttypmod) AS formatted_type,
       a.atttypid::bigint AS type_oid,
       a.atttypmod::bigint AS type_modifier,
       a.attnotnull,
       a.attisdropped,
       a.atthasdef,
       a.attgenerated::text AS generated,
       a.attidentity::text AS identity,
       (t.typcollation <> 0::oid) AS type_is_collatable,
       cn.nspname AS collation_schema,
       co.collname AS collation_name
FROM pg_catalog.pg_attribute AS a
JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid
LEFT JOIN pg_catalog.pg_collation AS co ON co.oid = a.attcollation AND a.attcollation <> 0::oid
LEFT JOIN pg_catalog.pg_namespace AS cn ON cn.oid = co.collnamespace
WHERE a.attrelid = $1::bigint::oid AND a.attnum > 0
ORDER BY a.attnum
"#;

const LOAD_PRIMARY_KEY_SQL: &str = r#"
SELECT i.indexrelid::bigint AS index_oid,
       i.indisunique,
       i.indisvalid,
       i.indisready,
       i.indimmediate,
       i.indnkeyatts::integer AS key_attribute_count,
       i.indnatts::integer AS total_attribute_count,
       (i.indpred IS NULL) AS has_no_predicate,
       (i.indexprs IS NULL) AS has_no_expressions,
       ARRAY(
           SELECT key.attnum::integer
           FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS key(attnum, ordinal)
           WHERE key.ordinal <= i.indnkeyatts
           ORDER BY key.ordinal
       ) AS key_attnums,
       ARRAY(
           SELECT a.attname::text
           FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS key(attnum, ordinal)
           LEFT JOIN pg_catalog.pg_attribute AS a
             ON a.attrelid = i.indrelid AND a.attnum = key.attnum
           WHERE key.ordinal <= i.indnkeyatts
           ORDER BY key.ordinal
       ) AS key_names
FROM pg_catalog.pg_index AS i
WHERE i.indrelid = $1::bigint::oid AND i.indisprimary
ORDER BY i.indexrelid
"#;

const LOAD_DISTRIBUTION_SQL: &str = r#"
SELECT p.policytype::text AS policy_type,
       p.numsegments,
       ARRAY(
           SELECT key.attnum::integer
           FROM unnest(p.distkey::smallint[]) WITH ORDINALITY AS key(attnum, ordinal)
           ORDER BY key.ordinal
       ) AS key_attnums,
       ARRAY(
           SELECT a.attname::text
           FROM unnest(p.distkey::smallint[]) WITH ORDINALITY AS key(attnum, ordinal)
           LEFT JOIN pg_catalog.pg_attribute AS a
             ON a.attrelid = p.localoid AND a.attnum = key.attnum
           ORDER BY key.ordinal
       ) AS key_names
FROM pg_catalog.gp_distribution_policy AS p
WHERE p.localoid = $1::bigint::oid
"#;

const LOCK_MANAGED_TYPE_SQL: &str = r#"
SELECT pipeline_id, definition_checksum, fencing_token
FROM pg2cb_meta.managed_types
WHERE type_schema = $1 AND type_name = $2
FOR UPDATE
"#;

const LOAD_TYPE_KIND_SQL: &str = r#"
SELECT t.typtype::text AS type_kind
FROM pg_catalog.pg_type AS t
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
WHERE n.nspname = $1 AND t.typname = $2
"#;

const LOAD_ENUM_LABELS_SQL: &str = r#"
SELECT e.enumlabel
FROM pg_catalog.pg_type AS t
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
JOIN pg_catalog.pg_enum AS e ON e.enumtypid = t.oid
WHERE n.nspname = $1 AND t.typname = $2
ORDER BY e.enumsortorder
"#;

const LOAD_DOMAIN_SQL: &str = r#"
SELECT t.typtype::text AS type_kind,
       t.typbasetype::bigint AS base_type_oid,
       t.typtypmod::bigint AS base_type_modifier,
       pg_catalog.format_type(t.typbasetype, t.typtypmod) AS formatted_base_type,
       t.typnotnull,
       t.typdefault,
       (SELECT count(*)::bigint
        FROM pg_catalog.pg_constraint AS c
        WHERE c.contypid = t.oid) AS constraint_count
FROM pg_catalog.pg_type AS t
JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace
WHERE n.nspname = $1 AND t.typname = $2
"#;

/// Bounded target-side waits for a reconciliation scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconciliationReaderTimeouts {
    pub lock_timeout: Duration,
    pub statement_timeout: Duration,
    pub idle_in_transaction_timeout: Duration,
}

impl Default for ReconciliationReaderTimeouts {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_secs(5),
            statement_timeout: Duration::from_secs(6 * 60 * 60),
            idle_in_transaction_timeout: Duration::from_secs(5 * 60),
        }
    }
}

#[derive(Debug, Error)]
pub enum ReconciliationReaderError {
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    ManagedTable(#[from] ManagedTableError),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    ReconciliationState(#[from] ReconciliationStateError),
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Sql(#[from] SqlRenderError),
    #[error("reconciliation timeout `{name}` must be between 1 millisecond and 24 hours")]
    InvalidTimeout { name: &'static str },
    #[error("reconciliation identity is inconsistent: {0}")]
    InvalidIdentity(String),
    #[error("caller-supplied CreateTablePlan for `{0}` is not the canonical plan for its schema")]
    NonCanonicalPlan(String),
    #[error("target reconciliation reader is not aligned: {0}")]
    Alignment(String),
    #[error("target table `{table}` failed reconciliation validation: {detail}")]
    CatalogMismatch { table: String, detail: String },
}

/// A validation-gated target snapshot. Its transaction is private so this API cannot accidentally
/// advance reconciliation state, checkpoints, or business data.
pub struct ReconciliationReader<'client> {
    transaction: Transaction<'client>,
    target: QualifiedName,
    relation_oid: i64,
    copy_sql: String,
}

impl<'client> ReconciliationReader<'client> {
    #[must_use]
    pub const fn relation_oid(&self) -> i64 {
        self.relation_oid
    }

    #[must_use]
    pub fn target(&self) -> &QualifiedName {
        &self.target
    }

    #[must_use]
    pub fn copy_sql(&self) -> &str {
        &self.copy_sql
    }

    /// Start the explicit-column, key-first canonical text stream. Rows are intentionally not
    /// sorted because the reconciliation digest is order independent.
    pub async fn copy_text<'session>(
        &'session mut self,
    ) -> Result<ReconciliationCopy<'session>, ReconciliationReaderError> {
        let statement = self.transaction.prepare(&self.copy_sql).await?;
        let stream = self.transaction.copy_out(&statement).await?;
        Ok(ReconciliationCopy {
            stream: Box::pin(stream),
            _reader: PhantomData,
        })
    }

    pub async fn commit(self) -> Result<(), ReconciliationReaderError> {
        self.transaction.commit().await?;
        Ok(())
    }

    pub async fn rollback(self) -> Result<(), ReconciliationReaderError> {
        self.transaction.rollback().await?;
        Ok(())
    }
}

/// COPY output tied to the mutable borrow of its owning reconciliation transaction.
pub struct ReconciliationCopy<'session> {
    stream: Pin<Box<CopyOutStream>>,
    _reader: PhantomData<&'session mut ()>,
}

impl Stream for ReconciliationCopy<'_> {
    type Item = Result<Bytes, tokio_postgres::Error>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(context)
    }
}

/// Start a target reconciliation snapshot using the production timeout profile.
pub async fn begin_reconciliation_reader<'client>(
    client: &'client mut Client,
    run: &ReconciliationRunIdentity,
    schema: &TableSchema,
    table: &CreateTablePlan,
) -> Result<ReconciliationReader<'client>, ReconciliationReaderError> {
    begin_reconciliation_reader_with_timeouts(
        client,
        run,
        schema,
        table,
        ReconciliationReaderTimeouts::default(),
    )
    .await
}

/// Start a repeatable-read transaction, acquire the pipeline/table identity locks, prove that the
/// same snapshot sees the run's marked checkpoint, and validate the complete physical scan
/// contract before returning any target bytes.
pub async fn begin_reconciliation_reader_with_timeouts<'client>(
    client: &'client mut Client,
    run: &ReconciliationRunIdentity,
    schema: &TableSchema,
    table: &CreateTablePlan,
    timeouts: ReconciliationReaderTimeouts,
) -> Result<ReconciliationReader<'client>, ReconciliationReaderError> {
    validate_request(run, schema, table)?;
    let settings_sql = canonical_settings_sql(timeouts)?;
    let copy_sql = reconciliation_copy_sql(schema, &table.target)?;
    let identity = TableApplyIdentity {
        target: run.target.clone(),
        source_relation_id: run.source_relation_id,
        table_generation: run.table_generation,
        schema_fingerprint: run.schema_fingerprint.clone(),
    };

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    // SET utilities do not establish the repeatable-read MVCC snapshot. The first catalog query
    // is therefore the fenced identity lock below.
    transaction.batch_execute(&settings_sql).await?;
    // The fence proves ownership and stops subsequent apply transactions. It is not an alignment
    // proof: the V10 scanning marker and node checkpoint below must agree in this RR snapshot.
    lock_active_apply_table(&transaction, run.fence, &identity).await?;
    validate_reconciliation_alignment(&transaction, run).await?;
    validate_prerequisite_types(&transaction, run, table).await?;

    let relation = load_relation(&transaction, &run.target).await?;
    validate_relation(run, table, &relation)?;
    let columns = load_columns(&transaction, relation.oid).await?;
    validate_columns(&transaction, schema, table, &columns).await?;
    let primary_key = load_primary_key(&transaction, relation.oid).await?;
    validate_primary_key(table, &primary_key)?;
    let distribution = load_distribution(&transaction, relation.oid).await?;
    validate_distribution(table, &distribution)?;

    Ok(ReconciliationReader {
        transaction,
        target: run.target.clone(),
        relation_oid: relation.oid,
        copy_sql,
    })
}

fn validate_request(
    run: &ReconciliationRunIdentity,
    schema: &TableSchema,
    table: &CreateTablePlan,
) -> Result<(), ReconciliationReaderError> {
    if run.target != table.target {
        return Err(invalid_identity(
            "run target differs from CreateTablePlan target",
        ));
    }
    if run.source_relation_id == 0 || run.source_relation_id != schema.relation_id {
        return Err(invalid_identity(
            "run source relation differs from TableSchema relation",
        ));
    }
    if run.table_generation == 0 {
        return Err(invalid_identity("run table generation must be positive"));
    }
    if schema.generation == 0 {
        return Err(invalid_identity(
            "TableSchema catalog generation must be positive",
        ));
    }
    if run.target_relation_oid == 0 {
        return Err(invalid_identity("run target relation OID must be positive"));
    }
    if run.schema_fingerprint.is_empty() || run.schema_fingerprint.contains('\0') {
        return Err(invalid_identity("run schema fingerprint is invalid"));
    }

    let mut attnums = HashSet::with_capacity(schema.columns.len());
    for column in &schema.columns {
        if column.attnum <= 0 || !attnums.insert(column.attnum) {
            return Err(invalid_identity(
                "TableSchema column attribute numbers must be positive and unique",
            ));
        }
    }
    let canonical = plan_create_table_with_storage(schema, table.target.clone(), table.storage)?;
    if canonical != *table {
        return Err(ReconciliationReaderError::NonCanonicalPlan(
            table.target.to_string(),
        ));
    }
    Ok(())
}

fn invalid_identity(detail: impl Into<String>) -> ReconciliationReaderError {
    ReconciliationReaderError::InvalidIdentity(detail.into())
}

fn canonical_settings_sql(
    timeouts: ReconciliationReaderTimeouts,
) -> Result<String, ReconciliationReaderError> {
    let lock = timeout_millis("lock_timeout", timeouts.lock_timeout)?;
    let statement = timeout_millis("statement_timeout", timeouts.statement_timeout)?;
    let idle = timeout_millis(
        "idle_in_transaction_session_timeout",
        timeouts.idle_in_transaction_timeout,
    )?;
    Ok(format!(
        "SET LOCAL client_encoding = 'UTF8';\n\
         SET LOCAL DateStyle = 'ISO, YMD';\n\
         SET LOCAL IntervalStyle = 'postgres';\n\
         SET LOCAL TimeZone = 'UTC';\n\
         SET LOCAL extra_float_digits = 3;\n\
         SET LOCAL bytea_output = 'hex';\n\
         SET LOCAL search_path = pg_catalog;\n\
         SET LOCAL lock_timeout = '{lock}ms';\n\
         SET LOCAL statement_timeout = '{statement}ms';\n\
         SET LOCAL idle_in_transaction_session_timeout = '{idle}ms';"
    ))
}

fn timeout_millis(name: &'static str, timeout: Duration) -> Result<u64, ReconciliationReaderError> {
    if timeout.is_zero() || timeout > MAX_TIMEOUT {
        return Err(ReconciliationReaderError::InvalidTimeout { name });
    }
    u64::try_from(timeout.as_millis())
        .map_err(|_| ReconciliationReaderError::InvalidTimeout { name })
}

fn reconciliation_copy_sql(
    schema: &TableSchema,
    target: &QualifiedName,
) -> Result<String, ReconciliationReaderError> {
    let mut key_columns = schema.primary_key();
    key_columns.sort_by_key(|column| column.primary_key_ordinal);
    for (index, column) in key_columns.iter().enumerate() {
        let expected = u16::try_from(index + 1)
            .map_err(|_| invalid_identity("primary-key ordinal exceeds u16"))?;
        if column.primary_key_ordinal != Some(expected) || column.nullable {
            return Err(invalid_identity(
                "primary-key ordinals must be contiguous and key columns non-nullable",
            ));
        }
    }
    let key_names = key_columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    let mut value_columns = schema
        .columns
        .iter()
        .filter(|column| !key_names.contains(column.name.as_str()))
        .collect::<Vec<_>>();
    value_columns.sort_by_key(|column| column.attnum);
    let projection = key_columns
        .into_iter()
        .chain(value_columns)
        .map(|column| quote_identifier(&column.name))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    if projection.is_empty() {
        return Err(invalid_identity("reconciliation COPY has no columns"));
    }
    Ok(format!(
        "COPY (SELECT {projection} FROM {}) TO STDOUT WITH (FORMAT text, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')",
        quote_qualified_name(target)?
    ))
}

async fn validate_reconciliation_alignment(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
) -> Result<(), ReconciliationReaderError> {
    let state =
        load_reconciliation_state_in_transaction(transaction, run.fence, run.source_relation_id)
            .await?;
    let checkpoint = load_node_checkpoint_locked(
        transaction,
        CheckpointKey {
            pipeline_id: run.fence.pipeline_id,
            topology_generation: run.fence.topology_generation,
            node_id: run.source_node_id,
        },
    )
    .await?;
    validate_alignment_catalog(run, state.as_ref(), checkpoint.as_ref())
}

fn validate_alignment_catalog(
    run: &ReconciliationRunIdentity,
    state: Option<&StoredReconciliationState>,
    checkpoint: Option<&StoredNodeCheckpoint>,
) -> Result<(), ReconciliationReaderError> {
    let state = state.ok_or_else(|| alignment("V10 reconciliation state is missing"))?;
    if state.run != *run {
        return Err(alignment(
            "persisted V10 reconciliation run identity differs from the requested run",
        ));
    }
    if state.state != ReconciliationState::Scanning {
        return Err(alignment(format!(
            "V10 reconciliation state is {:?}, expected Scanning",
            state.state
        )));
    }
    let marked_lsn = state
        .target_checkpoint_lsn
        .ok_or_else(|| alignment("Scanning state has no target checkpoint LSN"))?;
    let checkpoint = checkpoint.ok_or_else(|| alignment("source-node checkpoint is missing"))?;
    let expected_key = CheckpointKey {
        pipeline_id: run.fence.pipeline_id,
        topology_generation: run.fence.topology_generation,
        node_id: run.source_node_id,
    };
    if checkpoint.checkpoint.key != expected_key {
        return Err(alignment(
            "loaded source-node checkpoint key differs from the requested run",
        ));
    }
    if checkpoint.checkpoint.applied_lsn != marked_lsn {
        return Err(alignment(format!(
            "source-node checkpoint is {}, but Scanning was marked at {marked_lsn}",
            checkpoint.checkpoint.applied_lsn
        )));
    }
    if checkpoint.checkpoint.system_identifier != run.source_system_identifier
        || checkpoint.checkpoint.timeline != run.source_timeline
    {
        return Err(alignment(
            "source-node checkpoint system identifier or timeline differs from the run",
        ));
    }
    if checkpoint.fencing_token <= 0 || checkpoint.fencing_token > run.fence.fencing_token {
        return Err(alignment(format!(
            "source-node checkpoint fencing token is {}, current token is {}",
            checkpoint.fencing_token, run.fence.fencing_token
        )));
    }
    Ok(())
}

fn alignment(detail: impl Into<String>) -> ReconciliationReaderError {
    ReconciliationReaderError::Alignment(detail.into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManagedTypeCatalog {
    pipeline_id: PipelineId,
    definition_checksum: Vec<u8>,
    fencing_token: i64,
}

async fn validate_prerequisite_types(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    table: &CreateTablePlan,
) -> Result<(), ReconciliationReaderError> {
    let mut prerequisites = table.prerequisites.iter().collect::<Vec<_>>();
    prerequisites.sort_by(|left, right| {
        (&left.name.schema, &left.name.name).cmp(&(&right.name.schema, &right.name.name))
    });
    for prerequisite in prerequisites {
        let managed = load_managed_type(transaction, prerequisite)
            .await?
            .ok_or_else(|| {
                type_mismatch(
                    run,
                    prerequisite,
                    "has no pg2cb_meta.managed_types ownership record",
                )
            })?;
        validate_managed_type(run, prerequisite, &managed)?;
        match &prerequisite.definition {
            UserTypeDefinition::Enum { labels } => {
                validate_enum_type(transaction, run, prerequisite, labels).await?;
            }
            UserTypeDefinition::Domain { base_sql } => {
                validate_domain_type(transaction, run, prerequisite, base_sql).await?;
            }
        }
    }
    Ok(())
}

async fn load_managed_type(
    transaction: &Transaction<'_>,
    prerequisite: &UserTypePlan,
) -> Result<Option<ManagedTypeCatalog>, ReconciliationReaderError> {
    transaction
        .query_opt(
            LOCK_MANAGED_TYPE_SQL,
            &[&prerequisite.name.schema, &prerequisite.name.name],
        )
        .await?
        .map(|row| -> Result<ManagedTypeCatalog, tokio_postgres::Error> {
            Ok(ManagedTypeCatalog {
                pipeline_id: PipelineId::from_uuid(row.try_get("pipeline_id")?),
                definition_checksum: row.try_get("definition_checksum")?,
                fencing_token: row.try_get("fencing_token")?,
            })
        })
        .transpose()
        .map_err(Into::into)
}

fn validate_managed_type(
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    actual: &ManagedTypeCatalog,
) -> Result<(), ReconciliationReaderError> {
    let expected_checksum: [u8; 32] = Sha256::digest(prerequisite.create_sql.as_bytes()).into();
    if actual.pipeline_id != run.fence.pipeline_id {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!(
                "is owned by pipeline {}, expected {}",
                actual.pipeline_id, run.fence.pipeline_id
            ),
        ));
    }
    if actual.definition_checksum.as_slice() != expected_checksum {
        return Err(type_mismatch(
            run,
            prerequisite,
            "managed definition checksum differs from CreateTablePlan",
        ));
    }
    if actual.fencing_token <= 0 || actual.fencing_token > run.fence.fencing_token {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!(
                "managed fencing token is {}, current token is {}",
                actual.fencing_token, run.fence.fencing_token
            ),
        ));
    }
    Ok(())
}

async fn validate_enum_type(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    expected_labels: &[String],
) -> Result<(), ReconciliationReaderError> {
    let kind = load_type_kind(transaction, run, prerequisite).await?;
    let labels = transaction
        .query(
            LOAD_ENUM_LABELS_SQL,
            &[&prerequisite.name.schema, &prerequisite.name.name],
        )
        .await?
        .iter()
        .map(|row| row.try_get("enumlabel"))
        .collect::<Result<Vec<String>, _>>()?;
    validate_enum_catalog(run, prerequisite, &kind, &labels, expected_labels)
}

fn validate_enum_catalog(
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    kind: &str,
    actual_labels: &[String],
    expected_labels: &[String],
) -> Result<(), ReconciliationReaderError> {
    if kind != "e" {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!("physical type kind is `{kind}`, expected enum"),
        ));
    }
    if actual_labels != expected_labels {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!("enum labels/order are {actual_labels:?}, expected {expected_labels:?}"),
        ));
    }
    Ok(())
}

async fn load_type_kind(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
) -> Result<String, ReconciliationReaderError> {
    let rows = transaction
        .query(
            LOAD_TYPE_KIND_SQL,
            &[&prerequisite.name.schema, &prerequisite.name.name],
        )
        .await?;
    if rows.len() != 1 {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!("expected one physical type, found {}", rows.len()),
        ));
    }
    rows[0].try_get("type_kind").map_err(Into::into)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DomainCatalog {
    kind: String,
    base_type_oid: i64,
    base_type_modifier: i64,
    formatted_base_type: String,
    not_null: bool,
    default: Option<String>,
    constraint_count: i64,
}

async fn validate_domain_type(
    transaction: &Transaction<'_>,
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    expected_base_sql: &str,
) -> Result<(), ReconciliationReaderError> {
    let rows = transaction
        .query(
            LOAD_DOMAIN_SQL,
            &[&prerequisite.name.schema, &prerequisite.name.name],
        )
        .await?;
    if rows.len() != 1 {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!("expected one physical domain, found {}", rows.len()),
        ));
    }
    let actual = domain_from_row(&rows[0])?;
    validate_domain_shape(run, prerequisite, &actual)?;
    if !domain_base_requires_normalization(run, prerequisite, &actual, expected_base_sql)? {
        return Ok(());
    }

    // Built-in typmods use the same canonical spelling as `format_type` and must have matched
    // above. This fallback only normalizes a quoted, schema-qualified user type with typmod -1.
    let row = transaction
        .query_one(
            "SELECT pg_catalog.to_regtype($1)::oid::bigint AS type_oid, pg_catalog.format_type(pg_catalog.to_regtype($1)::oid, -1) AS formatted_type",
            &[&expected_base_sql],
        )
        .await?;
    let expected_oid: Option<i64> = row.try_get("type_oid")?;
    let expected_format: Option<String> = row.try_get("formatted_type")?;
    if expected_oid != Some(actual.base_type_oid)
        || expected_format.as_deref() != Some(actual.formatted_base_type.as_str())
    {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!(
                "domain base is OID {} format `{}`, expected OID {expected_oid:?} format {expected_format:?}",
                actual.base_type_oid, actual.formatted_base_type
            ),
        ));
    }
    Ok(())
}

fn domain_base_requires_normalization(
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    actual: &DomainCatalog,
    expected_base_sql: &str,
) -> Result<bool, ReconciliationReaderError> {
    if actual.formatted_base_type == expected_base_sql {
        return Ok(false);
    }
    if actual.base_type_modifier != -1 {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!(
                "domain base format is `{}`, expected `{expected_base_sql}`",
                actual.formatted_base_type
            ),
        ));
    }
    Ok(true)
}

fn domain_from_row(row: &Row) -> Result<DomainCatalog, ReconciliationReaderError> {
    Ok(DomainCatalog {
        kind: row.try_get("type_kind")?,
        base_type_oid: row.try_get("base_type_oid")?,
        base_type_modifier: row.try_get("base_type_modifier")?,
        formatted_base_type: row.try_get("formatted_base_type")?,
        not_null: row.try_get("typnotnull")?,
        default: row.try_get("typdefault")?,
        constraint_count: row.try_get("constraint_count")?,
    })
}

fn validate_domain_shape(
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    actual: &DomainCatalog,
) -> Result<(), ReconciliationReaderError> {
    if actual.kind != "d" {
        return Err(type_mismatch(
            run,
            prerequisite,
            format!("physical type kind is `{}`, expected domain", actual.kind),
        ));
    }
    if actual.base_type_oid <= 0 {
        return Err(type_mismatch(
            run,
            prerequisite,
            "domain has no valid base type OID",
        ));
    }
    if actual.not_null || actual.default.is_some() || actual.constraint_count != 0 {
        return Err(type_mismatch(
            run,
            prerequisite,
            "domain has an external NOT NULL, DEFAULT, or pg_constraint definition",
        ));
    }
    Ok(())
}

fn type_mismatch(
    run: &ReconciliationRunIdentity,
    prerequisite: &UserTypePlan,
    detail: impl Into<String>,
) -> ReconciliationReaderError {
    mismatch(
        &run.target,
        format!(
            "prerequisite type `{}`: {}",
            prerequisite.name,
            detail.into()
        ),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationCatalog {
    oid: i64,
    kind: String,
    persistence: String,
    is_partition: bool,
    row_security: bool,
    force_row_security: bool,
    access_method: Option<String>,
}

async fn load_relation(
    transaction: &Transaction<'_>,
    target: &QualifiedName,
) -> Result<RelationCatalog, ReconciliationReaderError> {
    let rows = transaction
        .query(LOAD_RELATION_SQL, &[&target.schema, &target.name])
        .await?;
    if rows.len() != 1 {
        return Err(mismatch(
            target,
            format!("expected one physical relation, found {}", rows.len()),
        ));
    }
    relation_from_row(&rows[0]).map_err(Into::into)
}

fn relation_from_row(row: &Row) -> Result<RelationCatalog, tokio_postgres::Error> {
    Ok(RelationCatalog {
        oid: row.try_get("relation_oid")?,
        kind: row.try_get("relation_kind")?,
        persistence: row.try_get("persistence")?,
        is_partition: row.try_get("relispartition")?,
        row_security: row.try_get("relrowsecurity")?,
        force_row_security: row.try_get("relforcerowsecurity")?,
        access_method: row.try_get("access_method")?,
    })
}

fn validate_relation(
    run: &ReconciliationRunIdentity,
    table: &CreateTablePlan,
    actual: &RelationCatalog,
) -> Result<(), ReconciliationReaderError> {
    if actual.oid != i64::from(run.target_relation_oid) {
        return Err(mismatch(
            &run.target,
            format!(
                "relation OID is {}, expected {}",
                actual.oid, run.target_relation_oid
            ),
        ));
    }
    if actual.kind != "r" || actual.persistence != "p" || actual.is_partition {
        return Err(mismatch(
            &run.target,
            "relation is not a persistent ordinary non-partition table",
        ));
    }
    if actual.row_security || actual.force_row_security {
        return Err(mismatch(
            &run.target,
            "row security would make a whole-table digest role dependent",
        ));
    }
    if actual.access_method.as_deref() != Some(table.storage.access_method()) {
        return Err(mismatch(
            &run.target,
            format!(
                "access method is {:?}, expected `{}`",
                actual.access_method,
                table.storage.access_method()
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnCatalog {
    attnum: i32,
    name: String,
    formatted_type: String,
    type_oid: i64,
    type_modifier: i64,
    not_null: bool,
    dropped: bool,
    has_default: bool,
    generated: String,
    identity: String,
    type_is_collatable: bool,
    collation: Option<QualifiedName>,
}

async fn load_columns(
    transaction: &Transaction<'_>,
    relation_oid: i64,
) -> Result<Vec<ColumnCatalog>, ReconciliationReaderError> {
    transaction
        .query(LOAD_COLUMNS_SQL, &[&relation_oid])
        .await?
        .iter()
        .map(column_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn column_from_row(row: &Row) -> Result<ColumnCatalog, tokio_postgres::Error> {
    let collation = match (
        row.try_get::<_, Option<String>>("collation_schema")?,
        row.try_get::<_, Option<String>>("collation_name")?,
    ) {
        (Some(schema), Some(name)) => Some(QualifiedName { schema, name }),
        _ => None,
    };
    Ok(ColumnCatalog {
        attnum: row.try_get("attnum")?,
        name: row.try_get("attname")?,
        formatted_type: row.try_get("formatted_type")?,
        type_oid: row.try_get("type_oid")?,
        type_modifier: row.try_get("type_modifier")?,
        not_null: row.try_get("attnotnull")?,
        dropped: row.try_get("attisdropped")?,
        has_default: row.try_get("atthasdef")?,
        generated: row.try_get("generated")?,
        identity: row.try_get("identity")?,
        type_is_collatable: row.try_get("type_is_collatable")?,
        collation,
    })
}

async fn validate_columns(
    transaction: &Transaction<'_>,
    schema: &TableSchema,
    table: &CreateTablePlan,
    actual: &[ColumnCatalog],
) -> Result<(), ReconciliationReaderError> {
    if actual.len() != table.columns.len() || schema.columns.len() != table.columns.len() {
        return Err(mismatch(
            &table.target,
            format!(
                "visible/dropped pg_attribute row count is {}, expected {}",
                actual.len(),
                table.columns.len()
            ),
        ));
    }

    for (index, ((source, planned), actual)) in schema
        .columns
        .iter()
        .zip(&table.columns)
        .zip(actual)
        .enumerate()
    {
        let expected_attnum = i32::try_from(index + 1)
            .map_err(|_| invalid_identity("target column ordinal exceeds i32"))?;
        if actual.attnum != expected_attnum || actual.dropped {
            return Err(column_mismatch(
                table,
                index,
                format!(
                    "attribute number/dropped state is {}/{}, expected {expected_attnum}/false",
                    actual.attnum, actual.dropped
                ),
            ));
        }
        if actual.name != planned.name || source.name != planned.name {
            return Err(column_mismatch(
                table,
                index,
                format!("name is `{}`, expected `{}`", actual.name, planned.name),
            ));
        }
        validate_column_type(transaction, source, table, index, actual).await?;
        if actual.not_null == planned.nullable {
            return Err(column_mismatch(
                table,
                index,
                format!(
                    "nullability is nullable={}, expected {}",
                    !actual.not_null, planned.nullable
                ),
            ));
        }
        if actual.has_default || !actual.generated.is_empty() || !actual.identity.is_empty() {
            return Err(column_mismatch(
                table,
                index,
                "default, generated, or identity metadata was added externally",
            ));
        }
        let expected_collation = expected_collation(source, table, actual.type_is_collatable);
        if actual.collation != expected_collation {
            return Err(column_mismatch(
                table,
                index,
                format!(
                    "collation is {:?}, expected {:?}",
                    actual.collation, expected_collation
                ),
            ));
        }
    }
    Ok(())
}

async fn validate_column_type(
    transaction: &Transaction<'_>,
    source: &ColumnSchema,
    table: &CreateTablePlan,
    index: usize,
    actual: &ColumnCatalog,
) -> Result<(), ReconciliationReaderError> {
    let expected = plan_type(&source.data_type, &table.target.schema)?.sql;
    if !has_user_type(&source.data_type.kind) {
        if actual.formatted_type != expected {
            return Err(column_mismatch(
                table,
                index,
                format!(
                    "format_type is `{}`, expected `{expected}`",
                    actual.formatted_type
                ),
            ));
        }
        return Ok(());
    }

    let row = transaction
        .query_one(
            "SELECT pg_catalog.to_regtype($1)::oid::bigint AS type_oid, pg_catalog.format_type(pg_catalog.to_regtype($1)::oid, -1) AS formatted_type",
            &[&expected],
        )
        .await?;
    let expected_oid: Option<i64> = row.try_get("type_oid")?;
    let expected_formatted: Option<String> = row.try_get("formatted_type")?;
    if expected_oid != Some(actual.type_oid)
        || actual.type_modifier != -1
        || expected_formatted.as_deref() != Some(actual.formatted_type.as_str())
    {
        return Err(column_mismatch(
            table,
            index,
            format!(
                "user type is OID {}/{}, format `{}`; expected OID {:?}, typmod -1, format {:?}",
                actual.type_oid,
                actual.type_modifier,
                actual.formatted_type,
                expected_oid,
                expected_formatted
            ),
        ));
    }
    Ok(())
}

fn has_user_type(kind: &PgTypeKind) -> bool {
    match kind {
        PgTypeKind::Enum { .. } | PgTypeKind::Domain { .. } => true,
        PgTypeKind::Array { element } => has_user_type(&element.kind),
        _ => false,
    }
}

fn expected_collation(
    source: &ColumnSchema,
    table: &CreateTablePlan,
    type_is_collatable: bool,
) -> Option<QualifiedName> {
    source.collation.as_ref().map_or_else(
        || {
            type_is_collatable
                .then(|| QualifiedName::new("pg_catalog", "default").expect("static name"))
        },
        |collation| {
            Some(if collation.schema == "pg_catalog" {
                collation.clone()
            } else {
                QualifiedName::new(&table.target.schema, &collation.name)
                    .expect("validated source and target identifiers")
            })
        },
    )
}

fn column_mismatch(
    table: &CreateTablePlan,
    index: usize,
    detail: impl Into<String>,
) -> ReconciliationReaderError {
    mismatch(
        &table.target,
        format!("column {}: {}", index + 1, detail.into()),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PrimaryKeyCatalog {
    index_oid: i64,
    unique: bool,
    valid: bool,
    ready: bool,
    immediate: bool,
    key_attribute_count: i32,
    total_attribute_count: i32,
    has_no_predicate: bool,
    has_no_expressions: bool,
    key_attnums: Vec<i32>,
    key_names: Vec<Option<String>>,
}

async fn load_primary_key(
    transaction: &Transaction<'_>,
    relation_oid: i64,
) -> Result<Vec<PrimaryKeyCatalog>, ReconciliationReaderError> {
    transaction
        .query(LOAD_PRIMARY_KEY_SQL, &[&relation_oid])
        .await?
        .iter()
        .map(primary_key_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn primary_key_from_row(row: &Row) -> Result<PrimaryKeyCatalog, tokio_postgres::Error> {
    Ok(PrimaryKeyCatalog {
        index_oid: row.try_get("index_oid")?,
        unique: row.try_get("indisunique")?,
        valid: row.try_get("indisvalid")?,
        ready: row.try_get("indisready")?,
        immediate: row.try_get("indimmediate")?,
        key_attribute_count: row.try_get("key_attribute_count")?,
        total_attribute_count: row.try_get("total_attribute_count")?,
        has_no_predicate: row.try_get("has_no_predicate")?,
        has_no_expressions: row.try_get("has_no_expressions")?,
        key_attnums: row.try_get("key_attnums")?,
        key_names: row.try_get("key_names")?,
    })
}

fn validate_primary_key(
    table: &CreateTablePlan,
    rows: &[PrimaryKeyCatalog],
) -> Result<(), ReconciliationReaderError> {
    if rows.len() != 1 {
        return Err(mismatch(
            &table.target,
            format!("expected one primary-key index, found {}", rows.len()),
        ));
    }
    let actual = &rows[0];
    let expected_count = i32::try_from(table.primary_key.len())
        .map_err(|_| invalid_identity("primary key arity exceeds i32"))?;
    let expected_attnums = table
        .primary_key
        .iter()
        .map(|name| {
            table
                .columns
                .iter()
                .position(|column| &column.name == name)
                .and_then(|index| i32::try_from(index + 1).ok())
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid_identity("primary key references an unknown target column"))?;
    let actual_names = actual.key_names.iter().cloned().collect::<Option<Vec<_>>>();
    if actual.index_oid <= 0
        || !actual.unique
        || !actual.valid
        || !actual.ready
        || !actual.immediate
        || !actual.has_no_predicate
        || !actual.has_no_expressions
        || actual.key_attribute_count != expected_count
        || actual.total_attribute_count != expected_count
        || actual.key_attnums != expected_attnums
        || actual_names.as_ref() != Some(&table.primary_key)
    {
        return Err(mismatch(
            &table.target,
            format!(
                "primary-key catalog contract differs (names={actual_names:?}, attnums={:?})",
                actual.key_attnums
            ),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DistributionCatalog {
    policy_type: String,
    num_segments: i32,
    key_attnums: Vec<i32>,
    key_names: Vec<Option<String>>,
}

async fn load_distribution(
    transaction: &Transaction<'_>,
    relation_oid: i64,
) -> Result<Vec<DistributionCatalog>, ReconciliationReaderError> {
    transaction
        .query(LOAD_DISTRIBUTION_SQL, &[&relation_oid])
        .await?
        .iter()
        .map(distribution_from_row)
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
}

fn distribution_from_row(row: &Row) -> Result<DistributionCatalog, tokio_postgres::Error> {
    Ok(DistributionCatalog {
        policy_type: row.try_get("policy_type")?,
        num_segments: row.try_get("numsegments")?,
        key_attnums: row.try_get("key_attnums")?,
        key_names: row.try_get("key_names")?,
    })
}

fn validate_distribution(
    table: &CreateTablePlan,
    rows: &[DistributionCatalog],
) -> Result<(), ReconciliationReaderError> {
    if rows.len() != 1 {
        return Err(mismatch(
            &table.target,
            format!(
                "expected one Cloudberry distribution policy, found {}",
                rows.len()
            ),
        ));
    }
    let actual = &rows[0];
    let expected_attnums = table
        .distribution_key
        .iter()
        .map(|name| {
            table
                .columns
                .iter()
                .position(|column| &column.name == name)
                .and_then(|index| i32::try_from(index + 1).ok())
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| invalid_identity("distribution key references an unknown target column"))?;
    let actual_names = actual.key_names.iter().cloned().collect::<Option<Vec<_>>>();
    if actual.policy_type != "p"
        || actual.num_segments <= 0
        || actual.key_attnums != expected_attnums
        || actual_names.as_ref() != Some(&table.distribution_key)
    {
        return Err(mismatch(
            &table.target,
            format!(
                "distribution contract differs (policy={}, segments={}, names={actual_names:?}, attnums={:?})",
                actual.policy_type, actual.num_segments, actual.key_attnums
            ),
        ));
    }
    Ok(())
}

fn mismatch(target: &QualifiedName, detail: impl Into<String>) -> ReconciliationReaderError {
    ReconciliationReaderError::CatalogMismatch {
        table: target.to_string(),
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use cloudberry_etl_core::{
        id::PipelineId,
        lsn::PgLsn,
        schema::{
            ColumnSchema, GeneratedColumn, IdentityColumn, PgType, ReplicaIdentity, TableKind,
        },
    };
    use uuid::Uuid;

    use super::*;
    use crate::{checkpoint::PipelineFence, storage::TargetStorage};

    fn column(
        attnum: i16,
        name: &str,
        oid: u32,
        type_name: &str,
        kind: PgTypeKind,
        key: Option<u16>,
    ) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid,
                name: QualifiedName::new("pg_catalog", type_name).unwrap(),
                kind,
            },
            nullable: key.is_none(),
            primary_key_ordinal: key,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn schema() -> TableSchema {
        TableSchema {
            relation_id: 42,
            generation: 3,
            name: QualifiedName::new("source", "orders").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns: vec![
                column(1, "payload", 25, "text", PgTypeKind::Text, None),
                column(2, "sequence", 20, "int8", PgTypeKind::Int8, Some(2)),
                column(3, "tenant", 23, "int4", PgTypeKind::Int4, Some(1)),
                column(4, "note", 25, "text", PgTypeKind::Text, None),
            ],
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    fn run(target: QualifiedName, oid: u32) -> ReconciliationRunIdentity {
        ReconciliationRunIdentity {
            fence: PipelineFence {
                pipeline_id: PipelineId::from_uuid(Uuid::from_u128(1)),
                topology_generation: 3,
                fencing_token: 7,
            },
            source_relation_id: 42,
            target,
            target_relation_oid: oid,
            table_generation: 3,
            schema_fingerprint: "sha256:orders-v3".to_owned(),
            run_id: Uuid::from_u128(2),
            source_node_id: 0,
            temporary_slot_name: "pg2cb_reconcile_orders_2".to_owned(),
            source_system_identifier: 123,
            source_timeline: 1,
            source_snapshot_lsn: PgLsn::new(0x100),
        }
    }

    fn plan() -> CreateTablePlan {
        plan_create_table_with_storage(
            &schema(),
            QualifiedName::new("analytics", "orders").unwrap(),
            TargetStorage::AoColumn,
        )
        .unwrap()
    }

    fn scanning_state(
        run: ReconciliationRunIdentity,
        checkpoint_lsn: PgLsn,
    ) -> StoredReconciliationState {
        StoredReconciliationState {
            run,
            state: ReconciliationState::Scanning,
            target_checkpoint_lsn: Some(checkpoint_lsn),
            source: None,
            target: None,
            started_at: UNIX_EPOCH,
            completed_at: None,
            last_consistent_at: None,
            last_mismatch_at: None,
            next_due_at: None,
            failure_reason: None,
            consecutive_failures: 0,
        }
    }

    fn checkpoint(run: &ReconciliationRunIdentity, applied_lsn: PgLsn) -> StoredNodeCheckpoint {
        StoredNodeCheckpoint {
            checkpoint: crate::checkpoint::NodeCheckpoint {
                key: CheckpointKey {
                    pipeline_id: run.fence.pipeline_id,
                    topology_generation: run.fence.topology_generation,
                    node_id: run.source_node_id,
                },
                system_identifier: run.source_system_identifier,
                timeline: run.source_timeline,
                slot_name: "pg2cb_main".to_owned(),
                applied_lsn,
            },
            fencing_token: run.fence.fencing_token,
        }
    }

    fn enum_prerequisite() -> UserTypePlan {
        plan_type(
            &PgType {
                oid: 90_001,
                name: QualifiedName::new("source", "order_status").unwrap(),
                kind: PgTypeKind::Enum {
                    labels: vec!["new".to_owned(), "done".to_owned()],
                },
            },
            "analytics",
        )
        .unwrap()
        .prerequisites
        .into_iter()
        .next()
        .unwrap()
    }

    #[test]
    fn copy_projection_matches_source_key_first_contract_without_sorting() {
        let schema = schema();
        let target = QualifiedName::new("Analytics", "Order\"Rows").unwrap();
        let sql = reconciliation_copy_sql(&schema, &target).unwrap();
        assert_eq!(
            sql,
            "COPY (SELECT \"tenant\", \"sequence\", \"payload\", \"note\" FROM \"Analytics\".\"Order\"\"Rows\") TO STDOUT WITH (FORMAT text, HEADER false, DELIMITER E'\\t', NULL E'\\\\N')"
        );
        assert!(!sql.contains("ORDER BY"));
    }

    #[test]
    fn canonical_session_is_repeatable_and_every_timeout_is_bounded() {
        let sql = canonical_settings_sql(ReconciliationReaderTimeouts::default()).unwrap();
        for required in [
            "client_encoding = 'UTF8'",
            "DateStyle = 'ISO, YMD'",
            "IntervalStyle = 'postgres'",
            "TimeZone = 'UTC'",
            "extra_float_digits = 3",
            "bytea_output = 'hex'",
            "search_path = pg_catalog",
            "lock_timeout = '5000ms'",
            "statement_timeout = '21600000ms'",
            "idle_in_transaction_session_timeout = '300000ms'",
        ] {
            assert!(sql.contains(required), "missing {required}");
        }
        let invalid = ReconciliationReaderTimeouts {
            lock_timeout: Duration::ZERO,
            ..ReconciliationReaderTimeouts::default()
        };
        assert!(canonical_settings_sql(invalid).is_err());
        let invalid = ReconciliationReaderTimeouts {
            statement_timeout: MAX_TIMEOUT + Duration::from_millis(1),
            ..ReconciliationReaderTimeouts::default()
        };
        assert!(canonical_settings_sql(invalid).is_err());
    }

    #[test]
    fn request_accepts_independent_generations_and_rejects_invalid_identity_or_plan() {
        let schema = schema();
        let mut table = plan();
        let mut run = run(table.target.clone(), 1234);
        validate_request(&run, &schema, &table).unwrap();

        run.table_generation += 1;
        validate_request(&run, &schema, &table)
            .expect("managed table and source catalog generations are independent");

        run.table_generation = 0;
        assert!(matches!(
            validate_request(&run, &schema, &table),
            Err(ReconciliationReaderError::InvalidIdentity(_))
        ));
        run.table_generation = 4;
        let mut invalid_schema = schema.clone();
        invalid_schema.generation = 0;
        assert!(matches!(
            validate_request(&run, &invalid_schema, &table),
            Err(ReconciliationReaderError::InvalidIdentity(_))
        ));

        table.columns[0].nullable = false;
        assert!(matches!(
            validate_request(&run, &schema, &table),
            Err(ReconciliationReaderError::NonCanonicalPlan(_))
        ));
    }

    #[test]
    fn relation_primary_key_and_distribution_fail_closed() {
        let table = plan();
        let run = run(table.target.clone(), 1234);
        let relation = RelationCatalog {
            oid: 1234,
            kind: "r".to_owned(),
            persistence: "p".to_owned(),
            is_partition: false,
            row_security: false,
            force_row_security: false,
            access_method: Some("ao_column".to_owned()),
        };
        validate_relation(&run, &table, &relation).unwrap();
        let mut wrong_relation = relation.clone();
        wrong_relation.access_method = Some("heap".to_owned());
        assert!(validate_relation(&run, &table, &wrong_relation).is_err());

        let primary = PrimaryKeyCatalog {
            index_oid: 8,
            unique: true,
            valid: true,
            ready: true,
            immediate: true,
            key_attribute_count: 2,
            total_attribute_count: 2,
            has_no_predicate: true,
            has_no_expressions: true,
            key_attnums: vec![3, 2],
            key_names: vec![Some("tenant".to_owned()), Some("sequence".to_owned())],
        };
        validate_primary_key(&table, std::slice::from_ref(&primary)).unwrap();
        let mut include = primary;
        include.total_attribute_count = 3;
        assert!(validate_primary_key(&table, &[include]).is_err());

        let distribution = DistributionCatalog {
            policy_type: "p".to_owned(),
            num_segments: 4,
            key_attnums: vec![3, 2],
            key_names: vec![Some("tenant".to_owned()), Some("sequence".to_owned())],
        };
        validate_distribution(&table, std::slice::from_ref(&distribution)).unwrap();
        let mut random = distribution;
        random.key_attnums.clear();
        random.key_names.clear();
        assert!(validate_distribution(&table, &[random]).is_err());
    }

    #[test]
    fn alignment_requires_the_exact_scanning_run_and_checkpoint_snapshot() {
        let run = run(QualifiedName::new("analytics", "orders").unwrap(), 1234);
        let lsn = PgLsn::new(0xff);
        let state = scanning_state(run.clone(), lsn);
        let stored_checkpoint = checkpoint(&run, lsn);
        validate_alignment_catalog(&run, Some(&state), Some(&stored_checkpoint)).unwrap();

        assert!(matches!(
            validate_alignment_catalog(&run, None, Some(&stored_checkpoint)),
            Err(ReconciliationReaderError::Alignment(_))
        ));
        let mut aligning = state.clone();
        aligning.state = ReconciliationState::Aligning;
        assert!(
            validate_alignment_catalog(&run, Some(&aligning), Some(&stored_checkpoint)).is_err()
        );

        let advanced = checkpoint(&run, PgLsn::new(0x100));
        assert!(validate_alignment_catalog(&run, Some(&state), Some(&advanced)).is_err());
        let mut wrong_source = stored_checkpoint;
        wrong_source.checkpoint.timeline += 1;
        assert!(validate_alignment_catalog(&run, Some(&state), Some(&wrong_source)).is_err());
    }

    #[test]
    fn managed_type_enum_and_domain_contracts_fail_closed() {
        let run = run(QualifiedName::new("analytics", "orders").unwrap(), 1234);
        let prerequisite = enum_prerequisite();
        let checksum = Sha256::digest(prerequisite.create_sql.as_bytes()).to_vec();
        let managed = ManagedTypeCatalog {
            pipeline_id: run.fence.pipeline_id,
            definition_checksum: checksum,
            fencing_token: run.fence.fencing_token,
        };
        validate_managed_type(&run, &prerequisite, &managed).unwrap();
        validate_enum_catalog(
            &run,
            &prerequisite,
            "e",
            &["new".to_owned(), "done".to_owned()],
            &["new".to_owned(), "done".to_owned()],
        )
        .unwrap();

        let mut wrong_checksum = managed.clone();
        wrong_checksum.definition_checksum[0] ^= 1;
        assert!(validate_managed_type(&run, &prerequisite, &wrong_checksum).is_err());
        let mut other_pipeline = managed.clone();
        other_pipeline.pipeline_id = PipelineId::new();
        assert!(validate_managed_type(&run, &prerequisite, &other_pipeline).is_err());
        let mut newer = managed;
        newer.fencing_token += 1;
        assert!(validate_managed_type(&run, &prerequisite, &newer).is_err());
        assert!(
            validate_enum_catalog(
                &run,
                &prerequisite,
                "e",
                &["done".to_owned(), "new".to_owned()],
                &["new".to_owned(), "done".to_owned()],
            )
            .is_err()
        );

        let domain = UserTypePlan {
            name: QualifiedName::new("analytics", "order_code").unwrap(),
            create_sql: "CREATE DOMAIN \"analytics\".\"order_code\" AS character varying(16)"
                .to_owned(),
            definition: UserTypeDefinition::Domain {
                base_sql: "character varying(16)".to_owned(),
            },
        };
        let physical = DomainCatalog {
            kind: "d".to_owned(),
            base_type_oid: 1043,
            base_type_modifier: 20,
            formatted_base_type: "character varying(16)".to_owned(),
            not_null: false,
            default: None,
            constraint_count: 0,
        };
        validate_domain_shape(&run, &domain, &physical).unwrap();
        assert!(
            !domain_base_requires_normalization(&run, &domain, &physical, "character varying(16)")
                .unwrap()
        );
        assert!(
            domain_base_requires_normalization(&run, &domain, &physical, "character varying(32)")
                .is_err()
        );
        let mut constrained = physical;
        constrained.constraint_count = 1;
        assert!(validate_domain_shape(&run, &domain, &constrained).is_err());
    }

    #[test]
    fn catalog_sql_uses_oid_scoped_structured_catalogs() {
        assert!(LOAD_RELATION_SQL.contains("c.oid::bigint"));
        assert!(LOAD_COLUMNS_SQL.contains("format_type(a.atttypid, a.atttypmod)"));
        assert!(LOAD_COLUMNS_SQL.contains("a.attisdropped"));
        assert!(LOAD_PRIMARY_KEY_SQL.contains("ordinal <= i.indnkeyatts"));
        assert!(LOAD_PRIMARY_KEY_SQL.contains("i.indnatts"));
        assert!(LOAD_DISTRIBUTION_SQL.contains("p.distkey::smallint[]"));
        assert!(LOAD_DISTRIBUTION_SQL.contains("p.policytype"));
        assert!(LOCK_MANAGED_TYPE_SQL.trim_end().ends_with("FOR UPDATE"));
        assert!(LOAD_ENUM_LABELS_SQL.contains("ORDER BY e.enumsortorder"));
        assert!(LOAD_DOMAIN_SQL.contains("format_type(t.typbasetype, t.typtypmod)"));
        assert!(LOAD_DOMAIN_SQL.contains("c.contypid = t.oid"));
    }
}
