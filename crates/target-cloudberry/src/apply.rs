//! Transactional staging apply and durable checkpoint advancement.

use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use cloudberry_etl_core::{
    change::Cell,
    schema::{QualifiedName, TableSchema},
};
use futures::SinkExt;
use thiserror::Error;
use tokio_postgres::{Client, Transaction};

use crate::{
    checkpoint::{
        AdvanceOutcome, CheckpointError, NodeCheckpoint, PipelineFence, advance_node_checkpoint,
        lock_pipeline_fence,
    },
    chunk::{
        ChunkLedgerError, DataChunkIdentity, PrepareDataChunkOutcome,
        PreparedTransactionCompletion, ProgressRegistration, TransactionChunkManifest,
        complete_transaction_checkpoint, prepare_data_chunk, prepare_transaction_completion,
        record_data_chunk, register_transaction_progress,
    },
    copy::{CopyEncodeError, encode_row},
    managed::{ManagedTableError, TableApplyIdentity, lock_active_apply_tables},
    schema::{CreateTablePlan, SchemaError, plan_create_table, quote_identifier_list},
    sql::{SqlRenderError, quote_identifier, quote_literal, quote_qualified_name},
};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ApplyPlanError {
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Sql(#[from] SqlRenderError),
}

#[derive(Debug, Error)]
pub enum ApplyError {
    #[error(transparent)]
    Database(#[from] tokio_postgres::Error),
    #[error(transparent)]
    Checkpoint(#[from] CheckpointError),
    #[error(transparent)]
    ChunkLedger(#[from] ChunkLedgerError),
    #[error(transparent)]
    Copy(#[from] CopyEncodeError),
    #[error(transparent)]
    ManagedTable(#[from] ManagedTableError),
    #[error("staging row has {actual} data cells, expected {expected}")]
    InvalidRowArity { expected: usize, actual: usize },
    #[error("primary-key column `{0}` is NULL or unchanged")]
    MissingPrimaryKey(String),
    #[error("move staging row has {actual} old-key cells, expected {expected}")]
    InvalidOldKeyArity { expected: usize, actual: usize },
    #[error("move staging row is missing its old primary key")]
    MissingOldPrimaryKey,
    #[error("only move staging rows may carry an old primary key")]
    UnexpectedOldPrimaryKey,
    #[error("old primary-key column `{0}` is NULL or unchanged")]
    MissingOldPrimaryKeyColumn(String),
    #[error("staging name `{0}` is repeated in one target transaction")]
    DuplicateStagingName(String),
    #[error("apply identity target `{identity}` does not match plan target `{plan}`")]
    IdentityTargetMismatch { identity: String, plan: String },
    #[error("Cloudberry rejected an invalid or non-collapsed staging batch for {0}")]
    InvalidBatch(String),
    #[error("empty transaction apply received a manifest with {record_count} records")]
    NonEmptyManifest { record_count: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceColumn {
    pub source_column_index: usize,
    pub source_column_name: String,
    pub staging_column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OldKeyColumn {
    pub source_column_index: usize,
    pub source_column_name: String,
    pub staging_column_name: String,
    pub type_sql: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyLayout {
    pub operation_column: String,
    pub data_columns: Vec<String>,
    pub presence_columns: Vec<PresenceColumn>,
    pub old_key_present_column: String,
    pub old_key_columns: Vec<OldKeyColumn>,
}

/// Pure SQL plan for applying one already-collapsed table batch.
///
/// The caller executes these statements in order in one target transaction,
/// then advances the node checkpoint through `checkpoint::advance_node_checkpoint`
/// before committing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyPlan {
    pub table: CreateTablePlan,
    pub staging_name: String,
    pub copy_layout: CopyLayout,
    pub create_staging_sql: String,
    pub copy_sql: String,
    pub validation_sql: String,
    pub materialize_moves_sql: Option<String>,
    pub delete_sql: String,
    pub delete_moved_sql: String,
    pub move_sql: String,
    pub update_sql: Option<String>,
    pub insert_sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOperation {
    Upsert,
    Delete,
    Move,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingRow {
    pub operation: StageOperation,
    pub cells: Vec<Cell>,
    /// Old PK cells in primary-key ordinal order. Present only for `Move`.
    pub old_key: Option<Vec<Cell>>,
}

#[derive(Debug, Clone)]
pub struct TableApplyBatch {
    pub identity: Arc<TableApplyIdentity>,
    pub plan: Arc<ApplyPlan>,
    pub rows: Vec<StagingRow>,
}

#[derive(Debug, Clone)]
pub struct ApplyRequest {
    pub fence: PipelineFence,
    pub checkpoint: NodeCheckpoint,
    pub tables: Vec<TableApplyBatch>,
}

/// One bounded part of an immutable source transaction.
#[derive(Debug, Clone)]
pub struct LedgeredDataChunkRequest {
    pub fence: PipelineFence,
    pub manifest: TransactionChunkManifest,
    pub chunk: DataChunkIdentity,
    pub tables: Vec<TableApplyBatch>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ApplyStats {
    pub staged_rows: u64,
    pub deleted_rows: u64,
    pub moved_rows: u64,
    pub updated_rows: u64,
    pub inserted_rows: u64,
}

/// How one requested data chunk related to the durable target prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataChunkDisposition {
    Applied { stats: ApplyStats },
    AlreadyCommitted,
    ResumeAt,
}

/// Result of one bounded target transaction.
///
/// `InProgress` always names a prefix before the manifest end. `Completed` is returned only after
/// the checkpoint and ledger retirement committed with the final chunk (or a replay that found an
/// already complete durable prefix).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgeredDataChunkOutcome {
    InProgress {
        next_seq: u64,
        disposition: DataChunkDisposition,
    },
    Completed {
        next_seq: u64,
        disposition: DataChunkDisposition,
        checkpoint: AdvanceOutcome,
    },
    /// The target checkpoint already covers this transaction and no DML ran.
    AlreadyCheckpointed {
        applied_lsn: cloudberry_etl_core::lsn::PgLsn,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LedgeredEmptyTransactionOutcome {
    Completed {
        checkpoint: AdvanceOutcome,
    },
    AlreadyCheckpointed {
        applied_lsn: cloudberry_etl_core::lsn::PgLsn,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub staged_rows: u64,
    pub deleted_rows: u64,
    pub moved_rows: u64,
    pub updated_rows: u64,
    pub inserted_rows: u64,
    pub checkpoint: AdvanceOutcome,
}

/// Applies all table batches and the node checkpoint in one Cloudberry transaction.
///
/// Returning success means the transaction committed and the caller may ACK the
/// source position. Any error rolls back both user data and checkpoint progress.
pub async fn execute_apply(
    client: &mut Client,
    request: &ApplyRequest,
) -> Result<ApplyOutcome, ApplyError> {
    validate_request(request)?;
    let transaction = client.transaction().await?;
    let identities = table_identities(&request.tables);
    lock_active_apply_tables(&transaction, request.fence, &identities).await?;
    let stats = apply_tables(&transaction, &request.tables).await?;
    let checkpoint =
        advance_node_checkpoint(&transaction, request.fence, &request.checkpoint).await?;
    transaction.commit().await?;
    Ok(ApplyOutcome {
        staged_rows: stats.staged_rows,
        deleted_rows: stats.deleted_rows,
        moved_rows: stats.moved_rows,
        updated_rows: stats.updated_rows,
        inserted_rows: stats.inserted_rows,
        checkpoint,
    })
}

/// Applies a bounded source-transaction chunk with its durable receipt.
///
/// The receipt and user-table DML commit in one target transaction. Replays and
/// repartitioned requests return the durable resume position without executing
/// the supplied table batches.
pub async fn execute_ledgered_data_chunk(
    client: &mut Client,
    request: &LedgeredDataChunkRequest,
) -> Result<LedgeredDataChunkOutcome, ApplyError> {
    validate_tables(&request.tables)?;
    let transaction = client.transaction().await?;
    let prepared = prepare_data_chunk(
        &transaction,
        request.fence,
        &request.manifest,
        request.chunk,
    )
    .await?;
    match prepared {
        PrepareDataChunkOutcome::Apply(prepared) => {
            let next_seq = prepared.next_seq();
            let identities = table_identities(&request.tables);
            lock_active_apply_tables(&transaction, request.fence, &identities).await?;
            record_data_chunk(&transaction, prepared).await?;
            let stats = apply_tables(&transaction, &request.tables).await?;
            finish_ledgered_data_chunk(
                transaction,
                request,
                next_seq,
                DataChunkDisposition::Applied { stats },
            )
            .await
        }
        PrepareDataChunkOutcome::AlreadyCommitted { next_seq } => {
            finish_ledgered_data_chunk(
                transaction,
                request,
                next_seq,
                DataChunkDisposition::AlreadyCommitted,
            )
            .await
        }
        PrepareDataChunkOutcome::ResumeAt { next_seq } => {
            finish_ledgered_data_chunk(
                transaction,
                request,
                next_seq,
                DataChunkDisposition::ResumeAt,
            )
            .await
        }
        PrepareDataChunkOutcome::AlreadyCheckpointed { applied_lsn } => {
            transaction.rollback().await?;
            Ok(LedgeredDataChunkOutcome::AlreadyCheckpointed { applied_lsn })
        }
    }
}

/// Commits a zero-record manifest, its checkpoint, and immediate ledger retirement atomically.
pub async fn execute_ledgered_empty_transaction(
    client: &mut Client,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<LedgeredEmptyTransactionOutcome, ApplyError> {
    if manifest.record_count != 0 {
        return Err(ApplyError::NonEmptyManifest {
            record_count: manifest.record_count,
        });
    }

    let transaction = client.transaction().await?;
    match register_transaction_progress(&transaction, fence, manifest).await? {
        ProgressRegistration::AlreadyCheckpointed { applied_lsn } => {
            transaction.rollback().await?;
            Ok(LedgeredEmptyTransactionOutcome::AlreadyCheckpointed { applied_lsn })
        }
        ProgressRegistration::Registered | ProgressRegistration::Existing { next_seq: 0 } => {
            let completion = prepare_transaction_completion(&transaction, fence, manifest).await?;
            let checkpoint = manifest.node_checkpoint();
            let checkpoint =
                complete_transaction_checkpoint(&transaction, completion, &checkpoint).await?;
            transaction.commit().await?;
            Ok(LedgeredEmptyTransactionOutcome::Completed { checkpoint })
        }
        ProgressRegistration::Existing { next_seq } => Err(ApplyError::ChunkLedger(
            ChunkLedgerError::InvalidPersistedValue {
                field: "next_seq",
                value: next_seq.to_string(),
            },
        )),
    }
}

async fn finish_ledgered_data_chunk(
    transaction: Transaction<'_>,
    request: &LedgeredDataChunkRequest,
    next_seq: u64,
    disposition: DataChunkDisposition,
) -> Result<LedgeredDataChunkOutcome, ApplyError> {
    if next_seq > request.manifest.record_count {
        return Err(ApplyError::ChunkLedger(
            ChunkLedgerError::InvalidPersistedValue {
                field: "next_seq",
                value: next_seq.to_string(),
            },
        ));
    }
    if next_seq == request.manifest.record_count {
        let completion =
            prepare_transaction_completion(&transaction, request.fence, &request.manifest).await?;
        let checkpoint = request.manifest.node_checkpoint();
        let checkpoint =
            complete_transaction_checkpoint(&transaction, completion, &checkpoint).await?;
        transaction.commit().await?;
        return Ok(LedgeredDataChunkOutcome::Completed {
            next_seq,
            disposition,
            checkpoint,
        });
    }

    if matches!(disposition, DataChunkDisposition::Applied { .. }) {
        transaction.commit().await?;
    } else {
        transaction.rollback().await?;
    }
    Ok(LedgeredDataChunkOutcome::InProgress {
        next_seq,
        disposition,
    })
}

/// Durably registers an immutable manifest without applying a data chunk.
///
/// Empty source transactions use this path before preparing their completion;
/// non-empty manifests are normally registered atomically with their first data
/// chunk by [`execute_ledgered_data_chunk`].
pub async fn execute_register_manifest(
    client: &mut Client,
    fence: PipelineFence,
    manifest: &TransactionChunkManifest,
) -> Result<ProgressRegistration, ApplyError> {
    let transaction = client.transaction().await?;
    lock_pipeline_fence(&transaction, fence).await?;
    let registration = register_transaction_progress(&transaction, fence, manifest).await?;
    transaction.commit().await?;
    Ok(registration)
}

/// Publishes a checkpoint after the ledger proved the full manifest committed.
///
/// `completion` must have been prepared using this same caller-owned
/// transaction. Consuming the transaction here keeps completion validation and
/// checkpoint publication in one atomic target operation.
pub async fn execute_ledgered_completion(
    transaction: Transaction<'_>,
    completion: PreparedTransactionCompletion,
    checkpoint: &NodeCheckpoint,
) -> Result<AdvanceOutcome, ApplyError> {
    let outcome = complete_transaction_checkpoint(&transaction, completion, checkpoint).await?;
    transaction.commit().await?;
    Ok(outcome)
}

pub fn encode_staging_row(plan: &ApplyPlan, row: &StagingRow) -> Result<Vec<u8>, ApplyError> {
    if row.cells.len() != plan.table.columns.len() {
        return Err(ApplyError::InvalidRowArity {
            expected: plan.table.columns.len(),
            actual: row.cells.len(),
        });
    }
    for (column, cell) in plan.table.columns.iter().zip(&row.cells) {
        if column.primary_key_ordinal.is_some() && matches!(cell, Cell::Null | Cell::UnchangedToast)
        {
            return Err(ApplyError::MissingPrimaryKey(column.name.clone()));
        }
    }

    match (row.operation, row.old_key.as_deref()) {
        (StageOperation::Move, None) => return Err(ApplyError::MissingOldPrimaryKey),
        (StageOperation::Upsert | StageOperation::Delete, Some(_)) => {
            return Err(ApplyError::UnexpectedOldPrimaryKey);
        }
        _ => {}
    }
    if let Some(old_key) = &row.old_key {
        if old_key.len() != plan.copy_layout.old_key_columns.len() {
            return Err(ApplyError::InvalidOldKeyArity {
                expected: plan.copy_layout.old_key_columns.len(),
                actual: old_key.len(),
            });
        }
        for (column, cell) in plan.copy_layout.old_key_columns.iter().zip(old_key) {
            if matches!(cell, Cell::Null | Cell::UnchangedToast) {
                return Err(ApplyError::MissingOldPrimaryKeyColumn(
                    column.source_column_name.clone(),
                ));
            }
        }
    }

    let mut fields = Vec::with_capacity(
        2 + plan.copy_layout.data_columns.len()
            + plan.copy_layout.presence_columns.len()
            + plan.copy_layout.old_key_columns.len(),
    );
    fields.push(Cell::Text(Bytes::from_static(match row.operation {
        StageOperation::Upsert => b"U",
        StageOperation::Delete => b"D",
        StageOperation::Move => b"M",
    })));
    fields.extend(row.cells.iter().map(|cell| match cell {
        Cell::UnchangedToast => Cell::Null,
        other => other.clone(),
    }));
    fields.extend(plan.copy_layout.presence_columns.iter().map(|presence| {
        let is_present = !matches!(
            row.cells[presence.source_column_index],
            Cell::UnchangedToast
        );
        Cell::Text(Bytes::from_static(if is_present { b"t" } else { b"f" }))
    }));
    fields.push(Cell::Text(Bytes::from_static(if row.old_key.is_some() {
        b"t"
    } else {
        b"f"
    })));
    match &row.old_key {
        Some(old_key) => fields.extend(old_key.iter().cloned()),
        None => fields.extend(plan.copy_layout.old_key_columns.iter().map(|_| Cell::Null)),
    }
    encode_row(&fields).map_err(ApplyError::from)
}

fn validate_request(request: &ApplyRequest) -> Result<(), ApplyError> {
    validate_tables(&request.tables)
}

fn validate_tables(tables: &[TableApplyBatch]) -> Result<(), ApplyError> {
    let mut staging_names = HashSet::new();
    for table in tables {
        if table.identity.target != table.plan.table.target {
            return Err(ApplyError::IdentityTargetMismatch {
                identity: table.identity.target.to_string(),
                plan: table.plan.table.target.to_string(),
            });
        }
        if !staging_names.insert(&table.plan.staging_name) {
            return Err(ApplyError::DuplicateStagingName(
                table.plan.staging_name.clone(),
            ));
        }
    }
    Ok(())
}

fn table_identities(tables: &[TableApplyBatch]) -> Vec<&TableApplyIdentity> {
    tables
        .iter()
        .filter(|table| !table.rows.is_empty())
        .map(|table| table.identity.as_ref())
        .collect()
}

async fn apply_tables(
    transaction: &Transaction<'_>,
    tables: &[TableApplyBatch],
) -> Result<ApplyStats, ApplyError> {
    let mut stats = ApplyStats::default();
    for table in tables {
        if table.rows.is_empty() {
            continue;
        }
        transaction
            .batch_execute(&table.plan.create_staging_sql)
            .await?;
        let sink = transaction.copy_in(&table.plan.copy_sql).await?;
        let mut sink = std::pin::pin!(sink);
        for row in &table.rows {
            let encoded = encode_staging_row(&table.plan, row)?;
            sink.send(Bytes::from(encoded)).await?;
        }
        let copied = sink.finish().await?;
        stats.staged_rows = stats.staged_rows.saturating_add(copied);

        let invalid: bool = transaction
            .query_one(&table.plan.validation_sql, &[])
            .await?
            .try_get("invalid_batch")?;
        if invalid {
            return Err(ApplyError::InvalidBatch(
                table.plan.table.target.to_string(),
            ));
        }
        if let Some(materialize_moves_sql) = &table.plan.materialize_moves_sql {
            transaction.execute(materialize_moves_sql, &[]).await?;
        }
        stats.deleted_rows = stats
            .deleted_rows
            .saturating_add(transaction.execute(&table.plan.delete_sql, &[]).await?);
        stats.deleted_rows = stats.deleted_rows.saturating_add(
            transaction
                .execute(&table.plan.delete_moved_sql, &[])
                .await?,
        );
        stats.moved_rows = stats
            .moved_rows
            .saturating_add(transaction.execute(&table.plan.move_sql, &[]).await?);
        if let Some(update_sql) = &table.plan.update_sql {
            stats.updated_rows = stats
                .updated_rows
                .saturating_add(transaction.execute(update_sql, &[]).await?);
        }
        stats.inserted_rows = stats
            .inserted_rows
            .saturating_add(transaction.execute(&table.plan.insert_sql, &[]).await?);
    }
    Ok(stats)
}

/// Plans a typed temporary staging table and colocated current-state apply SQL.
///
/// `U` upserts a stable key, `D` deletes a stable key, and `M` moves a row to a
/// new primary key. A false presence flag means the source sent
/// `UnchangedToast`; it is valid only when the target can supply the prior value.
/// Move rows are materialized before any old key is released, so chains and
/// swaps do not depend on statement snapshots or temporary unique-key gaps.
pub fn plan_apply(
    source: &TableSchema,
    target: QualifiedName,
    staging_name: &str,
) -> Result<ApplyPlan, ApplyPlanError> {
    let table = plan_create_table(source, target)?;
    let quoted_target = quote_qualified_name(&table.target)?;
    let quoted_staging = quote_identifier(staging_name)?;
    let control_prefix = unused_control_prefix(&table);
    let operation_column = format!("{control_prefix}op");
    let quoted_operation = quote_identifier(&operation_column)?;

    let key_names = &table.primary_key;
    let key_set: std::collections::HashSet<_> = key_names.iter().map(String::as_str).collect();
    let presence_columns: Vec<_> = table
        .columns
        .iter()
        .enumerate()
        .filter(|(_, column)| !key_set.contains(column.name.as_str()))
        .map(|(source_column_index, column)| PresenceColumn {
            source_column_index,
            source_column_name: column.name.clone(),
            staging_column_name: format!("{control_prefix}present_{}", column.source_attnum),
        })
        .collect();
    let old_key_present_column = format!("{control_prefix}old_key_present");
    let old_key_columns: Vec<_> = table
        .primary_key
        .iter()
        .map(|key_name| {
            let (source_column_index, column) = table
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| &column.name == key_name)
                .expect("planned primary key names always reference planned columns");
            OldKeyColumn {
                source_column_index,
                source_column_name: column.name.clone(),
                staging_column_name: format!("{control_prefix}old_{}", column.source_attnum),
                type_sql: column.type_sql.clone(),
            }
        })
        .collect();

    let mut staging_definitions = vec![format!(
        "    {quoted_operation} character(1) NOT NULL CHECK ({quoted_operation} IN ({}, {}, {}))",
        quote_literal("U")?,
        quote_literal("D")?,
        quote_literal("M")?
    )];
    staging_definitions.extend(
        table
            .columns
            .iter()
            .map(|column| {
                Ok(format!(
                    "    {} {}",
                    quote_identifier(&column.name)?,
                    column.type_sql
                ))
            })
            .collect::<Result<Vec<_>, SqlRenderError>>()?,
    );
    staging_definitions.extend(
        presence_columns
            .iter()
            .map(|column| {
                Ok(format!(
                    "    {} boolean NOT NULL",
                    quote_identifier(&column.staging_column_name)?
                ))
            })
            .collect::<Result<Vec<_>, SqlRenderError>>()?,
    );
    staging_definitions.push(format!(
        "    {} boolean NOT NULL",
        quote_identifier(&old_key_present_column)?
    ));
    staging_definitions.extend(
        old_key_columns
            .iter()
            .map(|column| {
                Ok(format!(
                    "    {} {}",
                    quote_identifier(&column.staging_column_name)?,
                    column.type_sql
                ))
            })
            .collect::<Result<Vec<_>, SqlRenderError>>()?,
    );

    let quoted_key = quote_identifier_list(key_names)?;
    let create_staging_sql = format!(
        "CREATE TEMPORARY TABLE {quoted_staging} (\n{}\n)\nUSING heap\nON COMMIT DROP\nDISTRIBUTED BY ({quoted_key})",
        staging_definitions.join(",\n")
    );

    let mut copy_columns = vec![operation_column.clone()];
    copy_columns.extend(table.columns.iter().map(|column| column.name.clone()));
    copy_columns.extend(
        presence_columns
            .iter()
            .map(|column| column.staging_column_name.clone()),
    );
    copy_columns.push(old_key_present_column.clone());
    copy_columns.extend(
        old_key_columns
            .iter()
            .map(|column| column.staging_column_name.clone()),
    );
    let copy_sql = format!(
        "COPY {quoted_staging} ({}) FROM STDIN WITH (FORMAT text, DELIMITER {}, NULL {})",
        quote_identifier_list(&copy_columns)?,
        quote_literal("\t")?,
        quote_literal("\\N")?
    );

    let key_join = equality_join("t", "s", key_names)?;
    let old_key_join = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "t.{} = s.{}",
                quote_identifier(&column.source_column_name)?,
                quote_identifier(&column.staging_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(" AND ");
    let key_is_null = qualified_columns("s", key_names)?
        .into_iter()
        .map(|column| format!("{column} IS NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let target_missing = format!(
        "(t.{} IS NULL)",
        quote_identifier(key_names.first().expect("schema validation requires a PK"))?
    );
    let old_target_missing = format!(
        "(old_t.{} IS NULL)",
        quote_identifier(key_names.first().expect("schema validation requires a PK"))?
    );
    let old_key_present = format!("s.{}", quote_identifier(&old_key_present_column)?);
    let old_key_is_null = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "s.{} IS NULL",
                quote_identifier(&column.staging_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(" OR ");
    let old_key_is_not_null = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "s.{} IS NOT NULL",
                quote_identifier(&column.staging_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(" OR ");
    let same_move_key = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "s.{} = s.{}",
                quote_identifier(&column.source_column_name)?,
                quote_identifier(&column.staging_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(" AND ");
    let all_present = if presence_columns.is_empty() {
        "TRUE".to_owned()
    } else {
        presence_columns
            .iter()
            .map(|column| {
                Ok(format!(
                    "s.{}",
                    quote_identifier(&column.staging_column_name)?
                ))
            })
            .collect::<Result<Vec<_>, SqlRenderError>>()?
            .join(" AND ")
    };
    let grouped_key = qualified_columns("s", key_names)?.join(", ");
    let release_by_delete_join = equality_join("r", "s", key_names)?;
    let release_by_move_join = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "r.{} = s.{}",
                quote_identifier(&column.staging_column_name)?,
                quote_identifier(&column.source_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(" AND ");
    let destination_released = format!(
        "EXISTS (\n                SELECT 1\n                FROM {quoted_staging} AS r\n                WHERE (r.{quoted_operation} = {} AND ({release_by_delete_join}))\n                   OR (r.{quoted_operation} = {} AND ({release_by_move_join}))\n            )",
        quote_literal("D")?,
        quote_literal("M")?,
    );
    let release_delete_projection = qualified_columns("r", key_names)?.join(", ");
    let release_move_projection = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "r.{} AS {}",
                quote_identifier(&column.staging_column_name)?,
                quote_identifier(&column.source_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(", ");
    let grouped_release_key = qualified_columns("released", key_names)?.join(", ");
    let old_validation_join = old_key_columns
        .iter()
        .map(|column| {
            Ok(format!(
                "old_t.{} = s.{}",
                quote_identifier(&column.source_column_name)?,
                quote_identifier(&column.staging_column_name)?
            ))
        })
        .collect::<Result<Vec<_>, SqlRenderError>>()?
        .join(" AND ");
    let validation_sql = format!(
        "SELECT (\n    EXISTS (\n        SELECT 1\n        FROM {quoted_staging} AS s\n        LEFT JOIN {quoted_target} AS t ON {key_join}\n        LEFT JOIN {quoted_target} AS old_t ON {old_validation_join}\n        WHERE s.{quoted_operation} NOT IN ({upsert}, {delete}, {move_op})\n           OR {key_is_null}\n           OR (s.{quoted_operation} IN ({upsert}, {delete}) AND ({old_key_present} OR ({old_key_is_not_null})))\n           OR (s.{quoted_operation} = {move_op} AND (NOT {old_key_present} OR ({old_key_is_null}) OR ({same_move_key})))\n           OR (s.{quoted_operation} = {move_op} AND {old_target_missing})\n           OR (s.{quoted_operation} = {move_op} AND NOT {target_missing} AND NOT ({destination_released}))\n           OR (s.{quoted_operation} = {upsert} AND ({target_missing} OR ({destination_released})) AND NOT ({all_present}))\n    )\n    OR EXISTS (\n        SELECT 1 FROM {quoted_staging} AS s\n        WHERE s.{quoted_operation} IN ({upsert}, {move_op})\n        GROUP BY {grouped_key}\n        HAVING count(*) > 1\n    )\n    OR EXISTS (\n        SELECT 1\n        FROM (\n            SELECT {release_delete_projection}\n            FROM {quoted_staging} AS r\n            WHERE r.{quoted_operation} = {delete}\n            UNION ALL\n            SELECT {release_move_projection}\n            FROM {quoted_staging} AS r\n            WHERE r.{quoted_operation} = {move_op}\n        ) AS released\n        GROUP BY {grouped_release_key}\n        HAVING count(*) > 1\n    )\n) AS invalid_batch",
        upsert = quote_literal("U")?,
        delete = quote_literal("D")?,
        move_op = quote_literal("M")?,
    );

    let delete_sql = format!(
        "DELETE FROM {quoted_target} AS t\nUSING {quoted_staging} AS s\nWHERE s.{quoted_operation} = {} AND {key_join}",
        quote_literal("D")?
    );

    let data_names: Vec<_> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let materialize_moves_sql = if presence_columns.is_empty() {
        None
    } else {
        let assignments = presence_columns
            .iter()
            .map(|presence| {
                let data = quote_identifier(&presence.source_column_name)?;
                let present = quote_identifier(&presence.staging_column_name)?;
                Ok(format!(
                    "    {data} = CASE WHEN s.{present} THEN s.{data} ELSE t.{data} END"
                ))
            })
            .collect::<Result<Vec<_>, SqlRenderError>>()?
            .join(",\n");
        Some(format!(
            "UPDATE {quoted_staging} AS s\nSET\n{assignments}\nFROM {quoted_target} AS t\nWHERE s.{quoted_operation} = {} AND {old_key_join}",
            quote_literal("M")?
        ))
    };
    let delete_moved_sql = format!(
        "DELETE FROM {quoted_target} AS t\nUSING {quoted_staging} AS s\nWHERE s.{quoted_operation} = {} AND {old_key_join}",
        quote_literal("M")?
    );
    let selected_columns = qualified_columns("s", &data_names)?.join(", ");
    let move_sql = format!(
        "INSERT INTO {quoted_target} ({})\nSELECT {selected_columns}\nFROM {quoted_staging} AS s\nWHERE s.{quoted_operation} = {}",
        quote_identifier_list(&data_names)?,
        quote_literal("M")?,
    );

    let update_sql = if presence_columns.is_empty() {
        None
    } else {
        let assignments = presence_columns
            .iter()
            .map(|presence| {
                let data = quote_identifier(&presence.source_column_name)?;
                let present = quote_identifier(&presence.staging_column_name)?;
                Ok(format!(
                    "    {data} = CASE WHEN s.{present} THEN s.{data} ELSE t.{data} END"
                ))
            })
            .collect::<Result<Vec<_>, SqlRenderError>>()?
            .join(",\n");
        Some(format!(
            "UPDATE {quoted_target} AS t\nSET\n{assignments}\nFROM {quoted_staging} AS s\nWHERE s.{quoted_operation} = {} AND {key_join}",
            quote_literal("U")?
        ))
    };

    let insert_sql = format!(
        "INSERT INTO {quoted_target} ({})\nSELECT {selected_columns}\nFROM {quoted_staging} AS s\nWHERE s.{quoted_operation} = {}\n  AND NOT EXISTS (SELECT 1 FROM {quoted_target} AS t WHERE {key_join})",
        quote_identifier_list(&data_names)?,
        quote_literal("U")?
    );

    Ok(ApplyPlan {
        table,
        staging_name: staging_name.to_owned(),
        copy_layout: CopyLayout {
            operation_column,
            data_columns: data_names,
            presence_columns,
            old_key_present_column,
            old_key_columns,
        },
        create_staging_sql,
        copy_sql,
        validation_sql,
        materialize_moves_sql,
        delete_sql,
        delete_moved_sql,
        move_sql,
        update_sql,
        insert_sql,
    })
}

fn unused_control_prefix(table: &CreateTablePlan) -> String {
    for suffix in 0_u32.. {
        let candidate = if suffix == 0 {
            "__pg2cb_".to_owned()
        } else {
            format!("__pg2cb{suffix}_")
        };
        if table
            .columns
            .iter()
            .all(|column| !column.name.starts_with(&candidate))
        {
            return candidate;
        }
    }
    unreachable!("u32 control prefixes cannot all be occupied by a finite table")
}

fn equality_join(left: &str, right: &str, columns: &[String]) -> Result<String, SqlRenderError> {
    columns
        .iter()
        .map(|column| {
            let column = quote_identifier(column)?;
            Ok(format!("{left}.{column} = {right}.{column}"))
        })
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join(" AND "))
}

fn qualified_columns(alias: &str, columns: &[String]) -> Result<Vec<String>, SqlRenderError> {
    columns
        .iter()
        .map(|column| Ok(format!("{alias}.{}", quote_identifier(column)?)))
        .collect()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use cloudberry_etl_core::schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, ReplicaIdentity,
        TableKind,
    };

    use super::*;

    fn column(attnum: i16, name: &str, primary_key_ordinal: Option<u16>) -> ColumnSchema {
        ColumnSchema {
            attnum,
            name: name.to_owned(),
            data_type: PgType {
                oid: 23,
                name: QualifiedName::new("pg_catalog", "int4").unwrap(),
                kind: PgTypeKind::Int4,
            },
            nullable: primary_key_ordinal.is_none(),
            primary_key_ordinal,
            generated: GeneratedColumn::None,
            identity: IdentityColumn::None,
            collation: None,
        }
    }

    fn table(columns: Vec<ColumnSchema>) -> TableSchema {
        TableSchema {
            relation_id: 1,
            generation: 1,
            name: QualifiedName::new("public", "orders").unwrap(),
            kind: TableKind::Ordinary,
            replica_identity: ReplicaIdentity::Default,
            columns,
            distribution_key: Vec::new(),
            partition_key: Vec::new(),
        }
    }

    #[test]
    fn plans_typed_colocated_apply_with_presence_masks() {
        let source = table(vec![
            column(1, "tenant", Some(2)),
            column(2, "id", Some(1)),
            column(3, "payload", None),
        ]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "orders").unwrap(),
            "stage_orders",
        )
        .unwrap();

        assert!(plan.create_staging_sql.contains("USING heap"));
        assert!(
            plan.create_staging_sql
                .ends_with("DISTRIBUTED BY (\"id\", \"tenant\")")
        );
        assert!(plan.copy_sql.starts_with("COPY \"stage_orders\""));
        assert_eq!(plan.copy_layout.presence_columns.len(), 1);
        assert_eq!(
            plan.copy_layout
                .old_key_columns
                .iter()
                .map(|column| column.source_column_name.as_str())
                .collect::<Vec<_>>(),
            ["id", "tenant"]
        );
        assert!(plan.create_staging_sql.contains("IN (E'U', E'D', E'M')"));
        assert!(plan.validation_sql.contains("HAVING count(*) > 1"));
        assert!(
            plan.delete_sql
                .contains("DELETE FROM \"target\".\"orders\"")
        );
        assert!(plan.update_sql.as_ref().unwrap().contains("CASE WHEN"));
        let materialize_moves = plan.materialize_moves_sql.as_ref().unwrap();
        assert!(materialize_moves.contains("UPDATE \"stage_orders\" AS s"));
        assert!(materialize_moves.contains("FROM \"target\".\"orders\" AS t"));
        assert!(materialize_moves.contains("ELSE t.\"payload\" END"));
        assert!(plan.delete_moved_sql.contains("s.\"__pg2cb_op\" = E'M'"));
        assert!(
            plan.move_sql
                .contains("SELECT s.\"tenant\", s.\"id\", s.\"payload\"")
        );
        assert!(!plan.move_sql.contains("JOIN \"target\".\"orders\""));
        assert!(plan.insert_sql.contains("NOT EXISTS"));
        assert!(plan.validation_sql.contains("UNION ALL"));
        assert!(
            plan.validation_sql
                .contains("s.\"__pg2cb_op\" IN (E'U', E'M')")
        );
        assert!(
            plan.validation_sql
                .contains("r.\"__pg2cb_old_2\" = s.\"id\"")
        );
    }

    #[test]
    fn apply_batch_identity_must_name_the_planned_target() {
        let source = table(vec![column(1, "id", Some(1))]);
        let plan = Arc::new(
            plan_apply(
                &source,
                QualifiedName::new("target", "orders").unwrap(),
                "stage_orders",
            )
            .unwrap(),
        );
        let identity = Arc::new(TableApplyIdentity {
            target: QualifiedName::new("target", "other").unwrap(),
            source_relation_id: source.relation_id,
            table_generation: 1,
            schema_fingerprint: "sha256:test".to_owned(),
        });
        assert!(matches!(
            validate_tables(&[TableApplyBatch {
                identity,
                plan,
                rows: Vec::new(),
            }]),
            Err(ApplyError::IdentityTargetMismatch { .. })
        ));
    }

    #[test]
    fn dynamically_avoids_source_column_control_name_collisions() {
        let source = table(vec![
            column(1, "id", Some(1)),
            column(2, "__pg2cb_op", None),
        ]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "orders").unwrap(),
            "stage\"; DROP TABLE x; --",
        )
        .unwrap();
        assert_eq!(plan.copy_layout.operation_column, "__pg2cb1_op");
        assert!(
            plan.create_staging_sql
                .starts_with("CREATE TEMPORARY TABLE \"stage\"\"; DROP TABLE x; --\"")
        );
    }

    #[test]
    fn omits_update_for_a_key_only_table() {
        let source = table(vec![column(1, "id", Some(1))]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "keys").unwrap(),
            "stage_keys",
        )
        .unwrap();
        assert!(plan.update_sql.is_none());
        assert!(plan.materialize_moves_sql.is_none());
        assert!(plan.validation_sql.contains("NOT (TRUE)"));
    }

    #[test]
    fn staging_encoding_materializes_presence_without_losing_null() {
        let source = table(vec![column(1, "id", Some(1)), column(2, "payload", None)]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "orders").unwrap(),
            "stage_orders",
        )
        .unwrap();
        let unchanged = encode_staging_row(
            &plan,
            &StagingRow {
                operation: StageOperation::Upsert,
                cells: vec![Cell::Text(Bytes::from_static(b"1")), Cell::UnchangedToast],
                old_key: None,
            },
        )
        .unwrap();
        let explicit_null = encode_staging_row(
            &plan,
            &StagingRow {
                operation: StageOperation::Upsert,
                cells: vec![Cell::Text(Bytes::from_static(b"1")), Cell::Null],
                old_key: None,
            },
        )
        .unwrap();
        assert_eq!(unchanged, b"U\t1\t\\N\tf\tf\t\\N\n");
        assert_eq!(explicit_null, b"U\t1\t\\N\tt\tf\t\\N\n");
    }

    #[test]
    fn staging_encoding_rejects_missing_key_and_wrong_arity() {
        let source = table(vec![column(1, "id", Some(1)), column(2, "payload", None)]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "orders").unwrap(),
            "stage_orders",
        )
        .unwrap();
        assert!(matches!(
            encode_staging_row(
                &plan,
                &StagingRow {
                    operation: StageOperation::Delete,
                    cells: vec![Cell::Null, Cell::Null],
                    old_key: None,
                }
            ),
            Err(ApplyError::MissingPrimaryKey(_))
        ));
        assert!(matches!(
            encode_staging_row(
                &plan,
                &StagingRow {
                    operation: StageOperation::Delete,
                    cells: vec![Cell::Text(Bytes::from_static(b"1"))],
                    old_key: None,
                }
            ),
            Err(ApplyError::InvalidRowArity { .. })
        ));
    }

    #[test]
    fn staging_encoding_carries_a_typed_old_key_for_moves() {
        let source = table(vec![
            column(1, "tenant", Some(2)),
            column(2, "id", Some(1)),
            column(3, "payload", None),
        ]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "orders").unwrap(),
            "stage_orders",
        )
        .unwrap();
        let encoded = encode_staging_row(
            &plan,
            &StagingRow {
                operation: StageOperation::Move,
                cells: vec![
                    Cell::Text(Bytes::from_static(b"20")),
                    Cell::Text(Bytes::from_static(b"2")),
                    Cell::UnchangedToast,
                ],
                old_key: Some(vec![
                    Cell::Text(Bytes::from_static(b"1")),
                    Cell::Text(Bytes::from_static(b"10")),
                ]),
            },
        )
        .unwrap();
        assert_eq!(encoded, b"M\t20\t2\t\\N\tf\tt\t1\t10\n");
    }

    #[test]
    fn staging_encoding_enforces_move_old_key_ownership_and_arity() {
        let source = table(vec![column(1, "id", Some(1)), column(2, "payload", None)]);
        let plan = plan_apply(
            &source,
            QualifiedName::new("target", "orders").unwrap(),
            "stage_orders",
        )
        .unwrap();
        let cells = vec![Cell::Text(Bytes::from_static(b"2")), Cell::UnchangedToast];

        assert!(matches!(
            encode_staging_row(
                &plan,
                &StagingRow {
                    operation: StageOperation::Move,
                    cells: cells.clone(),
                    old_key: None,
                }
            ),
            Err(ApplyError::MissingOldPrimaryKey)
        ));
        assert!(matches!(
            encode_staging_row(
                &plan,
                &StagingRow {
                    operation: StageOperation::Upsert,
                    cells: cells.clone(),
                    old_key: Some(vec![Cell::Text(Bytes::from_static(b"1"))]),
                }
            ),
            Err(ApplyError::UnexpectedOldPrimaryKey)
        ));
        assert!(matches!(
            encode_staging_row(
                &plan,
                &StagingRow {
                    operation: StageOperation::Move,
                    cells,
                    old_key: Some(Vec::new()),
                }
            ),
            Err(ApplyError::InvalidOldKeyArity { .. })
        ));
    }
}
