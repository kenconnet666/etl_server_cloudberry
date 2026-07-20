//! Transactional staging apply and durable checkpoint advancement.

use std::collections::HashSet;

use bytes::Bytes;
use cloudberry_etl_core::{
    change::Cell,
    schema::{QualifiedName, TableSchema},
};
use futures::SinkExt;
use thiserror::Error;
use tokio_postgres::Client;

use crate::{
    checkpoint::{
        AdvanceOutcome, CheckpointError, NodeCheckpoint, PipelineFence, advance_node_checkpoint,
        lock_pipeline_fence,
    },
    copy::{CopyEncodeError, encode_row},
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
    Copy(#[from] CopyEncodeError),
    #[error("staging row has {actual} data cells, expected {expected}")]
    InvalidRowArity { expected: usize, actual: usize },
    #[error("primary-key column `{0}` is NULL or unchanged")]
    MissingPrimaryKey(String),
    #[error("staging name `{0}` is repeated in one target transaction")]
    DuplicateStagingName(String),
    #[error("Cloudberry rejected an invalid or non-collapsed staging batch for {0}")]
    InvalidBatch(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresenceColumn {
    pub source_column_index: usize,
    pub source_column_name: String,
    pub staging_column_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyLayout {
    pub operation_column: String,
    pub data_columns: Vec<String>,
    pub presence_columns: Vec<PresenceColumn>,
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
    pub delete_sql: String,
    pub update_sql: Option<String>,
    pub insert_sql: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageOperation {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagingRow {
    pub operation: StageOperation,
    pub cells: Vec<Cell>,
}

#[derive(Debug, Clone)]
pub struct TableApplyBatch {
    pub plan: ApplyPlan,
    pub rows: Vec<StagingRow>,
}

#[derive(Debug, Clone)]
pub struct ApplyRequest {
    pub fence: PipelineFence,
    pub checkpoint: NodeCheckpoint,
    pub tables: Vec<TableApplyBatch>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub staged_rows: u64,
    pub deleted_rows: u64,
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
    lock_pipeline_fence(&transaction, request.fence).await?;

    let mut outcome = ApplyOutcome {
        staged_rows: 0,
        deleted_rows: 0,
        updated_rows: 0,
        inserted_rows: 0,
        checkpoint: AdvanceOutcome::Unchanged,
    };

    for table in &request.tables {
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
        outcome.staged_rows = outcome.staged_rows.saturating_add(copied);

        let invalid: bool = transaction
            .query_one(&table.plan.validation_sql, &[])
            .await?
            .try_get("invalid_batch")?;
        if invalid {
            return Err(ApplyError::InvalidBatch(
                table.plan.table.target.to_string(),
            ));
        }
        outcome.deleted_rows = outcome
            .deleted_rows
            .saturating_add(transaction.execute(&table.plan.delete_sql, &[]).await?);
        if let Some(update_sql) = &table.plan.update_sql {
            outcome.updated_rows = outcome
                .updated_rows
                .saturating_add(transaction.execute(update_sql, &[]).await?);
        }
        outcome.inserted_rows = outcome
            .inserted_rows
            .saturating_add(transaction.execute(&table.plan.insert_sql, &[]).await?);
    }

    outcome.checkpoint =
        advance_node_checkpoint(&transaction, request.fence, &request.checkpoint).await?;
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

    let mut fields = Vec::with_capacity(
        1 + plan.copy_layout.data_columns.len() + plan.copy_layout.presence_columns.len(),
    );
    fields.push(Cell::Text(Bytes::from_static(match row.operation {
        StageOperation::Upsert => b"U",
        StageOperation::Delete => b"D",
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
    encode_row(&fields).map_err(ApplyError::from)
}

fn validate_request(request: &ApplyRequest) -> Result<(), ApplyError> {
    let mut staging_names = HashSet::new();
    for table in &request.tables {
        if !staging_names.insert(&table.plan.staging_name) {
            return Err(ApplyError::DuplicateStagingName(
                table.plan.staging_name.clone(),
            ));
        }
    }
    Ok(())
}

/// Plans a typed temporary staging table and colocated delete/update/insert SQL.
///
/// The stage operation is `U` for upsert or `D` for delete. PK changes must be
/// collapsed upstream into `D(old key)` plus `U(new row)`. A false presence flag
/// means the source sent `UnchangedToast`; it is valid only when the target row
/// already exists.
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

    let mut staging_definitions = vec![format!(
        "    {quoted_operation} character(1) NOT NULL CHECK ({quoted_operation} IN ({}, {}))",
        quote_literal("U")?,
        quote_literal("D")?
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
    let copy_sql = format!(
        "COPY {quoted_staging} ({}) FROM STDIN WITH (FORMAT text, DELIMITER {}, NULL {})",
        quote_identifier_list(&copy_columns)?,
        quote_literal("\t")?,
        quote_literal("\\N")?
    );

    let key_join = equality_join("t", "s", key_names)?;
    let key_is_null = qualified_columns("s", key_names)?
        .into_iter()
        .map(|column| format!("{column} IS NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let target_missing = format!(
        "t.{} IS NULL",
        quote_identifier(key_names.first().expect("schema validation requires a PK"))?
    );
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
    let validation_sql = format!(
        "SELECT (\n    EXISTS (\n        SELECT 1\n        FROM {quoted_staging} AS s\n        LEFT JOIN {quoted_target} AS t ON {key_join}\n        WHERE s.{quoted_operation} NOT IN ({upsert}, {delete})\n           OR {key_is_null}\n           OR (s.{quoted_operation} = {upsert} AND {target_missing} AND NOT ({all_present}))\n    )\n    OR EXISTS (\n        SELECT 1 FROM {quoted_staging} AS s\n        GROUP BY {grouped_key}\n        HAVING count(*) > 1\n    )\n) AS invalid_batch",
        upsert = quote_literal("U")?,
        delete = quote_literal("D")?,
    );

    let delete_sql = format!(
        "DELETE FROM {quoted_target} AS t\nUSING {quoted_staging} AS s\nWHERE s.{quoted_operation} = {} AND {key_join}",
        quote_literal("D")?
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

    let data_names: Vec<_> = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect();
    let selected_columns = qualified_columns("s", &data_names)?.join(", ");
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
        },
        create_staging_sql,
        copy_sql,
        validation_sql,
        delete_sql,
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
        assert!(plan.validation_sql.contains("HAVING count(*) > 1"));
        assert!(
            plan.delete_sql
                .contains("DELETE FROM \"target\".\"orders\"")
        );
        assert!(plan.update_sql.as_ref().unwrap().contains("CASE WHEN"));
        assert!(plan.insert_sql.contains("NOT EXISTS"));
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
            },
        )
        .unwrap();
        let explicit_null = encode_staging_row(
            &plan,
            &StagingRow {
                operation: StageOperation::Upsert,
                cells: vec![Cell::Text(Bytes::from_static(b"1")), Cell::Null],
            },
        )
        .unwrap();
        assert_eq!(unchanged, b"U\t1\t\\N\tf\n");
        assert_eq!(explicit_null, b"U\t1\t\\N\tt\n");
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
                }
            ),
            Err(ApplyError::InvalidRowArity { .. })
        ));
    }
}
