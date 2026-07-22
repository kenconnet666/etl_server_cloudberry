//! Opt-in target canonical-reader coverage against Apache Cloudberry 2.1.

use std::error::Error;

use cloudberry_etl_core::{
    id::PipelineId,
    lsn::PgLsn,
    schema::{
        ColumnSchema, GeneratedColumn, IdentityColumn, PgType, PgTypeKind, QualifiedName,
        ReplicaIdentity, TableKind, TableSchema,
    },
};
use cloudberry_etl_target_cloudberry::{
    checkpoint::{
        CheckpointKey, NodeCheckpoint, PipelineFence, activate_pipeline_fence,
        advance_node_checkpoint,
    },
    managed::ManagedTableError,
    migration::migrate_target_database,
    reconciliation::{
        ReconciliationRunIdentity, begin_reconciliation, mark_reconciliation_scanning,
    },
    reconciliation_reader::{ReconciliationReaderError, begin_reconciliation_reader},
    schema::{CreateTablePlan, UserTypeDefinition, plan_create_table_with_storage},
    storage::TargetStorage,
};
use futures::TryStreamExt;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

const FINGERPRINT: &str = "sha256:target-reader-cloudberry21-v1";

#[tokio::test]
#[ignore = "requires Apache Cloudberry 2.1 and PG2CB_TEST_TARGET_DSN"]
async fn cloudberry21_reader_validates_aoco_and_rejects_external_drift()
-> Result<(), Box<dyn Error>> {
    let dsn = std::env::var("PG2CB_TEST_TARGET_DSN")?;
    let (mut client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("reconciliation reader integration connection ended: {error}");
        }
    });
    let suffix = Uuid::now_v7().simple().to_string();
    let target_schema = format!("pg2cb_reader_{}", &suffix[..12]);
    let pipeline_id = PipelineId::new();
    let result = run_test(&mut client, &target_schema, pipeline_id).await;
    cleanup(&client, &target_schema, pipeline_id).await;
    connection_task.abort();
    result
}

async fn run_test(
    client: &mut Client,
    target_schema: &str,
    pipeline_id: PipelineId,
) -> Result<(), Box<dyn Error>> {
    migrate_target_database(client).await?;
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {}",
            quote_identifier(target_schema)
        ))
        .await?;
    let schema = source_schema();
    let target = QualifiedName::new(target_schema, "orders")?;
    let plan = plan_create_table_with_storage(&schema, target, TargetStorage::AoColumn)?;
    client.batch_execute(&plan.create_sql).await?;
    client
        .batch_execute(&format!(
            "INSERT INTO {} (payload, sequence, tenant, note) VALUES ('first', 2, 1, NULL), ('second', 1, 2, 'n')",
            qualified(&plan.target)
        ))
        .await?;
    let fence = PipelineFence {
        pipeline_id,
        topology_generation: schema.generation,
        fencing_token: 1,
    };
    activate_pipeline_fence(client, fence).await?;
    let relation_oid = register_managed(client, fence, &schema, &plan).await?;
    let run = run_identity(fence, &schema, &plan, relation_oid);
    assert!(matches!(
        begin_reconciliation_reader(client, &run, &schema, &plan).await,
        Err(ReconciliationReaderError::Alignment(_))
    ));
    prepare_alignment(client, &run, PgLsn::new(0xff)).await?;

    let mut reader = begin_reconciliation_reader(client, &run, &schema, &plan).await?;
    assert_eq!(reader.relation_oid(), relation_oid as i64);
    let mut output = Vec::new();
    let mut stream = reader.copy_text().await?;
    while let Some(chunk) = stream.try_next().await? {
        output.extend_from_slice(&chunk);
    }
    drop(stream);
    reader.commit().await?;
    let mut rows = String::from_utf8(output)?
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    rows.sort();
    assert_eq!(rows, ["1\t2\tfirst\t\\N", "2\t1\tsecond\tn"]);

    client
        .batch_execute(&format!(
            "ALTER TABLE {} ALTER COLUMN payload TYPE varchar(64)",
            qualified(&plan.target)
        ))
        .await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &run, &schema, &plan).await,
        Err(ReconciliationReaderError::CatalogMismatch { .. })
    ));
    client
        .batch_execute(&format!(
            "ALTER TABLE {} ALTER COLUMN payload TYPE text; ALTER TABLE {} RENAME COLUMN note TO external_note",
            qualified(&plan.target),
            qualified(&plan.target)
        ))
        .await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &run, &schema, &plan).await,
        Err(ReconciliationReaderError::CatalogMismatch { .. })
    ));
    client
        .batch_execute(&format!(
            "ALTER TABLE {} RENAME COLUMN external_note TO note",
            qualified(&plan.target)
        ))
        .await?;

    client
        .execute(
            "UPDATE pg2cb_meta.managed_tables SET relation_oid = relation_oid + 1 WHERE pipeline_id = $1 AND target_schema = $2 AND target_table = $3",
            &[&pipeline_id.as_uuid(), &plan.target.schema, &plan.target.name],
        )
        .await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &run, &schema, &plan).await,
        Err(ReconciliationReaderError::ManagedTable(
            ManagedTableError::RelationIdentityMismatch { .. }
        ))
    ));
    client
        .execute(
            "UPDATE pg2cb_meta.managed_tables SET relation_oid = $1 WHERE pipeline_id = $2 AND target_schema = $3 AND target_table = $4",
            &[&i64::from(relation_oid), &pipeline_id.as_uuid(), &plan.target.schema, &plan.target.name],
        )
        .await?;

    let mut heap_schema = schema.clone();
    heap_schema.relation_id = 43;
    let heap_target = QualifiedName::new(target_schema, "orders_heap")?;
    let heap_plan =
        plan_create_table_with_storage(&heap_schema, heap_target, TargetStorage::AoColumn)?;
    client
        .batch_execute(&format!(
            "CREATE TABLE {} (payload text, sequence bigint NOT NULL, tenant integer NOT NULL, note text, PRIMARY KEY (tenant, sequence)) USING heap DISTRIBUTED BY (tenant, sequence)",
            qualified(&heap_plan.target)
        ))
        .await?;
    let heap_oid = register_managed(client, fence, &heap_schema, &heap_plan).await?;
    let heap_run = run_identity(fence, &heap_schema, &heap_plan, heap_oid);
    prepare_alignment(client, &heap_run, PgLsn::new(0xff)).await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &heap_run, &heap_schema, &heap_plan).await,
        Err(ReconciliationReaderError::CatalogMismatch { .. })
    ));

    let typed_schema = user_type_schema();
    let typed_target = QualifiedName::new(target_schema, "typed_orders")?;
    let typed_plan =
        plan_create_table_with_storage(&typed_schema, typed_target, TargetStorage::AoColumn)?;
    for prerequisite in &typed_plan.prerequisites {
        client.batch_execute(&prerequisite.create_sql).await?;
    }
    register_managed_types(client, fence, &typed_plan).await?;
    client.batch_execute(&typed_plan.create_sql).await?;
    client
        .batch_execute(&format!(
            "INSERT INTO {} (id, status, code) VALUES (1, 'new', 'A-1')",
            qualified(&typed_plan.target)
        ))
        .await?;
    let typed_oid = register_managed(client, fence, &typed_schema, &typed_plan).await?;
    let typed_run = run_identity(fence, &typed_schema, &typed_plan, typed_oid);
    prepare_alignment(client, &typed_run, PgLsn::new(0xff)).await?;
    begin_reconciliation_reader(client, &typed_run, &typed_schema, &typed_plan)
        .await?
        .commit()
        .await?;

    let domain = typed_plan
        .prerequisites
        .iter()
        .find(|prerequisite| matches!(prerequisite.definition, UserTypeDefinition::Domain { .. }))
        .expect("typed fixture has a domain");
    client
        .batch_execute(&format!(
            "ALTER DOMAIN {} ADD CONSTRAINT external_check CHECK (VALUE <> '')",
            qualified(&domain.name)
        ))
        .await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &typed_run, &typed_schema, &typed_plan).await,
        Err(ReconciliationReaderError::CatalogMismatch { .. })
    ));
    client
        .batch_execute(&format!(
            "ALTER DOMAIN {} DROP CONSTRAINT external_check",
            qualified(&domain.name)
        ))
        .await?;

    let enumeration = typed_plan
        .prerequisites
        .iter()
        .find(|prerequisite| matches!(prerequisite.definition, UserTypeDefinition::Enum { .. }))
        .expect("typed fixture has an enum");
    client
        .batch_execute(&format!(
            "ALTER TYPE {} ADD VALUE 'external'",
            qualified(&enumeration.name)
        ))
        .await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &typed_run, &typed_schema, &typed_plan).await,
        Err(ReconciliationReaderError::CatalogMismatch { .. })
    ));

    advance_checkpoint(client, &run, PgLsn::new(0x100)).await?;
    assert!(matches!(
        begin_reconciliation_reader(client, &run, &schema, &plan).await,
        Err(ReconciliationReaderError::Alignment(_))
    ));
    Ok(())
}

async fn prepare_alignment(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    checkpoint_lsn: PgLsn,
) -> Result<(), Box<dyn Error>> {
    advance_checkpoint(client, run, checkpoint_lsn).await?;
    begin_reconciliation(client, run).await?;
    mark_reconciliation_scanning(client, run, checkpoint_lsn).await?;
    Ok(())
}

async fn advance_checkpoint(
    client: &mut Client,
    run: &ReconciliationRunIdentity,
    checkpoint_lsn: PgLsn,
) -> Result<(), Box<dyn Error>> {
    let transaction = client.transaction().await?;
    advance_node_checkpoint(
        &transaction,
        run.fence,
        &NodeCheckpoint {
            key: CheckpointKey {
                pipeline_id: run.fence.pipeline_id,
                topology_generation: run.fence.topology_generation,
                node_id: run.source_node_id,
            },
            system_identifier: run.source_system_identifier,
            timeline: run.source_timeline,
            slot_name: "pg2cb_reader_main".to_owned(),
            applied_lsn: checkpoint_lsn,
        },
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

async fn register_managed_types(
    client: &Client,
    fence: PipelineFence,
    plan: &CreateTablePlan,
) -> Result<(), Box<dyn Error>> {
    for prerequisite in &plan.prerequisites {
        let checksum = Sha256::digest(prerequisite.create_sql.as_bytes()).to_vec();
        client
            .execute(
                "INSERT INTO pg2cb_meta.managed_types (type_schema, type_name, pipeline_id, definition_checksum, fencing_token) VALUES ($1, $2, $3, $4, $5)",
                &[
                    &prerequisite.name.schema,
                    &prerequisite.name.name,
                    &fence.pipeline_id.as_uuid(),
                    &checksum,
                    &fence.fencing_token,
                ],
            )
            .await?;
    }
    Ok(())
}

async fn register_managed(
    client: &Client,
    fence: PipelineFence,
    schema: &TableSchema,
    plan: &CreateTablePlan,
) -> Result<u32, Box<dyn Error>> {
    let relation_oid: i64 = client
        .query_one(
            "SELECT c.oid::bigint FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relname = $2",
            &[&plan.target.schema, &plan.target.name],
        )
        .await?
        .get(0);
    client
        .execute(
            "INSERT INTO pg2cb_meta.managed_tables (target_schema, target_table, pipeline_id, relation_oid, source_relation_id, table_generation, schema_fingerprint, state, fencing_token) VALUES ($1, $2, $3, $4, $5, $6, $7, 'active', $8)",
            &[
                &plan.target.schema,
                &plan.target.name,
                &fence.pipeline_id.as_uuid(),
                &relation_oid,
                &i64::from(schema.relation_id),
                &i64::try_from(schema.generation)?,
                &FINGERPRINT,
                &fence.fencing_token,
            ],
        )
        .await?;
    Ok(u32::try_from(relation_oid)?)
}

fn run_identity(
    fence: PipelineFence,
    schema: &TableSchema,
    plan: &CreateTablePlan,
    relation_oid: u32,
) -> ReconciliationRunIdentity {
    ReconciliationRunIdentity {
        fence,
        source_relation_id: schema.relation_id,
        target: plan.target.clone(),
        target_relation_oid: relation_oid,
        table_generation: schema.generation,
        schema_fingerprint: FINGERPRINT.to_owned(),
        run_id: Uuid::now_v7(),
        source_node_id: 0,
        temporary_slot_name: format!("pg2cb_reconcile_{}", Uuid::new_v4().simple()),
        source_system_identifier: 123,
        source_timeline: 1,
        source_snapshot_lsn: PgLsn::new(0x100),
    }
}

fn source_schema() -> TableSchema {
    TableSchema {
        relation_id: 42,
        generation: 1,
        name: QualifiedName::new("public", "orders").unwrap(),
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

fn user_type_schema() -> TableSchema {
    TableSchema {
        relation_id: 44,
        generation: 1,
        name: QualifiedName::new("public", "typed_orders").unwrap(),
        kind: TableKind::Ordinary,
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            column(1, "id", 20, "int8", PgTypeKind::Int8, Some(1)),
            ColumnSchema {
                attnum: 2,
                name: "status".to_owned(),
                data_type: PgType {
                    oid: 90_001,
                    name: QualifiedName::new("source_types", "order_status").unwrap(),
                    kind: PgTypeKind::Enum {
                        labels: vec!["new".to_owned(), "done".to_owned()],
                    },
                },
                nullable: true,
                primary_key_ordinal: None,
                generated: GeneratedColumn::None,
                identity: IdentityColumn::None,
                collation: None,
            },
            ColumnSchema {
                attnum: 3,
                name: "code".to_owned(),
                data_type: PgType {
                    oid: 90_002,
                    name: QualifiedName::new("source_types", "order_code").unwrap(),
                    kind: PgTypeKind::Domain {
                        base: Box::new(PgType {
                            oid: 1043,
                            name: QualifiedName::new("pg_catalog", "varchar").unwrap(),
                            kind: PgTypeKind::VarChar { length: Some(16) },
                        }),
                        constraints: Vec::new(),
                    },
                },
                nullable: true,
                primary_key_ordinal: None,
                generated: GeneratedColumn::None,
                identity: IdentityColumn::None,
                collation: None,
            },
        ],
        distribution_key: Vec::new(),
        partition_key: Vec::new(),
    }
}

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

async fn cleanup(client: &Client, target_schema: &str, pipeline_id: PipelineId) {
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.table_reconciliation_state WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.node_checkpoints WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.managed_types WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.managed_tables WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await;
    let _ = client
        .execute(
            "DELETE FROM pg2cb_meta.pipeline_state WHERE pipeline_id = $1",
            &[&pipeline_id.as_uuid()],
        )
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(target_schema)
        ))
        .await;
}

fn qualified(name: &QualifiedName) -> String {
    format!(
        "{}.{}",
        quote_identifier(&name.schema),
        quote_identifier(&name.name)
    )
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
