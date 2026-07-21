//! Opt-in PostgreSQL 18 integration coverage.
//!
//! Run explicitly with a disposable PG18 instance:
//! `PG2CB_TEST_SOURCE_DSN=postgres://... cargo test -p cloudberry-etl-source-postgres --test postgres18 -- --ignored --nocapture`
//!
//! The test is ignored by default and never removes objects it did not create.

use std::{collections::HashSet, panic::AssertUnwindSafe};

use bytes::Bytes;
use cloudberry_etl_core::{
    change::{DdlMessage, RowChange, SourceTransaction, TransactionChange},
    schema::{GeneratedColumn, PgTypeKind, QualifiedName},
};
use cloudberry_etl_source_postgres::{
    SourceResult,
    catalog::{CatalogOptions, PreflightOptions, load_tables, preflight},
    ddl::{DdlInstallSpec, ensure_ddl_capture},
    publication::{
        PublicationSpec, create_logical_slot, drop_logical_slot, ensure_publication,
        inspect_publication,
    },
    wal::{
        AssembledEvent, DecodedMessage, SourceNodeIdentity, TransactionAssembler,
        TransactionDecoder, parse_logical_payload,
    },
};
use futures::FutureExt;
use tokio_postgres::{Client, NoTls};
use uuid::Uuid;

struct TestObjects {
    business_schema: String,
    other_schema: String,
    metadata_schema: String,
    table_name: String,
    publication_name: String,
    slot_name: String,
    trigger_name: String,
}

impl TestObjects {
    fn new() -> Self {
        let suffix = Uuid::now_v7().simple().to_string();
        Self {
            business_schema: format!("pg2cb_it_s_{suffix}"),
            other_schema: format!("Pg2cb It O {suffix}"),
            metadata_schema: format!("pg2cb_it_m_{suffix}"),
            table_name: format!("items_{suffix}"),
            publication_name: format!("pg2cb_it_pub_{suffix}"),
            slot_name: format!("pg2cb_it_slot_{suffix}"),
            trigger_name: format!("pg2cb_it_ddl_{suffix}"),
        }
    }

    fn table(&self) -> QualifiedName {
        QualifiedName::new(self.business_schema.clone(), self.table_name.clone())
            .expect("generated identifiers are valid")
    }

    fn install_spec(&self) -> DdlInstallSpec {
        DdlInstallSpec {
            metadata_schema: self.metadata_schema.clone(),
            trigger_name: self.trigger_name.clone(),
            allow_citus_worker_guard: true,
        }
    }
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL 18 instance and PG2CB_TEST_SOURCE_DSN"]
async fn postgres18_source_contract_and_binary_pgoutput() -> SourceResult<()> {
    let dsn = std::env::var("PG2CB_TEST_SOURCE_DSN").map_err(|_| {
        cloudberry_etl_source_postgres::SourceError::Contract(
            "PG2CB_TEST_SOURCE_DSN is required for the ignored integration test".to_owned(),
        )
    })?;
    let (client, connection) = tokio_postgres::connect(&dsn, NoTls).await?;
    let connection_task = tokio::spawn(async move {
        if let Err(error) = connection.await {
            eprintln!("integration test PostgreSQL connection ended: {error}");
        }
    });
    let objects = TestObjects::new();
    let result = AssertUnwindSafe(run_test(&client, &objects))
        .catch_unwind()
        .await;
    cleanup(&client, &objects).await;
    connection_task.abort();
    match result {
        Ok(result) => result,
        Err(payload) => std::panic::resume_unwind(payload),
    }
}

async fn run_test(client: &Client, objects: &TestObjects) -> SourceResult<()> {
    let preflight = preflight(client, &PreflightOptions::default()).await?;
    assert_eq!(preflight.identity.server_version_num / 10_000, 18);
    assert_eq!(preflight.server_encoding, "UTF8");
    assert_eq!(preflight.wal_level.to_ascii_lowercase(), "logical");

    // Upgrade the database-level capture before creating test objects. The disposable instance
    // may still contain a UUID-named trigger from an earlier service binary; the canonical
    // installer removes only marker-owned legacy pairs under an advisory transaction lock.
    let canonical_spec = DdlInstallSpec::default();
    let _ = ensure_ddl_capture(client, &canonical_spec).await?;
    let canonical_triggers: i64 = client
        .query_one(
            "SELECT count(*)::int8
               FROM pg_event_trigger
              WHERE evtname IN ($1, $2)
                AND obj_description(oid, 'pg_event_trigger') = $3",
            &[
                &canonical_spec.trigger_name,
                &canonical_spec.drop_trigger_name()?,
                &cloudberry_etl_source_postgres::ddl::DDL_CAPTURE_MARKER,
            ],
        )
        .await?
        .try_get(0)?;
    assert_eq!(canonical_triggers, 2);

    let install = objects.install_spec();
    client
        .batch_execute(&format!(
            "CREATE EVENT TRIGGER {} ON ddl_command_end
             EXECUTE FUNCTION pg2cb_meta.emit_ddl_event()",
            quote_identifier(&objects.trigger_name)
        ))
        .await?;
    let ownership_conflict = ensure_ddl_capture(client, &install).await;
    client
        .batch_execute(&format!(
            "DROP EVENT TRIGGER {}",
            quote_identifier(&objects.trigger_name)
        ))
        .await?;
    assert!(
        ownership_conflict.is_err(),
        "an unmarked event trigger must never be overwritten"
    );

    let qi_schema = quote_identifier(&objects.business_schema);
    let qi_table = quote_identifier(&objects.table_name);
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {qi_schema};
             CREATE TABLE {qi_schema}.{qi_table} (
                 id bigint PRIMARY KEY,
                 payload text NOT NULL,
                 amount numeric(10,2),
                 doubled numeric(12,2)
                     GENERATED ALWAYS AS ((amount * 2)::numeric(12,2)) STORED
             )"
        ))
        .await?;
    assert!(ensure_ddl_capture(client, &install).await?);
    assert!(!ensure_ddl_capture(client, &install).await?);
    let capture_objects: i64 = client
        .query_one(
            "SELECT count(*)::int8
               FROM pg_event_trigger e
               JOIN pg_proc p ON p.oid = e.evtfoid
               JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = $1
                AND obj_description(e.oid, 'pg_event_trigger') = $2
                AND obj_description(p.oid, 'pg_proc') = $2",
            &[
                &objects.metadata_schema,
                &cloudberry_etl_source_postgres::ddl::DDL_CAPTURE_MARKER,
            ],
        )
        .await?
        .try_get(0)?;
    assert_eq!(capture_objects, 2);

    let missing_table = QualifiedName::new(
        objects.business_schema.clone(),
        format!("{}_missing", objects.table_name),
    )
    .expect("generated identifiers are valid");
    let invalid_publication = PublicationSpec::new(
        format!("{}_invalid", objects.publication_name),
        vec![missing_table],
    )?;
    assert!(
        ensure_publication(client, &invalid_publication, None)
            .await
            .is_err()
    );
    let connection_recovered: bool = client
        .query_one(
            "SELECT current_setting('pg2cb.internal_ddl', true) IS DISTINCT FROM 'on'",
            &[],
        )
        .await?
        .try_get(0)?;
    assert!(connection_recovered);

    let table = objects.table();
    let publication = PublicationSpec::new(objects.publication_name.clone(), vec![table.clone()])?;
    let publication_state = ensure_publication(client, &publication, None).await?;
    assert!(publication_state.publish_generated_columns_stored);
    let publication_audits: i64 = client
        .query_one(
            &format!(
                "SELECT count(*)::int8 FROM {}.ddl_audit WHERE command_tag LIKE '%PUBLICATION'",
                quote_identifier(&objects.metadata_schema)
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(publication_audits, 0);
    create_logical_slot(client, &objects.slot_name, "pgoutput").await?;

    // An operator changing the publication must be visible to the source stream. The reconciler
    // below is the service's own DDL and is marked internal, so it repairs the option without
    // hiding the external barrier or emitting a second audit row.
    client
        .batch_execute(&format!(
            "ALTER PUBLICATION {} SET (publish_generated_columns = 'none')",
            quote_identifier(&objects.publication_name)
        ))
        .await?;
    let external_publication_audits: i64 = client
        .query_one(
            &format!(
                "SELECT count(*)::int8 FROM {}.ddl_audit
                  WHERE command_tag = 'ALTER PUBLICATION'",
                quote_identifier(&objects.metadata_schema)
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(external_publication_audits, 1);
    let altered_state = inspect_publication(client, &objects.publication_name)
        .await?
        .expect("publication must still exist");
    assert!(!altered_state.publish_generated_columns_stored);
    let repaired_state = ensure_publication(client, &publication, None).await?;
    assert!(repaired_state.publish_generated_columns_stored);
    let publication_audits_after_repair: i64 = client
        .query_one(
            &format!(
                "SELECT count(*)::int8 FROM {}.ddl_audit
                  WHERE command_tag = 'ALTER PUBLICATION'",
                quote_identifier(&objects.metadata_schema)
            ),
            &[],
        )
        .await?
        .try_get(0)?;
    assert_eq!(publication_audits_after_repair, 1);

    let catalog = load_tables(
        client,
        &CatalogOptions {
            metadata_schema: objects.metadata_schema.clone(),
            include_schemas: Some([objects.business_schema.clone()].into_iter().collect()),
            ..CatalogOptions::default()
        },
    )
    .await?;
    let table_schema = catalog
        .iter()
        .find(|candidate| candidate.name == table)
        .expect("created test table must be discoverable");
    assert_eq!(table_schema.primary_key().len(), 1);
    assert_eq!(table_schema.columns[0].primary_key_ordinal, Some(1));
    assert_eq!(table_schema.columns[3].name, "doubled");
    assert_eq!(table_schema.columns[3].generated, GeneratedColumn::Stored);
    assert!(matches!(
        table_schema.columns[0].data_type.kind,
        PgTypeKind::Int8
    ));
    assert!(matches!(
        table_schema.columns[1].data_type.kind,
        PgTypeKind::Text
    ));
    assert!(matches!(
        table_schema.columns[2].data_type.kind,
        PgTypeKind::Numeric { .. }
    ));
    assert!(matches!(
        table_schema.columns[3].data_type.kind,
        PgTypeKind::Numeric { .. }
    ));

    let table_sql = format!("{qi_schema}.{qi_table}");
    // A text parameter plus an explicit cast keeps the test independent of a decimal client
    // feature while PostgreSQL still performs the numeric conversion.
    client
        .execute(
            &format!(
                "INSERT INTO {table_sql}(id, payload, amount) VALUES ($1, $2, $3::text::numeric)"
            ),
            &[&1_i64, &"before", &"10.25"],
        )
        .await?;
    client
        .execute(
            &format!("UPDATE {table_sql} SET payload = $2 WHERE id = $1"),
            &[&1_i64, &"after"],
        )
        .await?;
    client
        .execute(&format!("DELETE FROM {table_sql} WHERE id = $1"), &[&1_i64])
        .await?;

    // This ALTER is emitted by the installed event trigger as a transactional logical message.
    client
        .batch_execute(&format!("ALTER TABLE {table_sql} ADD COLUMN note text"))
        .await?;

    // DROP events are emitted by sql_drop because the dropped catalog rows are no longer
    // available to ddl_command_end. The schema is outside the selected publication but is kept
    // in the payload so the engine can ignore it without rebuilding the included scope.
    let other_schema = quote_identifier(&objects.other_schema);
    let other_type = format!("{other_schema}.drop_kind");
    let other_table = format!("{other_schema}.drop_items");
    client
        .batch_execute(&format!(
            "CREATE SCHEMA {other_schema};
             CREATE TYPE {other_type} AS ENUM ('a', 'b');
             CREATE TABLE {other_table} (id bigint PRIMARY KEY);
             DROP TABLE {other_table};
             DROP TYPE {other_type};
             DROP SCHEMA {other_schema}"
        ))
        .await?;

    let rows = client
        .query(
            "SELECT lsn::text, xid::text, data
               FROM pg_logical_slot_get_binary_changes(
                    $1, NULL, NULL,
                    'proto_version', '1',
                    'publication_names', $2,
                    'messages', 'true')",
            &[&objects.slot_name, &objects.publication_name],
        )
        .await?;
    assert!(!rows.is_empty(), "logical slot returned no changes");

    let mut decoder = TransactionDecoder::new();
    let identity = SourceNodeIdentity {
        node_id: 0,
        system_identifier: preflight.identity.system_identifier,
        timeline: preflight.identity.timeline,
    };
    let mut assembler = TransactionAssembler::new(identity);
    let mut transactions = Vec::<SourceTransaction>::new();
    let mut saw_insert = false;
    let mut saw_update = false;
    let mut saw_delete = false;
    let mut saw_ddl = false;
    let mut ddl_tags = HashSet::new();
    let mut ddl_messages = Vec::<DdlMessage>::new();
    let mut generated_value = None;
    let mut relation_ids = HashSet::new();

    for row in rows {
        let payload: Vec<u8> = row.try_get("data")?;
        let wire = parse_logical_payload(Bytes::from(payload))?;
        let decoded = decoder.decode(wire)?;
        if let DecodedMessage::Relation(relation) = &decoded {
            relation_ids.insert(relation.relation_id);
        }
        match &decoded {
            DecodedMessage::Ddl(message) => {
                saw_ddl = true;
                ddl_tags.insert(message.command_tag.clone());
                ddl_messages.push(message.clone());
            }
            DecodedMessage::Insert { new, .. } => {
                assert_eq!(new.cells.len(), 4);
                generated_value = new.cells.get(3).cloned();
                saw_insert = true;
            }
            DecodedMessage::Update { .. } => saw_update = true,
            DecodedMessage::Delete { .. } => saw_delete = true,
            _ => {}
        }
        if let Some(event) = assembler.push(decoded)?
            && let AssembledEvent::Transaction(committed) = event
        {
            assert_eq!(
                committed.transaction.final_position.system_identifier,
                identity.system_identifier
            );
            assert_eq!(
                committed.transaction.final_position.timeline,
                identity.timeline
            );
            transactions.push(committed.transaction);
        }
    }
    assembler.finish()?;
    assert!(!relation_ids.is_empty(), "no Relation message was decoded");
    assert!(
        saw_insert && saw_update && saw_delete,
        "missing row operation in pgoutput"
    );
    assert!(saw_ddl, "DDL event trigger message was not decoded");
    assert!(ddl_tags.contains("ALTER PUBLICATION"));
    assert!(ddl_tags.contains("ALTER TABLE"));
    let publication_ddl = ddl_messages
        .iter()
        .find(|message| message.command_tag == "ALTER PUBLICATION")
        .expect("external publication DDL must be captured");
    assert!(publication_ddl.affected_schemas.is_empty());
    for command_tag in [
        "CREATE SCHEMA",
        "CREATE TYPE",
        "CREATE TABLE",
        "DROP TABLE",
        "DROP TYPE",
        "DROP SCHEMA",
    ] {
        let audit = client
            .query_one(
                &format!(
                    "SELECT count(*)::int8,
                            COALESCE(bool_and(payload->'affected_schemas' =
                                jsonb_build_array($2::text)), false)
                       FROM {}.ddl_audit
                      WHERE command_tag = $1",
                    quote_identifier(&objects.metadata_schema)
                ),
                &[&command_tag, &objects.other_schema],
            )
            .await?;
        let audit_count: i64 = audit.try_get(0)?;
        let audit_scope_matches: bool = audit.try_get(1)?;
        assert_eq!(audit_count, 1, "{command_tag} must be captured once");
        assert!(
            audit_scope_matches,
            "unexpected audit scope for {command_tag}"
        );

        // Event triggers are database-wide. Other independently managed capture installations
        // can therefore add equivalent logical messages to this disposable slot; the audit table
        // above identifies and verifies the installation owned by this test.
        let message = ddl_messages
            .iter()
            .find(|message| {
                message.command_tag == command_tag
                    && message.affected_schemas.as_slice()
                        == std::slice::from_ref(&objects.other_schema)
            })
            .unwrap_or_else(|| panic!("missing {command_tag} DDL message"));
        assert_eq!(
            message.affected_schemas.as_slice(),
            std::slice::from_ref(&objects.other_schema),
            "unexpected scope for {command_tag}: {message:?}"
        );
    }
    assert!(matches!(
        generated_value,
        Some(cloudberry_etl_core::change::Cell::Text(value))
            if value.as_ref() == b"20.50"
    ));
    assert!(
        !transactions.is_empty(),
        "no committed transaction was assembled"
    );
    assert!(transactions.iter().any(|transaction| {
        transaction.changes.iter().any(|change| {
            matches!(change, TransactionChange::Row(row) if matches!(
                &row.change,
                RowChange::Insert { .. } | RowChange::Update { .. } | RowChange::Delete { .. }
            ))
        })
    }));
    Ok(())
}

async fn cleanup(client: &Client, objects: &TestObjects) {
    // Every statement is scoped to names generated by TestObjects. Best-effort cleanup is used so
    // a setup failure does not hide its original error; no broad schema/database operation occurs.
    let _ = drop_logical_slot(client, &objects.slot_name).await;
    let _ = client
        .batch_execute(&format!(
            "DROP PUBLICATION IF EXISTS {}",
            quote_identifier(&objects.publication_name)
        ))
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP EVENT TRIGGER IF EXISTS {}; DROP EVENT TRIGGER IF EXISTS {}",
            quote_identifier(&format!("{}_drop", objects.trigger_name)),
            quote_identifier(&objects.trigger_name)
        ))
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&objects.business_schema)
        ))
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&objects.other_schema)
        ))
        .await;
    let _ = client
        .batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            quote_identifier(&objects.metadata_schema)
        ))
        .await;
}

fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
