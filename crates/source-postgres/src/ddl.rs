//! Transactional DDL notifications emitted from the source metadata schema.
//!
//! Event-trigger APIs provide catalog-shaped facts without parsing `current_query()`. The
//! command-end trigger covers ordinary DDL, while the sql-drop trigger covers objects that no
//! longer exist by the time the command completes.

use std::collections::{BTreeMap, HashSet};

use cloudberry_etl_core::{
    change::{DdlMessage, RelationSchemaSnapshot, TableTransition, TransitionKind},
    schema::validate_identifier,
};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, GenericClient};

use crate::{
    SourceError, SourceResult,
    sql::{quote_identifier, quote_literal},
};

pub const DDL_MESSAGE_PREFIX: &str = "pg2cloudberry_ddl_v2";
pub const DDL_MESSAGE_VERSION: u16 = 2;
const LEGACY_DDL_MESSAGE_PREFIX: &str = "pg2cloudberry_ddl_v1";
const LEGACY_DDL_MESSAGE_VERSION: u16 = 1;
/// Marker stored on both event triggers and their functions. Bumped to v6 when the command-end
/// trigger began emitting typed per-relation after-schema snapshots in the v2 envelope.
pub const DDL_CAPTURE_MARKER: &str = "pg2cloudberry_ddl_capture_v6";
pub const CANONICAL_DDL_TRIGGER_NAME: &str = "pg2cb_ddl_command_end";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentRelationSchema {
    pub fingerprint: String,
    pub schema: RelationSchemaSnapshot,
}

#[derive(Debug, Clone)]
pub struct DdlInstallSpec {
    pub metadata_schema: String,
    pub trigger_name: String,
    pub allow_citus_worker_guard: bool,
}

#[derive(Debug)]
struct DdlFunctionSources {
    schema_snapshot: String,
    schema_fingerprint: String,
    emit_ddl_event: String,
    emit_sql_drop_event: String,
}

impl Default for DdlInstallSpec {
    fn default() -> Self {
        Self {
            metadata_schema: "pg2cb_meta".to_owned(),
            trigger_name: CANONICAL_DDL_TRIGGER_NAME.to_owned(),
            allow_citus_worker_guard: true,
        }
    }
}

impl DdlInstallSpec {
    pub fn validate(&self) -> SourceResult<()> {
        validate_schema(&self.metadata_schema)?;
        validate_schema(&self.trigger_name)?;
        self.drop_trigger_name().map(|_| ())
    }

    /// The companion trigger name is deterministic so a reconnect can validate both objects.
    pub fn drop_trigger_name(&self) -> SourceResult<String> {
        let name = format!("{}_drop", self.trigger_name);
        validate_schema(&name)?;
        Ok(name)
    }

    fn worker_guard_sql(&self) -> &'static str {
        if self.allow_citus_worker_guard {
            "        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus')
           AND to_regclass('pg_catalog.pg_dist_node') IS NULL THEN
            RETURN;
        END IF;"
        } else {
            "        NULL;"
        }
    }

    /// Keep the exact function source beside the installer. The current-state check compares
    /// pg_proc.prosrc with these strings, so a stale body cannot pass on its marker alone.
    fn function_sources(&self) -> SourceResult<DdlFunctionSources> {
        self.validate()?;
        let schema = quote_identifier(&self.metadata_schema)?;
        let snapshot = quote_identifier("schema_snapshot")?;
        let worker_guard = self.worker_guard_sql();

        let schema_snapshot = r#"
    SELECT jsonb_build_object(
        'relation_id', c.oid::bigint,
        'name', jsonb_build_object('schema', n.nspname, 'name', c.relname),
        'relation_kind', c.relkind::text,
        'replica_identity', c.relreplident::text,
        'columns', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'attnum', a.attnum,
                'name', a.attname,
                'type_oid', a.atttypid::bigint,
                'type_name', jsonb_build_object(
                    'schema', type_namespace.nspname,
                    'name', type_definition.typname
                ),
                'type_kind', type_definition.typtype::text,
                'type_modifier', a.atttypmod,
                'nullable', NOT a.attnotnull,
                'generated', a.attgenerated::text,
                'identity', a.attidentity::text,
                'collation', CASE WHEN collation_definition.oid IS NULL THEN NULL ELSE
                    jsonb_build_object(
                        'schema', collation_namespace.nspname,
                        'name', collation_definition.collname
                    )
                END,
                'default_expression', pg_get_expr(default_value.adbin, default_value.adrelid)
            ) ORDER BY a.attnum)
            FROM pg_attribute a
            JOIN pg_type type_definition ON type_definition.oid = a.atttypid
            JOIN pg_namespace type_namespace
              ON type_namespace.oid = type_definition.typnamespace
            LEFT JOIN pg_attrdef default_value
              ON default_value.adrelid = a.attrelid AND default_value.adnum = a.attnum
            LEFT JOIN pg_collation collation_definition
              ON collation_definition.oid = a.attcollation AND a.attcollation <> 0
            LEFT JOIN pg_namespace collation_namespace
              ON collation_namespace.oid = collation_definition.collnamespace
            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        ), '[]'::jsonb),
        'primary_key', COALESCE((
            SELECT to_jsonb(i.indkey::int2[])
            FROM pg_index i
            WHERE i.indrelid = c.oid AND i.indisprimary
              AND i.indisvalid AND i.indisready AND i.indimmediate
        ), '[]'::jsonb),
        'partition_key', COALESCE((
            SELECT to_jsonb(p.partattrs::int2[])
            FROM pg_partitioned_table p
            WHERE p.partrelid = c.oid
        ), '[]'::jsonb)
    )
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = target_oid
      AND c.relkind IN ('r', 'p', 'f')
      AND c.relpersistence = 'p';
"#
        .to_owned();

        let schema_fingerprint = format!(
            "\n    SELECT md5(COALESCE({schema}.{snapshot}(target_oid), '{{}}'::jsonb)::text);\n"
        );

        let emit_ddl_event = format!(
            r#"
DECLARE
    commands jsonb;
    relation_ids oid[];
    affected_schemas text[];
    fingerprint text;
    table_transitions jsonb;
    payload jsonb;
BEGIN
    -- DROP is emitted by the sql_drop trigger below. This prevents one logical DDL event from
    -- being emitted twice when PostgreSQL invokes both event-trigger phases.
    IF TG_TAG LIKE 'DROP %' THEN
        RETURN;
    END IF;
    IF current_setting('pg2cb.internal_ddl', true) = 'on' THEN
        RETURN;
    END IF;
{worker_guard}
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'class_id', classid::text,
        'object_id', objid::text,
        'object_type', object_type,
        'schema_name', schema_name,
        'object_identity', object_identity,
        'command_tag', command_tag
    ) ORDER BY classid, objid, objsubid), '[]'::jsonb)
      INTO commands
      FROM pg_event_trigger_ddl_commands();

    SELECT COALESCE(array_agg(relation_id ORDER BY relation_id), '{{}}'::oid[])
      INTO relation_ids
      FROM (
          SELECT DISTINCT command.objid AS relation_id
            FROM pg_event_trigger_ddl_commands() command
            JOIN pg_class relation ON relation.oid = command.objid
           WHERE command.classid = 'pg_class'::regclass
             AND relation.relkind IN ('r', 'p', 'f')
             AND relation.relpersistence = 'p'
      ) relations;

    SELECT COALESCE(array_agg(DISTINCT affected_schema ORDER BY affected_schema), '{{}}'::text[])
      INTO affected_schemas
      FROM (
          SELECT CASE
              WHEN object_type = 'schema' THEN (parse_ident(object_identity, true))[1]
              ELSE schema_name
          END AS affected_schema
            FROM pg_event_trigger_ddl_commands()
      ) objects
     WHERE affected_schema IS NOT NULL AND affected_schema <> '';

    -- Capture each table's post-command catalog shape inside the DDL transaction. Messages remain
    -- ordered in WAL. After commit, the consumer validates only the terminal post-state for each
    -- relation against pg_catalog; earlier snapshots describe intermediate shapes in a multi-DDL
    -- transaction and cannot all equal the final catalog.
    WITH snapshots AS (
        SELECT relation_id, {schema}.{snapshot}(relation_id) AS after_schema
          FROM unnest(relation_ids) AS relation_id
    )
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'relation_id', relation_id::text,
        'after_fingerprint', md5(after_schema::text),
        'after_schema', after_schema,
        'kind', CASE WHEN TG_TAG LIKE 'CREATE TABLE%' THEN 'add_table' ELSE 'unknown' END
    ) ORDER BY relation_id), '[]'::jsonb)
      INTO table_transitions
      FROM snapshots
     WHERE after_schema IS NOT NULL;
    fingerprint := md5(table_transitions::text);
    payload := jsonb_build_object(
        'version', {version},
        'command_tag', TG_TAG,
        'relation_ids', to_jsonb(ARRAY(SELECT value::text FROM unnest(relation_ids) AS value)),
        'affected_schemas', to_jsonb(affected_schemas),
        'schema_fingerprint', fingerprint,
        'table_transitions', table_transitions,
        'commands', commands
    );

    INSERT INTO {schema}.ddl_audit(command_tag, relation_ids, schema_fingerprint, payload)
    VALUES (TG_TAG, relation_ids, fingerprint, payload);

    PERFORM pg_logical_emit_message(true, '{prefix}', payload::text);
END;
"#,
            worker_guard = worker_guard,
            schema = schema,
            snapshot = snapshot,
            version = DDL_MESSAGE_VERSION,
            prefix = DDL_MESSAGE_PREFIX,
        );

        let emit_sql_drop_event = format!(
            r#"
DECLARE
    dropped jsonb;
    relation_ids oid[];
    affected_schemas text[];
    fingerprint text;
    table_transitions jsonb;
    payload jsonb;
BEGIN
    IF current_setting('pg2cb.internal_ddl', true) = 'on' THEN
        RETURN;
    END IF;
{worker_guard}
    -- Retain schema/table/type objects: these are the objects whose removal can invalidate the
    -- mirrored catalog. Other DROP commands still produce an empty-scope message, which the
    -- engine treats as unknown and therefore fail-closed.
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'class_id', classid::text,
        'object_id', objid::text,
        'object_sub_id', objsubid,
        'object_type', object_type,
        'schema_name', schema_name,
        'object_name', object_name,
        'object_identity', object_identity,
        'original', original,
        'normal', normal
    ) ORDER BY classid, objid, objsubid), '[]'::jsonb)
      INTO dropped
      FROM pg_event_trigger_dropped_objects()
     WHERE object_type IN (
         'schema', 'table', 'partitioned table', 'foreign table',
         'type', 'domain', 'composite type'
     );

    SELECT COALESCE(array_agg(objid ORDER BY objid), '{{}}'::oid[])
      INTO relation_ids
      FROM pg_event_trigger_dropped_objects()
     WHERE classid = 'pg_class'::regclass
       AND objsubid = 0
       AND object_type IN ('table', 'partitioned table', 'foreign table');

    SELECT COALESCE(array_agg(DISTINCT affected_schema ORDER BY affected_schema), '{{}}'::text[])
      INTO affected_schemas
      FROM (
          SELECT CASE
              WHEN object_type = 'schema' THEN (parse_ident(object_identity, true))[1]
              ELSE schema_name
          END AS affected_schema
            FROM pg_event_trigger_dropped_objects()
           WHERE object_type IN (
               'schema', 'table', 'partitioned table', 'foreign table',
               'type', 'domain', 'composite type'
           )
      ) objects
     WHERE affected_schema IS NOT NULL AND affected_schema <> '';

    fingerprint := md5(dropped::text);
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'relation_id', relation_id::text,
        'kind', 'drop_table'
    ) ORDER BY relation_id), '[]'::jsonb)
      INTO table_transitions
      FROM unnest(relation_ids) AS relation_id;
    payload := jsonb_build_object(
        'version', {version},
        'command_tag', TG_TAG,
        'relation_ids', to_jsonb(ARRAY(SELECT value::text FROM unnest(relation_ids) AS value)),
        'affected_schemas', to_jsonb(affected_schemas),
        'schema_fingerprint', fingerprint,
        'table_transitions', table_transitions,
        'commands', dropped
    );

    INSERT INTO {schema}.ddl_audit(command_tag, relation_ids, schema_fingerprint, payload)
    VALUES (TG_TAG, relation_ids, fingerprint, payload);

    PERFORM pg_logical_emit_message(true, '{prefix}', payload::text);
END;
"#,
            worker_guard = worker_guard,
            schema = schema,
            version = DDL_MESSAGE_VERSION,
            prefix = DDL_MESSAGE_PREFIX,
        );

        Ok(DdlFunctionSources {
            schema_snapshot,
            schema_fingerprint,
            emit_ddl_event,
            emit_sql_drop_event,
        })
    }

    fn singleton_cleanup_sql(&self) -> String {
        if self.trigger_name != CANONICAL_DDL_TRIGGER_NAME {
            return String::new();
        }
        format!(
            r#"DO $pg2cb$
DECLARE
    managed_trigger record;
BEGIN
    FOR managed_trigger IN
        SELECT e.evtname
          FROM pg_event_trigger e
         WHERE e.evtname NOT IN ('{canonical}', '{canonical}_drop')
           AND COALESCE(obj_description(e.oid, 'pg_event_trigger'), '')
               LIKE 'pg2cloudberry_ddl_capture_v%'
    LOOP
        EXECUTE format('DROP EVENT TRIGGER %I', managed_trigger.evtname);
    END LOOP;
END;
$pg2cb$;"#,
            canonical = CANONICAL_DDL_TRIGGER_NAME,
        )
    }

    /// Render an idempotent installer. All user-provided names are identifiers; no SQL values are
    /// interpolated. The returned batch is intended to run on the source coordinator only.
    pub fn install_sql(&self) -> SourceResult<String> {
        self.validate()?;
        let sources = self.function_sources()?;
        let schema = quote_identifier(&self.metadata_schema)?;
        let trigger = quote_identifier(&self.trigger_name)?;
        let drop_trigger_name = self.drop_trigger_name()?;
        let drop_trigger = quote_identifier(&drop_trigger_name)?;
        let trigger_name_literal = quote_literal(&self.trigger_name)?;
        let drop_trigger_name_literal = quote_literal(&drop_trigger_name)?;
        let function = quote_identifier("emit_ddl_event")?;
        let drop_function = quote_identifier("emit_sql_drop_event")?;
        let snapshot = quote_identifier("schema_snapshot")?;
        let fingerprint = quote_identifier("schema_fingerprint")?;
        let schema_snapshot_source = &sources.schema_snapshot;
        let schema_fingerprint_source = &sources.schema_fingerprint;
        let emit_ddl_event_source = &sources.emit_ddl_event;
        let emit_sql_drop_event_source = &sources.emit_sql_drop_event;
        let singleton_cleanup = self.singleton_cleanup_sql();
        Ok(format!(
            r#"CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.ddl_audit (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id bigint NOT NULL DEFAULT txid_current(),
    emitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    command_tag text NOT NULL,
    relation_ids oid[] NOT NULL,
    schema_fingerprint text NOT NULL,
    payload jsonb NOT NULL
);

CREATE OR REPLACE FUNCTION {schema}.{snapshot}(target_oid oid)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $pg2cb${schema_snapshot_source}$pg2cb$;

CREATE OR REPLACE FUNCTION {schema}.{fingerprint}(target_oid oid)
RETURNS text
LANGUAGE sql
STABLE
AS $pg2cb${schema_fingerprint_source}$pg2cb$;

CREATE OR REPLACE FUNCTION {schema}.{function}()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, {schema}
AS $pg2cb${emit_ddl_event_source}$pg2cb$;

CREATE OR REPLACE FUNCTION {schema}.{drop_function}()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, {schema}
AS $pg2cb${emit_sql_drop_event_source}$pg2cb$;

{singleton_cleanup}

DO $pg2cb$
DECLARE
    conflicting_trigger text;
BEGIN
    SELECT e.evtname
      INTO conflicting_trigger
      FROM pg_event_trigger e
     WHERE e.evtname IN ({trigger_name_literal}, {drop_trigger_name_literal})
       AND COALESCE(obj_description(e.oid, 'pg_event_trigger'), '')
           NOT LIKE 'pg2cloudberry_ddl_capture_v%'
     LIMIT 1;
    IF conflicting_trigger IS NOT NULL THEN
        RAISE EXCEPTION 'event trigger % is not managed by pg2cloudberry', conflicting_trigger;
    END IF;
END;
$pg2cb$;

DROP EVENT TRIGGER IF EXISTS {drop_trigger};
DROP EVENT TRIGGER IF EXISTS {trigger};
CREATE EVENT TRIGGER {trigger}
    ON ddl_command_end
    EXECUTE FUNCTION {schema}.{function}();
CREATE EVENT TRIGGER {drop_trigger}
    ON sql_drop
    EXECUTE FUNCTION {schema}.{drop_function}();
COMMENT ON EVENT TRIGGER {trigger} IS '{marker}';
COMMENT ON EVENT TRIGGER {drop_trigger} IS '{marker}';
COMMENT ON FUNCTION {schema}.{function}() IS '{marker}';
COMMENT ON FUNCTION {schema}.{drop_function}() IS '{marker}';
"#,
            marker = DDL_CAPTURE_MARKER,
            trigger_name_literal = trigger_name_literal,
            drop_trigger_name_literal = drop_trigger_name_literal,
        ))
    }

    pub fn uninstall_sql(&self) -> SourceResult<String> {
        self.validate()?;
        Ok(format!(
            "DROP EVENT TRIGGER IF EXISTS {}; DROP EVENT TRIGGER IF EXISTS {}",
            quote_identifier(&self.drop_trigger_name()?)?,
            quote_identifier(&self.trigger_name)?
        ))
    }
}

const DDL_CAPTURE_CURRENT_SQL: &str = r#"
WITH expected_functions(
    function_name, language_name, return_type, argument_type, volatility,
    security_definer, configuration, function_source, required_marker
) AS (
    VALUES
        (
            'schema_snapshot'::text, 'sql'::text, 'jsonb'::regtype, 'oid'::regtype,
            's'::"char", false, NULL::text[], $5::text, NULL::text
        ),
        (
            'schema_fingerprint'::text, 'sql'::text, 'text'::regtype, 'oid'::regtype,
            's'::"char", false, NULL::text[], $6::text, NULL::text
        ),
        (
            'emit_ddl_event'::text, 'plpgsql'::text, 'event_trigger'::regtype, NULL::regtype,
            'v'::"char", true,
            ARRAY['search_path=pg_catalog, ' || quote_ident($3)]::text[],
            $7::text, $4::text
        ),
        (
            'emit_sql_drop_event'::text, 'plpgsql'::text, 'event_trigger'::regtype, NULL::regtype,
            'v'::"char", true,
            ARRAY['search_path=pg_catalog, ' || quote_ident($3)]::text[],
            $8::text, $4::text
        )
),
valid_functions AS (
    SELECT count(*) = 4 AS valid
      FROM expected_functions expected
      JOIN pg_namespace namespace ON namespace.nspname = $3
      JOIN pg_proc function
        ON function.pronamespace = namespace.oid
       AND function.proname = expected.function_name
      JOIN pg_language language ON language.oid = function.prolang
     WHERE language.lanname = expected.language_name
       AND function.prokind = 'f'
       AND function.prorettype = expected.return_type
       AND function.pronargs =
           CASE WHEN expected.argument_type IS NULL THEN 0 ELSE 1 END
       AND (
           expected.argument_type IS NULL
           OR function.proargtypes[0] = expected.argument_type
       )
       AND NOT function.proretset
       AND function.provolatile = expected.volatility
       AND function.prosecdef = expected.security_definer
       AND NOT function.proisstrict
       AND NOT function.proleakproof
       AND function.proparallel = 'u'
       AND function.proconfig IS NOT DISTINCT FROM expected.configuration
       AND function.prosrc = expected.function_source
       AND (
           expected.required_marker IS NULL
           OR obj_description(function.oid, 'pg_proc') = expected.required_marker
       )
),
expected_triggers(trigger_name, trigger_event, function_name) AS (
    VALUES
        ($1::text, 'ddl_command_end'::text, 'emit_ddl_event'::text),
        ($2::text, 'sql_drop'::text, 'emit_sql_drop_event'::text)
),
valid_triggers AS (
    SELECT count(*) = 2 AS valid
      FROM expected_triggers expected
      JOIN pg_event_trigger trigger
        ON trigger.evtname = expected.trigger_name
       AND trigger.evtevent = expected.trigger_event
      JOIN pg_proc function ON function.oid = trigger.evtfoid
      JOIN pg_namespace namespace ON namespace.oid = function.pronamespace
     WHERE namespace.nspname = $3
       AND function.proname = expected.function_name
       AND trigger.evtenabled = 'O'
       AND trigger.evttags IS NULL
       AND obj_description(trigger.oid, 'pg_event_trigger') = $4
),
audit_relation AS (
    SELECT relation.oid
      FROM pg_class relation
      JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
     WHERE namespace.nspname = $3
       AND relation.relname = 'ddl_audit'
       AND relation.relkind = 'r'
       AND relation.relpersistence = 'p'
       AND NOT relation.relispartition
),
expected_columns(
    attribute_number, column_name, column_type, not_null, identity_kind, default_expression
) AS (
    VALUES
        (1::smallint, 'event_id'::text, 'bigint'::regtype, true, 'a'::"char", NULL::text),
        (2::smallint, 'transaction_id'::text, 'bigint'::regtype, true, ''::"char", 'txid_current()'::text),
        (3::smallint, 'emitted_at'::text, 'timestamptz'::regtype, true, ''::"char", 'clock_timestamp()'::text),
        (4::smallint, 'command_tag'::text, 'text'::regtype, true, ''::"char", NULL::text),
        (5::smallint, 'relation_ids'::text, 'oid[]'::regtype, true, ''::"char", NULL::text),
        (6::smallint, 'schema_fingerprint'::text, 'text'::regtype, true, ''::"char", NULL::text),
        (7::smallint, 'payload'::text, 'jsonb'::regtype, true, ''::"char", NULL::text)
),
valid_audit_columns AS (
    SELECT (
        SELECT count(*) = 7
          FROM audit_relation audit
          JOIN pg_attribute attribute ON attribute.attrelid = audit.oid
         WHERE attribute.attnum > 0
           AND NOT attribute.attisdropped
    ) AND (
        SELECT count(*) = 7
          FROM expected_columns expected
          JOIN audit_relation audit ON true
          JOIN pg_attribute attribute
            ON attribute.attrelid = audit.oid
           AND attribute.attnum = expected.attribute_number
           AND attribute.attname = expected.column_name
          LEFT JOIN pg_attrdef default_value
            ON default_value.adrelid = attribute.attrelid
           AND default_value.adnum = attribute.attnum
         WHERE attribute.atttypid = expected.column_type
           AND attribute.atttypmod = -1
           AND attribute.attnotnull = expected.not_null
           AND attribute.attidentity = expected.identity_kind
           AND attribute.attgenerated = ''
           AND pg_get_expr(default_value.adbin, default_value.adrelid)
               IS NOT DISTINCT FROM expected.default_expression
    ) AS valid
),
valid_audit_primary_key AS (
    SELECT count(*) = 1 AS valid
      FROM audit_relation audit
      JOIN pg_index index_definition ON index_definition.indrelid = audit.oid
      JOIN pg_attribute event_id
        ON event_id.attrelid = audit.oid
       AND event_id.attname = 'event_id'
     WHERE index_definition.indisprimary
       AND index_definition.indisunique
       AND index_definition.indisvalid
       AND index_definition.indisready
       AND index_definition.indnkeyatts = 1
       AND index_definition.indnatts = 1
       AND index_definition.indkey[0] = event_id.attnum
       AND index_definition.indexprs IS NULL
       AND index_definition.indpred IS NULL
)
SELECT valid_functions.valid
   AND valid_triggers.valid
   AND valid_audit_columns.valid
   AND valid_audit_primary_key.valid
  FROM valid_functions, valid_triggers, valid_audit_columns, valid_audit_primary_key
"#;

async fn ddl_capture_is_current(
    client: &Client,
    spec: &DdlInstallSpec,
    drop_trigger: &str,
    sources: &DdlFunctionSources,
) -> SourceResult<bool> {
    Ok(client
        .query_one(
            DDL_CAPTURE_CURRENT_SQL,
            &[
                &spec.trigger_name,
                &drop_trigger,
                &spec.metadata_schema,
                &DDL_CAPTURE_MARKER,
                &sources.schema_snapshot,
                &sources.schema_fingerprint,
                &sources.emit_ddl_event,
                &sources.emit_sql_drop_event,
            ],
        )
        .await?
        .try_get(0)?)
}

/// Install the capture triggers only when both marked definitions are present and current.
///
/// Re-running the full installer on every reconnect would itself generate DDL messages after an
/// existing checkpoint. The marker is attached after the complete installer succeeds, so a
/// failed/partial install is retried rather than treated as current.
pub async fn ensure_ddl_capture(client: &Client, spec: &DdlInstallSpec) -> SourceResult<bool> {
    spec.validate()?;
    let drop_trigger = spec.drop_trigger_name()?;
    let sources = spec.function_sources()?;
    let install_sql = spec.install_sql()?;
    client.batch_execute("BEGIN").await?;
    let result = async {
        client
            .batch_execute("SELECT pg_advisory_xact_lock(hashtextextended(current_database(), 0))")
            .await?;
        if let Some(row) = client
            .query_opt(
                "SELECT n.nspname
                   FROM pg_event_trigger e
                   JOIN pg_proc p ON p.oid = e.evtfoid
                   JOIN pg_namespace n ON n.oid = p.pronamespace
                  WHERE e.evtname IN ($1, $2)
                    AND COALESCE(obj_description(e.oid, 'pg_event_trigger'), '')
                        LIKE 'pg2cloudberry_ddl_capture_v%'
                    AND n.nspname <> $3
                  LIMIT 1",
                &[&spec.trigger_name, &drop_trigger, &spec.metadata_schema],
            )
            .await?
        {
            let existing_schema: String = row.try_get(0)?;
            return Err(SourceError::Ddl(format!(
                "DDL capture is already owned by metadata schema `{existing_schema}`"
            )));
        }
        let current = ddl_capture_is_current(client, spec, &drop_trigger, &sources).await?;
        client
            .batch_execute("SELECT set_config('pg2cb.internal_ddl', 'on', true)")
            .await?;
        if current {
            if spec.trigger_name == CANONICAL_DDL_TRIGGER_NAME {
                client.batch_execute(&spec.singleton_cleanup_sql()).await?;
            }
            return Ok::<bool, SourceError>(false);
        }
        client.batch_execute(&install_sql).await?;
        if !ddl_capture_is_current(client, spec, &drop_trigger, &sources).await? {
            return Err(SourceError::Ddl(
                "DDL capture installation did not produce the required catalog definitions"
                    .to_owned(),
            ));
        }
        Ok::<bool, SourceError>(true)
    }
    .await;
    let installed = match result {
        Ok(installed) => installed,
        Err(error) => {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(error);
        }
    };
    if let Err(error) = client.batch_execute("COMMIT").await {
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(error.into());
    }
    Ok(installed)
}

/// Read terminal catalog shapes through the same installed helper used by the event trigger.
///
/// Every requested relation is present in the returned map. A `None` value proves the OID no
/// longer names a persistent table-like relation; a present value carries PostgreSQL's canonical
/// jsonb fingerprint alongside the typed snapshot. The schema coordinator calls this once after
/// a committed DDL transaction, never from the row hot path.
pub async fn load_current_relation_schemas<C>(
    client: &C,
    metadata_schema: &str,
    relation_ids: &[u32],
) -> SourceResult<BTreeMap<u32, Option<CurrentRelationSchema>>>
where
    C: GenericClient + Sync,
{
    validate_schema(metadata_schema)?;
    let mut relation_ids = relation_ids.to_vec();
    relation_ids.sort_unstable();
    relation_ids.dedup();
    if relation_ids.contains(&0) {
        return Err(SourceError::Ddl(
            "relation OID zero is not valid for a catalog snapshot".to_owned(),
        ));
    }
    if relation_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let database_ids = relation_ids
        .iter()
        .copied()
        .map(i64::from)
        .collect::<Vec<_>>();
    let schema = quote_identifier(metadata_schema)?;
    let sql = format!(
        "WITH requested(relation_id) AS (
             SELECT value FROM unnest($1::bigint[]) AS value
         ), snapshots AS MATERIALIZED (
             SELECT relation_id,
                    {schema}.schema_snapshot(relation_id::oid) AS after_schema
               FROM requested
         )
         SELECT relation_id,
                after_schema,
                CASE WHEN after_schema IS NULL THEN NULL ELSE md5(after_schema::text) END
           FROM snapshots
          ORDER BY relation_id"
    );
    let rows = client.query(&sql, &[&database_ids]).await?;
    let mut current = BTreeMap::new();
    for row in rows {
        let relation_id = u32::try_from(row.try_get::<_, i64>(0)?).map_err(|_| {
            SourceError::Ddl("catalog snapshot returned an invalid relation OID".to_owned())
        })?;
        let snapshot = row.try_get::<_, Option<serde_json::Value>>(1)?;
        let fingerprint = row.try_get::<_, Option<String>>(2)?;
        let value = match (snapshot, fingerprint) {
            (None, None) => None,
            (Some(snapshot), Some(fingerprint)) if !fingerprint.is_empty() => {
                let snapshot = serde_json::from_value(snapshot)?;
                validate_relation_snapshot(&snapshot, relation_id)?;
                Some(CurrentRelationSchema {
                    fingerprint,
                    schema: snapshot,
                })
            }
            _ => {
                return Err(SourceError::Ddl(format!(
                    "catalog snapshot/fingerprint mismatch for relation {relation_id}"
                )));
            }
        };
        current.insert(relation_id, value);
    }
    if current.len() != relation_ids.len() {
        return Err(SourceError::Ddl(
            "catalog snapshot query did not return every requested relation".to_owned(),
        ));
    }
    Ok(current)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DdlEnvelope {
    version: u16,
    command_tag: String,
    #[serde(default)]
    relation_ids: Vec<serde_json::Value>,
    #[serde(default)]
    affected_schemas: Vec<String>,
    schema_fingerprint: String,
    /// Per-relation after-DDL fingerprints (v5 capture). Absent for an older
    /// message, in which case the engine has no per-relation hint and treats the
    /// event conservatively.
    #[serde(default)]
    relation_fingerprints: Vec<RelationFingerprint>,
    /// Typed v2 per-table post-command snapshots. Empty is valid for database-wide DDL such as
    /// ALTER PUBLICATION, whose scope must remain fail-closed.
    #[serde(default)]
    table_transitions: Vec<TableTransitionEnvelope>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RelationFingerprint {
    relation_id: String,
    fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TableTransitionEnvelope {
    relation_id: serde_json::Value,
    #[serde(default)]
    after_fingerprint: Option<String>,
    #[serde(default)]
    after_schema: Option<RelationSchemaSnapshot>,
    #[serde(default = "unknown_transition_kind")]
    kind: String,
}

fn unknown_transition_kind() -> String {
    "unknown".to_owned()
}

#[must_use]
pub fn is_ddl_message_prefix(prefix: &str) -> bool {
    matches!(prefix, DDL_MESSAGE_PREFIX | LEGACY_DDL_MESSAGE_PREFIX)
}

pub fn decode_ddl_message(prefix: &str, payload: &[u8]) -> SourceResult<DdlMessage> {
    let expected_version = match prefix {
        DDL_MESSAGE_PREFIX => DDL_MESSAGE_VERSION,
        LEGACY_DDL_MESSAGE_PREFIX => LEGACY_DDL_MESSAGE_VERSION,
        _ => {
            return Err(SourceError::Ddl(format!(
                "unknown DDL message prefix `{prefix}`"
            )));
        }
    };
    let envelope: DdlEnvelope = serde_json::from_slice(payload)?;
    if envelope.version != expected_version {
        return Err(SourceError::Ddl(format!(
            "DDL message prefix `{prefix}` does not match payload version {}",
            envelope.version,
        )));
    }
    let mut relation_ids = envelope
        .relation_ids
        .into_iter()
        .map(|value| parse_relation_id(value, "DDL message"))
        .collect::<SourceResult<Vec<_>>>()?;
    relation_ids.sort_unstable();
    relation_ids.dedup();
    if envelope.command_tag.is_empty() || envelope.schema_fingerprint.is_empty() {
        return Err(SourceError::Ddl(
            "DDL message has an empty command tag or fingerprint".to_owned(),
        ));
    }
    let affected_schemas = canonicalize_schemas(envelope.affected_schemas)?;
    // Turn each per-relation after-fingerprint into a transition carrying only the
    // after side. The source event trigger fires at command-end and cannot observe
    // the pre-DDL shape, so `kind` stays Unknown here; the engine refines it by
    // diffing against the relation's bound before-schema (classify_relation_diff).
    let mut transitions = if expected_version == LEGACY_DDL_MESSAGE_VERSION {
        envelope
            .relation_fingerprints
            .into_iter()
            .map(|relation| {
                let relation_id = relation.relation_id.parse::<u32>().map_err(|_| {
                    SourceError::Ddl(format!(
                        "invalid relation OID `{}` in DDL relation_fingerprints",
                        relation.relation_id
                    ))
                })?;
                Ok(TableTransition {
                    relation_id,
                    before_generation: None,
                    after_generation: None,
                    before_fingerprint: None,
                    after_fingerprint: Some(relation.fingerprint),
                    after_schema: None,
                    kind: TransitionKind::Unknown,
                })
            })
            .collect::<SourceResult<Vec<_>>>()?
    } else {
        envelope
            .table_transitions
            .into_iter()
            .map(decode_table_transition)
            .collect::<SourceResult<Vec<_>>>()?
    };
    transitions.sort_by_key(|transition| transition.relation_id);
    let mut seen = HashSet::with_capacity(transitions.len());
    for transition in &transitions {
        if !seen.insert(transition.relation_id) {
            return Err(SourceError::Ddl(format!(
                "DDL message repeats transition for relation {}",
                transition.relation_id
            )));
        }
        if !relation_ids.contains(&transition.relation_id) {
            return Err(SourceError::Ddl(format!(
                "DDL transition relation {} is absent from relation_ids",
                transition.relation_id
            )));
        }
    }
    Ok(DdlMessage {
        version: envelope.version,
        command_tag: envelope.command_tag,
        relation_ids,
        affected_schemas,
        schema_fingerprint: envelope.schema_fingerprint,
        transitions,
    })
}

fn parse_relation_id(value: serde_json::Value, context: &str) -> SourceResult<u32> {
    let text = match value {
        serde_json::Value::String(value) => value,
        serde_json::Value::Number(value) => value.to_string(),
        other => {
            return Err(SourceError::Ddl(format!(
                "invalid relation OID JSON value `{other}` in {context}"
            )));
        }
    };
    text.parse::<u32>()
        .ok()
        .filter(|relation_id| *relation_id != 0)
        .ok_or_else(|| SourceError::Ddl(format!("invalid relation OID `{text}` in {context}")))
}

fn decode_table_transition(wire: TableTransitionEnvelope) -> SourceResult<TableTransition> {
    let relation_id = parse_relation_id(wire.relation_id, "DDL table transition")?;
    let kind = match wire.kind.as_str() {
        "unknown" => TransitionKind::Unknown,
        "add_table" => TransitionKind::AddTable,
        "drop_table" => TransitionKind::DropTable,
        other => {
            return Err(SourceError::Ddl(format!(
                "unknown DDL table transition kind `{other}`"
            )));
        }
    };
    if wire.after_fingerprint.as_deref().is_some_and(str::is_empty) {
        return Err(SourceError::Ddl(
            "DDL table transition has an empty after fingerprint".to_owned(),
        ));
    }
    if let Some(snapshot) = &wire.after_schema {
        validate_relation_snapshot(snapshot, relation_id)?;
        if wire.after_fingerprint.is_none() {
            return Err(SourceError::Ddl(
                "DDL table transition has after-schema without a fingerprint".to_owned(),
            ));
        }
    } else if !matches!(kind, TransitionKind::DropTable) {
        return Err(SourceError::Ddl(
            "non-drop DDL table transition is missing after-schema".to_owned(),
        ));
    }
    Ok(TableTransition {
        relation_id,
        before_generation: None,
        after_generation: None,
        before_fingerprint: None,
        after_fingerprint: wire.after_fingerprint,
        after_schema: wire.after_schema,
        kind,
    })
}

fn validate_relation_snapshot(
    snapshot: &RelationSchemaSnapshot,
    relation_id: u32,
) -> SourceResult<()> {
    if snapshot.relation_id != relation_id
        || snapshot.relation_kind.len() != 1
        || snapshot.replica_identity.len() != 1
    {
        return Err(SourceError::Ddl(format!(
            "DDL after-schema identity does not match relation {relation_id}"
        )));
    }
    let mut previous_attnum = 0_i16;
    for column in &snapshot.columns {
        validate_identifier(&column.name).map_err(|error| SourceError::Ddl(error.to_string()))?;
        if column.attnum <= previous_attnum
            || column.type_oid == 0
            || column.type_kind.len() != 1
            || column.generated.len() > 1
            || column.identity.len() > 1
        {
            return Err(SourceError::Ddl(format!(
                "DDL after-schema has invalid column facts for relation {relation_id}"
            )));
        }
        previous_attnum = column.attnum;
    }
    if snapshot.primary_key.iter().any(|attnum| {
        *attnum <= 0
            || !snapshot
                .columns
                .iter()
                .any(|column| column.attnum == *attnum)
    }) {
        return Err(SourceError::Ddl(format!(
            "DDL after-schema has an invalid primary key for relation {relation_id}"
        )));
    }
    Ok(())
}

fn canonicalize_schemas(mut schemas: Vec<String>) -> SourceResult<Vec<String>> {
    if schemas
        .iter()
        .any(|schema| schema.is_empty() || schema.contains('\0'))
    {
        return Err(SourceError::Ddl(
            "DDL message contains an invalid affected schema".to_owned(),
        ));
    }
    schemas.sort_unstable();
    schemas.dedup();
    Ok(schemas)
}

fn validate_schema(value: &str) -> SourceResult<()> {
    if value.is_empty()
        || value.contains('\0')
        || value.len() > 63
        || value == "pg_catalog"
        || value == "information_schema"
        || value.starts_with("pg_toast")
        || value.starts_with("pg_temp")
    {
        return Err(SourceError::InvalidIdentifier(value.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installer_is_transactional_and_installs_both_capture_phases() {
        let spec = DdlInstallSpec::default();
        let sql = spec.install_sql().unwrap();
        assert!(sql.contains("pg_logical_emit_message(true"));
        assert!(sql.contains("ON ddl_command_end"));
        assert!(sql.contains("ON sql_drop"));
        assert!(sql.contains("pg_event_trigger_dropped_objects"));
        assert!(sql.contains("parse_ident(object_identity, true)"));
        assert!(sql.contains("TG_TAG LIKE 'DROP %'"));
        assert!(sql.contains("to_regclass('pg_catalog.pg_dist_node')"));
        assert!(sql.contains("current_setting('pg2cb.internal_ddl', true)"));
        assert!(sql.contains("'table_transitions', table_transitions"));
        assert!(sql.contains("'default_expression'"));
        assert!(sql.contains(DDL_MESSAGE_PREFIX));
        assert!(sql.contains(DDL_CAPTURE_MARKER));
        assert!(sql.contains("COMMENT ON FUNCTION"));
        assert!(!sql.contains("current_query"));
    }

    #[test]
    fn decoder_rejects_unknown_prefix_and_version() {
        assert!(decode_ddl_message("other", b"{}").is_err());
        let payload = br#"{"version":1,"command_tag":"ALTER TABLE","schema_fingerprint":"x"}"#;
        assert!(decode_ddl_message(DDL_MESSAGE_PREFIX, payload).is_err());
        let payload = br#"{"version":2,"command_tag":"ALTER TABLE","schema_fingerprint":"x"}"#;
        assert!(decode_ddl_message(LEGACY_DDL_MESSAGE_PREFIX, payload).is_err());
        let payload = br#"{"version":2,"command_tag":"ALTER TABLE","relation_ids":["0"],"schema_fingerprint":"x"}"#;
        assert!(decode_ddl_message(DDL_MESSAGE_PREFIX, payload).is_err());
    }

    #[test]
    fn decoder_maps_relation_ids_and_canonicalizes_schemas() {
        let payload = br#"{"version":1,"command_tag":"ALTER TABLE","relation_ids":["42"],"affected_schemas":["z","a","z"],"schema_fingerprint":"abc"}"#;
        let message = decode_ddl_message(LEGACY_DDL_MESSAGE_PREFIX, payload).unwrap();
        assert_eq!(message.relation_ids, vec![42]);
        assert_eq!(message.affected_schemas, ["a", "z"]);
    }

    #[test]
    fn old_payload_defaults_to_unknown_schema_scope() {
        let payload =
            br#"{"version":1,"command_tag":"ALTER PUBLICATION","schema_fingerprint":"abc"}"#;
        let message = decode_ddl_message(LEGACY_DDL_MESSAGE_PREFIX, payload).unwrap();
        assert!(message.affected_schemas.is_empty());
    }

    #[test]
    fn v2_decoder_preserves_typed_after_schema() {
        let payload = br#"{
            "version": 2,
            "command_tag": "ALTER TABLE",
            "relation_ids": ["42"],
            "affected_schemas": ["public"],
            "schema_fingerprint": "event-fingerprint",
            "table_transitions": [{
                "relation_id": "42",
                "after_fingerprint": "table-fingerprint",
                "kind": "unknown",
                "after_schema": {
                    "relation_id": 42,
                    "name": {"schema": "public", "name": "items"},
                    "relation_kind": "r",
                    "replica_identity": "d",
                    "columns": [{
                        "attnum": 1,
                        "name": "id",
                        "type_oid": 20,
                        "type_name": {"schema": "pg_catalog", "name": "int8"},
                        "type_kind": "b",
                        "type_modifier": -1,
                        "nullable": false,
                        "generated": "",
                        "identity": "",
                        "collation": null,
                        "default_expression": null
                    }],
                    "primary_key": [1],
                    "partition_key": []
                }
            }]
        }"#;
        let message = decode_ddl_message(DDL_MESSAGE_PREFIX, payload).unwrap();
        assert_eq!(message.version, 2);
        assert_eq!(message.transitions.len(), 1);
        let transition = &message.transitions[0];
        assert!(matches!(transition.kind, TransitionKind::Unknown));
        let snapshot = transition.after_schema.as_ref().unwrap();
        assert_eq!(snapshot.relation_id, 42);
        assert_eq!(snapshot.name.to_string(), "public.items");
        assert_eq!(snapshot.primary_key, [1]);
        assert_eq!(snapshot.columns[0].type_name.to_string(), "pg_catalog.int8");
    }

    #[test]
    fn v2_decoder_accepts_drop_without_after_schema() {
        let payload = br#"{
            "version": 2,
            "command_tag": "DROP TABLE",
            "relation_ids": ["42"],
            "affected_schemas": ["public"],
            "schema_fingerprint": "drop-fingerprint",
            "table_transitions": [{"relation_id": "42", "kind": "drop_table"}]
        }"#;
        let message = decode_ddl_message(DDL_MESSAGE_PREFIX, payload).unwrap();
        assert!(matches!(
            message.transitions.as_slice(),
            [TableTransition {
                kind: TransitionKind::DropTable,
                after_schema: None,
                ..
            }]
        ));
    }
}
