//! Transactional DDL notifications emitted from the source metadata schema.
//!
//! Event-trigger APIs provide catalog-shaped facts without parsing `current_query()`. The
//! command-end trigger covers ordinary DDL, while the sql-drop trigger covers objects that no
//! longer exist by the time the command completes.

use cloudberry_etl_core::change::DdlMessage;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::{
    SourceError, SourceResult,
    sql::{quote_identifier, quote_literal},
};

pub const DDL_MESSAGE_PREFIX: &str = "pg2cloudberry_ddl_v1";
pub const DDL_MESSAGE_VERSION: u16 = 1;
/// Marker stored on both event triggers and their functions.
pub const DDL_CAPTURE_MARKER: &str = "pg2cloudberry_ddl_capture_v4";
pub const CANONICAL_DDL_TRIGGER_NAME: &str = "pg2cb_ddl_command_end";

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
        let fingerprint = quote_identifier("schema_fingerprint")?;
        let worker_guard = self.worker_guard_sql();

        let schema_snapshot = r#"
    SELECT jsonb_build_object(
        'relation_id', c.oid::text,
        'schema', n.nspname,
        'name', c.relname,
        'relkind', c.relkind::text,
        'replica_identity', c.relreplident::text,
        'columns', COALESCE((
            SELECT jsonb_agg(jsonb_build_object(
                'attnum', a.attnum,
                'name', a.attname,
                'type_oid', a.atttypid::text,
                'type_modifier', a.atttypmod,
                'not_null', a.attnotnull,
                'generated', a.attgenerated::text,
                'identity', a.attidentity::text
            ) ORDER BY a.attnum)
            FROM pg_attribute a
            WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
        ), '[]'::jsonb),
        'primary_key', COALESCE((
            SELECT to_jsonb(i.indkey::text)
            FROM pg_index i
            WHERE i.indrelid = c.oid AND i.indisprimary
        ), 'null'::jsonb),
        'partition_key', COALESCE((
            SELECT to_jsonb(p.partattrs::text)
            FROM pg_partitioned_table p
            WHERE p.partrelid = c.oid
        ), 'null'::jsonb)
    )
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.oid = target_oid;
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

    SELECT COALESCE(array_agg(objid ORDER BY objid), '{{}}'::oid[])
      INTO relation_ids
      FROM pg_event_trigger_ddl_commands()
     WHERE classid = 'pg_class'::regclass;

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

    SELECT md5(COALESCE(
        (SELECT jsonb_agg({schema}.{fingerprint}(objid) ORDER BY objid)::text
           FROM pg_event_trigger_ddl_commands()
          WHERE classid = 'pg_class'::regclass),
        '[]')) INTO fingerprint;
    payload := jsonb_build_object(
        'version', {version},
        'command_tag', TG_TAG,
        'relation_ids', to_jsonb(ARRAY(SELECT value::text FROM unnest(relation_ids) AS value)),
        'affected_schemas', to_jsonb(affected_schemas),
        'schema_fingerprint', fingerprint,
        'commands', commands
    );

    INSERT INTO {schema}.ddl_audit(command_tag, relation_ids, schema_fingerprint, payload)
    VALUES (TG_TAG, relation_ids, fingerprint, payload);

    PERFORM pg_logical_emit_message(true, '{prefix}', payload::text);
END;
"#,
            worker_guard = worker_guard,
            schema = schema,
            fingerprint = fingerprint,
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
    payload := jsonb_build_object(
        'version', {version},
        'command_tag', TG_TAG,
        'relation_ids', to_jsonb(ARRAY(SELECT value::text FROM unnest(relation_ids) AS value)),
        'affected_schemas', to_jsonb(affected_schemas),
        'schema_fingerprint', fingerprint,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DdlEnvelope {
    version: u16,
    command_tag: String,
    #[serde(default)]
    relation_ids: Vec<serde_json::Value>,
    #[serde(default)]
    affected_schemas: Vec<String>,
    schema_fingerprint: String,
}

pub fn decode_ddl_message(prefix: &str, payload: &[u8]) -> SourceResult<DdlMessage> {
    if prefix != DDL_MESSAGE_PREFIX {
        return Err(SourceError::Ddl(format!(
            "unknown DDL message prefix `{prefix}`"
        )));
    }
    let envelope: DdlEnvelope = serde_json::from_slice(payload)?;
    if envelope.version != DDL_MESSAGE_VERSION {
        return Err(SourceError::Ddl(format!(
            "unsupported DDL message version {}",
            envelope.version
        )));
    }
    let relation_ids = envelope
        .relation_ids
        .into_iter()
        .map(|value| {
            let text = match value {
                serde_json::Value::String(value) => value,
                serde_json::Value::Number(value) => value.to_string(),
                other => {
                    return Err(SourceError::Ddl(format!(
                        "invalid relation OID JSON value `{other}` in DDL message"
                    )));
                }
            };
            text.parse::<u32>().map_err(|_| {
                SourceError::Ddl(format!("invalid relation OID `{text}` in DDL message"))
            })
        })
        .collect::<SourceResult<Vec<_>>>()?;
    if envelope.command_tag.is_empty() || envelope.schema_fingerprint.is_empty() {
        return Err(SourceError::Ddl(
            "DDL message has an empty command tag or fingerprint".to_owned(),
        ));
    }
    let affected_schemas = canonicalize_schemas(envelope.affected_schemas)?;
    Ok(DdlMessage {
        version: envelope.version,
        command_tag: envelope.command_tag,
        relation_ids,
        affected_schemas,
        schema_fingerprint: envelope.schema_fingerprint,
    })
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
        assert!(sql.contains(DDL_CAPTURE_MARKER));
        assert!(sql.contains("COMMENT ON FUNCTION"));
        assert!(!sql.contains("current_query"));
    }

    #[test]
    fn decoder_rejects_unknown_prefix_and_version() {
        assert!(decode_ddl_message("other", b"{}").is_err());
        let payload = br#"{"version":2,"command_tag":"ALTER TABLE","schema_fingerprint":"x"}"#;
        assert!(decode_ddl_message(DDL_MESSAGE_PREFIX, payload).is_err());
    }

    #[test]
    fn decoder_maps_relation_ids_and_canonicalizes_schemas() {
        let payload = br#"{"version":1,"command_tag":"ALTER TABLE","relation_ids":["42"],"affected_schemas":["z","a","z"],"schema_fingerprint":"abc"}"#;
        let message = decode_ddl_message(DDL_MESSAGE_PREFIX, payload).unwrap();
        assert_eq!(message.relation_ids, vec![42]);
        assert_eq!(message.affected_schemas, ["a", "z"]);
    }

    #[test]
    fn old_payload_defaults_to_unknown_schema_scope() {
        let payload =
            br#"{"version":1,"command_tag":"ALTER PUBLICATION","schema_fingerprint":"abc"}"#;
        let message = decode_ddl_message(DDL_MESSAGE_PREFIX, payload).unwrap();
        assert!(message.affected_schemas.is_empty());
    }
}
