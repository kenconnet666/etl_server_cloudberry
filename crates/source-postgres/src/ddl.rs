//! Transactional DDL notifications emitted from the source metadata schema.
//!
//! Event-trigger APIs provide catalog-shaped facts without parsing `current_query()`. The
//! command-end trigger covers ordinary DDL, while the sql-drop trigger covers objects that no
//! longer exist by the time the command completes.

use cloudberry_etl_core::change::DdlMessage;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::{SourceError, SourceResult, sql::quote_identifier};

pub const DDL_MESSAGE_PREFIX: &str = "pg2cloudberry_ddl_v1";
pub const DDL_MESSAGE_VERSION: u16 = 1;
/// Marker stored on both event triggers and their functions.
pub const DDL_CAPTURE_MARKER: &str = "pg2cloudberry_ddl_capture_v3";

#[derive(Debug, Clone)]
pub struct DdlInstallSpec {
    pub metadata_schema: String,
    pub trigger_name: String,
    pub allow_citus_worker_guard: bool,
}

impl Default for DdlInstallSpec {
    fn default() -> Self {
        Self {
            metadata_schema: "pg2cb_meta".to_owned(),
            trigger_name: "pg2cb_ddl_command_end".to_owned(),
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

    /// Render an idempotent installer. All user-provided names are identifiers; no SQL values are
    /// interpolated. The returned batch is intended to run on the source coordinator only.
    pub fn install_sql(&self) -> SourceResult<String> {
        self.validate()?;
        let schema = quote_identifier(&self.metadata_schema)?;
        let trigger = quote_identifier(&self.trigger_name)?;
        let drop_trigger = quote_identifier(&self.drop_trigger_name()?)?;
        let function = quote_identifier("emit_ddl_event")?;
        let drop_function = quote_identifier("emit_sql_drop_event")?;
        let snapshot = quote_identifier("schema_snapshot")?;
        let fingerprint = quote_identifier("schema_fingerprint")?;
        let worker_guard = if self.allow_citus_worker_guard {
            "        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'citus')
           AND to_regclass('pg_catalog.pg_dist_node') IS NULL THEN
            RETURN;
        END IF;"
        } else {
            "        NULL;"
        };
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
AS $pg2cb$
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
$pg2cb$;

CREATE OR REPLACE FUNCTION {schema}.{fingerprint}(target_oid oid)
RETURNS text
LANGUAGE sql
STABLE
AS $pg2cb$
    SELECT md5(COALESCE({schema}.{snapshot}(target_oid), '{{}}'::jsonb)::text);
$pg2cb$;

CREATE OR REPLACE FUNCTION {schema}.{function}()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, {schema}
AS $pg2cb$
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
$pg2cb$;

CREATE OR REPLACE FUNCTION {schema}.{drop_function}()
RETURNS event_trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, {schema}
AS $pg2cb$
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
            version = DDL_MESSAGE_VERSION,
            prefix = DDL_MESSAGE_PREFIX,
            marker = DDL_CAPTURE_MARKER,
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

/// Install the capture triggers only when both marked definitions are present and current.
///
/// Re-running the full installer on every reconnect would itself generate DDL messages after an
/// existing checkpoint. The marker is attached after the complete installer succeeds, so a
/// failed/partial install is retried rather than treated as current.
pub async fn ensure_ddl_capture(client: &Client, spec: &DdlInstallSpec) -> SourceResult<bool> {
    spec.validate()?;
    let drop_trigger = spec.drop_trigger_name()?;
    let current: bool = client
        .query_one(
            "SELECT count(*) = 2
               FROM (VALUES
                    ($1::text, 'ddl_command_end'::text, 'emit_ddl_event'::text),
                    ($2::text, 'sql_drop'::text, 'emit_sql_drop_event'::text)
               ) expected(trigger_name, trigger_event, function_name)
               JOIN pg_event_trigger e
                 ON e.evtname::text = expected.trigger_name
                AND e.evtevent::text = expected.trigger_event
                AND e.evtenabled = 'O'
               JOIN pg_proc p ON p.oid = e.evtfoid
               JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE n.nspname = $3
                AND p.proname::text = expected.function_name
                AND obj_description(e.oid, 'pg_event_trigger') = $4
                AND obj_description(p.oid, 'pg_proc') = $4",
            &[
                &spec.trigger_name,
                &drop_trigger,
                &spec.metadata_schema,
                &DDL_CAPTURE_MARKER,
            ],
        )
        .await?
        .try_get(0)?;
    if current {
        return Ok(false);
    }
    let install_sql = spec.install_sql()?;
    client.batch_execute("BEGIN").await?;
    let result = async {
        client
            .batch_execute("SELECT set_config('pg2cb.internal_ddl', 'on', true)")
            .await?;
        client.batch_execute(&install_sql).await?;
        Ok::<(), SourceError>(())
    }
    .await;
    if let Err(error) = result {
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(error);
    }
    if let Err(error) = client.batch_execute("COMMIT").await {
        let _ = client.batch_execute("ROLLBACK").await;
        return Err(error.into());
    }
    Ok(true)
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
