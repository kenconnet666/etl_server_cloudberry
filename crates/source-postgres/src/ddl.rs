//! Transactional DDL notifications emitted from the source metadata schema.
//!
//! The trigger records catalog-shaped facts from PostgreSQL's event-trigger API.  It never parses
//! `current_query()` (which is lossy for quoted identifiers and multi-statement DDL), and it emits
//! one logical message in the same transaction as the DDL.

use cloudberry_etl_core::change::DdlMessage;
use serde::{Deserialize, Serialize};

use crate::{SourceError, SourceResult, sql::quote_identifier};

pub const DDL_MESSAGE_PREFIX: &str = "pg2cloudberry_ddl_v1";
pub const DDL_MESSAGE_VERSION: u16 = 1;

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
        Ok(())
    }

    /// Render an idempotent installer. All user-provided names are identifiers; no SQL values are
    /// interpolated. The returned batch is intended to run on the source coordinator only.
    pub fn install_sql(&self) -> SourceResult<String> {
        self.validate()?;
        let schema = quote_identifier(&self.metadata_schema)?;
        let trigger = quote_identifier(&self.trigger_name)?;
        let function = quote_identifier("emit_ddl_event")?;
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
    fingerprint text;
    payload jsonb;
BEGIN
{worker_guard}
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'class_id', classid::text,
        'object_id', objid::text,
        'object_type', object_type,
        'schema_name', schema_name,
        'object_identity', object_identity,
        'command_tag', command_tag
    ) ORDER BY objid), '[]'::jsonb)
      INTO commands
      FROM pg_event_trigger_ddl_commands();

    SELECT COALESCE(array_agg(objid ORDER BY objid), '{{}}'::oid[])
      INTO relation_ids
      FROM pg_event_trigger_ddl_commands()
     WHERE classid = 'pg_class'::regclass;

    SELECT md5(COALESCE(
        (SELECT jsonb_agg({schema}.{fingerprint}(objid) ORDER BY objid)::text
           FROM pg_event_trigger_ddl_commands()
          WHERE classid = 'pg_class'::regclass),
        '[]')) INTO fingerprint;
    payload := jsonb_build_object(
        'version', {version},
        'command_tag', TG_TAG,
        'relation_ids', to_jsonb(ARRAY(SELECT value::text FROM unnest(relation_ids) AS value)),
        'schema_fingerprint', fingerprint,
        'commands', commands
    );

    INSERT INTO {schema}.ddl_audit(command_tag, relation_ids, schema_fingerprint, payload)
    VALUES (TG_TAG, relation_ids, fingerprint, payload);

    PERFORM pg_logical_emit_message(true, '{prefix}', payload::text);
END;
$pg2cb$;

DROP EVENT TRIGGER IF EXISTS {trigger};
CREATE EVENT TRIGGER {trigger}
    ON ddl_command_end
    EXECUTE FUNCTION {schema}.{function}();
"#,
            version = DDL_MESSAGE_VERSION,
            prefix = DDL_MESSAGE_PREFIX,
        ))
    }

    pub fn uninstall_sql(&self) -> SourceResult<String> {
        self.validate()?;
        Ok(format!(
            "DROP EVENT TRIGGER IF EXISTS {}",
            quote_identifier(&self.trigger_name)?
        ))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DdlEnvelope {
    version: u16,
    command_tag: String,
    #[serde(default)]
    relation_ids: Vec<serde_json::Value>,
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
    Ok(DdlMessage {
        version: envelope.version,
        command_tag: envelope.command_tag,
        relation_ids,
        schema_fingerprint: envelope.schema_fingerprint,
    })
}

fn validate_schema(value: &str) -> SourceResult<()> {
    if value.is_empty() || value.contains('\0') || value.len() > 63 {
        return Err(SourceError::InvalidIdentifier(value.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installer_is_transactional_and_does_not_use_current_query() {
        let sql = DdlInstallSpec::default().install_sql().unwrap();
        assert!(sql.contains("pg_logical_emit_message(true"));
        assert!(sql.contains("ON ddl_command_end"));
        assert!(sql.contains("to_regclass('pg_catalog.pg_dist_node')"));
        assert!(!sql.contains("current_query"));
    }

    #[test]
    fn decoder_rejects_unknown_prefix_and_version() {
        assert!(decode_ddl_message("other", b"{}").is_err());
        let payload = br#"{"version":2,"command_tag":"ALTER TABLE","schema_fingerprint":"x"}"#;
        assert!(decode_ddl_message(DDL_MESSAGE_PREFIX, payload).is_err());
    }

    #[test]
    fn decoder_maps_relation_ids() {
        let payload = br#"{"version":1,"command_tag":"ALTER TABLE","relation_ids":["42"],"schema_fingerprint":"abc"}"#;
        let message = decode_ddl_message(DDL_MESSAGE_PREFIX, payload).unwrap();
        assert_eq!(message.relation_ids, vec![42]);
    }
}
