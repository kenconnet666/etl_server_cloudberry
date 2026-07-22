//! Source-side consistency boundaries for online reconciliation.
//!
//! A temporary logical slot pins WAL and exports a snapshot at one exact consistent point. The
//! owning replication connection stays private so no command can invalidate that snapshot before
//! the caller imports it. Once imported, [`ReconciliationBoundaryGuard::cleanup`] drops the slot;
//! closing the connection is a second, server-enforced cleanup path for every error and `Drop`.

use std::{fmt, str::FromStr};

use cloudberry_etl_core::lsn::PgLsn;
use replication_postgres::SimpleQueryMessage;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client as SqlClient;
use uuid::Uuid;

use crate::{
    SourceError, SourceResult,
    connection::connect_replication,
    snapshot_slot::{
        OUTPUT_PLUGIN, drop_replication_slot_sql, parse_create_slot_response, validate_slot_name,
    },
    sql::quote_identifier,
};

pub const RECONCILIATION_MARKER_PREFIX: &str = "pg2cloudberry_reconcile_v1";
const RECONCILIATION_MARKER_VERSION: u16 = 1;
const MAX_MARKER_PAYLOAD_BYTES: usize = 512;

/// A source snapshot and WAL cutoff produced atomically by PostgreSQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationBoundary {
    pub slot_name: String,
    pub snapshot_name: String,
    pub consistent_point: PgLsn,
    pub source_database: String,
    pub system_identifier: u64,
    pub timeline: u32,
}

/// The versioned message placed after a reconciliation boundary.
///
/// Its transaction gives a quiet source a commit beyond `boundary_lsn`. The main slot therefore
/// has a deterministic transaction at which it can stop applying without acknowledging past the
/// boundary. The WAL assembler intentionally carries the empty transaction's end LSN but does not
/// turn this control message into a target row change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconciliationMarker {
    pub marker_id: Uuid,
    pub boundary_lsn: PgLsn,
}

/// A marker whose transaction has committed successfully on the source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmittedReconciliationMarker {
    pub marker: ReconciliationMarker,
    pub message_lsn: PgLsn,
}

/// Reads the writable source's current WAL head for the reconciliation lag gate.
///
/// This is intentionally separate from logical-slot retained bytes: retained bytes measure WAL
/// pinning from `restart_lsn`, while scheduling needs the nonnegative distance from the target's
/// durable apply checkpoint to the source head.
pub async fn current_wal_lsn(client: &SqlClient) -> SourceResult<PgLsn> {
    let text: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .try_get(0)?;
    let lsn = PgLsn::from_str(&text).map_err(|_| SourceError::InvalidLsn(text))?;
    if lsn == PgLsn::ZERO {
        return Err(SourceError::contract(
            "pg_current_wal_lsn returned the zero LSN",
        ));
    }
    Ok(lsn)
}

/// Owns the replication session and its temporary reconciliation slot.
pub struct ReconciliationBoundaryGuard {
    client: Option<replication_postgres::Client>,
    boundary: ReconciliationBoundary,
}

impl fmt::Debug for ReconciliationBoundaryGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReconciliationBoundaryGuard")
            .field("boundary", &self.boundary)
            .field("cleaned", &self.client.is_none())
            .finish_non_exhaustive()
    }
}

impl ReconciliationBoundaryGuard {
    /// Create a temporary `pgoutput` slot and export its snapshot.
    ///
    /// The only public constructor uses the canonical replication connector. The returned guard
    /// must remain alive until every source reader has imported `snapshot_name`.
    pub async fn create(replication_dsn: &str, slot_name: &str) -> SourceResult<Self> {
        validate_slot_name(slot_name)?;
        let client = connect_replication(replication_dsn).await?;
        Self::create_with_client(client, slot_name).await
    }

    async fn create_with_client(
        client: replication_postgres::Client,
        slot_name: &str,
    ) -> SourceResult<Self> {
        let identity_messages = client.simple_query("IDENTIFY_SYSTEM").await?;
        let identity = parse_identify_system_response(&identity_messages)?;
        let sql = create_temporary_exported_snapshot_sql(slot_name)?;
        let messages = client.simple_query(&sql).await?;
        let snapshot = match parse_create_slot_response(&messages, slot_name) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // The command has already created the temporary slot. Try the protocol cleanup,
                // then close this owning connection regardless; PostgreSQL drops temporary slots
                // when their session ends even if the response or network was malformed.
                if let Err(cleanup_error) = client
                    .simple_query(&drop_replication_slot_sql(slot_name)?)
                    .await
                {
                    tracing::warn!(
                        slot = %slot_name,
                        %cleanup_error,
                        "failed to clean up temporary reconciliation slot after parse error"
                    );
                }
                return Err(error);
            }
        };
        Ok(Self {
            client: Some(client),
            boundary: ReconciliationBoundary {
                slot_name: snapshot.slot_name,
                snapshot_name: snapshot.snapshot_name,
                consistent_point: snapshot.consistent_point,
                source_database: identity.database,
                system_identifier: identity.system_identifier,
                timeline: identity.timeline,
            },
        })
    }

    #[must_use]
    pub fn boundary(&self) -> &ReconciliationBoundary {
        &self.boundary
    }

    #[must_use]
    pub fn is_cleaned(&self) -> bool {
        self.client.is_none()
    }

    /// Drop the temporary slot and close its exporting replication session.
    ///
    /// This operation is idempotent. On a protocol error the client is still closed, which asks
    /// PostgreSQL to remove the session-owned slot; the error is returned because the caller must
    /// not assume server cleanup completed before retry/recovery observes it.
    pub async fn cleanup(&mut self) -> SourceResult<()> {
        let Some(client) = self.client.take() else {
            return Ok(());
        };
        let result = client
            .simple_query(&drop_replication_slot_sql(&self.boundary.slot_name)?)
            .await;
        drop(client);
        result?;
        Ok(())
    }
}

impl Drop for ReconciliationBoundaryGuard {
    fn drop(&mut self) {
        if self.client.is_some() {
            tracing::warn!(
                slot = %self.boundary.slot_name,
                "temporary reconciliation slot guard dropped before explicit cleanup"
            );
        }
        // Dropping the last client handle closes the owning PostgreSQL session. Temporary slots
        // are session-scoped, so this is a server-enforced fallback rather than best-effort SQL.
        self.client.take();
    }
}

/// Emit and commit a transactional marker on the same database as `boundary`.
///
/// The marker uses bind parameters and a bounded, versioned payload. A canonical session and the
/// boundary database are checked before emission. `marker_id` is caller-supplied so an ambiguous
/// commit can be retried with the same identity without inventing a second reconciliation run.
pub async fn emit_transactional_marker(
    client: &mut SqlClient,
    boundary: &ReconciliationBoundary,
    marker_id: Uuid,
) -> SourceResult<EmittedReconciliationMarker> {
    if marker_id.is_nil() {
        return Err(SourceError::contract(
            "reconciliation marker UUID must not be nil",
        ));
    }
    let marker = ReconciliationMarker {
        marker_id,
        boundary_lsn: boundary.consistent_point,
    };
    let payload = encode_reconciliation_marker(&marker)?;
    let transaction = client.transaction().await?;
    let settings = transaction
        .query_one(
            "SELECT current_database(), current_setting('client_encoding'),
                    current_setting('DateStyle'), current_setting('IntervalStyle'),
                    current_setting('TimeZone'), current_setting('extra_float_digits'),
                    current_setting('bytea_output')",
            &[],
        )
        .await?;
    let actual_database: String = settings.try_get(0)?;
    let validation = validate_marker_session(
        &actual_database,
        &boundary.source_database,
        &settings.try_get::<_, String>(1)?,
        &settings.try_get::<_, String>(2)?,
        &settings.try_get::<_, String>(3)?,
        &settings.try_get::<_, String>(4)?,
        &settings.try_get::<_, String>(5)?,
        &settings.try_get::<_, String>(6)?,
    );
    if let Err(error) = validation {
        transaction.rollback().await?;
        return Err(error);
    }
    let row = transaction
        .query_one(
            "SELECT pg_logical_emit_message(true, $1::text, $2::text)::text",
            &[&RECONCILIATION_MARKER_PREFIX, &payload],
        )
        .await?;
    let message_lsn_text: String = row.try_get(0)?;
    let message_lsn = PgLsn::from_str(&message_lsn_text)
        .map_err(|_| SourceError::InvalidLsn(message_lsn_text.clone()))?;
    if message_lsn <= boundary.consistent_point {
        transaction.rollback().await?;
        return Err(SourceError::contract(format!(
            "reconciliation marker LSN {message_lsn} did not advance beyond boundary {}",
            boundary.consistent_point
        )));
    }
    transaction.commit().await?;
    Ok(EmittedReconciliationMarker {
        marker,
        message_lsn,
    })
}

#[must_use]
pub fn is_reconciliation_marker_prefix(prefix: &str) -> bool {
    prefix == RECONCILIATION_MARKER_PREFIX
}

/// Decode and strictly validate a marker received from pgoutput.
pub fn decode_reconciliation_marker(
    prefix: &str,
    payload: &[u8],
) -> SourceResult<ReconciliationMarker> {
    if !is_reconciliation_marker_prefix(prefix) {
        return Err(SourceError::ReplicationProtocol(format!(
            "unknown reconciliation marker prefix `{prefix}`"
        )));
    }
    if payload.is_empty() || payload.len() > MAX_MARKER_PAYLOAD_BYTES {
        return Err(SourceError::ReplicationProtocol(format!(
            "reconciliation marker payload size {} is outside 1..={MAX_MARKER_PAYLOAD_BYTES}",
            payload.len()
        )));
    }
    let envelope: MarkerEnvelope = serde_json::from_slice(payload).map_err(|error| {
        SourceError::ReplicationProtocol(format!("invalid reconciliation marker JSON: {error}"))
    })?;
    if envelope.version != RECONCILIATION_MARKER_VERSION {
        return Err(SourceError::ReplicationProtocol(format!(
            "unsupported reconciliation marker version {}",
            envelope.version
        )));
    }
    if envelope.marker_id.is_nil() || envelope.boundary_lsn == PgLsn::ZERO {
        return Err(SourceError::ReplicationProtocol(
            "reconciliation marker has a nil UUID or zero boundary LSN".to_owned(),
        ));
    }
    Ok(ReconciliationMarker {
        marker_id: envelope.marker_id,
        boundary_lsn: envelope.boundary_lsn,
    })
}

/// Build the replication-protocol command for a session-owned slot.
pub fn create_temporary_exported_snapshot_sql(slot_name: &str) -> SourceResult<String> {
    validate_slot_name(slot_name)?;
    Ok(format!(
        "CREATE_REPLICATION_SLOT {} TEMPORARY LOGICAL {OUTPUT_PLUGIN} EXPORT_SNAPSHOT",
        quote_identifier(slot_name)?
    ))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MarkerEnvelope {
    version: u16,
    marker_id: Uuid,
    boundary_lsn: PgLsn,
}

fn encode_reconciliation_marker(marker: &ReconciliationMarker) -> SourceResult<String> {
    let payload = serde_json::to_string(&MarkerEnvelope {
        version: RECONCILIATION_MARKER_VERSION,
        marker_id: marker.marker_id,
        boundary_lsn: marker.boundary_lsn,
    })?;
    if payload.len() > MAX_MARKER_PAYLOAD_BYTES {
        return Err(SourceError::contract(
            "encoded reconciliation marker exceeds the payload limit",
        ));
    }
    Ok(payload)
}

#[derive(Debug, PartialEq, Eq)]
struct ReplicationIdentity {
    system_identifier: u64,
    timeline: u32,
    database: String,
}

fn parse_identify_system_response(
    messages: &[SimpleQueryMessage],
) -> SourceResult<ReplicationIdentity> {
    let mut row = None;
    for message in messages {
        if let SimpleQueryMessage::Row(value) = message {
            if row.is_some() {
                return Err(SourceError::InvalidSlotSnapshotResponse(
                    "IDENTIFY_SYSTEM returned more than one row".to_owned(),
                ));
            }
            row = Some(value);
        }
    }
    let row = row.ok_or_else(|| {
        SourceError::InvalidSlotSnapshotResponse(
            "IDENTIFY_SYSTEM returned no result row".to_owned(),
        )
    })?;
    parse_identity_fields(
        row.try_get("systemid")?,
        row.try_get("timeline")?,
        row.try_get("dbname")?,
    )
}

fn parse_identity_fields(
    system_identifier: Option<&str>,
    timeline: Option<&str>,
    database: Option<&str>,
) -> SourceResult<ReplicationIdentity> {
    let system_identifier = system_identifier
        .ok_or_else(|| {
            SourceError::InvalidSlotSnapshotResponse("IDENTIFY_SYSTEM omitted systemid".to_owned())
        })?
        .parse::<u64>()
        .map_err(|_| {
            SourceError::InvalidSlotSnapshotResponse(
                "IDENTIFY_SYSTEM returned an invalid systemid".to_owned(),
            )
        })?;
    let timeline = timeline
        .ok_or_else(|| {
            SourceError::InvalidSlotSnapshotResponse("IDENTIFY_SYSTEM omitted timeline".to_owned())
        })?
        .parse::<u32>()
        .map_err(|_| {
            SourceError::InvalidSlotSnapshotResponse(
                "IDENTIFY_SYSTEM returned an invalid timeline".to_owned(),
            )
        })?;
    let database = database.ok_or_else(|| {
        SourceError::InvalidSlotSnapshotResponse(
            "logical IDENTIFY_SYSTEM omitted dbname".to_owned(),
        )
    })?;
    if system_identifier == 0
        || timeline == 0
        || database.is_empty()
        || database.chars().any(char::is_control)
    {
        return Err(SourceError::InvalidSlotSnapshotResponse(
            "IDENTIFY_SYSTEM returned an invalid source identity".to_owned(),
        ));
    }
    Ok(ReplicationIdentity {
        system_identifier,
        timeline,
        database: database.to_owned(),
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_marker_session(
    actual_database: &str,
    expected_database: &str,
    client_encoding: &str,
    date_style: &str,
    interval_style: &str,
    time_zone: &str,
    extra_float_digits: &str,
    bytea_output: &str,
) -> SourceResult<()> {
    if actual_database != expected_database {
        return Err(SourceError::contract(format!(
            "reconciliation marker database `{actual_database}` does not match boundary database `{expected_database}`"
        )));
    }
    let canonical = client_encoding.eq_ignore_ascii_case("UTF8")
        && date_style.replace(' ', "").eq_ignore_ascii_case("ISO,YMD")
        && interval_style.eq_ignore_ascii_case("postgres")
        && time_zone.eq_ignore_ascii_case("UTC")
        && extra_float_digits == "3"
        && bytea_output.eq_ignore_ascii_case("hex");
    if !canonical {
        return Err(SourceError::contract(format!(
            "reconciliation marker session is not canonical: client_encoding={client_encoding}, DateStyle={date_style}, IntervalStyle={interval_style}, TimeZone={time_zone}, extra_float_digits={extra_float_digits}, bytea_output={bytea_output}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary() -> ReconciliationBoundary {
        ReconciliationBoundary {
            slot_name: "reconcile_slot".to_owned(),
            snapshot_name: "00000003-1".to_owned(),
            consistent_point: PgLsn::new(0x10),
            source_database: "source".to_owned(),
            system_identifier: 42,
            timeline: 1,
        }
    }

    #[test]
    fn temporary_slot_command_is_exact_and_identifier_safe() {
        assert_eq!(
            create_temporary_exported_snapshot_sql("reconcile_01").unwrap(),
            "CREATE_REPLICATION_SLOT \"reconcile_01\" TEMPORARY LOGICAL pgoutput EXPORT_SNAPSHOT"
        );
        for invalid in ["", "Slot", "slot-name", "slot;drop", "a\0b"] {
            assert!(create_temporary_exported_snapshot_sql(invalid).is_err());
        }
    }

    #[test]
    fn marker_payload_round_trips_and_rejects_noncanonical_envelopes() {
        let marker = ReconciliationMarker {
            marker_id: Uuid::parse_str("018f7777-7777-7777-8777-777777777777").unwrap(),
            boundary_lsn: PgLsn::new(0x16_b374_d848),
        };
        let payload = encode_reconciliation_marker(&marker).unwrap();
        assert_eq!(
            decode_reconciliation_marker(RECONCILIATION_MARKER_PREFIX, payload.as_bytes()).unwrap(),
            marker
        );
        assert!(decode_reconciliation_marker("other", payload.as_bytes()).is_err());
        assert!(
            decode_reconciliation_marker(
                RECONCILIATION_MARKER_PREFIX,
                br#"{"version":2,"marker_id":"018f7777-7777-7777-8777-777777777777","boundary_lsn":"0/00000010"}"#,
            )
            .is_err()
        );
        assert!(
            decode_reconciliation_marker(
                RECONCILIATION_MARKER_PREFIX,
                br#"{"version":1,"marker_id":"00000000-0000-0000-0000-000000000000","boundary_lsn":"0/00000010"}"#,
            )
            .is_err()
        );
        assert!(
            decode_reconciliation_marker(
                RECONCILIATION_MARKER_PREFIX,
                &[b'x'; MAX_MARKER_PAYLOAD_BYTES + 1],
            )
            .is_err()
        );
    }

    #[test]
    fn identify_system_fields_are_fail_closed() {
        assert_eq!(
            parse_identity_fields(Some("123"), Some("7"), Some("source")).unwrap(),
            ReplicationIdentity {
                system_identifier: 123,
                timeline: 7,
                database: "source".to_owned(),
            }
        );
        assert!(parse_identity_fields(None, Some("1"), Some("source")).is_err());
        assert!(parse_identity_fields(Some("0"), Some("1"), Some("source")).is_err());
        assert!(parse_identity_fields(Some("1"), Some("bad"), Some("source")).is_err());
        assert!(parse_identity_fields(Some("1"), Some("1"), None).is_err());
        assert!(parse_identity_fields(Some("1"), Some("1"), Some("bad\nname")).is_err());
    }

    #[test]
    fn marker_session_requires_database_and_full_canonical_profile() {
        validate_marker_session(
            "source", "source", "UTF8", "ISO, YMD", "postgres", "UTC", "3", "hex",
        )
        .unwrap();
        assert!(
            validate_marker_session(
                "other", "source", "UTF8", "ISO, YMD", "postgres", "UTC", "3", "hex",
            )
            .is_err()
        );
        assert!(
            validate_marker_session(
                "source", "source", "UTF8", "SQL, DMY", "postgres", "UTC", "3", "hex",
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn cleanup_is_idempotent_after_the_session_is_closed() {
        let mut guard = ReconciliationBoundaryGuard {
            client: None,
            boundary: boundary(),
        };
        guard.cleanup().await.unwrap();
        guard.cleanup().await.unwrap();
        assert!(guard.is_cleaned());
    }
}
