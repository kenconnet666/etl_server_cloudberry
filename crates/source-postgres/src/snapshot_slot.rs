//! Replication-slot exported snapshots used by initial table loads.
//!
//! PostgreSQL exposes `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` only through the replication
//! protocol.  The forked client is therefore deliberately contained in this module.  A guard
//! owns that client until every ordinary SQL snapshot reader has imported the returned snapshot;
//! callers cannot issue another replication command while the guard is alive.

use std::fmt;
use std::str::FromStr;

use cloudberry_etl_core::lsn::PgLsn;
use replication_postgres::SimpleQueryMessage;

use crate::{SourceError, SourceResult, connection::connect_replication, sql::quote_identifier};

pub(crate) const OUTPUT_PLUGIN: &str = "pgoutput";

/// The server-side values returned by `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSlotSnapshot {
    pub slot_name: String,
    pub consistent_point: PgLsn,
    pub snapshot_name: String,
}

/// Lifecycle state of an exported-snapshot guard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSlotState {
    /// The slot exists, but one or more snapshot readers still need to register readiness.
    Created,
    /// Every configured reader has imported the snapshot and the replication connection may be
    /// released.
    ReadersReady,
    /// The replication connection was released.  This state is terminal.
    Released,
}

/// Owns the replication connection that keeps an exported snapshot valid.
///
/// The client is intentionally private and there is no `Deref` implementation.  The only
/// operation available during the guard lifetime is registering snapshot readers; this prevents
/// an accidental `START_REPLICATION` or another replication command from racing the initial
/// snapshot.
pub struct SnapshotSlotGuard {
    client: Option<replication_postgres::Client>,
    snapshot: ReplicationSlotSnapshot,
    expected_readers: usize,
    ready_readers: usize,
    state: SnapshotSlotState,
}

impl fmt::Debug for SnapshotSlotGuard {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotSlotGuard")
            .field("snapshot", &self.snapshot)
            .field("expected_readers", &self.expected_readers)
            .field("ready_readers", &self.ready_readers)
            .field("state", &self.state)
            .finish_non_exhaustive()
    }
}

impl SnapshotSlotGuard {
    /// Create a logical `pgoutput` slot and export its repeatable-read snapshot.
    ///
    /// `reader_count` is the number of independent SQL connections that will execute
    /// `SET TRANSACTION SNAPSHOT` before [`Self::release`] is called.  The DSN is passed through
    /// the existing TLS-enabled replication connection entry point.
    pub async fn create(
        replication_dsn: &str,
        slot_name: &str,
        reader_count: usize,
    ) -> SourceResult<Self> {
        if reader_count == 0 {
            return Err(SourceError::contract(
                "an exported snapshot guard requires at least one reader",
            ));
        }
        validate_slot_name(slot_name)?;
        let client = connect_replication(replication_dsn).await?;
        Self::create_with_client(client, slot_name, reader_count).await
    }

    /// Create a guard from an already established replication client.
    ///
    /// This is kept crate-visible so connection setup remains centralized in
    /// [`crate::connection::connect_replication`].
    pub(crate) async fn create_with_client(
        client: replication_postgres::Client,
        slot_name: &str,
        reader_count: usize,
    ) -> SourceResult<Self> {
        if reader_count == 0 {
            return Err(SourceError::contract(
                "an exported snapshot guard requires at least one reader",
            ));
        }
        let sql = create_exported_snapshot_sql(slot_name)?;
        let messages = client.simple_query(&sql).await?;
        let snapshot = match parse_create_slot_response(&messages, slot_name) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                // The command has already created the requested slot.  If a server/fork response
                // cannot be decoded, remove only that newly requested name before returning the
                // parse error; this avoids leaking WAL retention on an otherwise failed start.
                if let Err(cleanup_error) = client
                    .simple_query(&drop_replication_slot_sql(slot_name)?)
                    .await
                {
                    tracing::warn!(
                        slot = %slot_name,
                        %cleanup_error,
                        "failed to clean up replication slot after response parse error"
                    );
                }
                return Err(error);
            }
        };
        Ok(Self {
            client: Some(client),
            snapshot,
            expected_readers: reader_count,
            ready_readers: 0,
            state: SnapshotSlotState::Created,
        })
    }

    /// Values returned by PostgreSQL for the created slot and exported snapshot.
    #[must_use]
    pub fn snapshot(&self) -> &ReplicationSlotSnapshot {
        &self.snapshot
    }

    /// Current lifecycle state.
    #[must_use]
    pub const fn state(&self) -> SnapshotSlotState {
        self.state
    }

    /// Number of readers required before release is allowed.
    #[must_use]
    pub const fn expected_readers(&self) -> usize {
        self.expected_readers
    }

    /// Number of readers that have imported the snapshot and called this method.
    #[must_use]
    pub const fn ready_readers(&self) -> usize {
        self.ready_readers
    }

    /// Register one SQL snapshot reader after it has successfully executed `SET TRANSACTION
    /// SNAPSHOT '<snapshot_name>'`.
    pub fn mark_reader_ready(&mut self) -> SourceResult<()> {
        match self.state {
            SnapshotSlotState::Released => Err(SourceError::SnapshotGuardReleased),
            SnapshotSlotState::ReadersReady => Err(SourceError::SnapshotReadersComplete {
                expected: self.expected_readers,
            }),
            SnapshotSlotState::Created => {
                if self.ready_readers >= self.expected_readers {
                    return Err(SourceError::SnapshotReadersComplete {
                        expected: self.expected_readers,
                    });
                }
                self.ready_readers += 1;
                if self.ready_readers == self.expected_readers {
                    self.state = SnapshotSlotState::ReadersReady;
                }
                Ok(())
            }
        }
    }

    /// Release the replication connection after all readers have imported the snapshot.
    ///
    /// Releasing does not drop the logical slot; the same slot is consumed later by the WAL
    /// streamer.  It only drops the connection that keeps the exported snapshot alive.  A guard
    /// can be retried after a pending-reader error because the method borrows, rather than
    /// consumes, `self`.
    pub fn release(&mut self) -> SourceResult<()> {
        match self.state {
            SnapshotSlotState::Released => Err(SourceError::SnapshotGuardReleased),
            SnapshotSlotState::Created => Err(SourceError::SnapshotReadersPending {
                ready: self.ready_readers,
                expected: self.expected_readers,
            }),
            SnapshotSlotState::ReadersReady => {
                // Dropping the client closes the replication session and releases the server's
                // exported-snapshot hold.  The logical slot itself intentionally remains.
                self.client.take();
                self.state = SnapshotSlotState::Released;
                Ok(())
            }
        }
    }
}

impl Drop for SnapshotSlotGuard {
    fn drop(&mut self) {
        if self.state != SnapshotSlotState::Released {
            tracing::warn!(
                slot = %self.snapshot.slot_name,
                ready_readers = self.ready_readers,
                expected_readers = self.expected_readers,
                "exported snapshot guard dropped before explicit release"
            );
        }
    }
}

/// Build the replication-protocol command.  `pgoutput` is a fixed plugin; only the slot name is
/// supplied by configuration and is quoted as an identifier.
pub fn create_exported_snapshot_sql(slot_name: &str) -> SourceResult<String> {
    validate_slot_name(slot_name)?;
    Ok(format!(
        "CREATE_REPLICATION_SLOT {} LOGICAL {OUTPUT_PLUGIN} EXPORT_SNAPSHOT",
        quote_identifier(slot_name)?
    ))
}

pub(crate) fn drop_replication_slot_sql(slot_name: &str) -> SourceResult<String> {
    validate_slot_name(slot_name)?;
    Ok(format!(
        "DROP_REPLICATION_SLOT {}",
        quote_identifier(slot_name)?
    ))
}

pub(crate) fn validate_slot_name(slot_name: &str) -> SourceResult<()> {
    if slot_name.is_empty()
        || slot_name.len() > 63
        || !slot_name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(SourceError::InvalidIdentifier(slot_name.to_owned()));
    }
    Ok(())
}

/// Parse the four-column row returned by the replication command.
///
/// Keeping this parser independent of the client makes malformed server responses testable and
/// ensures the wire values are validated before entering the core model.
fn parse_slot_fields(
    slot_name: &str,
    returned_slot: Option<&str>,
    consistent_point: Option<&str>,
    snapshot_name: Option<&str>,
    output_plugin: Option<&str>,
) -> SourceResult<ReplicationSlotSnapshot> {
    let returned_slot = required_field("slot_name", returned_slot)?;
    if returned_slot != slot_name {
        return Err(SourceError::InvalidSlotSnapshotResponse(format!(
            "server returned slot `{returned_slot}`, expected `{slot_name}`"
        )));
    }
    let consistent_point = required_field("consistent_point", consistent_point)?;
    let consistent_point = PgLsn::from_str(consistent_point).map_err(|_| {
        SourceError::InvalidSlotSnapshotResponse(format!(
            "invalid consistent_point `{consistent_point}`"
        ))
    })?;
    let snapshot_name = required_field("snapshot_name", snapshot_name)?;
    if snapshot_name.is_empty()
        || snapshot_name
            .chars()
            .any(|character| matches!(character, '\0' | '\n' | '\r'))
    {
        return Err(SourceError::InvalidSlotSnapshotResponse(
            "snapshot_name is empty or contains a control character".to_owned(),
        ));
    }
    let output_plugin = required_field("output_plugin", output_plugin)?;
    if output_plugin != OUTPUT_PLUGIN {
        return Err(SourceError::InvalidSlotSnapshotResponse(format!(
            "server returned output plugin `{output_plugin}`, expected `{OUTPUT_PLUGIN}`"
        )));
    }
    Ok(ReplicationSlotSnapshot {
        slot_name: returned_slot.to_owned(),
        consistent_point,
        snapshot_name: snapshot_name.to_owned(),
    })
}

fn required_field<'value>(name: &str, value: Option<&'value str>) -> SourceResult<&'value str> {
    value.filter(|value| !value.is_empty()).ok_or_else(|| {
        SourceError::InvalidSlotSnapshotResponse(format!("missing or empty `{name}` field"))
    })
}

pub(crate) fn parse_create_slot_response(
    messages: &[SimpleQueryMessage],
    requested_slot: &str,
) -> SourceResult<ReplicationSlotSnapshot> {
    let mut row = None;
    for message in messages {
        if let SimpleQueryMessage::Row(value) = message {
            if row.is_some() {
                return Err(SourceError::InvalidSlotSnapshotResponse(
                    "replication command returned more than one row".to_owned(),
                ));
            }
            row = Some(value);
        }
    }
    let row = row.ok_or_else(|| {
        SourceError::InvalidSlotSnapshotResponse(
            "replication command returned no result row".to_owned(),
        )
    })?;
    parse_slot_fields(
        requested_slot,
        row.try_get("slot_name")?,
        row.try_get("consistent_point")?,
        row.try_get("snapshot_name")?,
        row.try_get("output_plugin")?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_quotes_slot_and_uses_replication_protocol() {
        let sql = create_exported_snapshot_sql("slot_name").unwrap();
        assert_eq!(
            sql,
            "CREATE_REPLICATION_SLOT \"slot_name\" LOGICAL pgoutput EXPORT_SNAPSHOT"
        );
        assert!(create_exported_snapshot_sql("").is_err());
        assert!(create_exported_snapshot_sql("Slot").is_err());
        assert!(create_exported_snapshot_sql("slot-name").is_err());
    }

    #[test]
    fn parser_converts_lsn_and_rejects_mismatches() {
        let snapshot = parse_slot_fields(
            "slot_a",
            Some("slot_a"),
            Some("16/B374D848"),
            Some("00000003-1"),
            Some("pgoutput"),
        )
        .unwrap();
        assert_eq!(snapshot.slot_name, "slot_a");
        assert_eq!(snapshot.consistent_point.to_string(), "16/B374D848");
        assert_eq!(snapshot.snapshot_name, "00000003-1");
        assert!(
            parse_slot_fields(
                "slot_a",
                Some("slot_b"),
                Some("0/1"),
                Some("snap"),
                Some("pgoutput"),
            )
            .is_err()
        );
        assert!(
            parse_slot_fields(
                "slot_a",
                Some("slot_a"),
                Some("bad"),
                Some("snap"),
                Some("pgoutput"),
            )
            .is_err()
        );
    }

    #[test]
    fn guard_state_requires_every_reader_before_release() {
        let mut guard = SnapshotSlotGuard {
            client: None,
            snapshot: ReplicationSlotSnapshot {
                slot_name: "slot".to_owned(),
                consistent_point: PgLsn::new(1),
                snapshot_name: "snap".to_owned(),
            },
            expected_readers: 2,
            ready_readers: 0,
            state: SnapshotSlotState::Created,
        };
        assert!(matches!(
            guard.release(),
            Err(SourceError::SnapshotReadersPending {
                ready: 0,
                expected: 2
            })
        ));
        guard.mark_reader_ready().unwrap();
        assert_eq!(guard.state(), SnapshotSlotState::Created);
        assert!(matches!(
            guard.release(),
            Err(SourceError::SnapshotReadersPending {
                ready: 1,
                expected: 2
            })
        ));
        guard.mark_reader_ready().unwrap();
        assert_eq!(guard.state(), SnapshotSlotState::ReadersReady);
        guard.release().unwrap();
        assert_eq!(guard.state(), SnapshotSlotState::Released);
        assert!(matches!(
            guard.release(),
            Err(SourceError::SnapshotGuardReleased)
        ));
        assert!(matches!(
            guard.mark_reader_ready(),
            Err(SourceError::SnapshotGuardReleased)
        ));
    }

    #[test]
    fn parser_rejects_missing_fields_and_wrong_plugin() {
        assert!(
            parse_slot_fields("slot", Some("slot"), Some("0/1"), None, Some("pgoutput"),).is_err()
        );
        assert!(
            parse_slot_fields(
                "slot",
                Some("slot"),
                Some("0/1"),
                Some("snap"),
                Some("wal2json"),
            )
            .is_err()
        );
    }
}
