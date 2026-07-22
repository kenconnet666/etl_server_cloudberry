//! Bounded decoding of canonical PostgreSQL `COPY ... FORMAT text` output.
//!
//! The reader query must emit key columns first, followed by value columns,
//! with a tab delimiter, `\N` as the NULL marker, UTF-8 encoding, and no
//! header. Rows are incorporated into an order-independent digest as soon as
//! their terminating newline arrives; the stream never retains earlier rows.

use std::mem;

use bytes::Bytes;
use cloudberry_etl_core::change::Cell;
use thiserror::Error;

use crate::reconcile::{
    CanonicalMultisetDigest, CanonicalMultisetDigestBuilder, CanonicalRow, DigestContext,
    ReconcileError,
};

const COPY_DELIMITER: u8 = b'\t';
const COPY_RECORD_END: u8 = b'\n';
const COPY_ESCAPE: u8 = b'\\';
const COPY_NULL: &[u8] = b"\\N";

/// A constant-memory decoder and digest sink for canonical `COPY text` data.
///
/// Chunk boundaries may occur between any two bytes, including inside an
/// escape sequence. `max_row_bytes` counts wire bytes other than the final
/// record newline, including delimiters and escape bytes. Once a call fails,
/// the stream remains failed and can never return a digest.
#[derive(Debug)]
pub struct CanonicalCopyTextDigestStream<'context> {
    builder: CanonicalMultisetDigestBuilder<'context>,
    expected_columns: usize,
    key_columns: usize,
    max_row_bytes: usize,
    row_wire_bytes: usize,
    row_index: u64,
    fields: Vec<Vec<u8>>,
    field: Vec<u8>,
    escaped: bool,
    failure: Option<CanonicalCopyError>,
}

impl<'context> CanonicalCopyTextDigestStream<'context> {
    pub fn new(
        context: &'context DigestContext,
        max_row_bytes: usize,
    ) -> Result<Self, CanonicalCopyError> {
        if max_row_bytes == 0 {
            return Err(CanonicalCopyError::InvalidMaxRowBytes);
        }

        let expected_columns = context
            .key_columns
            .len()
            .checked_add(context.value_columns.len())
            .ok_or(CanonicalCopyError::ColumnCountOverflow)?;
        let builder = CanonicalMultisetDigestBuilder::new(context)?;

        Ok(Self {
            builder,
            expected_columns,
            key_columns: context.key_columns.len(),
            max_row_bytes,
            row_wire_bytes: 0,
            row_index: 0,
            fields: Vec::with_capacity(expected_columns),
            field: Vec::new(),
            escaped: false,
            failure: None,
        })
    }

    /// Consumes the next arbitrary chunk of `COPY` output.
    pub fn feed(&mut self, chunk: impl AsRef<[u8]>) -> Result<(), CanonicalCopyError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }

        for &byte in chunk.as_ref() {
            if let Err(error) = self.consume_byte(byte) {
                self.failure = Some(error.clone());
                return Err(error);
            }
        }
        Ok(())
    }

    /// Completes the stream. A partial final record is rejected even when all
    /// of its fields would otherwise be valid.
    pub fn finish(self) -> Result<CanonicalMultisetDigest, CanonicalCopyError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.escaped {
            return Err(CanonicalCopyError::DanglingEscape {
                row: self.row_index,
                column: self.fields.len(),
                byte_offset: self.field.len().saturating_sub(1),
            });
        }
        if self.row_wire_bytes != 0 || !self.fields.is_empty() || !self.field.is_empty() {
            return Err(CanonicalCopyError::UnterminatedRow {
                row: self.row_index,
            });
        }
        Ok(self.builder.finish())
    }

    fn consume_byte(&mut self, byte: u8) -> Result<(), CanonicalCopyError> {
        if self.escaped {
            self.reserve_row_byte()?;
            self.field.push(byte);
            self.escaped = false;
            return Ok(());
        }

        match byte {
            COPY_ESCAPE => {
                self.reserve_row_byte()?;
                self.field.push(byte);
                self.escaped = true;
            }
            COPY_DELIMITER => {
                self.reserve_row_byte()?;
                // A row with N expected columns may contain at most N - 1 delimiters. Reject
                // the first excess delimiter before retaining another field so a malformed row
                // cannot turn the wire-byte limit into millions of per-field Vec allocations.
                if self.fields.len() >= self.expected_columns.saturating_sub(1) {
                    return Err(CanonicalCopyError::Arity {
                        row: self.row_index,
                        expected: self.expected_columns,
                        actual: self.fields.len().saturating_add(2),
                    });
                }
                self.fields.push(mem::take(&mut self.field));
            }
            COPY_RECORD_END => self.complete_row()?,
            b'\r' => {
                return Err(CanonicalCopyError::UnescapedCarriageReturn {
                    row: self.row_index,
                });
            }
            _ => {
                self.reserve_row_byte()?;
                self.field.push(byte);
            }
        }
        Ok(())
    }

    fn reserve_row_byte(&mut self) -> Result<(), CanonicalCopyError> {
        let next =
            self.row_wire_bytes
                .checked_add(1)
                .ok_or(CanonicalCopyError::RowSizeOverflow {
                    row: self.row_index,
                })?;
        if next > self.max_row_bytes {
            return Err(CanonicalCopyError::RowTooLarge {
                row: self.row_index,
                max_row_bytes: self.max_row_bytes,
            });
        }
        self.row_wire_bytes = next;
        Ok(())
    }

    fn complete_row(&mut self) -> Result<(), CanonicalCopyError> {
        self.fields.push(mem::take(&mut self.field));
        let raw_fields = mem::take(&mut self.fields);
        let actual_columns = raw_fields.len();
        if actual_columns != self.expected_columns {
            return Err(CanonicalCopyError::Arity {
                row: self.row_index,
                expected: self.expected_columns,
                actual: actual_columns,
            });
        }

        let mut key = Vec::with_capacity(self.key_columns);
        let mut values = Vec::with_capacity(self.expected_columns - self.key_columns);
        for (column, raw) in raw_fields.into_iter().enumerate() {
            let cell = decode_field(&raw, self.row_index, column)?;
            if column < self.key_columns {
                key.push(cell);
            } else {
                values.push(cell);
            }
        }

        let row = CanonicalRow::try_from_cells(key, values)?;
        self.builder.push(&row)?;
        self.row_index = self
            .row_index
            .checked_add(1)
            .ok_or(CanonicalCopyError::RowCountOverflow)?;
        self.row_wire_bytes = 0;
        self.fields = Vec::with_capacity(self.expected_columns);
        Ok(())
    }
}

fn decode_field(raw: &[u8], row: u64, column: usize) -> Result<Cell, CanonicalCopyError> {
    if raw == COPY_NULL {
        return Ok(Cell::Null);
    }

    let mut decoded = Vec::with_capacity(raw.len());
    let mut offset = 0;
    while offset < raw.len() {
        if raw[offset] != COPY_ESCAPE {
            decoded.push(raw[offset]);
            offset += 1;
            continue;
        }

        let escape_offset = offset;
        offset += 1;
        let Some(&escape) = raw.get(offset) else {
            return Err(CanonicalCopyError::DanglingEscape {
                row,
                column,
                byte_offset: escape_offset,
            });
        };

        match escape {
            b'b' => {
                decoded.push(0x08);
                offset += 1;
            }
            b'f' => {
                decoded.push(0x0c);
                offset += 1;
            }
            b'n' => {
                decoded.push(b'\n');
                offset += 1;
            }
            b'r' => {
                decoded.push(b'\r');
                offset += 1;
            }
            b't' => {
                decoded.push(b'\t');
                offset += 1;
            }
            b'v' => {
                decoded.push(0x0b);
                offset += 1;
            }
            COPY_ESCAPE => {
                decoded.push(COPY_ESCAPE);
                offset += 1;
            }
            b'0'..=b'7' => {
                let mut value = 0_u16;
                let mut digits = 0;
                while digits < 3 {
                    let Some(&digit @ b'0'..=b'7') = raw.get(offset) else {
                        break;
                    };
                    value = value * 8 + u16::from(digit - b'0');
                    offset += 1;
                    digits += 1;
                }
                let value =
                    u8::try_from(value).map_err(|_| CanonicalCopyError::OctalEscapeOutOfRange {
                        row,
                        column,
                        byte_offset: escape_offset,
                    })?;
                decoded.push(value);
            }
            b'x' => {
                offset += 1;
                let first_digit_offset = offset;
                let mut value = 0_u8;
                let mut digits = 0;
                while digits < 2 {
                    let Some(&digit) = raw.get(offset) else {
                        break;
                    };
                    let Some(nibble) = hex_value(digit) else {
                        break;
                    };
                    value = value * 16 + nibble;
                    offset += 1;
                    digits += 1;
                }
                if offset == first_digit_offset {
                    return Err(CanonicalCopyError::MissingHexDigits {
                        row,
                        column,
                        byte_offset: escape_offset,
                    });
                }
                decoded.push(value);
            }
            _ => {
                return Err(CanonicalCopyError::UnsupportedEscape {
                    row,
                    column,
                    byte_offset: escape_offset,
                    escape,
                });
            }
        }
    }

    if std::str::from_utf8(&decoded).is_err() {
        return Err(CanonicalCopyError::InvalidUtf8 { row, column });
    }
    Ok(Cell::Text(Bytes::from(decoded)))
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CanonicalCopyError {
    #[error("maximum COPY row size must be greater than zero")]
    InvalidMaxRowBytes,
    #[error("COPY column count overflowed usize")]
    ColumnCountOverflow,
    #[error("COPY row {row} size overflowed usize")]
    RowSizeOverflow { row: u64 },
    #[error("COPY row count overflowed u64")]
    RowCountOverflow,
    #[error("COPY row {row} exceeds the configured maximum of {max_row_bytes} wire bytes")]
    RowTooLarge { row: u64, max_row_bytes: usize },
    #[error("COPY row {row} has {actual} columns; expected {expected}")]
    Arity {
        row: u64,
        expected: usize,
        actual: usize,
    },
    #[error("COPY row {row}, column {column} has a dangling escape at byte {byte_offset}")]
    DanglingEscape {
        row: u64,
        column: usize,
        byte_offset: usize,
    },
    #[error(
        "COPY row {row}, column {column} has unsupported escape byte 0x{escape:02x} at byte {byte_offset}"
    )]
    UnsupportedEscape {
        row: u64,
        column: usize,
        byte_offset: usize,
        escape: u8,
    },
    #[error(
        "COPY row {row}, column {column} has an octal escape outside the byte range at byte {byte_offset}"
    )]
    OctalEscapeOutOfRange {
        row: u64,
        column: usize,
        byte_offset: usize,
    },
    #[error(
        "COPY row {row}, column {column} has a hex escape without digits at byte {byte_offset}"
    )]
    MissingHexDigits {
        row: u64,
        column: usize,
        byte_offset: usize,
    },
    #[error("COPY row {row}, column {column} is not valid UTF-8 after unescaping")]
    InvalidUtf8 { row: u64, column: usize },
    #[error("COPY row {row} contains an unescaped carriage return")]
    UnescapedCarriageReturn { row: u64 },
    #[error("COPY stream ended with unterminated row {row}")]
    UnterminatedRow { row: u64 },
    #[error(transparent)]
    Reconcile(#[from] ReconcileError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::{DigestColumn, digest_multiset};

    fn context(value_columns: usize) -> DigestContext {
        DigestContext {
            version_domain: "copy-text-test-v1".to_owned(),
            schema_fingerprint: "schema-v1".to_owned(),
            key_columns: vec![DigestColumn {
                ordinal: 1,
                portable_type_tag: "int8".to_owned(),
            }],
            value_columns: (0..value_columns)
                .map(|index| DigestColumn {
                    ordinal: u32::try_from(index + 2).unwrap(),
                    portable_type_tag: "text".to_owned(),
                })
                .collect(),
        }
    }

    fn text(value: &'static [u8]) -> Cell {
        Cell::Text(Bytes::from_static(value))
    }

    #[test]
    fn decodes_every_escape_null_empty_and_literal_null_at_every_chunk_boundary() {
        let context = context(7);
        let wire = b"1\tplain\t\\N\t\t\\\\N\tback\\\\slash\\ttab\\nline\\rreturn\\bback\\fform\\vvert\t\\101\\x42\tutf8-\xc3\xa9\n";
        let expected_row = CanonicalRow {
            key: vec![Bytes::from_static(b"1")],
            values: vec![
                text(b"plain"),
                Cell::Null,
                text(b""),
                text(b"\\N"),
                text(b"back\\slash\ttab\nline\rreturn\x08back\x0cform\x0bvert"),
                text(b"AB"),
                text(b"utf8-\xc3\xa9"),
            ],
        };
        let expected = digest_multiset(&context, [&expected_row]).unwrap();

        for split in 0..=wire.len() {
            let mut stream = CanonicalCopyTextDigestStream::new(&context, wire.len()).unwrap();
            stream.feed(Bytes::copy_from_slice(&wire[..split])).unwrap();
            stream.feed(Bytes::copy_from_slice(&wire[split..])).unwrap();
            assert_eq!(stream.finish().unwrap(), expected, "split at {split}");
        }

        let mut bytewise = CanonicalCopyTextDigestStream::new(&context, wire.len()).unwrap();
        for byte in wire {
            bytewise.feed(Bytes::copy_from_slice(&[*byte])).unwrap();
        }
        assert_eq!(bytewise.finish().unwrap(), expected);
    }

    #[test]
    fn streams_multiple_rows_and_accepts_an_empty_stream() {
        let context = context(1);
        let rows = [
            CanonicalRow {
                key: vec![Bytes::from_static(b"1")],
                values: vec![text(b"first")],
            },
            CanonicalRow {
                key: vec![Bytes::from_static(b"2")],
                values: vec![Cell::Null],
            },
        ];
        let expected = digest_multiset(&context, &rows).unwrap();
        let mut stream = CanonicalCopyTextDigestStream::new(&context, 16).unwrap();
        stream
            .feed(Bytes::from_static(b"1\tfirst\n2\t\\N\n"))
            .unwrap();
        assert_eq!(stream.finish().unwrap(), expected);

        let empty_expected = digest_multiset(&context, std::iter::empty()).unwrap();
        let empty = CanonicalCopyTextDigestStream::new(&context, 1)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(empty, empty_expected);
    }

    #[test]
    fn rejects_invalid_input_and_never_finishes_a_failed_digest() {
        let context = context(1);
        let mut stream = CanonicalCopyTextDigestStream::new(&context, 64).unwrap();
        let error = stream
            .feed(Bytes::from_static(b"1\tok\n2\tbad\\q\n"))
            .unwrap_err();
        assert!(matches!(
            error,
            CanonicalCopyError::UnsupportedEscape {
                row: 1,
                column: 1,
                escape: b'q',
                ..
            }
        ));
        assert_eq!(
            stream.feed(Bytes::from_static(b"3\tok\n")),
            Err(error.clone())
        );
        assert_eq!(stream.finish(), Err(error));
    }

    #[test]
    fn rejects_arity_utf8_partial_rows_and_bad_escapes() {
        let context = context(1);

        let mut wrong_arity = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        assert!(matches!(
            wrong_arity.feed(Bytes::from_static(b"1\n")),
            Err(CanonicalCopyError::Arity {
                expected: 2,
                actual: 1,
                ..
            })
        ));

        let mut too_many = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        assert_eq!(
            too_many.feed(Bytes::from_static(b"1\tvalue\t")),
            Err(CanonicalCopyError::Arity {
                row: 0,
                expected: 2,
                actual: 3,
            })
        );
        assert_eq!(too_many.fields.len(), 1);

        let mut invalid_utf8 = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        assert!(matches!(
            invalid_utf8.feed(Bytes::from_static(b"1\t\xff\n")),
            Err(CanonicalCopyError::InvalidUtf8 { column: 1, .. })
        ));

        let mut missing_hex = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        assert!(matches!(
            missing_hex.feed(Bytes::from_static(b"1\t\\xZ\n")),
            Err(CanonicalCopyError::MissingHexDigits { column: 1, .. })
        ));

        let mut octal_overflow = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        assert!(matches!(
            octal_overflow.feed(Bytes::from_static(b"1\t\\400\n")),
            Err(CanonicalCopyError::OctalEscapeOutOfRange { column: 1, .. })
        ));

        let mut partial = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        partial.feed(Bytes::from_static(b"1\tvalue")).unwrap();
        assert_eq!(
            partial.finish(),
            Err(CanonicalCopyError::UnterminatedRow { row: 0 })
        );

        let mut dangling = CanonicalCopyTextDigestStream::new(&context, 32).unwrap();
        dangling.feed(Bytes::from_static(b"1\tvalue\\")).unwrap();
        assert!(matches!(
            dangling.finish(),
            Err(CanonicalCopyError::DanglingEscape { column: 1, .. })
        ));
    }

    #[test]
    fn enforces_wire_row_limit_before_accepting_the_oversized_byte() {
        let context = context(0);
        assert_eq!(
            CanonicalCopyTextDigestStream::new(&context, 0).unwrap_err(),
            CanonicalCopyError::InvalidMaxRowBytes
        );

        let mut exact = CanonicalCopyTextDigestStream::new(&context, 2).unwrap();
        exact.feed(Bytes::from_static(b"ab\n")).unwrap();
        assert_eq!(exact.finish().unwrap().row_count, 1);

        let mut oversized = CanonicalCopyTextDigestStream::new(&context, 1).unwrap();
        assert_eq!(
            oversized.feed(Bytes::from_static(b"ab")),
            Err(CanonicalCopyError::RowTooLarge {
                row: 0,
                max_row_bytes: 1,
            })
        );
        assert!(oversized.finish().is_err());
    }

    #[test]
    fn null_key_and_unescaped_carriage_return_fail_closed() {
        let context = context(0);
        let mut null_key = CanonicalCopyTextDigestStream::new(&context, 8).unwrap();
        assert!(matches!(
            null_key.feed(Bytes::from_static(b"\\N\n")),
            Err(CanonicalCopyError::Reconcile(ReconcileError::NullKey {
                index: 0
            }))
        ));

        let mut carriage_return = CanonicalCopyTextDigestStream::new(&context, 8).unwrap();
        assert_eq!(
            carriage_return.feed(Bytes::from_static(b"1\r\n")),
            Err(CanonicalCopyError::UnescapedCarriageReturn { row: 0 })
        );
    }
}
