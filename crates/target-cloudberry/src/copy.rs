//! Typed PostgreSQL text COPY encoding.

use cloudberry_etl_core::change::Cell;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CopyEncodeError {
    #[error("COPY rows must contain at least one field")]
    EmptyRow,
    #[error("UnchangedToast has no field value; materialize it with a presence mask first")]
    UnchangedToast,
    #[error("a pgoutput binary value cannot be written through PostgreSQL text COPY")]
    BinaryValue,
}

/// Encodes one core cell as a PostgreSQL text COPY field.
///
/// `UnchangedToast` is deliberately rejected. Apply staging must represent it as
/// a NULL placeholder plus a false presence flag, never as SQL NULL by itself.
pub fn encode_field(cell: &Cell) -> Result<Vec<u8>, CopyEncodeError> {
    let mut output = Vec::new();
    encode_field_into(cell, &mut output)?;
    Ok(output)
}

/// Encodes one complete PostgreSQL text COPY row, including its final newline.
pub fn encode_row(cells: &[Cell]) -> Result<Vec<u8>, CopyEncodeError> {
    let Some((first, rest)) = cells.split_first() else {
        return Err(CopyEncodeError::EmptyRow);
    };

    let mut output = Vec::new();
    encode_field_into(first, &mut output)?;
    for cell in rest {
        output.push(b'\t');
        encode_field_into(cell, &mut output)?;
    }
    output.push(b'\n');
    Ok(output)
}

fn encode_field_into(cell: &Cell, output: &mut Vec<u8>) -> Result<(), CopyEncodeError> {
    match cell {
        Cell::Null => output.extend_from_slice(br"\N"),
        Cell::UnchangedToast => return Err(CopyEncodeError::UnchangedToast),
        Cell::Binary(_) => return Err(CopyEncodeError::BinaryValue),
        Cell::Text(value) => encode_text_bytes(value, output),
    }
    Ok(())
}

fn encode_text_bytes(value: &[u8], output: &mut Vec<u8>) {
    for byte in value {
        match byte {
            b'\\' => output.extend_from_slice(br"\\"),
            b'\t' => output.extend_from_slice(br"\t"),
            b'\n' => output.extend_from_slice(br"\n"),
            b'\r' => output.extend_from_slice(br"\r"),
            0x08 => output.extend_from_slice(br"\b"),
            0x0c => output.extend_from_slice(br"\f"),
            0x0b => output.extend_from_slice(br"\v"),
            0x00 => output.extend_from_slice(br"\000"),
            byte => output.push(*byte),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    #[test]
    fn distinguishes_null_from_the_text_null_marker() {
        assert_eq!(encode_field(&Cell::Null).unwrap(), br"\N");
        assert_eq!(
            encode_field(&Cell::Text(Bytes::from_static(br"\N"))).unwrap(),
            br"\\N"
        );
    }

    #[test]
    fn escapes_all_copy_text_control_bytes() {
        let value = Cell::Text(Bytes::from_static(b"a\\b\tc\nd\re\x08f\x0cg\x0bh\0i"));
        assert_eq!(
            encode_field(&value).unwrap(),
            br"a\\b\tc\nd\re\bf\fg\vh\000i"
        );
    }

    #[test]
    fn encodes_rows_with_delimiters_and_record_terminator() {
        let row = [
            Cell::Text(Bytes::from_static(b"1")),
            Cell::Null,
            Cell::Text(Bytes::new()),
        ];
        assert_eq!(encode_row(&row).unwrap(), b"1\t\\N\t\n");
    }

    #[test]
    fn refuses_values_without_a_text_copy_representation() {
        assert_eq!(
            encode_field(&Cell::UnchangedToast),
            Err(CopyEncodeError::UnchangedToast)
        );
        assert_eq!(
            encode_field(&Cell::Binary(Bytes::from_static(b"binary"))),
            Err(CopyEncodeError::BinaryValue)
        );
        assert_eq!(encode_row(&[]), Err(CopyEncodeError::EmptyRow));
    }
}
