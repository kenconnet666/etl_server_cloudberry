//! Safe SQL identifier and literal rendering.

use std::fmt::Write as _;

use cloudberry_etl_core::schema::QualifiedName;
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SqlRenderError {
    #[error("SQL identifiers cannot be empty")]
    EmptyIdentifier,
    #[error("{kind} contains a NUL byte")]
    NulByte { kind: &'static str },
}

/// Quotes one PostgreSQL identifier without relying on the server's search path.
pub fn quote_identifier(identifier: &str) -> Result<String, SqlRenderError> {
    if identifier.is_empty() {
        return Err(SqlRenderError::EmptyIdentifier);
    }
    if identifier.contains('\0') {
        return Err(SqlRenderError::NulByte { kind: "identifier" });
    }

    let mut quoted = String::with_capacity(identifier.len() + 2);
    quoted.push('"');
    for character in identifier.chars() {
        if character == '"' {
            quoted.push('"');
        }
        quoted.push(character);
    }
    quoted.push('"');
    Ok(quoted)
}

/// Quotes a schema-qualified PostgreSQL name.
pub fn quote_qualified_name(name: &QualifiedName) -> Result<String, SqlRenderError> {
    Ok(format!(
        "{}.{}",
        quote_identifier(&name.schema)?,
        quote_identifier(&name.name)?
    ))
}

/// Quotes a PostgreSQL escape-string literal.
///
/// Escape strings make the result independent from `standard_conforming_strings`.
pub fn quote_literal(value: &str) -> Result<String, SqlRenderError> {
    if value.contains('\0') {
        return Err(SqlRenderError::NulByte { kind: "literal" });
    }

    let mut quoted = String::with_capacity(value.len() + 3);
    quoted.push_str("E'");
    for character in value.chars() {
        match character {
            '\'' => quoted.push_str("''"),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            '\u{0008}' => quoted.push_str("\\b"),
            '\u{000C}' => quoted.push_str("\\f"),
            character if character.is_control() => {
                write!(quoted, "\\u{:04X}", u32::from(character))
                    .expect("writing to a String cannot fail");
            }
            character => quoted.push(character),
        }
    }
    quoted.push('\'');
    Ok(quoted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_identifiers_including_reserved_words_and_quotes() {
        assert_eq!(quote_identifier("select").unwrap(), r#""select""#);
        assert_eq!(
            quote_identifier("a\"; DROP TABLE x; --").unwrap(),
            r#""a""; DROP TABLE x; --""#
        );
        assert_eq!(quote_identifier("订单").unwrap(), r#""订单""#);
    }

    #[test]
    fn quotes_literals_independently_of_string_settings() {
        assert_eq!(quote_literal("O'Reilly").unwrap(), "E'O''Reilly'");
        assert_eq!(quote_literal("a\\b\nc\t").unwrap(), "E'a\\\\b\\nc\\t'");
        assert_eq!(
            quote_literal("'; DROP TABLE x; --").unwrap(),
            "E'''; DROP TABLE x; --'"
        );
    }

    #[test]
    fn rejects_values_postgres_cannot_represent() {
        assert_eq!(quote_identifier(""), Err(SqlRenderError::EmptyIdentifier));
        assert!(matches!(
            quote_identifier("bad\0name"),
            Err(SqlRenderError::NulByte { kind: "identifier" })
        ));
        assert!(matches!(
            quote_literal("bad\0value"),
            Err(SqlRenderError::NulByte { kind: "literal" })
        ));
    }
}
