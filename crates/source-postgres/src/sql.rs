use crate::error::{SourceError, SourceResult};

/// Quote one PostgreSQL identifier. Values must still be passed as parameters.
pub(crate) fn quote_identifier(identifier: &str) -> SourceResult<String> {
    if identifier.is_empty() || identifier.contains('\0') {
        return Err(SourceError::InvalidIdentifier(identifier.to_owned()));
    }
    Ok(format!("\"{}\"", identifier.replace('"', "\"\"")))
}

pub(crate) fn quote_qualified(schema: &str, name: &str) -> SourceResult<String> {
    Ok(format!(
        "{}.{}",
        quote_identifier(schema)?,
        quote_identifier(name)?
    ))
}

pub(crate) fn quote_literal(identifier: &str) -> SourceResult<String> {
    // This is only used for protocol options where PostgreSQL does not accept a bind
    // parameter. Keep it separate from identifier quoting to avoid accidental misuse.
    if identifier.contains('\0') {
        return Err(SourceError::InvalidIdentifier(identifier.to_owned()));
    }
    Ok(format!("'{}'", identifier.replace('\'', "''")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quotes_embedded_quotes() {
        assert_eq!(quote_identifier("a\"b").unwrap(), "\"a\"\"b\"");
        assert_eq!(quote_qualified("s", "t").unwrap(), "\"s\".\"t\"");
    }

    #[test]
    fn rejects_nul() {
        assert!(quote_identifier("a\0b").is_err());
    }
}
