use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CoreError, CoreResult, schema::QualifiedName};

const POSTGRES_IDENTIFIER_BYTES: usize = 63;
const HASH_HEX_BYTES: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourcePrefix(String);

impl SourcePrefix {
    pub fn new(value: impl Into<String>) -> CoreResult<Self> {
        let value = value.into();
        let mut chars = value.chars();
        let valid = matches!(chars.next(), Some('a'..='z'))
            && chars.all(|character| {
                character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
            })
            && value.len() <= 24;
        if !valid {
            return Err(CoreError::InvalidPrefix(value));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TargetTableName {
    pub database: String,
    pub relation: QualifiedName,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultNameMapper {
    pub target_database: String,
    pub source_prefix: SourcePrefix,
    pub source_database: String,
}

impl DefaultNameMapper {
    pub fn map(&self, source: &QualifiedName) -> CoreResult<TargetTableName> {
        let raw_schema = format!(
            "{}__{}__{}",
            self.source_prefix.as_str(),
            self.source_database,
            source.schema
        );
        Ok(TargetTableName {
            database: self.target_database.clone(),
            relation: QualifiedName::new(shorten_identifier(&raw_schema), &source.name)?,
        })
    }
}

#[must_use]
pub fn shorten_identifier(value: &str) -> String {
    if value.len() <= POSTGRES_IDENTIFIER_BYTES {
        return value.to_owned();
    }

    let digest = Sha256::digest(value.as_bytes());
    let suffix = digest[..HASH_HEX_BYTES / 2]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let maximum_prefix_bytes = POSTGRES_IDENTIFIER_BYTES - 1 - suffix.len();
    let mut boundary = maximum_prefix_bytes.min(value.len());
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}_{}", &value[..boundary], suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_prefix() {
        assert!(SourcePrefix::new("erp_01").is_ok());
        assert!(SourcePrefix::new("ERP").is_err());
        assert!(SourcePrefix::new("1erp").is_err());
    }

    #[test]
    fn shortening_is_stable_and_utf8_safe() {
        let name = "schema_数据_".repeat(10);
        let first = shorten_identifier(&name);
        assert_eq!(first, shorten_identifier(&name));
        assert!(first.len() <= POSTGRES_IDENTIFIER_BYTES);
        assert!(first.contains('_'));
    }
}
