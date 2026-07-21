use serde::{Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CoreError, CoreResult,
    schema::{POSTGRES_IDENTIFIER_MAX_BYTES, QualifiedName, validate_identifier},
};

// A 96-bit suffix keeps accidental collisions negligible while retaining a readable prefix.
const HASH_SUFFIX_BYTES: usize = 12;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
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

impl<'de> Deserialize<'de> for SourcePrefix {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
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
        validate_identifier(&self.source_database)?;
        validate_identifier(&self.target_database)?;
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
    if value.len() <= POSTGRES_IDENTIFIER_MAX_BYTES {
        return value.to_owned();
    }

    let digest = Sha256::digest(value.as_bytes());
    let suffix = digest[..HASH_SUFFIX_BYTES]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let maximum_prefix_bytes = POSTGRES_IDENTIFIER_MAX_BYTES - 1 - suffix.len();
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
        assert!(first.len() <= POSTGRES_IDENTIFIER_MAX_BYTES);
        assert!(first.is_char_boundary(first.len()));
        assert!(first.contains('_'));
    }

    #[test]
    fn shortening_has_a_stable_96_bit_suffix() {
        let name = "a".repeat(64);
        let shortened = shorten_identifier(&name);

        assert_eq!(
            shortened,
            format!("{}_ffe054fe7ae0cb6dc65c3af9", "a".repeat(38))
        );
        assert_eq!(shortened.len(), POSTGRES_IDENTIFIER_MAX_BYTES);
        assert_ne!(
            shortened,
            shorten_identifier(&format!("{}b", "a".repeat(63)))
        );
        assert_eq!(shorten_identifier(&"a".repeat(63)), "a".repeat(63));
    }

    #[test]
    fn default_mapping_shortens_derived_names_but_rejects_invalid_database_names() {
        let source = QualifiedName::new("s".repeat(63), "items").unwrap();
        let mapper = DefaultNameMapper {
            target_database: "analytics".into(),
            source_prefix: SourcePrefix::new("p".repeat(24)).unwrap(),
            source_database: "d".repeat(63),
        };
        let mapped = mapper.map(&source).unwrap();
        assert_eq!(mapped.database, "analytics");
        assert!(mapped.relation.schema.len() <= POSTGRES_IDENTIFIER_MAX_BYTES);
        assert_eq!(mapped.relation.name, "items");

        let invalid_source = DefaultNameMapper {
            source_database: "d".repeat(64),
            ..mapper.clone()
        };
        assert!(invalid_source.map(&source).is_err());

        let invalid_target = DefaultNameMapper {
            target_database: "界".repeat(22),
            ..mapper
        };
        assert!(invalid_target.map(&source).is_err());
    }

    #[test]
    fn prefix_deserialization_keeps_validation_invariants() {
        assert!(serde_json::from_str::<SourcePrefix>("\"valid_01\"").is_ok());
        assert!(serde_json::from_str::<SourcePrefix>("\"Invalid\"").is_err());
    }
}
