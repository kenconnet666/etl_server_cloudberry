//! Physical storage profiles for replicated business tables.

use cloudberry_etl_core::schema::QualifiedName;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_postgres::GenericClient;

/// A bounded set of analytical Cloudberry 2.1 storage profiles for business tables.
///
/// Metadata and temporary staging tables intentionally do not use this setting; they remain heap
/// tables because they are small, highly mutable control structures. Compression settings are
/// fixed per profile so stored configuration cannot become an arbitrary SQL fragment. Heap is
/// deliberately absent: replicated business tables are an analytical replica.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetStorage {
    /// Append-optimized column storage is the production business-table default.
    #[default]
    AoColumn,
    /// Opt-in PAX evaluation profile. This does not claim complete SQL or operational support.
    PaxExperimental,
}

impl TargetStorage {
    #[must_use]
    pub const fn access_method(self) -> &'static str {
        match self {
            Self::AoColumn => "ao_column",
            Self::PaxExperimental => "pax",
        }
    }

    #[must_use]
    pub const fn create_clause(self) -> &'static str {
        match self {
            Self::AoColumn => "USING ao_column WITH (compresstype='zstd', compresslevel=1)",
            Self::PaxExperimental => {
                "USING pax WITH (storage_format='porc', compresstype='zstd', compresslevel=1)"
            }
        }
    }

    /// Stable identity included in managed-table fingerprints.
    #[must_use]
    pub const fn fingerprint_tag(self) -> &'static str {
        match self {
            Self::AoColumn => "ao-column-zstd-1-v1",
            Self::PaxExperimental => "pax-experimental-porc-zstd-1-v1",
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageCapabilityError {
    #[error("failed to inspect Cloudberry table access methods: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("Cloudberry does not provide required table access method `{0}`")]
    MissingAccessMethod(&'static str),
}

/// Verify that a configured storage profile exists as a table access method on this target.
///
/// PAX is compile-time optional in Cloudberry, so a 2.1 version string alone is insufficient.
pub async fn verify_storage_available<C>(
    client: &C,
    storage: TargetStorage,
) -> Result<(), StorageCapabilityError>
where
    C: GenericClient + Sync,
{
    let method = storage.access_method();
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_am WHERE amtype = 't' AND amname = $1)",
            &[&method],
        )
        .await?
        .try_get(0)?;
    if exists {
        Ok(())
    } else {
        Err(StorageCapabilityError::MissingAccessMethod(method))
    }
}

/// Read a physical relation's table access method without relying on `\d` output.
pub async fn load_relation_storage<C>(
    client: &C,
    relation: &QualifiedName,
) -> Result<Option<String>, StorageCapabilityError>
where
    C: GenericClient + Sync,
{
    let row = client
        .query_opt(
            "SELECT am.amname FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace JOIN pg_catalog.pg_am AS am ON am.oid = c.relam WHERE n.nspname = $1 AND c.relname = $2",
            &[&relation.schema, &relation.name],
        )
        .await?;
    row.map(|row| row.try_get(0))
        .transpose()
        .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn profile_json_and_ddl_are_stable_and_bounded() {
        assert_eq!(
            serde_json::to_value(TargetStorage::AoColumn).unwrap(),
            json!("ao_column")
        );
        assert_eq!(
            serde_json::from_value::<TargetStorage>(json!("pax_experimental")).unwrap(),
            TargetStorage::PaxExperimental
        );
        assert!(TargetStorage::AoColumn.create_clause().contains("zstd"));
        assert!(
            TargetStorage::PaxExperimental
                .create_clause()
                .contains("storage_format='porc'")
        );
        assert!(serde_json::from_value::<TargetStorage>(json!("heap")).is_err());
        assert!(serde_json::from_value::<TargetStorage>(json!("pax")).is_err());
    }
}
