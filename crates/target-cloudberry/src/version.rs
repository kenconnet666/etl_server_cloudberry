//! Target Apache Cloudberry version verification.

use tokio_postgres::Client;

/// Verify that the target is Apache Cloudberry 2.1.x.
///
/// This service only supports Apache Cloudberry 2.1.x as the target. Other versions
/// (2.0, 2.2, etc.) are explicitly rejected to maintain a single, well-tested
/// compatibility matrix.
pub async fn verify_cloudberry_21_version(client: &Client) -> Result<(), String> {
    let row = client
        .query_one("SELECT version()", &[])
        .await
        .map_err(|error| format!("failed to query version: {error}"))?;

    let version_string: String = row.get(0);

    // Apache Cloudberry 2.1.x version string contains "Apache Cloudberry 2.1"
    if !version_string.contains("Apache Cloudberry 2.1") {
        return Err(format!(
            "This service only supports Apache Cloudberry 2.1.x target. Found: {version_string}"
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn version_check_logic() {
        // Valid versions
        assert!("Apache Cloudberry 2.1.0".contains("Apache Cloudberry 2.1"));
        assert!("Apache Cloudberry 2.1.5-incubating".contains("Apache Cloudberry 2.1"));

        // Invalid versions
        assert!(!"Apache Cloudberry 2.0.0".contains("Apache Cloudberry 2.1"));
        assert!(!"Apache Cloudberry 2.2.0".contains("Apache Cloudberry 2.1"));
        assert!(!"PostgreSQL 14.0".contains("Apache Cloudberry 2.1"));
    }
}
