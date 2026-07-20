use std::{env, fs, net::SocketAddr, path::Path};

use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read configuration {path}: {source}")]
    Read {
        path: String,
        source: std::io::Error,
    },
    #[error("invalid TOML configuration: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("invalid configuration: {0}")]
    Invalid(String),
    #[error("required environment variable {0} is not set")]
    MissingEnvironment(String),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    #[serde(default)]
    pub server: ServerConfig,
    pub admin: AdminConfig,
    pub control: ControlConfig,
    pub security: SecurityConfig,
}

impl BootstrapConfig {
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.display().to_string(),
            source,
        })?;
        Self::from_toml(&contents)
    }

    pub fn from_toml(contents: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(contents)?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.admin.username.trim().is_empty() {
            return Err(ConfigError::Invalid("admin.username is empty".into()));
        }
        if !self
            .admin
            .password_hash
            .expose_secret()
            .starts_with("$argon2id$")
        {
            return Err(ConfigError::Invalid(
                "admin.password_hash must be an Argon2id PHC string".into(),
            ));
        }
        validate_environment_name(&self.control.database_url_env)?;
        validate_environment_name(&self.security.master_key_env)?;
        Ok(())
    }

    pub fn control_database_url(&self) -> Result<SecretString, ConfigError> {
        secret_from_environment(&self.control.database_url_env)
    }

    pub fn master_key(&self) -> Result<SecretString, ConfigError> {
        secret_from_environment(&self.security.master_key_env)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    pub listen: SocketAddr,
    pub secure_cookies: bool,
    pub session_ttl_seconds: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:8080"
                .parse()
                .expect("valid default listen address"),
            secure_cookies: true,
            session_ttl_seconds: 8 * 60 * 60,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    pub username: String,
    pub password_hash: SecretString,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlConfig {
    pub database_url_env: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    pub master_key_env: String,
}

fn validate_environment_name(name: &str) -> Result<(), ConfigError> {
    let valid = !name.is_empty()
        && name.chars().all(|character| {
            character.is_ascii_uppercase() || character.is_ascii_digit() || character == '_'
        });
    if valid {
        Ok(())
    } else {
        Err(ConfigError::Invalid(format!(
            "invalid environment variable name `{name}`"
        )))
    }
}

fn secret_from_environment(name: &str) -> Result<SecretString, ConfigError> {
    env::var(name)
        .map(SecretString::from)
        .map_err(|_| ConfigError::MissingEnvironment(name.to_owned()))
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;

    use super::*;

    const CONFIG: &str = r#"
        [server]
        listen = "127.0.0.1:9090"
        secure_cookies = false
        session_ttl_seconds = 3600

        [admin]
        username = "admin"
        password_hash = "$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA"

        [control]
        database_url_env = "ETL_CONTROL_DATABASE_URL"

        [security]
        master_key_env = "ETL_MASTER_KEY"
    "#;

    #[test]
    fn parses_strict_config() {
        let config = BootstrapConfig::from_toml(CONFIG).unwrap();
        assert_eq!(config.server.listen.port(), 9090);
        assert_eq!(config.admin.username, "admin");
        assert!(
            config
                .admin
                .password_hash
                .expose_secret()
                .starts_with("$argon2id$")
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let config = CONFIG.replace("secure_cookies = false", "unknown = true");
        assert!(BootstrapConfig::from_toml(&config).is_err());
    }
}
