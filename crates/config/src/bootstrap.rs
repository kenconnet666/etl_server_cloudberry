use std::{
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

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
    #[error("failed to read secret file {path} selected by {environment}: {source}")]
    ReadSecret {
        environment: String,
        path: String,
        source: std::io::Error,
    },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub engine: EngineConfig,
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
        match (&self.admin.password_hash, &self.admin.password_hash_env) {
            (Some(password_hash), None) => validate_password_hash(password_hash)?,
            (None, Some(environment)) => validate_environment_name(environment)?,
            (Some(_), Some(_)) => {
                return Err(ConfigError::Invalid(
                    "admin.password_hash and admin.password_hash_env are mutually exclusive".into(),
                ));
            }
            (None, None) => {
                return Err(ConfigError::Invalid(
                    "one of admin.password_hash or admin.password_hash_env is required".into(),
                ));
            }
        }
        validate_environment_name(&self.control.database_url_env)?;
        validate_environment_name(&self.security.master_key_env)?;
        self.server.validate()?;
        self.engine.validate()?;
        Ok(())
    }

    pub fn control_database_url(&self) -> Result<SecretString, ConfigError> {
        secret_from_environment(&self.control.database_url_env)
    }

    pub fn master_key(&self) -> Result<SecretString, ConfigError> {
        secret_from_environment(&self.security.master_key_env)
    }

    pub fn admin_password_hash(&self) -> Result<SecretString, ConfigError> {
        let password_hash = match (&self.admin.password_hash, &self.admin.password_hash_env) {
            (Some(password_hash), None) => password_hash.clone(),
            (None, Some(environment)) => secret_from_environment(environment)?,
            _ => {
                return Err(ConfigError::Invalid(
                    "administrator password hash configuration is invalid".into(),
                ));
            }
        };
        validate_password_hash(&password_hash)?;
        Ok(password_hash)
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

impl ServerConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !self.listen.ip().is_loopback() && !self.secure_cookies {
            return Err(ConfigError::Invalid(
                "server.secure_cookies must be true when server.listen is not loopback".into(),
            ));
        }
        if self.session_ttl_seconds == 0 {
            return Err(ConfigError::Invalid(
                "server.session_ttl_seconds must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfig {
    /// Service-local root for versioned WAL transaction journals.
    pub spool_directory: PathBuf,
    pub reconcile_interval_seconds: u64,
    pub lease_ttl_seconds: u64,
    pub lease_renew_interval_seconds: u64,
    pub restart_backoff_initial_seconds: u64,
    pub restart_backoff_max_seconds: u64,
    pub restart_backoff_reset_seconds: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            spool_directory: PathBuf::from("data/spool"),
            reconcile_interval_seconds: 2,
            lease_ttl_seconds: 30,
            lease_renew_interval_seconds: 10,
            restart_backoff_initial_seconds: 1,
            restart_backoff_max_seconds: 60,
            restart_backoff_reset_seconds: 300,
        }
    }
}

impl EngineConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.spool_directory.as_os_str().is_empty() {
            return Err(ConfigError::Invalid(
                "engine.spool_directory must not be empty".into(),
            ));
        }
        if self.reconcile_interval_seconds == 0 {
            return Err(ConfigError::Invalid(
                "engine.reconcile_interval_seconds must be greater than zero".into(),
            ));
        }
        if self.lease_ttl_seconds == 0 {
            return Err(ConfigError::Invalid(
                "engine.lease_ttl_seconds must be greater than zero".into(),
            ));
        }
        if self.lease_renew_interval_seconds == 0
            || self.lease_renew_interval_seconds > self.lease_ttl_seconds / 3
        {
            return Err(ConfigError::Invalid(
                "engine.lease_renew_interval_seconds must be greater than zero and no more than one third of engine.lease_ttl_seconds"
                    .into(),
            ));
        }
        if self.restart_backoff_initial_seconds == 0
            || self.restart_backoff_initial_seconds > self.restart_backoff_max_seconds
        {
            return Err(ConfigError::Invalid(
                "engine restart backoff must be positive and initial must not exceed maximum"
                    .into(),
            ));
        }
        if self.restart_backoff_reset_seconds == 0 {
            return Err(ConfigError::Invalid(
                "engine.restart_backoff_reset_seconds must be greater than zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    pub username: String,
    #[serde(default)]
    pub password_hash: Option<SecretString>,
    #[serde(default)]
    pub password_hash_env: Option<String>,
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

fn validate_password_hash(password_hash: &SecretString) -> Result<(), ConfigError> {
    if password_hash.expose_secret().starts_with("$argon2id$") {
        Ok(())
    } else {
        Err(ConfigError::Invalid(
            "administrator password hash must be an Argon2id PHC string".into(),
        ))
    }
}

fn secret_from_environment(name: &str) -> Result<SecretString, ConfigError> {
    let file_environment = format!("{name}_FILE");
    let inline = env::var(name).ok();
    let file = env::var(&file_environment).ok();
    let value = match (inline, file) {
        (Some(_), Some(_)) => {
            return Err(ConfigError::Invalid(format!(
                "{name} and {file_environment} are mutually exclusive"
            )));
        }
        (Some(value), None) => value,
        (None, Some(path)) => fs::read_to_string(&path)
            .map_err(|source| ConfigError::ReadSecret {
                environment: file_environment,
                path,
                source,
            })?
            .trim_end_matches(['\r', '\n'])
            .to_owned(),
        (None, None) => {
            return Err(ConfigError::MissingEnvironment(format!(
                "{name} (or {name}_FILE)"
            )));
        }
    };
    if value.is_empty() {
        return Err(ConfigError::Invalid(format!(
            "secret selected by {name} is empty"
        )));
    }
    Ok(SecretString::from(value))
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

        [engine]
        spool_directory = "data/spool"
        reconcile_interval_seconds = 2
        lease_ttl_seconds = 30
        lease_renew_interval_seconds = 10
        restart_backoff_initial_seconds = 1
        restart_backoff_max_seconds = 60
        restart_backoff_reset_seconds = 300

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
                .as_ref()
                .unwrap()
                .expose_secret()
                .starts_with("$argon2id$")
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let config = CONFIG.replace("secure_cookies = false", "unknown = true");
        assert!(BootstrapConfig::from_toml(&config).is_err());
    }

    #[test]
    fn rejects_lease_renewal_without_two_intervals_of_safety_margin() {
        let config = CONFIG.replace(
            "lease_renew_interval_seconds = 10",
            "lease_renew_interval_seconds = 11",
        );
        assert!(BootstrapConfig::from_toml(&config).is_err());
    }

    #[test]
    fn rejects_zero_session_ttl() {
        let config = CONFIG.replace("session_ttl_seconds = 3600", "session_ttl_seconds = 0");
        assert!(BootstrapConfig::from_toml(&config).is_err());
    }

    #[test]
    fn rejects_insecure_cookies_on_non_loopback_listener() {
        let config = CONFIG.replace("127.0.0.1:9090", "0.0.0.0:9090");
        let error = BootstrapConfig::from_toml(&config).unwrap_err();
        assert!(error.to_string().contains("secure_cookies must be true"));
    }

    #[test]
    fn accepts_password_hash_environment_reference() {
        let config = CONFIG.replace(
            "password_hash = \"$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA\"",
            "password_hash_env = \"ETL_ADMIN_PASSWORD_HASH\"",
        );
        let config = BootstrapConfig::from_toml(&config).unwrap();
        assert_eq!(
            config.admin.password_hash_env.as_deref(),
            Some("ETL_ADMIN_PASSWORD_HASH")
        );
    }

    #[test]
    fn rejects_ambiguous_password_hash_sources() {
        let config = CONFIG.replace(
            "password_hash = \"$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA\"",
            "password_hash = \"$argon2id$v=19$m=19456,t=2,p=1$c2FsdA$aGFzaA\"\npassword_hash_env = \"ETL_ADMIN_PASSWORD_HASH\"",
        );
        let error = BootstrapConfig::from_toml(&config).unwrap_err();
        assert!(error.to_string().contains("mutually exclusive"));
    }
}
