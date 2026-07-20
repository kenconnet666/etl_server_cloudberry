use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use argon2::{
    Argon2, PasswordHasher,
    password_hash::{SaltString, rand_core::OsRng},
};
use clap::{Parser, Subcommand};
use cloudberry_etl_api::{
    auth::AuthState,
    router,
    state::{AppState, ConnectionReport, ConnectionTester},
};
use cloudberry_etl_config::BootstrapConfig;
use cloudberry_etl_engine::supervisor::PipelineSupervisor;
use cloudberry_etl_metadata::{
    crypto::MasterKey,
    migration::migrate_control_database,
    store::{ControlStore, PostgresControlStore},
};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use secrecy::{ExposeSecret, SecretString};
use tokio_postgres::{Client, Config as PgConfig};
use tower_http::services::{ServeDir, ServeFile};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    version,
    about = "PostgreSQL 18 current-state replication to Cloudberry"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Serve {
        #[arg(short, long, default_value = "etl-server-cloudberry.toml")]
        config: PathBuf,
        #[arg(long, default_value = "web/dist")]
        web_dir: PathBuf,
    },
    Migrate {
        #[arg(short, long, default_value = "etl-server-cloudberry.toml")]
        config: PathBuf,
    },
    CheckConfig {
        #[arg(short, long, default_value = "etl-server-cloudberry.toml")]
        config: PathBuf,
    },
    HashPassword,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Serve {
        config: "etl-server-cloudberry.toml".into(),
        web_dir: "web/dist".into(),
    }) {
        Command::Serve { config, web_dir } => serve(config, web_dir).await,
        Command::Migrate { config } => migrate(config).await,
        Command::CheckConfig { config } => check_config(config),
        Command::HashPassword => hash_password(),
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("etl_server_cloudberry=info,cloudberry_etl=info,tower_http=info")
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .compact()
        .init();
}

fn load_config(path: &PathBuf) -> Result<BootstrapConfig> {
    BootstrapConfig::from_path(path)
        .with_context(|| format!("failed to load bootstrap config {}", path.display()))
}

fn check_config(path: PathBuf) -> Result<()> {
    let config = load_config(&path)?;
    config
        .control_database_url()
        .context("control database environment is invalid")?;
    MasterKey::from_base64(&config.master_key()?).context("master key is invalid")?;
    println!("configuration is valid");
    Ok(())
}

fn hash_password() -> Result<()> {
    let password = rpassword::prompt_password("Administrator password: ")?;
    if password.len() < 12 {
        bail!("administrator password must contain at least 12 characters");
    }
    let salt = SaltString::generate(&mut OsRng);
    let hash = Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    println!("{hash}");
    Ok(())
}

async fn migrate(path: PathBuf) -> Result<()> {
    let config = load_config(&path)?;
    let dsn = config.control_database_url()?;
    let mut client = connect_database(&dsn).await?;
    migrate_control_database(&mut client).await?;
    println!("control database is up to date");
    Ok(())
}

async fn serve(path: PathBuf, web_dir: PathBuf) -> Result<()> {
    let config = load_config(&path)?;
    let control_dsn = config.control_database_url()?;
    let client = connect_database(&control_dsn).await?;
    ensure_control_database_migrated(&client).await?;

    let control: Arc<dyn ControlStore> = Arc::new(PostgresControlStore::new(client));
    let master_key = Arc::new(MasterKey::from_base64(&config.master_key()?)?);
    let supervisor = Arc::new(PipelineSupervisor::new());
    let state = AppState {
        control,
        master_key,
        supervisor: supervisor.clone(),
        connection_tester: Arc::new(PostgresConnectionTester),
    };
    let auth = AuthState::new(
        config.admin.username.clone(),
        config.admin.password_hash.clone(),
        config.server.secure_cookies,
        Duration::from_secs(config.server.session_ttl_seconds),
    );

    let index = web_dir.join("index.html");
    let static_files = ServeDir::new(&web_dir).not_found_service(ServeFile::new(index));
    let app = router(state, auth).fallback_service(static_files);
    let listener = tokio::net::TcpListener::bind(config.server.listen)
        .await
        .with_context(|| format!("failed to bind {}", config.server.listen))?;
    tracing::info!(listen = %config.server.listen, "management server started");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(supervisor))
    .await?;
    Ok(())
}

async fn shutdown_signal(supervisor: Arc<PipelineSupervisor>) {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "failed to install Ctrl-C handler");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => tracing::error!(%error, "failed to install SIGTERM handler"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
    tracing::info!("shutdown requested");
    if let Err(error) = supervisor.stop_all().await {
        tracing::error!(%error, "pipeline shutdown failed");
    }
}

async fn connect_database(dsn: &SecretString) -> Result<Client> {
    let config: PgConfig = dsn
        .expose_secret()
        .parse()
        .context("invalid PostgreSQL connection string")?;
    let connector = TlsConnector::builder()
        .build()
        .context("failed to initialize TLS")?;
    let (client, connection) = config
        .connect(MakeTlsConnector::new(connector))
        .await
        .context("PostgreSQL connection failed")?;
    tokio::spawn(async move {
        if let Err(error) = connection.await {
            tracing::warn!(%error, "PostgreSQL connection closed");
        }
    });
    Ok(client)
}

async fn ensure_control_database_migrated(client: &Client) -> Result<()> {
    let row = client
        .query_one(
            "SELECT to_regclass('cloudberry_etl_control.schema_migrations') IS NOT NULL",
            &[],
        )
        .await?;
    if !row.get::<_, bool>(0) {
        bail!("control database is not migrated; run `etl-server-cloudberry migrate`");
    }
    let version: i64 = client
        .query_one(
            "SELECT COALESCE(max(version), 0) FROM cloudberry_etl_control.schema_migrations",
            &[],
        )
        .await?
        .get(0);
    if version != 1 {
        bail!("unsupported control database migration version {version}; expected 1");
    }
    Ok(())
}

#[derive(Debug)]
struct PostgresConnectionTester;

#[async_trait::async_trait]
impl ConnectionTester for PostgresConnectionTester {
    async fn test_source(&self, dsn: &SecretString) -> Result<ConnectionReport, String> {
        let client = connect_database(dsn)
            .await
            .map_err(sanitize_connection_error)?;
        let row = client
            .query_one(
                "SELECT current_setting('server_version_num'), current_setting('server_encoding'),\
                        current_setting('wal_level'),\
                        COALESCE((SELECT extversion FROM pg_extension WHERE extname='citus'), '')",
                &[],
            )
            .await
            .map_err(|error| sanitize_connection_error(error.into()))?;
        let version_num: String = row.get(0);
        let encoding: String = row.get(1);
        let wal_level: String = row.get(2);
        let citus_version: String = row.get(3);
        if !version_num.starts_with("18") {
            return Err(format!(
                "source must be PostgreSQL 18; server reported {version_num}"
            ));
        }
        if encoding != "UTF8" {
            return Err(format!(
                "source database encoding must be UTF8; server reported {encoding}"
            ));
        }
        if wal_level != "logical" {
            return Err(format!(
                "source wal_level must be logical; server reported {wal_level}"
            ));
        }
        let topology = if citus_version.is_empty() {
            "postgresql"
        } else {
            "citus"
        };
        let mut warnings = Vec::new();
        if topology == "citus" && !citus_version.starts_with("14.1") {
            warnings.push(format!(
                "Citus {citus_version} is not the validated 14.1 release"
            ));
        }
        Ok(ConnectionReport {
            server_version: if citus_version.is_empty() {
                version_num
            } else {
                format!("PostgreSQL {version_num} / Citus {citus_version}")
            },
            topology: topology.into(),
            warnings,
        })
    }

    async fn test_target(&self, dsn: &SecretString) -> Result<ConnectionReport, String> {
        let client = connect_database(dsn)
            .await
            .map_err(sanitize_connection_error)?;
        let row = client
            .query_one(
                "SELECT version(), current_setting('server_version_num'), current_setting('gp_role', true)",
                &[],
            )
            .await
            .map_err(|error| sanitize_connection_error(error.into()))?;
        let version: String = row.get(0);
        let version_num: String = row.get(1);
        let gp_role: Option<String> = row.get(2);
        if gp_role.is_none() && !version.to_ascii_lowercase().contains("cloudberry") {
            return Err("target is PostgreSQL but does not identify as Cloudberry".into());
        }
        Ok(ConnectionReport {
            server_version: format!("{version_num} ({version})"),
            topology: "cloudberry".into(),
            warnings: Vec::new(),
        })
    }
}

fn sanitize_connection_error(error: anyhow::Error) -> String {
    tracing::warn!(error = %error, "connection test failed");
    "connection failed; verify address, credentials, TLS, and server settings".into()
}
