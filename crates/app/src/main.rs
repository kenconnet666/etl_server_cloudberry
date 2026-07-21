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
use cloudberry_etl_engine::{
    runtime::{
        PostgresCloudberryJobFactory,
        reconciler::{PipelineReconciler, ReconcilerConfig},
    },
    supervisor::PipelineSupervisor,
};
use cloudberry_etl_metadata::{
    crypto::MasterKey,
    migration::{CONTROL_SCHEMA_VERSION, migrate_control_database},
    store::{ControlStore, PostgresControlStore, configure_control_session},
};
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use native_tls::TlsConnector;
use postgres_native_tls::MakeTlsConnector;
use secrecy::{ExposeSecret, SecretString};
use tokio_postgres::{Client, Config as PgConfig};
use tokio_util::sync::CancellationToken;
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
    drop(client);

    let control_pool = build_control_pool(&control_dsn, "pg2cb-control", 8)?;
    let lease_pool = build_control_pool(&control_dsn, "pg2cb-control-lease", 4)?;
    let control: Arc<dyn ControlStore> = Arc::new(PostgresControlStore::with_lease_pool(
        control_pool,
        lease_pool,
    ));
    let master_key = Arc::new(MasterKey::from_base64(&config.master_key()?)?);
    let supervisor = Arc::new(PipelineSupervisor::new());
    let state = AppState {
        control: Arc::clone(&control),
        master_key: Arc::clone(&master_key),
        supervisor: Arc::clone(&supervisor),
        connection_tester: Arc::new(PostgresConnectionTester),
        metrics_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        connection_test_gate: Arc::new(tokio::sync::Semaphore::new(4)),
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
    let factory = Arc::new(PostgresCloudberryJobFactory::new(
        Arc::clone(&control),
        master_key,
    ));
    let engine = &config.engine;
    let reconciler = PipelineReconciler::new(
        control,
        supervisor,
        factory,
        ReconcilerConfig {
            poll_interval: Duration::from_secs(engine.reconcile_interval_seconds),
            lease_ttl: Duration::from_secs(engine.lease_ttl_seconds),
            lease_renew_interval: Duration::from_secs(engine.lease_renew_interval_seconds),
            restart_backoff_initial: Duration::from_secs(engine.restart_backoff_initial_seconds),
            restart_backoff_max: Duration::from_secs(engine.restart_backoff_max_seconds),
            restart_backoff_reset_after: Duration::from_secs(engine.restart_backoff_reset_seconds),
        },
    )
    .context("invalid pipeline engine configuration")?;
    let shutdown = CancellationToken::new();
    let reconciler_shutdown = shutdown.clone();
    let reconciler_task = tokio::spawn(async move { reconciler.run(reconciler_shutdown).await });
    let signal_task = tokio::spawn(shutdown_signal(shutdown.clone()));
    tracing::info!(listen = %config.server.listen, "management server started");
    let server_shutdown = shutdown.clone();
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(async move { server_shutdown.cancelled().await });
    let result = supervise_service(
        shutdown,
        std::future::IntoFuture::into_future(server),
        reconciler_task,
    )
    .await;
    signal_task.abort();
    let _ = signal_task.await;
    result
}

async fn supervise_service<S>(
    shutdown: CancellationToken,
    server: S,
    mut reconciler_task: tokio::task::JoinHandle<()>,
) -> Result<()>
where
    S: std::future::Future<Output = std::io::Result<()>>,
{
    tokio::pin!(server);
    tokio::select! {
        server_result = &mut server => {
            let shutdown_requested = shutdown.is_cancelled();
            shutdown.cancel();
            reconciler_task
                .await
                .context("pipeline reconciler task terminated unexpectedly")?;
            server_result?;
            if shutdown_requested {
                Ok(())
            } else {
                bail!("management server exited unexpectedly")
            }
        }
        reconciler_result = &mut reconciler_task => {
            let shutdown_requested = shutdown.is_cancelled();
            shutdown.cancel();
            let server_result = server.await;
            match reconciler_result {
                Ok(()) if shutdown_requested => {
                    server_result?;
                    Ok(())
                }
                Ok(()) => {
                    if let Err(error) = server_result {
                        tracing::warn!(%error, "management server shutdown returned an error");
                    }
                    bail!("pipeline reconciler exited unexpectedly")
                }
                Err(error) => {
                    if let Err(server_error) = server_result {
                        tracing::warn!(%server_error, "management server shutdown returned an error");
                    }
                    Err(error).context("pipeline reconciler task terminated unexpectedly")
                }
            }
        }
    }
}

async fn shutdown_signal(shutdown: CancellationToken) {
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
    shutdown.cancel();
}

async fn connect_database(dsn: &SecretString) -> Result<Client> {
    let mut config: PgConfig = dsn
        .expose_secret()
        .parse()
        .context("invalid PostgreSQL connection string")?;
    config.connect_timeout(Duration::from_secs(5));
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

fn build_control_pool(dsn: &SecretString, application_name: &str, max_size: usize) -> Result<Pool> {
    let mut pg_config: PgConfig = dsn
        .expose_secret()
        .parse()
        .context("invalid PostgreSQL connection string")?;
    pg_config.application_name(application_name);
    configure_control_session(&mut pg_config);
    let connector = TlsConnector::builder()
        .build()
        .context("failed to initialize TLS")?;
    let manager = Manager::from_config(
        pg_config,
        MakeTlsConnector::new(connector),
        ManagerConfig {
            recycling_method: RecyclingMethod::Verified,
        },
    );
    Pool::builder(manager)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .wait_timeout(Some(Duration::from_secs(5)))
        .create_timeout(Some(Duration::from_secs(5)))
        .recycle_timeout(Some(Duration::from_secs(5)))
        .build()
        .context("failed to build control database connection pool")
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
    if version != CONTROL_SCHEMA_VERSION {
        bail!(
            "unsupported control database migration version {version}; expected {CONTROL_SCHEMA_VERSION}"
        );
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
                r#"SELECT current_setting('server_version_num'),
                          current_setting('server_encoding'),
                          current_setting('wal_level'),
                          COALESCE(
                              (SELECT extversion FROM pg_extension WHERE extname='citus'),
                              ''
                          )"#,
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

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use super::*;

    #[tokio::test]
    async fn unexpected_reconciler_exit_cancels_the_server_and_returns_an_error() {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server_stopped = Arc::new(AtomicBool::new(false));
        let stopped = Arc::clone(&server_stopped);
        let server = async move {
            server_shutdown.cancelled().await;
            stopped.store(true, Ordering::SeqCst);
            Ok::<(), std::io::Error>(())
        };
        let reconciler = tokio::spawn(async {});

        let error = supervise_service(shutdown.clone(), server, reconciler)
            .await
            .expect_err("unexpected reconciler exit must fail the service");

        assert!(shutdown.is_cancelled());
        assert!(server_stopped.load(Ordering::SeqCst));
        assert!(error.to_string().contains("reconciler exited unexpectedly"));
    }

    #[tokio::test]
    async fn reconciler_panic_cancels_the_server_and_returns_an_error() {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let server = async move {
            server_shutdown.cancelled().await;
            Ok::<(), std::io::Error>(())
        };
        let reconciler = tokio::spawn(async { panic!("reconciler test panic") });

        let error = supervise_service(shutdown.clone(), server, reconciler)
            .await
            .expect_err("reconciler panic must fail the service");

        assert!(shutdown.is_cancelled());
        assert!(
            error
                .to_string()
                .contains("reconciler task terminated unexpectedly")
        );
    }

    #[tokio::test]
    async fn requested_shutdown_allows_both_tasks_to_exit_cleanly() {
        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let reconciler_shutdown = shutdown.clone();
        let server = async move {
            server_shutdown.cancelled().await;
            Ok::<(), std::io::Error>(())
        };
        let reconciler = tokio::spawn(async move { reconciler_shutdown.cancelled().await });
        shutdown.cancel();

        supervise_service(shutdown, server, reconciler)
            .await
            .expect("requested shutdown succeeds");
    }
}
