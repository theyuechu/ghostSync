mod config;
mod db;
mod engine;
mod logger;
mod rule;
mod scheduler;
mod store;

use std::path::PathBuf;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio::sync::watch;

/// GhostSync — Lightweight data sync & desensitization proxy.
#[derive(Parser, Debug)]
#[command(name = "ghostsync", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run sync task(s) once and exit
    Run {
        /// Path to the YAML config file
        config: PathBuf,

        /// Optional: run only a specific task by name
        #[arg(long)]
        task: Option<String>,

        /// Dry-run: print what would be done, don't actually sync
        #[arg(long, default_value_t = false)]
        dry_run: bool,
    },

    /// Start the GhostSync daemon (cron scheduler + HTTP API)
    Serve {
        /// Path to the YAML config file
        config: PathBuf,

        /// Port for the HTTP API (default: 9710)
        #[arg(long, default_value_t = 9710)]
        port: u16,

        /// Path to the SQLite store database (default: ~/.ghostsync/store.db)
        #[arg(long)]
        store: Option<String>,
    },

    /// Check connectivity to all configured sources and targets
    Check {
        /// Path to the YAML config file
        config: PathBuf,
    },

    /// Print the configuration after parsing (for debugging)
    Inspect {
        /// Path to the YAML config file
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Register SQLx AnyPool drivers (MySQL, PostgreSQL, SQLite)
    // Must be called before any database connection is established.
    sqlx::any::install_default_drivers();

    logger::init_logging()?;

    let cli = Cli::parse();

    match &cli.command {
        Commands::Run {
            config,
            task,
            dry_run,
        } => cmd_run(config, task.as_deref(), *dry_run).await,
        Commands::Serve {
            config,
            port,
            store,
        } => cmd_serve(config, *port, store.as_deref()).await,
        Commands::Check { config } => cmd_check(config).await,
        Commands::Inspect { config } => cmd_inspect(config),
    }
}

async fn cmd_run(config_path: &PathBuf, task_name: Option<&str>, dry_run: bool) -> anyhow::Result<()> {
    let cfg = config::loader::load_config(config_path)?;
    config::validator::validate_config(&cfg)?;

    tracing::info!("Loaded config: {} source(s), {} target(s), {} task(s)",
        cfg.sources.len(), cfg.targets.len(), cfg.tasks.len());

    if dry_run {
        tracing::info!("DRY RUN mode — no data will be synced");
    }

    let tasks: Vec<_> = if let Some(name) = task_name {
        cfg.tasks
            .iter()
            .filter(|t| t.name == name)
            .cloned()
            .collect::<Vec<_>>()
    } else {
        cfg.tasks
    };

    if tasks.is_empty() {
        anyhow::bail!("No matching tasks found");
    }

    for task in &tasks {
        tracing::info!("Running task: {} (source={}, target={})", task.name, task.source, task.target);

        // Resolve source and target connection configs
        let source_cfg = cfg.sources.iter().find(|s| s.name == task.source)
            .ok_or_else(|| anyhow::anyhow!("Source '{}' not found in config", task.source))?;
        let target_cfg = cfg.targets.iter().find(|t| t.name == task.target)
            .ok_or_else(|| anyhow::anyhow!("Target '{}' not found in config", task.target))?;

        // Resolve connection URLs with correct database type
        let mut source_conn = source_cfg.connection.clone();
        let mut target_conn = target_cfg.connection.clone();
        source_conn.resolve_with_kind(&source_cfg.db_kind)?;
        target_conn.resolve_with_kind(&target_cfg.db_kind)?;

        if !dry_run {
            match engine::run_task(
                std::sync::Arc::new(task.clone()),
                &source_cfg.db_kind,
                &source_conn,
                &target_cfg.db_kind,
                &target_conn,
            )
            .await
            {
                Ok(result) => {
                    if result.success {
                        tracing::info!(
                            "✅ Task '{}' completed: {} rows processed in {:.2}s ({:.0} rows/s)",
                            result.task_name,
                            result.stats.processed_rows,
                            result.stats.elapsed_secs,
                            result.stats.rows_per_sec
                        );
                    } else {
                        tracing::error!(
                            "❌ Task '{}' failed: {}",
                            result.task_name,
                            result.error.unwrap_or_default()
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("❌ Task '{}' error: {:?}", task.name, e);
                    eprintln!("❌ Task '{}' error: {:?}", task.name, e);
                }
            }
        } else {
            for table in &task.tables {
                tracing::info!("  Would sync table: {}", table.name);
                if let Some(rules) = &table.rules {
                    for rule in rules {
                        tracing::info!("    {} -> {}", rule.field, rule.rule);
                    }
                }
            }
        }
    }

    Ok(())
}

/// Default directory for GhostSync data files.
fn default_data_dir() -> PathBuf {
    // Use $HOME/.ghostsync or XDG_DATA_HOME/ghostsync
    if let Ok(home) = std::env::var("HOME") {
        PathBuf::from(home).join(".ghostsync")
    } else if let Ok(data) = std::env::var("XDG_DATA_HOME") {
        PathBuf::from(data).join("ghostsync")
    } else {
        PathBuf::from(".ghostsync")
    }
}

async fn cmd_serve(config_path: &PathBuf, port: u16, store_path: Option<&str>) -> anyhow::Result<()> {
    let cfg = config::loader::load_config(config_path)?;
    config::validator::validate_config(&cfg)?;

    // Determine store path
    let store_path = match store_path {
        Some(p) => p.to_string(),
        None => {
            let dir = default_data_dir();
            tokio::fs::create_dir_all(&dir).await?;
            dir.join("store.db")
                .to_string_lossy()
                .into_owned()
        }
    };

    tracing::info!("Using store database at: {}", store_path);

    // Initialize store
    let store = Arc::new(store::Store::open(&store_path).await?);
    tracing::info!("SQLite store initialized");

    let cfg = Arc::new(cfg);
    let store_opt = Some(store);

    // Create shutdown channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Handle Ctrl+C
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        tracing::info!("Shutdown signal received — stopping daemon...");
        let _ = shutdown_tx.send(true);
    });

    tracing::info!(
        "Starting GhostSync daemon — HTTP API on port {}, {} task(s), {} scheduled",
        port,
        cfg.tasks.len(),
        cfg.tasks.iter().filter(|t| t.schedule.is_some()).count(),
    );

    // Start daemon (blocks until shutdown)
    scheduler::start_daemon(cfg, store_opt, port, shutdown_rx).await?;

    tracing::info!("GhostSync daemon stopped");
    Ok(())
}

async fn cmd_check(config_path: &PathBuf) -> anyhow::Result<()> {
    let cfg = config::loader::load_config(config_path)?;
    config::validator::validate_config(&cfg)?;

    tracing::info!("Checking connectivity...");

    for source in &cfg.sources {
        let url = source.connection.url.as_deref().unwrap_or("unknown");
        tracing::info!("  Testing source '{}' ({}): {}", source.name, source.db_kind, url);
        match db::connector::test_single_connection(&source.db_kind, &source.connection).await {
            Ok(version) => tracing::info!("    ✅ Connected: {}", version),
            Err(e) => tracing::error!("    ❌ Failed: {:?}", e),
        }
    }

    for target in &cfg.targets {
        let url = target.connection.url.as_deref().unwrap_or("unknown");
        tracing::info!("  Testing target '{}' ({}): {}", target.name, target.db_kind, url);
        match db::connector::test_single_connection(&target.db_kind, &target.connection).await {
            Ok(version) => tracing::info!("    ✅ Connected: {}", version),
            Err(e) => tracing::error!("    ❌ Failed: {:?}", e),
        }
    }

    Ok(())
}

fn cmd_inspect(config_path: &PathBuf) -> anyhow::Result<()> {
    let cfg = config::loader::load_config(config_path)?;
    config::validator::validate_config(&cfg)?;

    let yaml_output = serde_yaml::to_string(&cfg)?;
    println!("{}", yaml_output);

    Ok(())
}
