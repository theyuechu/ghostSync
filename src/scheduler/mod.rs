/// Task scheduler and HTTP API server.
///
/// Phase 4 implementation:
/// - Cron-based scheduled execution
/// - Axum HTTP API for manual trigger and history inspection
/// - Integration with Store (SQLite) for run logging

use std::sync::Arc;

use anyhow::{Context, Result};
use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use cron::Schedule;
use serde::Serialize;
use tokio::sync::watch;

use crate::config::types::{Config, TaskConfig};
use crate::engine::run_task;
use crate::store::Store;

/// Shared application state for the HTTP API.
struct ApiState {
    config: Arc<Config>,
    store: Option<Arc<Store>>,
}

// ─── Public API ────────────────────────────────────────────────────────────

/// Start the daemon: HTTP API server + cron-based scheduled tasks.
///
/// Blocks until a shutdown signal is received (Ctrl+C).
pub async fn start_daemon(
    cfg: Arc<Config>,
    store: Option<Arc<Store>>,
    port: u16,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let (cron_shutdown_tx, cron_shutdown_rx) = watch::channel(false);
    let cron_store = store.clone();
    let cron_cfg = cfg.clone();

    // Start cron tasks in the background
    let cron_handle = tokio::spawn(async move {
        run_cron_scheduler(cron_cfg, cron_store, cron_shutdown_rx).await;
    });

    // Start API server
    serve_api(cfg, store, port, shutdown_rx).await?;

    // Signal cron to stop and wait for it
    let _ = cron_shutdown_tx.send(true);
    let _ = cron_handle.await;

    Ok(())
}

// ─── Cron Scheduler ────────────────────────────────────────────────────────

async fn run_cron_scheduler(
    cfg: Arc<Config>,
    store: Option<Arc<Store>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    // Collect tasks that have a cron schedule
    let scheduled_tasks: Vec<TaskConfig> = cfg
        .tasks
        .iter()
        .filter(|t| t.schedule.is_some())
        .cloned()
        .collect();

    if scheduled_tasks.is_empty() {
        tracing::info!("No scheduled tasks found — cron scheduler idle");
        // Stay alive until shutdown
        while !*shutdown_rx.borrow() {
            shutdown_rx.changed().await.ok();
        }
        return;
    }

    tracing::info!(
        "Starting cron scheduler with {} scheduled task(s)",
        scheduled_tasks.len()
    );

    // Spawn one background task per cron expression
    let mut handles = Vec::new();
    for task in scheduled_tasks {
        let cfg = cfg.clone();
        let store = store.clone();
        let mut shutdown = shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            loop {
                // Parse cron expression
                let expr = task
                    .schedule
                    .as_ref()
                    .expect("schedule must be set — filter guarantees this");
                let cron_schedule = match expr.parse::<Schedule>() {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!(
                            "Task '{}': invalid cron expression '{}': {}",
                            task.name,
                            expr,
                            e
                        );
                        // Don't retry, just exit this task
                        return;
                    }
                };

                // Find the next upcoming time
                let now = Utc::now();
                let next = cron_schedule.upcoming(Utc).next();

                let sleep_dur = match next {
                    Some(t) if t > now => {
                        let dur = t - now;
                        tracing::info!(
                            "Task '{}': next run at {} (in {:.0}s)",
                            task.name,
                            t.to_rfc3339(),
                            dur.num_seconds()
                        );
                        dur.to_std().unwrap_or(std::time::Duration::from_secs(60))
                    }
                    _ => {
                        // No upcoming time or already past — wait 60s and re-check
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        continue;
                    }
                };

                // Wait until the scheduled time, but also listen for shutdown
                let sleep = tokio::time::sleep(sleep_dur);
                tokio::select! {
                    _ = sleep => {
                        // Time to run
                        execute_and_log_task(&task, &cfg, &store).await;
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!("Task '{}': cron loop shutting down", task.name);
                            return;
                        }
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for shutdown signal, then drop
    while !*shutdown_rx.borrow() {
        shutdown_rx.changed().await.ok();
    }

    // Wait for all cron tasks to finish
    for handle in handles {
        let _ = handle.await;
    }
}

// ─── Task Execution (shared by cron + API) ────────────────────────────────

async fn execute_and_log_task(task: &TaskConfig, cfg: &Config, store: &Option<Arc<Store>>) {
    tracing::info!(
        "⏰ Triggered task: {} (source={}, target={})",
        task.name,
        task.source,
        task.target
    );

    // Record start
    let run_id = if let Some(s) = store {
        match s.log_run_start(&task.name).await {
            Ok(id) => {
                tracing::debug!("Logged run start (id={})", id);
                Some(id)
            }
            Err(e) => {
                tracing::warn!("Failed to log run start: {:?}", e);
                None
            }
        }
    } else {
        None
    };

    // Resolve source & target configs
    let source_cfg = match cfg.sources.iter().find(|s| s.name == task.source) {
        Some(s) => s,
        None => {
            let msg = format!("Source '{}' not found in config", task.source);
            tracing::error!("{}", msg);
            if let (Some(s), Some(id)) = (store.as_ref(), run_id) {
                let _ = s.log_run_complete(id, "failed", 0, 0, 0.0, Some(&msg)).await;
            }
            return;
        }
    };

    let target_cfg = match cfg.targets.iter().find(|t| t.name == task.target) {
        Some(t) => t,
        None => {
            let msg = format!("Target '{}' not found in config", task.target);
            tracing::error!("{}", msg);
            if let (Some(s), Some(id)) = (store.as_ref(), run_id) {
                let _ = s.log_run_complete(id, "failed", 0, 0, 0.0, Some(&msg)).await;
            }
            return;
        }
    };

    if task.dry_run {
        tracing::info!("DRY RUN — skipping execution for task '{}'", task.name);
        if let (Some(s), Some(id)) = (store.as_ref(), run_id) {
            let _ = s.log_run_complete(id, "dry_run", 0, 0, 0.0, None).await;
        }
        return;
    }

    // Run the engine
    let task_arc = Arc::new(task.clone());
    match run_task(
        task_arc,
        &source_cfg.db_kind,
        &source_cfg.connection,
        &target_cfg.db_kind,
        &target_cfg.connection,
        store.clone(),
    )
    .await
    {
        Ok(result) => {
            if result.success {
                tracing::info!(
                    "✅ Task '{}' completed: {} rows in {:.2}s ({:.0} rows/s)",
                    result.task_name,
                    result.stats.processed_rows,
                    result.stats.elapsed_secs,
                    result.stats.rows_per_sec,
                );
                if let (Some(s), Some(id)) = (store.as_ref(), run_id) {
                    let _ = s
                        .log_run_complete(
                            id,
                            "success",
                            result.stats.processed_rows,
                            result.stats.processed_rows,
                            result.stats.rows_per_sec,
                            None,
                        )
                        .await;
                }
            } else {
                let err = result.error.unwrap_or_else(|| "Unknown error".into());
                tracing::error!("❌ Task '{}' failed: {}", result.task_name, err);
                if let (Some(s), Some(id)) = (store.as_ref(), run_id) {
                    let _ = s
                        .log_run_complete(id, "failed", result.stats.processed_rows, result.stats.processed_rows, 0.0, Some(&err))
                        .await;
                }
            }
        }
        Err(e) => {
            let err = format!("{:?}", e);
            tracing::error!("❌ Task '{}' error: {}", task.name, err);
            if let (Some(s), Some(id)) = (store.as_ref(), run_id) {
                let _ = s.log_run_complete(id, "error", 0, 0, 0.0, Some(&err)).await;
            }
        }
    }
}

// ─── HTTP API Server ───────────────────────────────────────────────────────

async fn serve_api(
    cfg: Arc<Config>,
    store: Option<Arc<Store>>,
    port: u16,
    shutdown_rx: watch::Receiver<bool>,
) -> Result<()> {
    let state = Arc::new(ApiState { config: cfg, store });

    let app = Router::new()
        .route("/api/health", get(health_handler))
        .route("/api/tasks", get(list_tasks_handler))
        .route("/api/tasks/{name}/run", post(run_task_handler))
        .route("/api/tasks/{name}/logs", get(task_logs_handler))
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    tracing::info!("HTTP API listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind to {}", addr))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let mut rx = shutdown_rx;
            while !*rx.borrow() {
                rx.changed().await.ok();
            }
        })
        .await
        .context("HTTP server error")?;

    Ok(())
}

// ─── Handlers ──────────────────────────────────────────────────────────────

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime: f64,
}

async fn health_handler(State(_state): State<Arc<ApiState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        uptime: 0.0,
    })
}

#[derive(Serialize)]
struct TaskSummary {
    name: String,
    source: String,
    target: String,
    schedule: Option<String>,
    tables: Vec<String>,
}

async fn list_tasks_handler(
    State(state): State<Arc<ApiState>>,
) -> Json<Vec<TaskSummary>> {
    let tasks: Vec<TaskSummary> = state
        .config
        .tasks
        .iter()
        .map(|t| TaskSummary {
            name: t.name.clone(),
            source: t.source.clone(),
            target: t.target.clone(),
            schedule: t.schedule.clone(),
            tables: t.tables.iter().map(|tbl| tbl.name.clone()).collect(),
        })
        .collect();
    Json(tasks)
}

#[derive(Serialize)]
struct RunResponse {
    message: String,
    task: String,
    run_id: Option<i64>,
}

async fn run_task_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Json<RunResponse> {
    let task = match state.config.tasks.iter().find(|t| t.name == name) {
        Some(t) => t.clone(),
        None => {
            return Json(RunResponse {
                message: format!("Task '{}' not found", name),
                task: name,
                run_id: None,
            });
        }
    };

    // Record start
    let run_id = if let Some(s) = &state.store {
        match s.log_run_start(&task.name).await {
            Ok(id) => Some(id),
            Err(_) => None,
        }
    } else {
        None
    };

    // Execute in background — spawn so HTTP can return immediately
    let cfg = state.config.clone();
    let store = state.store.clone();

    // For simplicity, run synchronously and return result
    // (a production version would spawn this and return 202 Accepted)
    tokio::spawn(async move {
        execute_and_log_task(&task, &cfg, &store).await;
    });

    Json(RunResponse {
        message: format!("Task '{}' triggered", name),
        task: name,
        run_id,
    })
}

async fn task_logs_handler(
    State(state): State<Arc<ApiState>>,
    Path(name): Path<String>,
) -> Json<Vec<crate::store::RunRecord>> {
    if let Some(s) = &state.store {
        match s.get_run_history(&name, 50).await {
            Ok(records) => return Json(records),
            Err(e) => {
                tracing::warn!("Failed to get run history: {:?}", e);
            }
        }
    }
    Json(Vec::new())
}
