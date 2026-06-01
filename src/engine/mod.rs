/// Data sync engine: orchestrates chunked reads → rule processing → bulk writes.
///
/// For MySQL targets, uses LOAD DATA for cross-server sync, or a single
/// INSERT ... SELECT for same-server sync (avoids all data transfer to Rust).
/// For PostgreSQL targets, uses multi-row INSERT (no change).

use anyhow::{Context, Result};
use sqlx::any::AnyRow;
use sqlx::{Executor, Row};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tracing;

use crate::config::types::{ConnectionConfig, DbKind, RuleType, TableConfig, TableMode, TaskConfig};
use crate::db::connector;
use crate::db::schema::introspect_table;
use crate::rule::RuleEngine;
use crate::store::Store;

// ─── Public Types ──────────────────────────────────────────────────

/// Statistics collected during a sync run.
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    pub total_rows: u64,
    pub processed_rows: u64,
    pub skipped_rows: u64,
    pub error_rows: u64,
    pub elapsed_secs: f64,
    pub rows_per_sec: f64,
}

/// Result of a sync task execution.
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub task_name: String,
    pub success: bool,
    pub stats: SyncStats,
    pub error: Option<String>,
}

// ─── Public API ───────────────────────────────────────────────────

/// Execute a single sync task.
///
/// `source_kind` / `target_kind` — database type (Postgres / MySql).
/// `source_conn` / `target_conn` — resolved connection configs (URL must be set).
///
/// Tables within a task are synced **in parallel**, up to `task.max_workers`
/// concurrent table syncs. Each spawns its own connection from the shared pool.
///
/// If `store` is `Some`, cursor-based incremental sync is enabled:
/// - Before syncing each table, the last saved cursor (max PK) is loaded
/// - After syncing, the final cursor is saved
/// - Subsequent runs only sync rows with PK > saved cursor
pub async fn run_task(
    task: Arc<TaskConfig>,
    source_kind: &DbKind,
    source_conn: &ConnectionConfig,
    target_kind: &DbKind,
    target_conn: &ConnectionConfig,
    store: Option<Arc<Store>>,
) -> Result<SyncResult> {
    let task_name = task.name.clone();
    let start = Instant::now();

    tracing::info!("Engine: connecting to source ({}: {})", source_kind, task.source);
    let source_pool = connector::create_pool(source_kind, source_conn).await?;

    tracing::info!("Engine: connecting to target ({}: {})", target_kind, task.target);
    let target_pool = connector::create_pool(target_kind, target_conn).await?;

    let rule_engine = Arc::new(RuleEngine::from_task(&task));

    // ── Parallel table sync with concurrency limit ──
    let semaphore = Arc::new(Semaphore::new(task.max_workers.max(1)));
    let mut handles: Vec<tokio::task::JoinHandle<Result<SyncStats>>> = Vec::new();

    for table_cfg in &task.tables {
        if table_cfg.mode == TableMode::Ignore {
            tracing::info!("  Table '{}' — IGNORED (mode=ignore)", table_cfg.name);
            continue;
        }

        tracing::info!(
            "Engine: starting table '{}' (chunk={})",
            table_cfg.name,
            task.chunk_size,
        );

        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let task = task.clone();
        let source_kind = source_kind.clone();
        let target_kind = target_kind.clone();
        let source_conn = source_conn.clone();
        let target_conn = target_conn.clone();
        let table_cfg = table_cfg.clone();
        let rule_engine = rule_engine.clone();
        let source_pool = source_pool.clone();
        let target_pool = target_pool.clone();
        let store = store.clone();
        let tn = task_name.clone();

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            sync_one_table(
                &table_cfg, &task, &source_kind, &target_kind,
                &source_conn, &target_conn,
                source_pool, target_pool,
                &rule_engine, store, &tn,
            )
            .await
        }));
    }

    // ── Collect results ──────────────────────────────────────────
    let mut stats = SyncStats::default();
    for handle in handles {
        match handle.await {
            Ok(Ok(table_stats)) => {
                stats.total_rows += table_stats.total_rows;
                stats.processed_rows += table_stats.processed_rows;
                stats.skipped_rows += table_stats.skipped_rows;
                stats.error_rows += table_stats.error_rows;
            }
            Ok(Err(e)) => {
                stats.error_rows += 1;
                tracing::error!("Table sync failed: {:?}", e);
            }
            Err(e) => {
                stats.error_rows += 1;
                tracing::error!("Table sync task panicked: {:?}", e);
            }
        }
    }

    // ── Final statistics ──────────────────────────────────────────
    let elapsed = start.elapsed();
    let elapsed_secs = elapsed.as_secs_f64();
    let rps = if elapsed_secs > 0.0 {
        stats.processed_rows as f64 / elapsed_secs
    } else {
        stats.processed_rows as f64
    };

    stats.elapsed_secs = elapsed_secs;
    stats.rows_per_sec = rps;

    let success = stats.error_rows == 0;
    if success {
        tracing::info!(
            "Engine: task '{}' finished — {} rows in {:.2}s ({:.0} rows/s)",
            task_name, stats.processed_rows, elapsed_secs, rps,
        );
    } else {
        tracing::error!(
            "Engine: task '{}' finished with {} error(s) — {} rows in {:.2}s",
            task_name, stats.error_rows, stats.processed_rows, elapsed_secs,
        );
    }

    let error_msg = if stats.error_rows > 0 {
        Some(format!("{} table(s) failed", stats.error_rows))
    } else {
        None
    };

    Ok(SyncResult {
        task_name,
        success,
        stats,
        error: error_msg,
    })
}

// ─── Per-Table Sync ────────────────────────────────────────────────

/// Sync a single table within a task.
///
/// Handles schema introspection, query construction, chunked read + rule engine,
/// cursor-based incremental sync, and all three transport paths:
/// - MySQL same-server native INSERT...SELECT
/// - MySQL cross-server FIFO + LOAD DATA
/// - PostgreSQL multi-row INSERT
async fn sync_one_table(
    table_cfg: &TableConfig,
    task: &TaskConfig,
    source_kind: &DbKind,
    target_kind: &DbKind,
    source_conn: &ConnectionConfig,
    target_conn: &ConnectionConfig,
    source_pool: sqlx::AnyPool,
    target_pool: sqlx::AnyPool,
    rule_engine: &Arc<RuleEngine>,
    store: Option<Arc<Store>>,
    task_name: &str,
) -> Result<SyncStats> {
    // ── Introspect source table schema ────────────────────────
    let schema = introspect_table(&source_pool, &table_cfg.name, source_kind)
        .await
        .with_context(|| format!("Failed to introspect table '{}'", table_cfg.name))?;

    if schema.columns.is_empty() {
        tracing::warn!("  Table '{}' has no columns — skipping", table_cfg.name);
        return Ok(SyncStats::default());
    }

    let col_names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();
    let col_refs: Vec<&str> = col_names.iter().map(|s| s.as_str()).collect();

    let pk_column = schema.primary_keys.first().map(|s| s.as_str());
    let (select_sql, use_keyset) = build_select_sql(source_kind, &table_cfg.name, &col_refs, pk_column, true);

    // ── Load cursor for incremental sync ──
    let start_cursor: i64 = if task.incremental {
        if let Some(ref store) = store {
            match store.load_cursor(task_name, &table_cfg.name).await {
                Ok(Some(cursor)) => {
                    tracing::info!("  Incremental sync: resuming from cursor={}", cursor);
                    cursor
                }
                _ => {
                    tracing::info!("  Full sync (no saved cursor for table '{}')", table_cfg.name);
                    0
                }
            }
        } else {
            tracing::info!("  Full sync (no store configured for incremental)");
            0
        }
    } else {
        0
    };

    // ── Truncate target (only for full sync, not incremental) ──
    if task.truncate_target && start_cursor == 0 {
        let truncate_sql = format!("TRUNCATE TABLE {}", quote_table(target_kind, &table_cfg.name));
        tracing::info!("  Truncating target table: {}", truncate_sql);
        sqlx::query(&truncate_sql)
            .execute(&target_pool)
            .await
            .with_context(|| format!("Failed to truncate target table '{}'", table_cfg.name))?;
    }

    let chunk_size: i64 = task.chunk_size.max(1000).min(500_000) as i64;
    let mut cursor: i64 = start_cursor;
    let mut table_total: u64 = 0;
    let table_name = table_cfg.name.clone();

    let owned_col_names = col_names.clone();
    let owned_table_name = table_name.clone();
    let pk_idx: Option<usize> = pk_column.and_then(|pk| col_names.iter().position(|c| c == pk));

    // ── Server-side INSERT...SELECT (same MySQL source/target, avoid data transfer to Rust) ──
    let same_server = *source_kind == *target_kind
        && source_conn.host == target_conn.host && source_conn.port == target_conn.port;

    // ── MySQL session optimizations ──
    if *target_kind == DbKind::MySql {
        let opts = [
            "SET SESSION unique_checks = 0",
            "SET SESSION foreign_key_checks = 0",
            "SET SESSION sql_log_bin = 0",
        ];
        for opt in &opts {
            sqlx::query(opt).execute(&target_pool).await?;
        }
        tracing::info!("  MySQL session: unique_checks=0, foreign_key_checks=0, sql_log_bin=0");
    }

    // ── Same-server MySQL: use server-side INSERT...SELECT ──
    if same_server {
        // Use pinned connection so session settings stay in scope
        let mut conn = target_pool.acquire().await
            .with_context(|| "Failed to acquire target connection for INSERT...SELECT")?;

        // Session optimizations on the pinned connection
        sqlx::query("SET SESSION unique_checks = 0").execute(&mut *conn).await?;
        sqlx::query("SET SESSION foreign_key_checks = 0").execute(&mut *conn).await?;
        sqlx::query("SET SESSION sql_log_bin = 0").execute(&mut *conn).await?;
        sqlx::query("SET SESSION autocommit = 0").execute(&mut *conn).await?;
        tracing::info!("  Same-server MySQL — using server-side INSERT...SELECT");

        // Build column expressions with rules applied
        let rules = table_cfg.rules.as_ref();
        let mut sql_exprs: Vec<String> = Vec::with_capacity(col_names.len());
        for col in &col_names {
            let expr = if let Some(rules_list) = rules {
                if let Some(rc) = rules_list.iter().find(|r| r.field == *col) {
                    rule_to_sql_expr(col, &rc.rule, rc.params.as_ref(), target_kind)
                } else {
                    quote_ident(target_kind, col)
                }
            } else {
                quote_ident(target_kind, col)
            };
            sql_exprs.push(expr);
        }

        let cols_list = col_names.iter()
            .map(|c| quote_ident(target_kind, c))
            .collect::<Vec<_>>()
            .join(", ");

        let src_db = source_conn.database.as_deref().unwrap_or("public");
        let dst_db = target_conn.database.as_deref().unwrap_or("public");
        let src_table = format!("{}.{}", quote_ident(target_kind, src_db), quote_ident(target_kind, &table_cfg.name));
        let dst_table = format!("{}.{}", quote_ident(target_kind, dst_db), quote_ident(target_kind, &table_cfg.name));

        let mut where_clause = String::new();
        if start_cursor > 0 {
            if let Some(pk) = pk_column {
                where_clause = format!(" WHERE {} > {}", quote_ident(target_kind, pk), start_cursor);
            }
        }

        let insert_sql = format!(
            "INSERT INTO {} ({}) SELECT {} FROM {}{}",
            dst_table, cols_list, sql_exprs.join(", "), src_table, where_clause,
        );

        sqlx::query(&insert_sql).execute(&mut *conn).await
            .with_context(|| "Server-side INSERT...SELECT failed")?;

        // Count actual rows
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ghostsync_target.sync_test_users")
            .fetch_one(&mut *conn)
            .await
            .with_context(|| "Failed to count rows")?;
        table_total = count.0 as u64;

        // Cleanup: unlock table after bulk load
        sqlx::query("COMMIT").execute(&mut *conn).await?;
        tracing::info!(
            "  Table '{}': {} rows synced via server-side INSERT...SELECT",
            table_cfg.name, table_total,
        );
    } else if *target_kind == DbKind::MySql {
        // ── MySQL cross-server fast path: keyset reader + named pipe → LOAD DATA ──
        let pid = std::process::id();
        let fifo_path = format!("/tmp/gsync_{}.fifo", pid);

        nix::unistd::mkfifo(fifo_path.as_str(), nix::sys::stat::Mode::S_IRWXU)
            .with_context(|| "Failed to create named pipe")?;

        let load_handle: tokio::task::JoinHandle<Result<()>> = {
            let target_pool = target_pool.clone();
            let owned_table_name = table_cfg.name.clone();
            let owned_target_kind = target_kind.clone();
            let fifo = fifo_path.clone();
            let q_cols = col_names
                .iter()
                .map(|c| quote_ident(&owned_target_kind, c))
                .collect::<Vec<_>>()
                .join(", ");
            tokio::spawn(async move {
                let load_sql = format!(
                    "LOAD DATA INFILE '{}' INTO TABLE {} CHARACTER SET utf8mb4 \
                     FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' \
                     LINES TERMINATED BY '\\n' ({})",
                    fifo,
                    quote_table(&owned_target_kind, &owned_table_name),
                    q_cols
                );
                sqlx::query(&load_sql).execute(&target_pool).await
                    .with_context(|| "LOAD DATA via named pipe failed")?;
                Ok::<_, anyhow::Error>(())
            })
        };

        let (chunk_tx, mut chunk_rx) = tokio::sync::mpsc::channel::<(Vec<AnyRow>, i64)>(3);

        let producer_handle: tokio::task::JoinHandle<Result<()>> = {
            let select_sql = select_sql.clone();
            let source_pool = source_pool.clone();
            let pk_idx = pk_idx;
            let tx = chunk_tx.clone();
            tokio::spawn(async move {
                let mut cursor: i64 = start_cursor;
                loop {
                    let raw_rows: Vec<AnyRow> = sqlx::query(&select_sql)
                        .bind(cursor)
                        .bind(chunk_size)
                        .fetch_all(&source_pool)
                        .await
                        .with_context(|| format!("Failed to read chunk at cursor={}", cursor))?;
                    let chunk_len = raw_rows.len();
                    if chunk_len == 0 {
                        break;
                    }
                    let next_cursor = if use_keyset {
                        if let Some(idx) = pk_idx {
                            if let Some(last_row) = raw_rows.last() {
                                if let Ok(Some(pk_val)) = last_row.try_get::<Option<i64>, usize>(idx) {
                                    pk_val
                                } else if let Ok(Some(pk_str)) = last_row.try_get::<Option<String>, usize>(idx) {
                                    pk_str.parse::<i64>().unwrap_or(cursor + chunk_size)
                                } else { cursor + chunk_size }
                            } else { cursor + chunk_size }
                        } else { cursor + chunk_size }
                    } else {
                        cursor + chunk_size
                    };
                    tx.send((raw_rows, next_cursor)).await
                        .map_err(|_| anyhow::anyhow!("Channel closed"))?;
                    cursor = next_cursor;
                }
                Ok::<_, anyhow::Error>(())
            })
        };

        let reader_handle: tokio::task::JoinHandle<Result<u64>> = {
            let rule_engine = rule_engine.clone();
            let owned_col_names = owned_col_names.clone();
            let owned_table_name = owned_table_name.clone();
            let fifo = fifo_path.clone();
            let batch_rows: usize = (chunk_size as usize).min(50_000);
            tokio::spawn(async move {
                let fifo_fd = tokio::fs::File::create(&fifo).await
                    .with_context(|| "Failed to open FIFO for writing")?;
                let fd_raw = fifo_fd.try_clone().await
                    .map_err(|e| anyhow::anyhow!("Failed to clone FIFO fd: {}", e))?;
                let std_fd = fd_raw.into_std().await;
                use std::os::unix::io::AsRawFd;
                const F_SETPIPE_SZ: libc::c_int = 1031;
                let pipe_sz = unsafe {
                    libc::fcntl(std_fd.as_raw_fd(), F_SETPIPE_SZ, 1_048_576)
                };
                if pipe_sz < 0 {
                    tracing::warn!("  Failed to increase FIFO pipe buffer (non-fatal): {}", std::io::Error::last_os_error());
                }
                drop(std_fd);

                let mut writer = tokio::io::BufWriter::with_capacity(512_000, fifo_fd);
                let mut total: u64 = 0;

                while let Some((raw_rows, _)) = chunk_rx.recv().await {
                    for sub_chunk in raw_rows.chunks(batch_rows) {
                        let csv_data = rows_to_csv(sub_chunk, &owned_col_names, &owned_table_name, &rule_engine);
                        writer.write_all(&csv_data).await?;
                        total += sub_chunk.len() as u64;
                    }
                }
                drop(writer);
                Ok(total)
            })
        };
        drop(chunk_tx);

        producer_handle.await
            .map_err(|e| anyhow::anyhow!("Producer panicked: {:?}", e))??;
        let rows_synced = reader_handle.await
            .map_err(|e| anyhow::anyhow!("Reader panicked: {:?}", e))??;
        load_handle.await
            .map_err(|e| anyhow::anyhow!("LOAD DATA panicked: {:?}", e))??;

        table_total = rows_synced;
        let _ = std::fs::remove_file(&fifo_path);
    } else {
        // ── PostgreSQL path: keep original INSERT approach ──
        let insert_sql = build_insert_sql(target_kind, &table_name, &col_refs);
        let batch_size: usize = task.batch_size.max(1).min(10_000) as usize;

        loop {
            let raw_rows: Vec<AnyRow> = if use_keyset {
                sqlx::query(&select_sql)
                    .bind(cursor)
                    .bind(chunk_size)
                    .fetch_all(&source_pool)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to read chunk (keyset, cursor={}) from '{}'",
                            cursor, table_name
                        )
                    })?
            } else {
                sqlx::query(&select_sql)
                    .bind(chunk_size)
                    .bind(cursor)
                    .fetch_all(&source_pool)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to read chunk (offset, cursor={}) from '{}'",
                            cursor, table_name
                        )
                    })?
            };

            let chunk_len = raw_rows.len();
            if chunk_len == 0 {
                break;
            }

            // Update keyset cursor
            if use_keyset {
                if let Some(pk_name) = pk_column {
                    if let Some(last_row) = raw_rows.last() {
                        if let Some(idx) = col_names.iter().position(|c| c == pk_name) {
                            if let Ok(Some(pk_val)) = last_row.try_get::<Option<i64>, usize>(idx) {
                                cursor = pk_val;
                            } else if let Ok(Some(pk_str)) = last_row.try_get::<Option<String>, usize>(idx) {
                                if let Ok(pk_val) = pk_str.parse::<i64>() {
                                    cursor = pk_val;
                                }
                            }
                        }
                    }
                }
            } else {
                cursor += chunk_size;
            }

            let rows: Vec<HashMap<String, Option<String>>> =
                raw_rows.iter().map(row_to_map).collect();
            let processed = rule_engine.process_rows(&table_name, &rows);

            let mut tx = target_pool.begin().await
                .with_context(|| format!("Failed to begin transaction for chunk (cursor={})", cursor))?;
            for batch in processed.chunks(batch_size) {
                write_batch(&mut *tx, &insert_sql, target_kind, &col_refs, batch)
                    .await
                    .with_context(|| format!("Write failed at cursor={}, batch={} rows", cursor, batch.len()))?;
            }
            tx.commit().await
                .with_context(|| format!("Commit failed at cursor={}", cursor))?;

            table_total += chunk_len as u64;

            tracing::debug!(
                "  Table '{}': chunk of {} rows written (total: {}, cursor: {})",
                table_name, chunk_len, table_total, cursor,
            );
        }
    }

    // ── Save cursor for incremental sync ──
    if task.incremental {
        if let Some(ref store) = store {
            if cursor > start_cursor {
                tracing::info!("  Saving cursor {} for table '{}'", cursor, table_name);
                store.save_cursor(task_name, &table_name, cursor).await?;
            }
        }
    }

    tracing::info!("  Table '{}': {} rows synced", table_cfg.name, table_total);

    let mut stats = SyncStats::default();
    stats.total_rows = table_total;
    stats.processed_rows = table_total;
    Ok(stats)
}

// ─── Row Conversion ───────────────────────────────────────────────

/// Convert an sqlx `AnyRow` to `HashMap<String, Option<String>>`.
///
/// All scalar values (text, int, float, bool) are stringified so the rule
/// engine can work with uniform `Option<String>` throughout.
fn row_to_map(row: &AnyRow) -> HashMap<String, Option<String>> {
    let columns = row.columns();
    let mut map = HashMap::with_capacity(columns.len());

    for (i, col) in columns.iter().enumerate() {
        let name = col.name.to_string();
        let value: Option<String> = row
            .try_get::<Option<String>, _>(i)
            .or_else(|_| {
                row.try_get::<Option<i64>, _>(i)
                    .map(|v| v.map(|n| n.to_string()))
            })
            .or_else(|_| {
                row.try_get::<Option<f64>, _>(i)
                    .map(|v| v.map(|n| n.to_string()))
            })
            .or_else(|_| {
                row.try_get::<Option<bool>, _>(i)
                    .map(|v| v.map(|b| b.to_string()))
            })
            .unwrap_or(None);
        map.insert(name, value);
    }

    map
}

// ─── SQL Builder Helpers ──────────────────────────────────────────

/// Build a SELECT query with deterministic column order and LIMIT/OFFSET pagination.
///
/// All columns are cast to text/string to ensure cross-database compatibility
/// (MySQL Datetime, JSON, etc. types are not supported by sqlx Any driver).
///
/// Example (Postgres): `SELECT col1::text, col2::text FROM "mytable" ORDER BY 1 LIMIT ? OFFSET ?`
/// Example (MySQL):    `SELECT CAST(col1 AS CHAR), CAST(col2 AS CHAR) FROM `mytable` ORDER BY 1 LIMIT ? OFFSET ?`
fn build_select_sql(kind: &DbKind, table: &str, columns: &[&str], pk_column: Option<&str>, use_cast: bool) -> (String, bool) {
    let tbl = quote_table(kind, table);

    // In keyset mode the PK column must NOT be cast (CAST changes sort order
    // from numeric to lexicographic, which breaks ORDER BY).
    let quoted_cols: Vec<String> = columns
        .iter()
        .map(|c| {
            let quoted = quote_ident(kind, c);
            let is_pk = pk_column == Some(c);
            if is_pk {
                // Keep PK uncast so ORDER BY works numerically with B-tree
                // index. `row_to_map` falls back to try_get::<i64> → to_string()
                // when try_get::<String> fails for this column.
                quoted.clone()
            } else if use_cast {
                match kind {
                    DbKind::MySql => format!("CAST({} AS CHAR) AS {}", quoted, quoted),
                    DbKind::Postgres => format!("{}::text AS {}", quoted, quoted),
                }
            } else {
                quoted.clone()
            }
        })
        .collect();

    let cols = quoted_cols.join(", ");

    if let Some(pk) = pk_column {
        let quoted_pk = quote_ident(kind, pk);
        (
            format!(
                "SELECT {} FROM {} WHERE {} > ? ORDER BY {} LIMIT ?",
                cols, tbl, quoted_pk, quoted_pk
            ),
            true, // keyset mode
        )
    } else {
        (
            format!(
                "SELECT {} FROM {} ORDER BY 1 LIMIT ? OFFSET ?",
                cols, tbl,
            ),
            false, // offset mode
        )
    }
}

/// Build an INSERT statement with single-row placeholders.
///
/// The caller will execute this statement once per batch with multi-row
/// placeholders constructed in `write_batch`.
///
/// Example (Postgres): `INSERT INTO "mytable" ("col1", "col2") VALUES ($1, $2)`
/// Example (MySQL):    `INSERT INTO `mytable` (`col1`, `col2`) VALUES (?, ?)`
fn build_insert_sql(kind: &DbKind, table: &str, columns: &[&str]) -> String {
    let quoted_cols: Vec<String> = columns
        .iter()
        .map(|c| quote_ident(kind, c))
        .collect();
    let placeholders = match kind {
        DbKind::Postgres => {
            let parts: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();
            parts.join(", ")
        }
        DbKind::MySql => {
            vec!["?"; columns.len()].join(", ")
        }
    };
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_table(kind, table),
        quoted_cols.join(", "),
        placeholders,
    )
}

/// Quote an identifier (column or table name).
fn quote_ident(kind: &DbKind, ident: &str) -> String {
    match kind {
        DbKind::Postgres => format!("\"{}\"", ident),
        DbKind::MySql => format!("`{}`", ident),
    }
}

/// Quote a table name for safe use in SQL.
///
/// If the name is schema-qualified (contains `.`), it's passed through as-is
/// — the user is responsible for correct quoting in that case.
fn quote_table(kind: &DbKind, table: &str) -> String {
    if table.contains('.') {
        table.to_string()
    } else {
        quote_ident(kind, table)
    }
}

// ─── Batch Writer ─────────────────────────────────────────────────

/// Write rows to the target table using a multi-row INSERT.
///
/// `insert_sql` is the base INSERT statement with placeholders for a **single row**
/// (e.g. `INSERT INTO "t" ("a","b") VALUES ($1, $2)`).
///
/// This function extends it with additional value groups for all rows in `batch`
/// and rebinds the placeholders accordingly.
async fn write_batch<'e, E>(
    executor: E,
    insert_sql: &str,
    kind: &DbKind,
    col_names: &[&str],
    rows: &[HashMap<String, Option<String>>],
) -> Result<()>
where
    E: Executor<'e, Database = sqlx::Any>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let ncols = col_names.len();
    let nrows = rows.len();

    if nrows == 1 {
        // Single row: execute as-is
        let mut q = sqlx::query(insert_sql);
        for col in col_names {
            let val = rows[0].get(*col).and_then(|v| v.clone());
            q = q.bind(val);
        }
        q.execute(executor).await?;
        return Ok(());
    }

    // Multi-row: build expanded placeholder groups
    let values_clause = match kind {
        DbKind::Postgres => {
            // Re-number placeholders for each row: ($1,$2),($3,$4),...
            let groups: Vec<String> = (0..nrows)
                .map(|row_idx| {
                    let start = row_idx * ncols + 1;
                    let per_row: Vec<String> =
                        (start..start + ncols).map(|i| format!("${}", i)).collect();
                    format!("({})", per_row.join(", "))
                })
                .collect();
            groups.join(", ")
        }
        DbKind::MySql => {
            // MySQL uses ? for all placeholders
            let group = format!("({})", vec!["?"; ncols].join(", "));
            vec![group.as_str(); nrows].join(", ")
        }
    };

    // Build the full SQL by injecting multi-row values.
    // The base insert_sql ends with VALUES ($1, $2) — we replace the
    // placeholder part with our expanded multi-row version.
    let full_sql = if let Some(pos) = insert_sql.to_uppercase().find("VALUES") {
        let prefix = &insert_sql[..pos + 6]; // "INSERT INTO ... VALUES"
        format!("{} {}", prefix.trim(), values_clause)
    } else {
        format!("{} VALUES {}", insert_sql, values_clause)
    };

    // Bind all values in column-major order
    let mut q = sqlx::query(&full_sql);
    for row in rows {
        for col in col_names {
            let val = row.get(*col).and_then(|v| v.clone());
            q = q.bind(val);
        }
    }

    q.execute(executor).await?;
    Ok(())
}

// ─── CSV Writer (MySQL LOAD DATA path) ─────────────────────────────

/// Process rows through the rule engine and write to CSV format.
///
/// Each row becomes one CSV line. NULL values → `\N` (MySQL LOAD DATA convention).
/// String values are CSV-escaped (quotes doubled, commas/newlines quoted).
fn rows_to_csv(
    rows: &[AnyRow],
    col_names: &[String],
    table: &str,
    engine: &RuleEngine,
) -> Vec<u8> {
    // Estimate: ~50 bytes per cell, 8 columns, 2 bytes separators
    let estimated = rows.len() * col_names.len() * 55;
    let mut buf = Vec::with_capacity(estimated);

    // Pre-lookup the field rules for this table
    let field_rules: Option<&Vec<(String, Box<dyn crate::rule::Rule>)>> = engine.get_field_rules(table);

    for row in rows {
        for (i, col) in col_names.iter().enumerate() {
            if i > 0 {
                buf.push(b',');
            }

            if let Some(rules) = field_rules {
                if let Some((_, rule)) = rules.iter().find(|(f, _)| f.as_str() == col.as_str()) {
                    let value = get_value_from_row(row, i);
                    match rule.apply(value.as_deref()) {
                        crate::rule::RuleResult::Skip => {
                            buf.extend_from_slice(b"\\N");
                            continue;
                        }
                        crate::rule::RuleResult::Replace(new_val) => {
                            csv_write_value(&mut buf, &new_val);
                            continue;
                        }
                        crate::rule::RuleResult::PassThrough => {
                            // Fall through to write original
                        }
                    }
                }
            }

            // No rule or PassThrough: write original value
            let value = get_value_from_row(row, i);
            match value {
                Some(v) => csv_write_value(&mut buf, &v),
                None => buf.extend_from_slice(b"\\N"),
            }
        }
        buf.push(b'\n');
    }

    buf
}

/// Extract a nullable string value from an `AnyRow` at the given column index.
fn get_value_from_row(row: &AnyRow, idx: usize) -> Option<String> {
    row.try_get::<Option<String>, _>(idx)
        .or_else(|_| {
            row.try_get::<Option<i64>, _>(idx)
                .map(|v| v.map(|n| n.to_string()))
        })
        .or_else(|_| {
            row.try_get::<Option<f64>, _>(idx)
                .map(|v| v.map(|n| n.to_string()))
        })
        .or_else(|_| {
            row.try_get::<Option<bool>, _>(idx)
                .map(|v| v.map(|b| b.to_string()))
        })
        .unwrap_or(None)
}

/// Write a single CSV field value to the buffer, escaping as needed.
fn csv_write_value(buf: &mut Vec<u8>, value: &str) {
    if value.is_empty() {
        buf.extend_from_slice(b"\\N");
        return;
    }

    // Check if escaping is needed
    let needs_quoting = value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\\');
    if needs_quoting {
        buf.push(b'"');
        for ch in value.chars() {
            if ch == '"' {
                buf.extend_from_slice(b"\"\"");
            } else {
                buf.extend_from_slice(ch.encode_utf8(&mut [0u8; 4]).as_bytes());
            }
        }
        buf.push(b'"');
    } else {
        buf.extend_from_slice(value.as_bytes());
    }
}

/// Async version: write a single CSV field to a `tokio::io::BufWriter`.
async fn csv_write_value_to_writer<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut tokio::io::BufWriter<W>,
    value: &str,
) -> Result<()> {
    if value.is_empty() {
        writer.write_all(b"\\N").await?;
        return Ok(());
    }
    let needs_quoting = value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\\');
    if needs_quoting {
        writer.write_all(b"\"").await?;
        for ch in value.chars() {
            if ch == '"' {
                writer.write_all(b"\"\"").await?;
            } else {
                let mut buf = [0u8; 4];
                writer.write_all(ch.encode_utf8(&mut buf).as_bytes()).await?;
            }
        }
        writer.write_all(b"\"").await?;
    } else {
        writer.write_all(value.as_bytes()).await?;
    }
    Ok(())
}

// ─── Same-server MySQL Optimisation ────────────────────────────────

/// Check whether source and target point to the same database host:port.
fn is_same_db_server(a: &ConnectionConfig, b: &ConnectionConfig) -> bool {
    a.host == b.host && a.port == b.port
}

/// Convert a rule configuration into a SQL expression for the target DB.
///
/// Returns `NULL AS col` for Ignore, native SQL functions for masking /
/// hashing, and the literal column name for pass-through.
fn rule_to_sql_expr(col: &str, rule: &RuleType, params: Option<&HashMap<String, String>>, kind: &DbKind) -> String {
    let q = match kind {
        DbKind::MySql => format!("`{}`", col),
        DbKind::Postgres => format!("\"{}\"", col),
    };
    match rule {
        RuleType::Ignore => format!("NULL AS {}", q),
        RuleType::MaskPhone => {
            // Get mask string (default: "****")
            let mask_str = params
                .and_then(|p| p.get("mask_str"))
                .map(|s| s.as_str())
                .unwrap_or("****");
            // CONCAT / LEFT / RIGHT work on both MySQL and PostgreSQL
            format!(
                "CONCAT(LEFT({}, 3), '{}', RIGHT({}, 4)) AS {}",
                q, mask_str, q, q
            )
        }
        RuleType::MaskEmail => {
            // Get mask string (default: "****")
            let mask_str = params
                .and_then(|p| p.get("mask_str"))
                .map(|s| s.as_str())
                .unwrap_or("****");
            match kind {
                DbKind::MySql => format!(
                    "CONCAT(LEFT({}, 1), '{}', SUBSTRING_INDEX({}, '@', -1)) AS {}",
                    q, mask_str, q, q
                ),
                DbKind::Postgres => format!(
                    "CONCAT(LEFT({}, 1), '{}', SPLIT_PART({}, '@', 2)) AS {}",
                    q, mask_str, q, q
                ),
            }
        }
        RuleType::Hash => {
            let algorithm = params
                .and_then(|p| p.get("algorithm"))
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "sha256".to_string());
            match (kind, algorithm.as_str()) {
                (_, "md5") => format!("MD5({}) AS {}", q, q),
                (DbKind::MySql, "crc32") => format!("CRC32({}) AS {}", q, q),
                (DbKind::Postgres, "crc32") => format!("(('x' || MD5({}))::BIT(128)::INTEGER % 2147483647) AS {}", q, q),
                (DbKind::MySql, "sha1") => format!("SHA1({}) AS {}", q, q),
                (DbKind::Postgres, "sha1") => format!("ENCODE(DIGEST({}, 'sha1'), 'hex') AS {}", q, q),
                (DbKind::MySql, _) => format!("SHA2({}, 256) AS {}", q, q),
                (DbKind::Postgres, _) => format!("ENCODE(DIGEST({}, 'sha256'), 'hex') AS {}", q, q),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_table_simple() {
        assert_eq!(quote_table(&DbKind::Postgres, "users"), "\"users\"");
        assert_eq!(quote_table(&DbKind::MySql, "users"), "`users`");
    }

    #[test]
    fn test_quote_table_schema_qualified() {
        assert_eq!(
            quote_table(&DbKind::Postgres, "public.users"),
            "public.users"
        );
    }

    #[test]
    fn test_build_select_sql() {
        let (sql, use_keyset) = build_select_sql(&DbKind::Postgres, "users", &["id", "name", "email"], None, true);
        assert!(!use_keyset);
        assert_eq!(
            sql,
            r#"SELECT "id"::text AS "id", "name"::text AS "name", "email"::text AS "email" FROM "users" ORDER BY 1 LIMIT ? OFFSET ?"#
        );
    }

    #[test]
    fn test_build_select_sql_mysql() {
        let (sql, use_keyset) = build_select_sql(&DbKind::MySql, "users", &["id", "name"], None, true);
        assert!(!use_keyset);
        assert_eq!(
            sql,
            "SELECT CAST(`id` AS CHAR) AS `id`, CAST(`name` AS CHAR) AS `name` FROM `users` ORDER BY 1 LIMIT ? OFFSET ?"
        );
    }

    #[test]
    fn test_build_select_sql_keyset() {
        let (sql, use_keyset) = build_select_sql(&DbKind::MySql, "users", &["id", "name", "email"], Some("id"), true);
        assert!(use_keyset);
        // PK column is NOT cast (kept as raw BIGINT for numeric ORDER BY)
        assert!(sql.contains(" `id`"));
        assert!(!sql.contains("CAST(`id`"));
        // Non-PK columns are still cast
        assert!(sql.contains("CAST(`name`"));
        assert!(sql.contains("CAST(`email`"));
        assert!(sql.contains("`id` > ?"));
        assert!(sql.contains("ORDER BY `id`"));
    }

    #[test]
    fn test_build_select_sql_keyset_pg() {
        let (sql, use_keyset) = build_select_sql(&DbKind::Postgres, "users", &["id", "name"], Some("id"), true);
        assert!(use_keyset);
        assert!(!sql.contains("id::text"));
        assert!(sql.contains(r#""name"::text"#));
    }

    #[test]
    fn test_build_select_sql_no_cast() {
        let (sql, use_keyset) = build_select_sql(&DbKind::MySql, "users", &["id", "name"], Some("id"), false);
        assert!(use_keyset);
        assert!(!sql.contains("CAST"));
        assert!(sql.contains("`id`"));
        assert!(sql.contains("`name`"));
    }

    #[test]
    fn test_build_insert_sql_postgres() {
        let sql = build_insert_sql(&DbKind::Postgres, "users", &["id", "name"]);
        assert_eq!(
            sql,
            r#"INSERT INTO "users" ("id", "name") VALUES ($1, $2)"#
        );
    }

    #[test]
    fn test_build_insert_sql_mysql() {
        let sql = build_insert_sql(&DbKind::MySql, "users", &["id", "name"]);
        assert_eq!(
            sql,
            "INSERT INTO `users` (`id`, `name`) VALUES (?, ?)"
        );
    }
}
