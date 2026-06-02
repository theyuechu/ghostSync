/// Data sync engine: orchestrates chunked reads → rule processing → bulk writes.
///
/// For same-host MySQL, uses SELECT INTO OUTFILE + LOAD DATA for maximum
/// throughput (avoids TCP transfer and sqlx AnyRow overhead).
/// For cross-server connections, uses chunked SELECT + multi-row INSERT
/// (works for both MySQL and PostgreSQL).
/// No named pipes or FIFOs — every path is stable on macOS and Linux.

use anyhow::{Context, Result};
use sqlx::any::AnyRow;
use sqlx::{Executor, Row};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tracing;

use crate::config::types::{ConnectionConfig, DbKind, TableConfig, TableMode, TaskConfig};
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
        let table_cfg = table_cfg.clone();
        let rule_engine = rule_engine.clone();
        let source_pool = source_pool.clone();
        let target_pool = target_pool.clone();
        let store = store.clone();
        let tn = task_name.clone();
        let source_conn = source_conn.clone();
        let target_conn = target_conn.clone();

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

    let _owned_col_names = col_names.clone();
    let _owned_table_name = table_name.clone();
    let _pk_idx: Option<usize> = pk_column.and_then(|pk| col_names.iter().position(|c| c == pk));

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

    // ── Same-server MySQL: SELECT INTO OUTFILE → Rust → FIFO → LOAD DATA ──
    let is_same_server = *source_kind == DbKind::MySql && *target_kind == DbKind::MySql
        && source_conn.host == target_conn.host && source_conn.port == target_conn.port;

    if is_same_server {
        // This path avoids sqlx AnyRow overhead and TCP data transfer by having
        // MySQL write a temp CSV file on the server, then Rust reads it directly
        // for rule processing, and finally LOAD DATA INFILE writes to the target.
        let pid = std::process::id();
        let src_db = source_conn.database.as_deref().unwrap_or("public");

        // ──────────────────────────────────────────────
        // Strategy for same-server MySQL
        // 1. SELECT INTO OUTFILE  →  temp CSV (MySQL writing, no TCP)
        // 2. Rust reads the file, applies rules, writes /tmp/gsync_{pid}_out.csv
        // 3. LOAD DATA INFILE from the output CSV
        // ──────────────────────────────────────────────

        // Step 1 — SELECT INTO OUTFILE
        let cols_list = col_names.iter()
            .map(|c| quote_ident(target_kind, c))
            .collect::<Vec<_>>()
            .join(", ");
        let quoted_table = format!("{}.{}", quote_ident(target_kind, src_db), quote_ident(target_kind, &table_cfg.name));
        let raw_csv = format!("/tmp/gsync_{}_raw.csv", pid);
        let out_csv = format!("/tmp/gsync_{}_out.csv", pid);

        let dump_sql = format!(
            "SELECT {} FROM {} INTO OUTFILE '{}' \
             FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' \
             LINES TERMINATED BY '\\n'",
            cols_list, quoted_table, raw_csv
        );
        sqlx::query(&dump_sql).execute(&source_pool).await
            .with_context(|| "SELECT INTO OUTFILE failed")?;

        // Step 2 — Read raw CSV, apply rules, write processed CSV (on spawn_blocking)
        tracing::info!("  Same-server MySQL — SELECT INTO OUTFILE → Rust process → LOAD DATA");
        let csv_handle: tokio::task::JoinHandle<Result<u64>> = {
            let owned_col_names = col_names.clone();
            let owned_table_name = table_name.clone();
            let r = rule_engine.clone();
            let raw = raw_csv.clone();
            let out = out_csv.clone();
            tokio::task::spawn_blocking(move || -> Result<u64> {
                let data = std::fs::read_to_string(&raw)
                    .with_context(|| format!("Failed to read {}", &raw))?;
                let mut out_buf = Vec::with_capacity(data.len());
                let field_rules = r.get_field_rules(&owned_table_name);
                let mut total: u64 = 0;

                for line in data.lines() {
                    let fields = parse_csv_line(line);
                    for (i, col) in owned_col_names.iter().enumerate() {
                        if i > 0 { out_buf.push(b','); }
                        let original = fields.get(i).map(|s| s.as_str());
                        let value = if let Some(rules) = &field_rules {
                            if let Some((_, rule)) = rules.iter().find(|(f, _)| f.as_str() == col.as_str()) {
                                match rule.apply(original) {
                                    crate::rule::RuleResult::Skip => { out_buf.extend_from_slice(b"\\N"); continue; }
                                    crate::rule::RuleResult::Replace(v) => v,
                                    crate::rule::RuleResult::PassThrough => original.unwrap_or("").to_string(),
                                }
                            } else { original.unwrap_or("").to_string() }
                        } else { original.unwrap_or("").to_string() };
                        crate::engine::csv_write_value(&mut out_buf, &value);
                    }
                    out_buf.push(b'\n');
                    total += 1;
                }

                std::fs::write(&out, &out_buf)
                    .with_context(|| format!("Failed to write {}", &out))?;
                Ok(total)
            })
        };

        let rows_synced = csv_handle.await
            .map_err(|e| anyhow::anyhow!("CSV processing panicked: {:?}", e))??;

        // Step 3 — LOAD DATA from the processed CSV
        let load_sql = format!(
            "LOAD DATA INFILE '{}' INTO TABLE {} CHARACTER SET utf8mb4 \
             FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' \
             LINES TERMINATED BY '\\n' ({})",
            out_csv,
            quote_table(target_kind, &table_cfg.name),
            cols_list,
        );
        sqlx::query(&load_sql).execute(&target_pool).await
            .with_context(|| "LOAD DATA INFILE failed")?;

        table_total = rows_synced;

        // Cleanup
        let _ = std::fs::remove_file(&raw_csv);
        let _ = std::fs::remove_file(&out_csv);
    } else {
        // ── Generic INSERT path (cross-server MySQL + PostgreSQL) ──
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

/// Parse a single CSV line into fields, handling standard CSV quoting.
///
/// Supports: quoted fields with `""` escapes, unquoted fields, and MySQL's
/// `\N` NULL representation (which is an unquoted `\N`).
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '"' if !in_quotes => {
                in_quotes = true;
            }
            '"' if in_quotes => {
                // Check for escaped quote ("")
                if chars.peek() == Some(&'"') {
                    chars.next(); // consume second quote
                    current.push('"');
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.clone());
                current.clear();
            }
            '\n' | '\r' if !in_quotes => {
                // Skip newlines outside quotes, they're leftover from \r\n
            }
            _ => {
                current.push(ch);
            }
        }
    }
    fields.push(current);
    fields
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
