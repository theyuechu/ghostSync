/// Data sync engine: orchestrates chunked reads → rule processing → batch writes.
///
/// # Architecture
///
/// For each table in a task:
///   1. Introspect schema (columns + primary keys)
///   2. If `truncate_target` is set, TRUNCATE the target table
///   3. Chunked read loop using OFFSET/LIMIT pagination:
///      - Read `chunk_size` rows from source
///      - Convert rows to `HashMap<String, Option<String>>` for rule engine
///      - Process rows through `RuleEngine` (per-field de-identification)
///      - Batch insert into target with original column ordering
///   4. Collect per-table + overall statistics

use anyhow::{Context, Result};
use sqlx::any::AnyRow;
use sqlx::{AnyPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing;

use crate::config::types::{ConnectionConfig, DbKind, TaskConfig};
use crate::db::connector;
use crate::db::schema::introspect_table;
use crate::rule::RuleEngine;

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
pub async fn run_task(
    task: Arc<TaskConfig>,
    source_kind: &DbKind,
    source_conn: &ConnectionConfig,
    target_kind: &DbKind,
    target_conn: &ConnectionConfig,
) -> Result<SyncResult> {
    let task_name = task.name.clone();
    let start = Instant::now();

    tracing::info!("Engine: connecting to source ({}: {})", source_kind, task.source);
    let source_pool = connector::create_pool(source_kind, source_conn).await?;

    tracing::info!("Engine: connecting to target ({}: {})", target_kind, task.target);
    let target_pool = connector::create_pool(target_kind, target_conn).await?;

    // Build the rule engine once — it's immutable and thread-safe
    let rule_engine = RuleEngine::from_task(&task);

    let mut stats = SyncStats::default();

    for table_cfg in &task.tables {
        if table_cfg.mode == crate::config::types::TableMode::Ignore {
            tracing::info!("  Table '{}' — IGNORED (mode=ignore)", table_cfg.name);
            stats.skipped_rows += 1;
            continue;
        }

        tracing::info!(
            "Engine: syncing table '{}' (chunk={}, batch={})",
            table_cfg.name,
            task.chunk_size,
            task.batch_size,
        );

        // ── Introspect source table schema ────────────────────────
        let schema = introspect_table(&source_pool, &table_cfg.name)
            .await
            .with_context(|| format!("Failed to introspect table '{}'", table_cfg.name))?;

        if schema.columns.is_empty() {
            tracing::warn!("  Table '{}' has no columns — skipping", table_cfg.name);
            continue;
        }

        // Keep a deterministic column order for both read and write
        let col_names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();

        // ── Build queries ─────────────────────────────────────────
        let select_sql = build_select_sql(source_kind, &table_cfg.name, &col_names);
        let insert_sql = build_insert_sql(target_kind, &table_cfg.name, &col_names);

        // Optionally truncate target before syncing
        if task.truncate_target {
            let truncate_sql = format!("TRUNCATE TABLE {}", quote_table(target_kind, &table_cfg.name));
            tracing::info!("  Truncating target table: {}", truncate_sql);
            sqlx::query(&truncate_sql)
                .execute(&target_pool)
                .await
                .with_context(|| format!("Failed to truncate target table '{}'", table_cfg.name))?;
        }

        // ── Chunked read loop ─────────────────────────────────────
        let chunk_size: i64 = task.chunk_size.max(100).min(100_000) as i64;
        let batch_size: usize = task.batch_size.max(1).min(10_000) as usize;
        let mut offset: i64 = 0;
        let mut table_total: u64 = 0;

        loop {
            let raw_rows: Vec<AnyRow> = sqlx::query(&select_sql)
                .bind(chunk_size)
                .bind(offset)
                .fetch_all(&source_pool)
                .await
                .with_context(|| {
                    format!(
                        "Failed to read chunk at offset {} from '{}'",
                        offset, table_cfg.name
                    )
                })?;

            let chunk_len = raw_rows.len();
            if chunk_len == 0 {
                break;
            }

            // Convert raw rows → HashMap for the rule engine
            let rows: Vec<HashMap<String, Option<String>>> =
                raw_rows.iter().map(row_to_map).collect();

            // Apply de-identification rules
            let processed = rule_engine.process_rows(&table_cfg.name, &rows);

            // Write to target in sub-batches
            for batch in processed.chunks(batch_size) {
                write_batch(&target_pool, &insert_sql, target_kind, &col_names, batch)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to write batch to '{}' (offset={}, count={})",
                            table_cfg.name,
                            offset,
                            batch.len(),
                        )
                    })?;
            }

            table_total += chunk_len as u64;
            offset += chunk_size;

            tracing::debug!(
                "  Table '{}': chunk of {} rows written (total: {}, offset: {})",
                table_cfg.name,
                chunk_len,
                table_total,
                offset,
            );
        }

        stats.total_rows += table_total;
        stats.processed_rows += table_total;

        tracing::info!("  Table '{}': {} rows synced", table_cfg.name, table_total);
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

    tracing::info!(
        "Engine: task '{}' finished — {} rows in {:.2}s ({:.0} rows/s)",
        task_name,
        stats.processed_rows,
        elapsed_secs,
        rps,
    );

    Ok(SyncResult {
        task_name,
        success: true,
        stats,
        error: None,
    })
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
/// Example: `SELECT "col1", "col2" FROM "mytable" ORDER BY 1 LIMIT ? OFFSET ?`
fn build_select_sql(kind: &DbKind, table: &str, columns: &[&str]) -> String {
    let quoted_cols: Vec<String> = columns
        .iter()
        .map(|c| quote_ident(kind, c))
        .collect();
    format!(
        "SELECT {} FROM {} ORDER BY 1 LIMIT ? OFFSET ?",
        quoted_cols.join(", "),
        quote_table(kind, table),
    )
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
async fn write_batch(
    pool: &AnyPool,
    insert_sql: &str,
    kind: &DbKind,
    col_names: &[&str],
    rows: &[HashMap<String, Option<String>>],
) -> Result<()> {
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
        q.execute(pool).await?;
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

    q.execute(pool).await?;
    Ok(())
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
        let sql = build_select_sql(&DbKind::Postgres, "users", &["id", "name", "email"]);
        assert_eq!(
            sql,
            r#"SELECT "id", "name", "email" FROM "users" ORDER BY 1 LIMIT ? OFFSET ?"#
        );
    }

    #[test]
    fn test_build_select_sql_mysql() {
        let sql = build_select_sql(&DbKind::MySql, "users", &["id", "name"]);
        assert_eq!(
            sql,
            "SELECT `id`, `name` FROM `users` ORDER BY 1 LIMIT ? OFFSET ?"
        );
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
