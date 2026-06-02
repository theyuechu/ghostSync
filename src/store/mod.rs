/// Local storage layer (SQLite) for task run logs and history.
///
/// Persists execution results locally so users can inspect past runs
/// via the HTTP API without needing an external database.

use anyhow::{Context, Result};
use chrono::Utc;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::SqlitePool;

/// A single task execution record.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RunRecord {
    pub id: i64,
    pub task_name: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub processed_rows: i64,
    pub total_rows: i64,
    pub error_message: Option<String>,
    pub rps: f64,
}

/// Persistent store backed by a single SQLite file.
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    /// Open or create the SQLite database and run migrations.
    pub async fn open(path: &str) -> Result<Self> {
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(path)
            .await
            .with_context(|| format!("Failed to open SQLite store at: {}", path))?;

        Self::migrate(&pool).await?;

        Ok(Self { pool })
    }

    async fn migrate(pool: &SqlitePool) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_runs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                task_name       TEXT    NOT NULL,
                status          TEXT    NOT NULL DEFAULT 'running',
                started_at      TEXT    NOT NULL,
                finished_at     TEXT,
                processed_rows  INTEGER NOT NULL DEFAULT 0,
                total_rows      INTEGER NOT NULL DEFAULT 0,
                error_message   TEXT,
                rps             REAL    NOT NULL DEFAULT 0.0
            )
            "#,
        )
        .execute(pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS task_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id      INTEGER NOT NULL,
                level       TEXT    NOT NULL DEFAULT 'info',
                message     TEXT    NOT NULL,
                timestamp   TEXT    NOT NULL,
                FOREIGN KEY (run_id) REFERENCES task_runs(id)
            )
            "#,
        )
        .execute(pool)
        .await?;

        // Index for fast lookups by task name
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_task_runs_name
                ON task_runs (task_name, started_at DESC)
            "#,
        )
        .execute(pool)
        .await?;

        // Sync cursors table for incremental sync
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS sync_cursors (
                task_name   TEXT NOT NULL,
                table_name  TEXT NOT NULL,
                cursor_val  INTEGER NOT NULL DEFAULT 0,
                updated_at  TEXT NOT NULL,
                PRIMARY KEY (task_name, table_name)
            )
            "#,
        )
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Save the cursor position for a (task, table) pair.
    /// Used by incremental sync to resume from where it left off.
    pub async fn save_cursor(&self, task_name: &str, table_name: &str, cursor_val: i64) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO sync_cursors (task_name, table_name, cursor_val, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(task_name, table_name) DO UPDATE SET
                cursor_val = excluded.cursor_val,
                updated_at = excluded.updated_at
            "#,
        )
        .bind(task_name)
        .bind(table_name)
        .bind(cursor_val)
        .bind(&now)
        .execute(&self.pool)
        .await
        .context("Failed to save sync cursor")?;
        Ok(())
    }

    /// Load the last saved cursor for a (task, table) pair.
    /// Returns `None` if no prior sync has been recorded (first full sync).
    pub async fn load_cursor(&self, task_name: &str, table_name: &str) -> Result<Option<i64>> {
        let row: Option<i64> = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT cursor_val FROM sync_cursors
            WHERE task_name = ? AND table_name = ?
            "#,
        )
        .bind(task_name)
        .bind(table_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    /// Record the start of a task run. Returns the new run_id.
    pub async fn log_run_start(&self, task_name: &str) -> Result<i64> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query_scalar::<_, i64>(
            r#"
            INSERT INTO task_runs (task_name, started_at)
            VALUES (?, ?)
            RETURNING id
            "#,
        )
        .bind(task_name)
        .bind(&now)
        .fetch_one(&self.pool)
        .await
        .context("Failed to insert task run record")?;

        Ok(result)
    }

    /// Record the completion of a task run.
    pub async fn log_run_complete(
        &self,
        run_id: i64,
        status: &str,
        processed_rows: u64,
        total_rows: u64,
        rps: f64,
        error: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            UPDATE task_runs
            SET status = ?, finished_at = ?, processed_rows = ?,
                total_rows = ?, error_message = ?, rps = ?
            WHERE id = ?
            "#,
        )
        .bind(status)
        .bind(&now)
        .bind(processed_rows as i64)
        .bind(total_rows as i64)
        .bind(error)
        .bind(rps)
        .bind(run_id)
        .execute(&self.pool)
        .await
        .context("Failed to update task run record")?;

        Ok(())
    }

    /// Add a log line to a specific run.
    #[allow(dead_code)]
    pub async fn log_message(&self, run_id: i64, level: &str, message: &str) -> Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO task_logs (run_id, level, message, timestamp)
            VALUES (?, ?, ?, ?)
            "#,
        )
        .bind(run_id)
        .bind(level)
        .bind(message)
        .bind(&now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get run history for a specific task, most recent first.
    pub async fn get_run_history(&self, task_name: &str, limit: u64) -> Result<Vec<RunRecord>> {
        let records = sqlx::query_as::<_, (i64, String, String, String, Option<String>, i64, i64, Option<String>, f64)>(
            r#"
            SELECT id, task_name, status, started_at, finished_at,
                   processed_rows, total_rows, error_message, rps
            FROM task_runs
            WHERE task_name = ?
            ORDER BY started_at DESC
            LIMIT ?
            "#,
        )
        .bind(task_name)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .context("Failed to query task run history")?;

        Ok(records
            .into_iter()
            .map(|(id, task_name, status, started_at, finished_at, processed_rows, total_rows, error_message, rps)| {
                RunRecord {
                    id,
                    task_name,
                    status,
                    started_at,
                    finished_at,
                    processed_rows,
                    total_rows,
                    error_message,
                    rps,
                }
            })
            .collect())
    }

}
