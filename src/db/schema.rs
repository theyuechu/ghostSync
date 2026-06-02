use anyhow::{Context, Result};
use sqlx::AnyPool;
use sqlx::Row;

use crate::config::types::DbKind;    

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
}

/// Table schema: primary key + columns
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: Vec<ColumnInfo>,
    pub primary_keys: Vec<String>,
}

/// Introspect the schema of a given table.
pub async fn introspect_table(pool: &AnyPool, table: &str, kind: &DbKind) -> Result<TableSchema> {
    let columns = list_columns(pool, table, kind).await?;
    let primary_keys = list_primary_keys(pool, table).await?;

    Ok(TableSchema {
        columns,
        primary_keys,
    })
}

/// List columns for a table.
pub async fn list_columns(pool: &AnyPool, table: &str, kind: &DbKind) -> Result<Vec<ColumnInfo>> {
    let table_name = table.split('.').last().unwrap_or(table);

    // Filter by database/schema to avoid duplicates from other databases
    let schema_filter = match kind {
        DbKind::MySql => " AND table_schema = DATABASE()",
        DbKind::Postgres => " AND table_schema = 'public'",
    };

    let query = format!(
        "SELECT column_name FROM information_schema.columns WHERE table_name = ?{schema_filter} ORDER BY ordinal_position"
    );

    let rows = sqlx::query_as::<_, (String,)>(&query)
        .bind(table_name)
        .fetch_all(pool)
        .await
        .context("Failed to list columns")?;

    let columns = rows
        .into_iter()
        .map(|(name,)| ColumnInfo { name })
        .collect();

    Ok(columns)
}

/// List primary key columns for a table.
pub async fn list_primary_keys(pool: &AnyPool, table: &str) -> Result<Vec<String>> {
    let table_name = table.split('.').last().unwrap_or(table);

    // Approach 1: information_schema JOIN (PostgreSQL, MySQL)
    let query = format!(
        r#"
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND kcu.table_name = ?
        ORDER BY kcu.ordinal_position
        "#
    );

    match sqlx::query_as::<_, (String,)>(&query)
        .bind(table_name)
        .fetch_all(pool)
        .await
    {
        Ok(rows) if !rows.is_empty() => {
            return Ok(rows.into_iter().map(|(pk,)| pk).collect());
        }
        Ok(_) => {} // empty — try fallback
        Err(_) => {} // error — try fallback
    }

    // Approach 2: MySQL/MariaDB SHOW KEYS
    let fallback = format!("SHOW KEYS FROM `{}` WHERE Key_name = 'PRIMARY'", table_name);
    let rows = sqlx::query(&fallback)
        .fetch_all(pool)
        .await
        .with_context(|| format!("Failed to detect primary key for table '{}'", table))?;

    let pks: Vec<String> = rows
        .iter()
        .filter_map(|row| row.try_get::<String, &str>("Column_name").ok())
        .collect();

    if pks.is_empty() {
        anyhow::bail!(
            "Table '{}' has no primary key detected. \
             GhostSync requires a primary key for chunked reads.",
            table
        );
    }
    Ok(pks)
}
