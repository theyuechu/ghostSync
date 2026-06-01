use anyhow::{Context, Result};
use sqlx::AnyPool;

/// Column metadata
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
}

/// Table schema: primary key + columns
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
    pub primary_keys: Vec<String>,
}

/// Introspect the schema of a given table.
pub async fn introspect_table(pool: &AnyPool, table: &str) -> Result<TableSchema> {
    let columns = list_columns(pool, table).await?;
    let primary_keys = list_primary_keys(pool, table).await?;

    Ok(TableSchema {
        name: table.to_string(),
        columns,
        primary_keys,
    })
}

/// List columns for a table.
pub async fn list_columns(pool: &AnyPool, table: &str) -> Result<Vec<ColumnInfo>> {
    let table_name = table.split('.').last().unwrap_or(table);

    let query = format!(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1 ORDER BY ordinal_position"
    );

    let rows = sqlx::query_as::<_, (String, String)>(&query)
        .bind(table_name)
        .fetch_all(pool)
        .await
        .context("Failed to list columns")?;

    let columns = rows
        .into_iter()
        .map(|(name, data_type)| ColumnInfo { name, data_type })
        .collect();

    Ok(columns)
}

/// List primary key columns for a table.
pub async fn list_primary_keys(pool: &AnyPool, table: &str) -> Result<Vec<String>> {
    let table_name = table.split('.').last().unwrap_or(table);

    // Try information_schema approach (works for PostgreSQL)
    let query = format!(
        r#"
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND kcu.table_name = $1
        ORDER BY kcu.ordinal_position
        "#
    );

    match sqlx::query_as::<_, (String,)>(&query)
        .bind(table_name)
        .fetch_all(pool)
        .await
    {
        Ok(rows) => {
            if rows.is_empty() {
                anyhow::bail!(
                    "Table '{}' has no primary key detected. \
                     GhostSync requires a primary key for chunked reads. \
                     Add a primary key or use a different table.",
                    table
                );
            }
            Ok(rows.into_iter().map(|(pk,)| pk).collect())
        }
        Err(_) => {
            // Fallback: try MySQL-style SHOW KEYS
            let fallback = format!("SHOW KEYS FROM `{}` WHERE Key_name = 'PRIMARY'", table_name);

            let rows = sqlx::query_as::<_, (String,)>(&fallback)
                .fetch_all(pool)
                .await
                .with_context(|| {
                    format!("Failed to detect primary key for table '{}'", table)
                })?;

            let pks: Vec<String> = rows.into_iter().map(|(pk,)| pk).collect();
            if pks.is_empty() {
                anyhow::bail!(
                    "Table '{}' has no primary key detected. \
                     GhostSync requires a primary key for chunked reads.",
                    table
                );
            }
            Ok(pks)
        }
    }
}
