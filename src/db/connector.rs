use anyhow::{Context, Result};
use sqlx::any::{AnyConnectOptions, AnyPoolOptions};
use sqlx::ConnectOptions;
use sqlx::AnyPool;
use std::str::FromStr;

use crate::config::types::{ConnectionConfig, DbKind};

/// Create a connection pool from the given config.
pub async fn create_pool(kind: &DbKind, conn: &ConnectionConfig) -> Result<AnyPool> {
    let mut url = conn
        .url
        .as_ref()
        .context("Connection URL is required (resolve() must be called first)")?
        .clone();

    // Enable LOCAL INFILE for MySQL (required by LOAD DATA LOCAL INFILE)
    if *kind == DbKind::MySql && !url.contains("local-infile") {
        let separator = if url.contains('?') { "&" } else { "?" };
        url.push_str(&format!("{}local-infile=true", separator));
    }

    let opts = AnyConnectOptions::from_str(&url)
        .with_context(|| format!("Failed to parse connection URL: {}", url))?;

    let pool_opts = AnyPoolOptions::new()
        .max_connections(conn.pool_size)
        .min_connections(1);

    // Disable verbose SQL logging
    let pool = pool_opts
        .connect_with(opts.disable_statement_logging())
        .await
        .with_context(|| format!("Failed to connect to {} at {}", kind, url))?;

    Ok(pool)
}

/// Test a single connection: create pool, run health check, close.
pub async fn test_single_connection(kind: &DbKind, conn: &ConnectionConfig) -> Result<String> {
    let url = conn
        .url
        .as_ref()
        .context("Connection URL is required")?;

    let opts = AnyConnectOptions::from_str(url)
        .with_context(|| format!("Failed to parse connection URL: {}", url))?;

    let pool = AnyPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.disable_statement_logging())
        .await
        .with_context(|| format!("Failed to connect to {} at {}", kind, url))?;

    let row: (String,) = sqlx::query_as("SELECT version()")
        .fetch_one(&pool)
        .await
        .context("Failed to execute health check query")?;

    pool.close().await;
    Ok(row.0)
}

/// Test connectivity: open a connection and run a simple health query.
pub async fn test_connection(pool: &AnyPool) -> Result<String> {
    let row: (String,) = sqlx::query_as::<_, (String,)>("SELECT version()")
        .fetch_one(pool)
        .await
        .context("Failed to execute health check query")?;
    Ok(row.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_url_resolution() {
        let mut conn = ConnectionConfig {
            url: Some("postgres://user:pass@localhost:5432/mydb".into()),
            ..Default::default()
        };
        assert!(conn.resolve().is_ok());
        assert_eq!(
            conn.url.as_deref().unwrap(),
            "postgres://user:pass@localhost:5432/mydb"
        );
    }
}
