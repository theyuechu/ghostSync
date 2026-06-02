/// Backup module — local CSV file + optional S3 / S3-compatible object storage upload.
///
/// After the sync engine writes processed CSV files, this module handles:
/// 1. Local file management (already done inline in engine)
/// 2. S3 upload via `aws-sdk-s3` (or any S3-compatible store: Cloudflare R2, MinIO, etc.)
/// 3. Cleanup of temporary files

use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::config::types::{BackupMode, S3Config, TaskConfig};

/// Handle post-sync backup for a single table.
///
/// Called after the CSV file has been written (and optionally gzipped).
///
/// - `local_path`: full path to the local CSV file on disk (may already be .gz)
/// - `rows_count`: number of rows in the file (for logging)
pub async fn handle_backup(
    local_path: &str,
    rows_count: u64,
    task: &TaskConfig,
    table_name: &str,
) -> Result<()> {
    let backup = match &task.backup {
        Some(b) => b,
        None => return Ok(()),
    };

    match &backup.mode {
        BackupMode::None | BackupMode::File => {
            // Local file backup is handled inline by the engine.
            Ok(())
        }
        BackupMode::S3 => {
            if let Some(s3_cfg) = &backup.s3 {
                upload_to_s3(s3_cfg, local_path, table_name).await?;
                tracing::info!(
                    "  S3 backup complete: {} rows from '{}'",
                    rows_count,
                    table_name
                );
            } else {
                tracing::warn!(
                    "  Backup mode is S3 but no S3 configuration provided for task '{}'",
                    task.name
                );
            }
            Ok(())
        }
    }
}

/// Upload a local file to S3 (or S3-compatible store).
async fn upload_to_s3(s3_cfg: &S3Config, local_path: &str, _table_name: &str) -> Result<()> {
    let local = std::path::Path::new(local_path);
    let file_name = local
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown.csv");

    // Build S3 object key
    let key = match &s3_cfg.key_prefix {
        Some(prefix) => {
            let p = prefix.trim_end_matches('/');
            if p.is_empty() {
                file_name.to_string()
            } else {
                format!("{}/{}", p, file_name)
            }
        }
        None => file_name.to_string(),
    };

    // Load AWS config from environment
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    // Build S3 client with optional customizations
    let mut client_builder = aws_sdk_s3::config::Builder::from(&config);

    if let Some(ep) = &s3_cfg.endpoint {
        client_builder = client_builder.endpoint_url(ep);
    }
    client_builder = client_builder.force_path_style(true);

    let client = Client::from_conf(client_builder.build());

    // Read file into memory and upload
    let data = tokio::fs::read(local_path)
        .await
        .with_context(|| format!("Failed to read local file for S3 upload: {}", local_path))?;
    let body = ByteStream::new(bytes::Bytes::from(data).into());

    client
        .put_object()
        .bucket(&s3_cfg.bucket)
        .key(&key)
        .body(body)
        .send()
        .await
        .with_context(|| format!("Failed to upload {} to s3://{}/{}", local_path, s3_cfg.bucket, key))?;

    tracing::info!("  S3 upload: s3://{}/{} ({})", s3_cfg.bucket, key, local_path);
    Ok(())
}
