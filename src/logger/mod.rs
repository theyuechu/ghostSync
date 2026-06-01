/// Telemetry and logging configuration.
///
/// Sets up tracing with structured JSON output, optional file rotation,
/// and environment-based filter levels.

use anyhow::Result;
use tracing_subscriber::EnvFilter;

/// Initialize the global tracing subscriber.
pub fn init_logging() -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive("ghostsync=info".parse().unwrap())
        .with_env_var("GHOSTSYNC_LOG")
        .from_env_lossy();

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_file(true)
        .with_line_number(true)
        .compact()
        .init();

    Ok(())
}
