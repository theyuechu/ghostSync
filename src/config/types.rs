use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Top-level GhostSync configuration
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Config {
    pub sources: Vec<SourceConfig>,
    pub targets: Vec<TargetConfig>,
    pub tasks: Vec<TaskConfig>,
}

// ─── Data Source ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceConfig {
    pub name: String,
    #[serde(rename = "kind")]
    pub db_kind: DbKind,
    #[serde(flatten)]
    pub connection: ConnectionConfig,
}

// ─── Data Target ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TargetConfig {
    pub name: String,
    #[serde(rename = "kind")]
    pub db_kind: DbKind,
    #[serde(flatten)]
    pub connection: ConnectionConfig,
}

// ─── Database Kind ─────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum DbKind {
    #[serde(rename = "postgres")]
    Postgres,
    #[serde(rename = "mysql")]
    MySql,
}

impl std::fmt::Display for DbKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DbKind::Postgres => write!(f, "postgres"),
            DbKind::MySql => write!(f, "mysql"),
        }
    }
}

// ─── Connection Config ────────────────────────────────────────────

/// Supports two modes:
/// 1. Direct URL: `url: "postgres://user:pass@host:5432/db"`
/// 2. Individual fields: `host`, `port`, `database`, `user`, `password`
#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ConnectionConfig {
    /// Full connection URL — takes priority over individual fields.
    pub url: Option<String>,

    // Individual connection fields (used when `url` is absent)
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,

    /// SSL mode: `disable`, `prefer`, `require` (DB-specific)
    pub ssl_mode: Option<String>,

    /// Max pool size (default: 10)
    #[serde(default = "default_pool_size")]
    pub pool_size: u32,
}

impl ConnectionConfig {
    /// Resolve the connection URL.
    ///
    /// If `url` is already set, use it as-is (env vars should already be expanded).
    /// Otherwise, construct a URL from the individual fields.
    pub fn resolve(&mut self) -> anyhow::Result<()> {
        if self.url.is_some() {
            return Ok(());
        }

        let kind = "postgres"; // will be overridden by caller with correct kind
        let host = self.host.as_deref().unwrap_or("localhost");
        let port = self.port.map(|p| p.to_string()).unwrap_or_else(|| "5432".to_string());
        let db = self.database.as_deref().unwrap_or("postgres");
        let user = self.user.as_deref().unwrap_or("postgres");
        let pass = self.password.as_deref().unwrap_or("");

        let constructed = if pass.is_empty() {
            format!("{}://{}@{}:{}/{}", kind, user, host, port, db)
        } else {
            format!("{}://{}:{}@{}:{}/{}", kind, user, pass, host, port, db)
        };

        // Append ssl_mode if set
        let constructed = match &self.ssl_mode {
            Some(mode) if !mode.is_empty() => format!("{}?sslmode={}", constructed, mode),
            _ => constructed,
        };

        self.url = Some(constructed);
        Ok(())
    }

    /// Resolve with a known DB kind (constructs the correct URL scheme).
    pub fn resolve_with_kind(&mut self, kind: &DbKind) -> anyhow::Result<()> {
        if self.url.is_some() {
            return Ok(());
        }

        let scheme = match kind {
            DbKind::Postgres => "postgres",
            DbKind::MySql => "mysql",
        };
        let default_port = match kind {
            DbKind::Postgres => "5432",
            DbKind::MySql => "3306",
        };

        let host = self.host.as_deref().unwrap_or("localhost");
        let port = self.port.map(|p| p.to_string()).unwrap_or_else(|| default_port.to_string());
        let db = self.database.as_deref().unwrap_or("postgres");
        let user = self.user.as_deref().unwrap_or("root");
        let pass = self.password.as_deref().unwrap_or("");

        let constructed = if pass.is_empty() {
            format!("{}://{}@{}:{}/{}", scheme, user, host, port, db)
        } else {
            format!("{}://{}:{}@{}:{}/{}", scheme, user, pass, host, port, db)
        };

        let constructed = match &self.ssl_mode {
            Some(mode) if !mode.is_empty() => format!("{}?sslmode={}", constructed, mode),
            _ => constructed,
        };

        self.url = Some(constructed);
        Ok(())
    }
}

const fn default_pool_size() -> u32 {
    10
}

// ─── Task Config ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TaskConfig {
    /// Unique task name
    pub name: String,

    /// Reference to a source name
    pub source: String,

    /// Reference to a target name
    pub target: String,

    /// Per-table rules
    pub tables: Vec<TableConfig>,

    /// Rows per chunk when reading from source (default: 5000)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u64,

    /// Rows per batch when writing to target (default: 1000)
    #[serde(default = "default_batch_size")]
    pub batch_size: u64,

    /// Max concurrent workers for rule processing (default: 4)
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,

    /// Rate limit in rows/second. 0 = unlimited (default: 0)
    #[serde(default)]
    pub rate_limit: u64,

    /// Cron expression for scheduled execution (e.g. "0 2 * * 5")
    pub schedule: Option<String>,

    /// If true, only print what would be done (default: false)
    #[serde(default)]
    pub dry_run: bool,

    /// If true, truncate target table before sync (default: false)
    #[serde(default)]
    pub truncate_target: bool,
}

const fn default_chunk_size() -> u64 {
    20000
}

const fn default_batch_size() -> u64 {
    5000
}

const fn default_max_workers() -> usize {
    4
}

// ─── Table Config ──────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TableConfig {
    /// Table name (schema-qualified: "public.users" or just "users")
    pub name: String,

    /// Action for this table (default: Sync)
    #[serde(default)]
    pub mode: TableMode,

    /// Per-field transformation rules
    pub rules: Option<Vec<RuleConfig>>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, PartialEq)]
pub enum TableMode {
    /// Skip this table entirely
    #[serde(rename = "ignore")]
    Ignore,

    /// Sync this table (default)
    #[serde(rename = "sync")]
    #[default]
    Sync,
}

// ─── Rule Config ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RuleConfig {
    /// Target field name
    pub field: String,

    /// Rule type
    pub rule: RuleType,

    /// Optional parameters for the rule
    pub params: Option<HashMap<String, String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum RuleType {
    /// Ignore this field during sync
    #[serde(rename = "ignore")]
    Ignore,

    /// Mask phone number: 138****0000
    #[serde(rename = "mask_phone")]
    MaskPhone,

    /// Mask email: r****@example.com
    #[serde(rename = "mask_email")]
    MaskEmail,

    /// Hash with SHA-256 or MD5
    #[serde(rename = "hash")]
    Hash,
}

impl std::fmt::Display for RuleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuleType::Ignore => write!(f, "ignore"),
            RuleType::MaskPhone => write!(f, "mask_phone"),
            RuleType::MaskEmail => write!(f, "mask_email"),
            RuleType::Hash => write!(f, "hash"),
        }
    }
}
