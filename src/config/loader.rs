use anyhow::{Context, Result};
use regex::Regex;
use std::env;
use std::path::Path;

use super::types::Config;

/// Load and parse a YAML config file, expanding `${VAR}` environment variables.
pub fn load_config(path: &Path) -> Result<Config> {
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;

    let expanded = expand_env_vars(&raw);

    let config: Config = serde_yaml::from_str(&expanded)
        .with_context(|| format!("Failed to parse YAML config from: {}", path.display()))?;

    // Resolve connections: if only individual fields are given, construct URL
    let mut config = config;
    for source in &mut config.sources {
        source
            .connection
            .resolve_with_kind(&source.db_kind)
            .context(format!(
                "Failed to resolve connection URL for source '{}'",
                source.name
            ))?;
    }
    for target in &mut config.targets {
        target
            .connection
            .resolve_with_kind(&target.db_kind)
            .context(format!(
                "Failed to resolve connection URL for target '{}'",
                target.name
            ))?;
    }

    Ok(config)
}

/// Replace `${VAR_NAME}` or `${VAR_NAME:-default}` patterns with env values.
fn expand_env_vars(input: &str) -> String {
    // Matches `${VAR_NAME}` or `${VAR_NAME:-default_value}`
    let re = Regex::new(r"\$\{([^:}]+)(?::-([^}]*))?\}").unwrap();
    re.replace_all(input, |caps: &regex::Captures| {
        let var_name = &caps[1];
        let default = caps.get(2).map(|m| m.as_str());
        match env::var(var_name) {
            Ok(val) => val,
            Err(_) => default.unwrap_or("").to_string(),
        }
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expand_env_vars() {
        temp_env::with_var("GHOST_DB_URL", Some("postgres://localhost:5432/test"), || {
            let input = "url: ${GHOST_DB_URL}";
            let result = expand_env_vars(input);
            assert_eq!(result, "url: postgres://localhost:5432/test");
        });
    }

    #[test]
    fn test_expand_env_vars_with_default() {
        let input = "password: ${UNSET_VAR:-default123}";
        let result = expand_env_vars(input);
        assert_eq!(result, "password: default123");
    }

    #[test]
    fn test_expand_env_vars_missing_no_default() {
        let input = "password: ${TOTALLY_MISSING}";
        let result = expand_env_vars(input);
        assert_eq!(result, "password: ");
    }
}
