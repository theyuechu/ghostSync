use anyhow::{bail, Result};

use super::types::{Config, RuleType, TableMode};

/// Validate the entire configuration.
/// Returns `Ok(())` if valid, or an error describing the first issue found.
pub fn validate_config(config: &Config) -> Result<()> {
    // 1. No duplicate source names
    let mut source_names = std::collections::HashSet::new();
    for source in &config.sources {
        if !source_names.insert(&source.name) {
            bail!("Duplicate source name: '{}'", source.name);
        }
    }

    // 2. No duplicate target names
    let mut target_names = std::collections::HashSet::new();
    for target in &config.targets {
        if !target_names.insert(&target.name) {
            bail!("Duplicate target name: '{}'", target.name);
        }
    }

    // 3. No duplicate task names
    let mut task_names = std::collections::HashSet::new();
    for task in &config.tasks {
        if !task_names.insert(&task.name) {
            bail!("Duplicate task name: '{}'", task.name);
        }
    }

    // 4. Each task refers to valid source and target
    for task in &config.tasks {
        if !source_names.contains(&task.source) {
            bail!(
                "Task '{}' references unknown source '{}'. Available sources: {:?}",
                task.name,
                task.source,
                source_names
            );
        }
        if !target_names.contains(&task.target) {
            bail!(
                "Task '{}' references unknown target '{}'. Available targets: {:?}",
                task.name,
                task.target,
                target_names
            );
        }
    }

    // 5. Validate each task's table & rule config
    for task in &config.tasks {
        let mut table_names = std::collections::HashSet::new();
        for table in &task.tables {
            if !table_names.insert(&table.name) {
                bail!(
                    "Task '{}' has duplicate table definition: '{}'",
                    task.name,
                    table.name
                );
            }

            // If mode is Ignore, there should be no rules
            if table.mode == TableMode::Ignore {
                if table.rules.is_some() {
                    bail!(
                        "Task '{}', table '{}': mode is 'ignore' but rules are defined",
                        task.name,
                        table.name
                    );
                }
                continue;
            }

            // Validate per-field rules
            if let Some(rules) = &table.rules {
                let mut field_names = std::collections::HashSet::new();
                for rule_conf in rules {
                    if !field_names.insert(&rule_conf.field) {
                        bail!(
                            "Task '{}', table '{}': duplicate rule for field '{}'",
                            task.name,
                            table.name,
                            rule_conf.field
                        );
                    }

                    // Rule-specific parameter validation
                    match rule_conf.rule {
                        RuleType::Hash => {
                            if let Some(params) = &rule_conf.params {
                                if let Some(algo) = params.get("algorithm") {
                                    if algo != "sha256" && algo != "md5" {
                                        bail!(
                                            "Task '{}', table '{}', field '{}': hash algorithm must be 'sha256' or 'md5', got '{}'",
                                            task.name, table.name, rule_conf.field, algo
                                        );
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    // 6. Validate chunk/batch size
    for task in &config.tasks {
        if task.chunk_size == 0 {
            bail!("Task '{}': chunk_size must be > 0", task.name);
        }
        if task.batch_size == 0 {
            bail!("Task '{}': batch_size must be > 0", task.name);
        }
        if task.max_workers == 0 {
            bail!("Task '{}': max_workers must be > 0", task.name);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::*;

    fn make_valid_config() -> Config {
        Config {
            sources: vec![SourceConfig {
                name: "prod".into(),
                db_kind: DbKind::Postgres,
                connection: ConnectionConfig {
                    url: Some("postgres://localhost:5432/db".into()),
                    ..Default::default()
                },
            }],
            targets: vec![TargetConfig {
                name: "staging".into(),
                db_kind: DbKind::Postgres,
                connection: ConnectionConfig {
                    url: Some("postgres://localhost:5433/db".into()),
                    ..Default::default()
                },
            }],
            tasks: vec![TaskConfig {
                name: "nightly-sync".into(),
                source: "prod".into(),
                target: "staging".into(),
                tables: vec![TableConfig {
                    name: "users".into(),
                    mode: TableMode::Sync,
                    rules: Some(vec![RuleConfig {
                        field: "phone".into(),
                        rule: RuleType::MaskPhone,
                        params: None,
                    }]),
                }],
                chunk_size: 5000,
                batch_size: 1000,
                max_workers: 4,
                rate_limit: 0,
                schedule: None,
                dry_run: false,
                truncate_target: false,
            }],
        }
    }

    #[test]
    fn test_valid_config() {
        let cfg = make_valid_config();
        assert!(validate_config(&cfg).is_ok());
    }

    #[test]
    fn test_duplicate_source() {
        let mut cfg = make_valid_config();
        cfg.sources.push(cfg.sources[0].clone());
        assert!(validate_config(&cfg).is_err());
    }

    #[test]
    fn test_unknown_source() {
        let mut cfg = make_valid_config();
        cfg.tasks[0].source = "nonexistent".into();
        assert!(validate_config(&cfg).is_err());
    }
}
