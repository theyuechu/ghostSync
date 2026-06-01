/// Rule engine for data desensitization.
///
/// Architecture:
/// - `Rule` trait — each transformation is a struct implementing this trait.
/// - `RuleEngine` — holds per-table per-field rules, processes rows.
/// - Concrete rules: MaskPhone, MaskEmail, Hash, Ignore.
use std::collections::{HashMap, HashSet};
use std::fmt;

pub(crate) mod hash;
pub(crate) mod ignore;
pub(crate) mod mask_email;
pub(crate) mod mask_phone;

use crate::config::types::{RuleConfig, RuleType, TableMode, TaskConfig};

// ─── RuleResult ─────────────────────────────────────────────────────

/// Result of applying a rule to a field value.
#[derive(Debug, Clone, PartialEq)]
pub enum RuleResult {
    /// Omit this field from the target row entirely
    Skip,
    /// Replace the value with this new string
    Replace(String),
    /// Keep the original value unchanged
    PassThrough,
}

impl RuleResult {
    /// Resolve into an `Option<String>` suitable for the output row.
    /// `original` is the value before any rule was applied.
    pub fn into_option(self, original: Option<String>) -> Option<String> {
        match self {
            RuleResult::Skip => None,
            RuleResult::Replace(v) => Some(v),
            RuleResult::PassThrough => original,
        }
    }
}

// ─── Rule Trait ─────────────────────────────────────────────────────

/// A single field-level transformation rule.
///
/// Implementations must be stateless (or contain only immutable config).
/// They are shared across worker threads via `&Box<dyn Rule>`.
pub trait Rule: Send + Sync + fmt::Debug {
    /// Human-readable rule type name (e.g. "mask_phone", "hash")
    fn name(&self) -> &'static str;

    /// Apply the rule to an optional string value.
    ///
    /// - If `value` is `None` (SQL NULL), most rules return `PassThrough`.
    /// - `IgnoreRule` always returns `Skip`.
    fn apply(&self, value: Option<&str>) -> RuleResult;
}

// ─── RuleEngine ─────────────────────────────────────────────────────

/// Holds all configured rules for a single task, organized by table.
///
/// Thread-safe: contains only `&dyn Rule` references and static config.
#[derive(Debug)]
pub struct RuleEngine {
    /// Table names that should be entirely skipped
    ignored_tables: HashSet<String>,
    /// Per-table field rules: table_name -> [(field_name, rule)]
    field_rules: HashMap<String, Vec<(String, Box<dyn Rule>)>>,
    /// Whether to truncate target before sync
    pub truncate_target: bool,
}

impl RuleEngine {
    /// Build a `RuleEngine` from a task's table configurations.
    pub fn from_task(task: &TaskConfig) -> Self {
        let mut ignored_tables = HashSet::new();
        let mut field_rules: HashMap<String, Vec<(String, Box<dyn Rule>)>> = HashMap::new();

        for table_cfg in &task.tables {
            let table_name = table_cfg.name.clone();

            if table_cfg.mode == TableMode::Ignore {
                ignored_tables.insert(table_name);
                continue;
            }

            let mut rules: Vec<(String, Box<dyn Rule>)> = Vec::new();
            if let Some(rule_configs) = &table_cfg.rules {
                for rc in rule_configs {
                    if let Some(rule) = build_rule(rc) {
                        rules.push((rc.field.clone(), rule));
                    }
                }
            }

            if !rules.is_empty() {
                field_rules.insert(table_name, rules);
            }
        }

        RuleEngine {
            ignored_tables,
            field_rules,
            truncate_target: task.truncate_target,
        }
    }

    /// Check if this entire table should be skipped.
    pub fn is_table_ignored(&self, table: &str) -> bool {
        self.ignored_tables.contains(table)
    }

    /// Apply all configured rules to a single row.
    ///
    /// Returns `(transformed_row, was_modified)`.
    /// Fields with `RuleResult::Skip` are removed from the output row.
    pub fn process_row(
        &self,
        table: &str,
        row: &HashMap<String, Option<String>>,
    ) -> (HashMap<String, Option<String>>, bool) {
        let mut modified = false;
        let mut output = HashMap::with_capacity(row.len());

        if let Some(field_rules) = self.field_rules.get(table) {
            // Table has rules — process each field
            for (field, rule) in field_rules {
                let original = row.get(field).and_then(|v| v.clone());
                let original_str = original.as_deref();

                match rule.apply(original_str) {
                    RuleResult::Skip => {
                        modified = true;
                        // Field omitted — do not insert
                    }
                    RuleResult::Replace(new_val) => {
                        modified = true;
                        output.insert(field.clone(), Some(new_val));
                    }
                    RuleResult::PassThrough => {
                        // Keep original (or None)
                        output.insert(field.clone(), original);
                    }
                }
            }

            // Carry over any fields not covered by rules
            for (field, value) in row {
                if !output.contains_key(field) {
                    // Check if any rule targets this field (already handled above)
                    // If not, pass through
                    let has_rule = field_rules.iter().any(|(f, _)| f == field);
                    if !has_rule {
                        output.insert(field.clone(), value.clone());
                    }
                }
            }
        } else {
            // No rules for this table — pass through everything
            output = row.clone();
        }

        (output, modified)
    }

    /// Process multiple rows (batch) — each row is independent.
    /// Useful for worker pool concurrency.
    pub fn process_rows(
        &self,
        table: &str,
        rows: &[HashMap<String, Option<String>>],
    ) -> Vec<HashMap<String, Option<String>>> {
        rows.iter()
            .map(|row| self.process_row(table, row).0)
            .collect()
    }
}

// ─── Rule Factory ───────────────────────────────────────────────────

/// Instantiate a `Box<dyn Rule>` from a `RuleConfig`.
fn build_rule(config: &RuleConfig) -> Option<Box<dyn Rule>> {
    match config.rule {
        RuleType::Ignore => Some(Box::new(ignore::IgnoreFieldRule)),
        RuleType::MaskPhone => {
            let mask_char = config
                .params
                .as_ref()
                .and_then(|p| p.get("mask_char"))
                .and_then(|s| s.chars().next())
                .unwrap_or('*');
            Some(Box::new(mask_phone::MaskPhoneRule::new(mask_char)))
        }
        RuleType::MaskEmail => {
            let mask_char = config
                .params
                .as_ref()
                .and_then(|p| p.get("mask_char"))
                .and_then(|s| s.chars().next())
                .unwrap_or('*');
            let keep_first = config
                .params
                .as_ref()
                .and_then(|p| p.get("keep_first"))
                .map(|s| s == "true")
                .unwrap_or(true);
            Some(Box::new(mask_email::MaskEmailRule {
                mask_char,
                keep_first,
            }))
        }
        RuleType::Hash => {
            let algorithm = config
                .params
                .as_ref()
                .and_then(|p| p.get("algorithm"))
                .map(|s| s.to_lowercase())
                .unwrap_or_else(|| "sha256".to_string());
            Some(Box::new(hash::HashRule { algorithm }))
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::{RuleConfig, RuleType, TableConfig, TableMode, TaskConfig};
    use std::collections::HashMap;

    fn make_task() -> TaskConfig {
        TaskConfig {
            name: "test-task".into(),
            source: "src".into(),
            target: "dst".into(),
            tables: vec![
                TableConfig {
                    name: "users".into(),
                    mode: TableMode::Sync,
                    rules: Some(vec![
                        RuleConfig {
                            field: "phone".into(),
                            rule: RuleType::MaskPhone,
                            params: None,
                        },
                        RuleConfig {
                            field: "email".into(),
                            rule: RuleType::MaskEmail,
                            params: None,
                        },
                        RuleConfig {
                            field: "password_hash".into(),
                            rule: RuleType::Hash,
                            params: Some(
                                vec![("algorithm".into(), "sha256".into())]
                                    .into_iter()
                                    .collect(),
                            ),
                        },
                    ]),
                },
                TableConfig {
                    name: "internal_logs".into(),
                    mode: TableMode::Ignore,
                    rules: None,
                },
            ],
            chunk_size: 5000,
            batch_size: 1000,
            max_workers: 4,
            rate_limit: 0,
            schedule: None,
            dry_run: false,
            truncate_target: false,
        }
    }

    #[test]
    fn test_ignore_table() {
        let engine = RuleEngine::from_task(&make_task());
        assert!(engine.is_table_ignored("internal_logs"));
        assert!(!engine.is_table_ignored("users"));
    }

    #[test]
    fn test_mask_phone_rule() {
        let engine = RuleEngine::from_task(&make_task());
        let mut row = HashMap::new();
        row.insert("phone".into(), Some("13812345678".into()));
        row.insert("email".into(), Some("alice@example.com".into()));
        row.insert("password_hash".into(), Some("secret".into()));
        row.insert("name".into(), Some("Alice".into()));

        let (result, modified) = engine.process_row("users", &row);
        assert!(modified, "should be modified");

        let phone = result.get("phone").and_then(|v| v.as_deref()).unwrap();
        assert_eq!(phone, "138****5678", "phone should be masked");

        let email = result.get("email").and_then(|v| v.as_deref()).unwrap();
        assert!(email.contains('@'), "email should still have @");
        assert!(email.starts_with('a'), "email should keep first char");
        assert_ne!(email, "alice@example.com", "email should be masked");

        let hash = result.get("password_hash").and_then(|v| v.as_deref()).unwrap();
        assert_ne!(hash, "secret", "password should be hashed");
        assert_eq!(hash.len(), 64, "sha256 hex should be 64 chars");

        let name = result.get("name").and_then(|v| v.as_deref()).unwrap();
        assert_eq!(name, "Alice", "uncovered fields should pass through");
    }

    #[test]
    fn test_ignore_field() {
        let mut task = make_task();
        task.tables[0].rules = Some(vec![
            RuleConfig {
                field: "ssn".into(),
                rule: RuleType::Ignore,
                params: None,
            },
        ]);

        let engine = RuleEngine::from_task(&task);
        let mut row = HashMap::new();
        row.insert("ssn".into(), Some("123-45-6789".into()));
        row.insert("name".into(), Some("Bob".into()));

        let (result, _) = engine.process_row("users", &row);
        assert!(
            !result.contains_key("ssn"),
            "ssn should be removed by Ignore rule"
        );
        assert_eq!(
            result.get("name").and_then(|v| v.as_deref()).unwrap(),
            "Bob"
        );
    }

    #[test]
    fn test_null_values_pass_through() {
        let engine = RuleEngine::from_task(&make_task());
        let mut row = HashMap::new();
        row.insert("phone".into(), None);
        row.insert("name".into(), Some("Charlie".into()));

        let (result, _) = engine.process_row("users", &row);
        assert!(result.get("phone").unwrap().is_none(), "NULL phone stays NULL");
        assert_eq!(
            result.get("name").and_then(|v| v.as_deref()).unwrap(),
            "Charlie"
        );
    }

    #[test]
    fn test_no_rules_no_modification() {
        let engine = RuleEngine::from_task(&make_task());
        let mut row = HashMap::new();
        row.insert("id".into(), Some("42".into()));

        // "no rules" should still call process_row; table "users" has rules but id has none
        let (result, modified) = engine.process_row("users", &row);
        assert!(!modified, "no matching rules = no modification");
        assert_eq!(
            result.get("id").and_then(|v| v.as_deref()).unwrap(),
            "42"
        );
    }

    #[test]
    fn test_rule_result_into_option() {
        let orig = Some("hello".to_string());
        assert_eq!(RuleResult::Skip.into_option(orig.clone()), None);
        assert_eq!(
            RuleResult::Replace("world".into()).into_option(orig.clone()),
            Some("world".to_string())
        );
        assert_eq!(RuleResult::PassThrough.into_option(orig.clone()), orig);
    }

    #[test]
    fn test_process_rows_batch() {
        let engine = RuleEngine::from_task(&make_task());
        let rows: Vec<HashMap<String, Option<String>>> = vec![
            {
                let mut r = HashMap::new();
                r.insert("phone".into(), Some("13900001111".into()));
                r
            },
            {
                let mut r = HashMap::new();
                r.insert("phone".into(), Some("13899998888".into()));
                r
            },
        ];

        let results = engine.process_rows("users", &rows);
        assert_eq!(results.len(), 2);
        for row in &results {
            let phone = row.get("phone").and_then(|v| v.as_deref()).unwrap();
            assert!(phone.contains("****"), "all phones should be masked");
        }
    }
}
