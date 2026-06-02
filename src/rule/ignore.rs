use super::{Rule, RuleResult};

/// A rule that always skips/removes the field from the output.
///
/// This is equivalent to "do not sync this column".
#[derive(Debug)]
pub struct IgnoreFieldRule;

impl Rule for IgnoreFieldRule {
    fn apply(&self, _value: Option<&str>) -> RuleResult {
        RuleResult::Skip
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ignore_always_skips() {
        let rule = IgnoreFieldRule;
        assert_eq!(rule.apply(None), RuleResult::Skip);
        assert_eq!(rule.apply(Some("anything")), RuleResult::Skip);
        assert_eq!(rule.apply(Some("")), RuleResult::Skip);
    }
}
