use super::{Rule, RuleResult};

/// Mask email addresses: keep first character of local part, replace rest with mask char.
///
/// - "alice@example.com" → "a****@example.com" (with mask_char='*', keep_first=true)
/// - "b@short.com" → "b@short.com" (only 1 char local part — unchanged)
/// - NULL / non-email → PassThrough
#[derive(Debug)]
pub struct MaskEmailRule {
    pub mask_char: char,
    pub keep_first: bool,
}

impl Default for MaskEmailRule {
    fn default() -> Self {
        Self {
            mask_char: '*',
            keep_first: true,
        }
    }
}

impl Rule for MaskEmailRule {
    fn apply(&self, value: Option<&str>) -> RuleResult {
        let s = match value {
            Some(v) if !v.is_empty() => v,
            _ => return RuleResult::PassThrough,
        };

        let at_pos = s.find('@');
        let at_pos = match at_pos {
            Some(p) => p,
            None => return RuleResult::PassThrough, // not an email
        };

        let local = &s[..at_pos];
        let domain = &s[at_pos..]; // includes '@'

        // Mask the local part
        if self.keep_first && local.len() > 1 {
            let masked_local = format!(
                "{}{}",
                &local[..1],
                format!("{}", self.mask_char).repeat(local.len() - 1)
            );
            RuleResult::Replace(format!("{}{}", masked_local, domain))
        } else if local.len() <= 1 {
            // Too short to meaningfully mask — return as-is
            RuleResult::PassThrough
        } else {
            let masked_local = format!("{}", self.mask_char).repeat(local.len());
            RuleResult::Replace(format!("{}{}", masked_local, domain))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_normal_email() {
        let rule = MaskEmailRule::default();
        assert_eq!(
            rule.apply(Some("alice@example.com")),
            RuleResult::Replace("a****@example.com".into())
        );
    }

    #[test]
    fn test_short_local_part() {
        let rule = MaskEmailRule::default();
        assert_eq!(
            rule.apply(Some("a@b.com")),
            RuleResult::PassThrough
        );
    }

    #[test]
    fn test_no_at_sign() {
        let rule = MaskEmailRule::default();
        assert_eq!(rule.apply(Some("notanemail")), RuleResult::PassThrough);
    }

    #[test]
    fn test_null_passthrough() {
        let rule = MaskEmailRule::default();
        assert_eq!(rule.apply(None), RuleResult::PassThrough);
    }

    #[test]
    fn test_custom_mask_char() {
        let rule = MaskEmailRule {
            mask_char: 'x',
            keep_first: true,
        };
        assert_eq!(
            rule.apply(Some("bob@test.com")),
            RuleResult::Replace("bxx@test.com".into())
        );
    }

    #[test]
    fn test_dont_keep_first() {
        let rule = MaskEmailRule {
            mask_char: '*',
            keep_first: false,
        };
        assert_eq!(
            rule.apply(Some("hello@world.com")),
            RuleResult::Replace("*****@world.com".into())
        );
    }
}
