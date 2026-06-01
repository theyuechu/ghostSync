use super::{Rule, RuleResult};

/// Mask phone numbers in the format: 138****5678
///
/// - If the value is NULL or doesn't look like a phone number, it passes through unchanged.
/// - A "phone-like" value is 10-15 digits, with an optional leading `+`.
#[derive(Debug)]
pub struct MaskPhoneRule {
    mask_char: char,
}

impl MaskPhoneRule {
    pub fn new(mask_char: char) -> Self {
        Self { mask_char }
    }
}

impl Default for MaskPhoneRule {
    fn default() -> Self {
        Self { mask_char: '*' }
    }
}

impl Rule for MaskPhoneRule {
    fn name(&self) -> &'static str {
        "mask_phone"
    }

    fn apply(&self, value: Option<&str>) -> RuleResult {
        let s = match value {
            Some(v) if !v.is_empty() => v,
            _ => return RuleResult::PassThrough,
        };

        // Strip non-digit chars (spaces, dashes, parens)
        let digits: String = s.chars().filter(|c| c.is_ascii_digit()).collect();

        // Must have at least 10 digits to be a valid phone
        if digits.len() < 10 || digits.len() > 15 {
            return RuleResult::PassThrough;
        }

        let visible_prefix = 3;
        let visible_suffix = 4;

        let masked_digits: String = digits
            .chars()
            .enumerate()
            .map(|(i, ch)| {
                if i < visible_prefix || i >= digits.len() - visible_suffix {
                    ch
                } else {
                    self.mask_char
                }
            })
            .collect();

        // Rebuild: prefix + mask + suffix (preserving original formatting is tricky;
        // for MVP just return the masked digit string)
        RuleResult::Replace(masked_digits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_11_digit() {
        let rule = MaskPhoneRule::default();
        assert_eq!(rule.apply(Some("13812345678")), RuleResult::Replace("138****5678".into()));
    }

    #[test]
    fn test_mask_with_dashes() {
        let rule = MaskPhoneRule::default();
        // "138-1234-5678" -> digits "13812345678" -> masked "138****5678"
        // We lose the dashes — acceptable for MVP
        let result = rule.apply(Some("138-1234-5678"));
        assert_eq!(result, RuleResult::Replace("138****5678".into()));
    }

    #[test]
    fn test_too_short_not_a_phone() {
        let rule = MaskPhoneRule::default();
        assert_eq!(rule.apply(Some("12345")), RuleResult::PassThrough);
    }

    #[test]
    fn test_null_passthrough() {
        let rule = MaskPhoneRule::default();
        assert_eq!(rule.apply(None), RuleResult::PassThrough);
    }

    #[test]
    fn test_empty_string_passthrough() {
        let rule = MaskPhoneRule::default();
        assert_eq!(rule.apply(Some("")), RuleResult::PassThrough);
    }

    #[test]
    fn test_custom_mask_char() {
        let rule = MaskPhoneRule::new('x');
        assert_eq!(rule.apply(Some("13812345678")), RuleResult::Replace("138xxxx5678".into()));
    }

    #[test]
    fn test_us_number_with_country_code() {
        let rule = MaskPhoneRule::default();
        // +14155550100 -> digits "14155550100" (11 digits)
        let result = rule.apply(Some("+14155550100"));
        assert_eq!(result, RuleResult::Replace("141****0100".into()));
    }
}
