use sha2::{Digest, Sha256};
use md5::Md5;
use crc32fast::Hasher as Crc32;

use super::{Rule, RuleResult};

/// Hash a string value using SHA-256, MD5, or CRC32.
///
/// - Returns the hex-encoded hash.
/// - NULL values pass through unchanged (not hashed).
/// - Empty strings are hashed (non-null empty → hash of "").
#[derive(Debug)]
pub struct HashRule {
    pub algorithm: String,
}

impl Rule for HashRule {
    fn apply(&self, value: Option<&str>) -> RuleResult {
        let s = match value {
            Some(v) => v,
            None => return RuleResult::PassThrough,
        };

        let hash = match self.algorithm.as_str() {
            "md5" => {
                let mut hasher = Md5::new();
                hasher.update(s.as_bytes());
                format!("{:x}", hasher.finalize())
            }
            "crc32" => {
                let mut hasher = Crc32::new();
                hasher.update(s.as_bytes());
                format!("{:08x}", hasher.finalize())
            }
            // Default to sha256
            _ => {
                let mut hasher = Sha256::new();
                hasher.update(s.as_bytes());
                format!("{:x}", hasher.finalize())
            }
        };

        RuleResult::Replace(hash)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha256_hash() {
        let rule = HashRule {
            algorithm: "sha256".into(),
        };
        let result = rule.apply(Some("hello"));
        // known sha256 of "hello"
        let expected = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assert_eq!(result, RuleResult::Replace(expected.into()));
    }

    #[test]
    fn test_md5_hash() {
        let rule = HashRule {
            algorithm: "md5".into(),
        };
        let result = rule.apply(Some("hello"));
        // known md5 of "hello"
        let expected = "5d41402abc4b2a76b9719d911017c592";
        assert_eq!(result, RuleResult::Replace(expected.into()));
    }

    #[test]
    fn test_crc32_hash() {
        let rule = HashRule {
            algorithm: "crc32".into(),
        };
        let result = rule.apply(Some("hello"));
        // known crc32 of "hello"
        let expected = "3610a686";
        assert_eq!(result, RuleResult::Replace(expected.into()));
    }

    #[test]
    fn test_null_passthrough() {
        let rule = HashRule {
            algorithm: "sha256".into(),
        };
        assert_eq!(rule.apply(None), RuleResult::PassThrough);
    }

    #[test]
    fn test_empty_string() {
        let rule = HashRule {
            algorithm: "sha256".into(),
        };
        let expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        assert_eq!(rule.apply(Some("")), RuleResult::Replace(expected.into()));
    }

    #[test]
    fn test_unknown_algorithm_falls_back_to_sha256() {
        let rule = HashRule {
            algorithm: "invalid".into(),
        };
        let result = rule.apply(Some("test"));
        let expected = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08";
        assert_eq!(result, RuleResult::Replace(expected.into()));
    }
}
