use super::{generate_from_schema, Generate};
use serde_json::{json, Value};

pub struct TextGenerator {
    min_size: usize,
    max_size: Option<usize>,
}

impl TextGenerator {
    pub fn with_min_size(mut self, min: usize) -> Self {
        self.min_size = min;
        self
    }

    pub fn with_max_size(mut self, max: usize) -> Self {
        self.max_size = Some(max);
        self
    }
}

impl Generate<String> for TextGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        let mut schema = json!({"type": "string"});

        if self.min_size > 0 {
            schema["min_length"] = json!(self.min_size);
        }

        if let Some(max) = self.max_size {
            schema["max_length"] = json!(max);
        }

        Some(schema)
    }
}

pub fn text() -> TextGenerator {
    TextGenerator {
        min_size: 0,
        max_size: None,
    }
}

pub struct RegexGenerator {
    pattern: String,
    fullmatch: bool,
}

impl RegexGenerator {
    /// Require the entire string to match the pattern, not just contain a match.
    pub fn fullmatch(mut self) -> Self {
        self.fullmatch = true;
        self
    }
}

impl Generate<String> for RegexGenerator {
    fn generate(&self) -> String {
        generate_from_schema(&self.schema().unwrap())
    }

    fn schema(&self) -> Option<Value> {
        let mut schema = json!({
            "type": "regex",
            "pattern": self.pattern
        });
        if self.fullmatch {
            schema["fullmatch"] = json!(true);
        }
        Some(schema)
    }
}

/// Generate strings that contain a match for the given regex pattern.
///
/// Use `.fullmatch()` to require the entire string to match.
pub fn from_regex(pattern: &str) -> RegexGenerator {
    RegexGenerator {
        pattern: pattern.to_string(),
        fullmatch: false,
    }
}
