use reedline::{ValidationResult, Validator};

pub struct SqlValidator;

impl Validator for SqlValidator {
    fn validate(&self, line: &str) -> ValidationResult {
        let trimmed = line.trim();

        // Empty input or dot commands are always complete
        if trimmed.is_empty() || trimmed.starts_with('.') {
            return ValidationResult::Complete;
        }

        // Unbalanced parentheses → incomplete
        let mut depth: i32 = 0;
        let mut in_string = false;
        for ch in trimmed.chars() {
            if ch == '\'' {
                in_string = !in_string;
            } else if !in_string {
                match ch {
                    '(' => depth += 1,
                    ')' => depth -= 1,
                    _ => {}
                }
            }
        }
        if depth > 0 {
            return ValidationResult::Incomplete;
        }

        // SQL must end with semicolon
        if !trimmed.ends_with(';') {
            return ValidationResult::Incomplete;
        }

        ValidationResult::Complete
    }
}
