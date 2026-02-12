use std::borrow::Cow::{self, Owned};

use rustyline::completion::{Completer, Pair};
use rustyline::highlight::{CmdKind, Highlighter};
use rustyline::hint::{Hinter, HistoryHinter};
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Context, Helper, Result};

use crate::theme;

// ---------------------------------------------------------------------------
// SQL keyword / function lists
// ---------------------------------------------------------------------------

const SQL_KEYWORDS: &[&str] = &[
    "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "AS", "ON", "JOIN", "LEFT",
    "RIGHT", "INNER", "OUTER", "CROSS", "HAVING", "DISTINCT", "UNION", "ALL", "INSERT", "INTO",
    "VALUES", "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "DROP", "ALTER", "INDEX", "VIEW",
    "CASE", "WHEN", "THEN", "ELSE", "END", "IN", "BETWEEN", "LIKE", "IS", "NULL", "EXISTS",
    "ASC", "DESC", "OFFSET", "FETCH", "WITH", "RECURSIVE", "EXCEPT", "INTERSECT",
];

const AGG_FUNCTIONS: &[&str] = &["SUM", "AVG", "MIN", "MAX", "COUNT"];

const OPERATORS: &[&str] = &["AND", "OR", "NOT"];

const DOT_COMMANDS: &[&str] = &[".mode", ".tables", ".timer", ".help", ".quit", ".exit"];

// ---------------------------------------------------------------------------
// SqlHelper
// ---------------------------------------------------------------------------

pub struct SqlHelper {
    hinter: HistoryHinter,
    pub column_cache: Vec<String>,
    pub table_paths: Vec<String>,
    pub table_names: Vec<String>,
}

impl SqlHelper {
    pub fn new() -> Self {
        // Glob cwd for CSV files
        let table_paths = std::fs::read_dir(".")
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok())
            .filter_map(|e| {
                let name = e.file_name().to_string_lossy().to_string();
                if name.ends_with(".csv") {
                    Some(name)
                } else {
                    None
                }
            })
            .collect();

        SqlHelper {
            hinter: HistoryHinter::new(),
            column_cache: Vec::new(),
            table_paths,
            table_names: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Highlighter — regex-free token scan
// ---------------------------------------------------------------------------

impl Highlighter for SqlHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        use theme::*;
        // Dot commands
        if line.starts_with('.') {
            return Owned(format!("{DOT_CMD}{line}{R}"));
        }

        let mut out = String::with_capacity(line.len() + 128);
        let bytes = line.as_bytes();
        let len = bytes.len();
        let mut i = 0;

        while i < len {
            let b = bytes[i];

            // Single-quoted string literal
            if b == b'\'' {
                let start = i;
                i += 1;
                while i < len && bytes[i] != b'\'' {
                    i += 1;
                }
                if i < len {
                    i += 1; // consume closing quote
                }
                out.push_str(STR);
                out.push_str(&line[start..i]);
                out.push_str(R);
                continue;
            }

            // Number literal
            if b.is_ascii_digit() || (b == b'-' && i + 1 < len && bytes[i + 1].is_ascii_digit()) {
                let start = i;
                if b == b'-' {
                    i += 1;
                }
                while i < len && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
                    i += 1;
                }
                // Only color as number if not followed by letter (otherwise it's part of an identifier)
                if i < len && (bytes[i].is_ascii_alphabetic() || bytes[i] == b'_') {
                    out.push_str(&line[start..i]);
                } else {
                    out.push_str(NUM);
                    out.push_str(&line[start..i]);
                    out.push_str(R);
                }
                continue;
            }

            // Word (identifier or keyword)
            if b.is_ascii_alphabetic() || b == b'_' {
                let start = i;
                while i < len && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
                let word = &line[start..i];
                let upper = word.to_ascii_uppercase();

                if AGG_FUNCTIONS.contains(&upper.as_str()) {
                    out.push_str(BOLD);
                    out.push_str(FN_CLR);
                    out.push_str(word);
                    out.push_str(R);
                } else if OPERATORS.contains(&upper.as_str()) {
                    out.push_str(BOLD);
                    out.push_str(OP);
                    out.push_str(word);
                    out.push_str(R);
                } else if SQL_KEYWORDS.contains(&upper.as_str()) {
                    out.push_str(BOLD);
                    out.push_str(KW);
                    out.push_str(word);
                    out.push_str(R);
                } else {
                    out.push_str(word);
                }
                continue;
            }

            // Operators: =, <, >, !, !=, <=, >=
            if b == b'=' || b == b'<' || b == b'>' || b == b'!' {
                let start = i;
                i += 1;
                if i < len && bytes[i] == b'=' {
                    i += 1;
                }
                out.push_str(OP);
                out.push_str(&line[start..i]);
                out.push_str(R);
                continue;
            }

            // Everything else (whitespace, parens, commas, etc.)
            out.push(b as char);
            i += 1;
        }

        Owned(out)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        _prompt: &'p str,
        default: bool,
    ) -> Cow<'b, str> {
        if default {
            Owned(format!("{}{}▸{} ", theme::BOLD, theme::PROMPT, theme::R))
        } else {
            Owned(format!("{}  ...  {} ", theme::PROMPT_CONT, theme::R))
        }
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        Owned(format!("{}{hint}{}", theme::HINT, theme::R))
    }

    fn highlight_char(&self, _line: &str, _pos: usize, kind: CmdKind) -> bool {
        // Re-highlight on every keystroke (not just cursor moves)
        kind != CmdKind::MoveCursor
    }
}

// ---------------------------------------------------------------------------
// Completer — context-aware
// ---------------------------------------------------------------------------

impl Completer for SqlHelper {
    type Candidate = Pair;

    fn complete(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Result<(usize, Vec<Pair>)> {
        let before = &line[..pos];

        // Find the start of the current word
        let word_start = before
            .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == '.')
            .map(|i| i + 1)
            .unwrap_or(0);
        let prefix = &before[word_start..];
        let prefix_upper = prefix.to_ascii_uppercase();

        // Determine context from the last significant keyword before cursor
        let before_upper = before.to_ascii_uppercase();
        let context = last_keyword_context(&before_upper);

        let mut candidates: Vec<Pair> = Vec::new();

        match context {
            // After FROM/JOIN → suggest stored tables + file paths
            CompletionContext::Table => {
                add_matching(&self.table_names, prefix, &mut candidates);
                add_matching(&self.table_paths, prefix, &mut candidates);
            }
            // After SELECT/WHERE/GROUP BY/ORDER BY → columns + keywords + functions
            CompletionContext::Column => {
                add_matching(&self.column_cache, prefix, &mut candidates);
                add_matching_strs(AGG_FUNCTIONS, &prefix_upper, prefix, &mut candidates);
                add_matching_strs(SQL_KEYWORDS, &prefix_upper, prefix, &mut candidates);
            }
            // Dot command completion
            CompletionContext::DotCommand => {
                let dot_prefix = &before[before.rfind('.').unwrap_or(0)..];
                let dot_start = pos - dot_prefix.len();
                let mut dot_candidates = Vec::new();
                for cmd in DOT_COMMANDS {
                    if cmd.starts_with(dot_prefix) {
                        dot_candidates.push(Pair {
                            display: cmd.to_string(),
                            replacement: cmd.to_string(),
                        });
                    }
                }
                return Ok((dot_start, dot_candidates));
            }
            // Default: keywords + functions + columns
            CompletionContext::General => {
                add_matching(&self.column_cache, prefix, &mut candidates);
                add_matching_strs(SQL_KEYWORDS, &prefix_upper, prefix, &mut candidates);
                add_matching_strs(AGG_FUNCTIONS, &prefix_upper, prefix, &mut candidates);
                add_matching(&self.table_paths, prefix, &mut candidates);
            }
        }

        Ok((word_start, candidates))
    }
}

#[derive(Debug)]
enum CompletionContext {
    Table,
    Column,
    DotCommand,
    General,
}

fn last_keyword_context(upper: &str) -> CompletionContext {
    // Check for dot command
    if upper.trim_start().starts_with('.') {
        return CompletionContext::DotCommand;
    }

    // Find last significant SQL keyword to determine context
    let table_kws = ["FROM ", "JOIN "];
    let col_kws = [
        "SELECT ", "WHERE ", "BY ", "HAVING ", "ON ", "SET ", "ORDER BY ",
    ];

    let mut last_table: Option<usize> = None;
    let mut last_col: Option<usize> = None;

    for kw in &table_kws {
        if let Some(pos) = upper.rfind(kw) {
            last_table = Some(last_table.map_or(pos, |prev: usize| prev.max(pos)));
        }
    }

    for kw in &col_kws {
        if let Some(pos) = upper.rfind(kw) {
            last_col = Some(last_col.map_or(pos, |prev: usize| prev.max(pos)));
        }
    }

    match (last_table, last_col) {
        (Some(t), Some(c)) => {
            if t > c {
                CompletionContext::Table
            } else {
                CompletionContext::Column
            }
        }
        (Some(_), None) => CompletionContext::Table,
        (None, Some(_)) => CompletionContext::Column,
        (None, None) => CompletionContext::General,
    }
}

fn add_matching(items: &[String], prefix: &str, out: &mut Vec<Pair>) {
    let prefix_lower = prefix.to_lowercase();
    for item in items {
        if item.to_lowercase().starts_with(&prefix_lower) {
            out.push(Pair {
                display: item.clone(),
                replacement: item.clone(),
            });
        }
    }
}

fn add_matching_strs(items: &[&str], prefix_upper: &str, _raw_prefix: &str, out: &mut Vec<Pair>) {
    for &item in items {
        if item.starts_with(prefix_upper) {
            out.push(Pair {
                display: item.to_string(),
                replacement: item.to_string(),
            });
        }
    }
}

// ---------------------------------------------------------------------------
// Hinter — delegates to HistoryHinter
// ---------------------------------------------------------------------------

impl Hinter for SqlHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, ctx: &Context<'_>) -> Option<String> {
        self.hinter.hint(line, pos, ctx)
    }
}

// ---------------------------------------------------------------------------
// Validator — multi-line SQL accumulation
// ---------------------------------------------------------------------------

impl Validator for SqlHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> Result<ValidationResult> {
        let input = ctx.input();
        let trimmed = input.trim();

        // Empty input is valid (just prints a new prompt)
        if trimmed.is_empty() {
            return Ok(ValidationResult::Valid(None));
        }

        // Dot commands are always complete (single line)
        if trimmed.starts_with('.') {
            return Ok(ValidationResult::Valid(None));
        }

        // Check for unbalanced parentheses
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
            return Ok(ValidationResult::Incomplete);
        }

        // SQL must end with semicolon
        if !trimmed.ends_with(';') {
            return Ok(ValidationResult::Incomplete);
        }

        Ok(ValidationResult::Valid(None))
    }
}

// ---------------------------------------------------------------------------
// Helper marker trait
// ---------------------------------------------------------------------------

impl Helper for SqlHelper {}
