# Rich Terminal REPL — Design Document

**Goal:** Replace rustyline with reedline to get IDE-style popup completions, fuzzy matching, rich multi-line editing, interactive history search, and inline hints.

## Architecture

Swap `rustyline = "15"` for `reedline = "0.45"`. Split the monolithic `helper.rs` into focused trait implementations. Keep all output formatting unchanged.

### File Layout

```
crates/teide-cli/src/
├── main.rs          # CLI args, run_repl, output formatting
├── completer.rs     # SqlCompleter — context-aware + fuzzy matching
├── highlighter.rs   # SqlHighlighter — token scan → StyledText
├── validator.rs     # SqlValidator — semicolons + paren balancing
├── prompt.rs        # SqlPrompt — ▸ and ··· indicators
└── theme.rs         # Style constructors (replaces ANSI constants)
```

### Dependency Change

```toml
# Remove:
rustyline = "15"
# Add:
reedline = "0.45"
```

## Feature 1: IdeMenu Popup Completions

VS Code-style dropdown below cursor. Shows SQL keywords, table names, column names with type annotations in the description panel.

```rust
IdeMenu::default()
    .with_name("completion_menu")
    .with_max_completion_height(10)
    .with_padding(1)
    .with_description_mode(DescriptionMode::PreferRight)
    .with_max_description_width(40)
    .with_default_border()
```

Suggestions carry rich metadata:

| Context | Value | Description |
|---------|-------|-------------|
| After SELECT | `id1` | `enum — column from t` |
| After SELECT | `SUM` | `SUM(col) → aggregate` |
| After FROM | `'sales.csv'` | `file — 1.2 MB` |
| After FROM | `t` | `table — 10M rows, 9 cols` |
| Dot command | `.timer` | `Show query execution time` |

## Feature 2: Fuzzy Matching

Subsequence matching with scoring: prefix bonus (+5), consecutive bonus (+3), base match (+1). Results sorted by score. `match_indices` field enables highlighted characters in the popup.

Typing `slct` matches **S**E**L**E**CT** with indices `[0,2,4,5]`.

Replaces the current `starts_with` matching. Prefix matches still score highest.

## Feature 3: Rich Multi-line Prompt

Reedline re-renders all lines on every keystroke. Benefits over rustyline:
- Full syntax highlighting on continuation lines
- Cursor moves freely between lines (arrow keys)
- Can edit earlier lines after pressing Enter

Prompt indicators:
- Primary: `▸ ` (bold cyan)
- Continuation: `  ··· ` (gray)
- Right prompt: empty (reserved for future mode indicator)

## Feature 4: Interactive History Search

Ctrl+R opens filterable history popup. Reedline handles the full UI. File-backed persistence via `FileBackedHistory` at `~/.teide_history`.

## Feature 5: Inline Hints

Fish-style gray ghost text via `DefaultHinter`. Right-arrow accepts. Same behavior as current `HistoryHinter` but rendered natively by reedline.

## Feature 6: Description Panel

IdeMenu's `DescriptionMode::PreferRight` shows a side panel with extra info for the selected suggestion. Column completions show type, table completions show row/col counts, function completions show signatures.

## Trigger Behavior

- Auto-popup after 2+ characters typed (completer returns empty vec for <2 chars)
- Tab forces popup open regardless of prefix length
- Escape dismisses
- Arrow keys navigate
- Enter selects

## What Stays Unchanged

- All output formatting (print_table, print_csv, print_json)
- Dot command handling
- Banner
- Cell formatting
- CLI args (clap)
- build.rs (git hash embedding)
- theme.rs color choices (adapted to nu_ansi_term::Style)
