// Terminal color theme using standard ANSI palette colors.
//
// Uses the terminal's own 16-color palette so colors automatically adapt
// when the user switches terminal profiles (dark ↔ light) mid-session,
// exactly like DuckDB. No background detection needed.

// Formatting
pub const BOLD: &str       = "\x1b[1m";
pub const ITALIC: &str     = "\x1b[3m";
pub const R: &str          = "\x1b[0m";

// Table structure
pub const BORDER: &str     = "\x1b[90m";            // bright black (gray)
pub const HEADER: &str     = "\x1b[1;36m";           // bold cyan
pub const TYPE_DIM: &str   = "\x1b[90m";             // gray
pub const TEXT: &str       = "\x1b[39m";             // default foreground
pub const NULL_CLR: &str   = "\x1b[90m";             // gray
pub const FOOTER: &str     = "\x1b[90m";             // gray

// Status
pub const ERROR: &str      = "\x1b[1;31m";           // bold red
pub const SUCCESS: &str    = "\x1b[32m";             // green
pub const TIMER: &str      = "\x1b[90m";             // gray

// SQL syntax highlighting
pub const KW: &str         = "\x1b[34m";             // blue
pub const FN_CLR: &str     = "\x1b[36m";             // cyan
pub const STR: &str        = "\x1b[33m";             // yellow
pub const NUM: &str        = "\x1b[35m";             // magenta
pub const OP: &str         = "\x1b[34m";             // blue
pub const DOT_CMD: &str    = "\x1b[36m";             // cyan
pub const HINT: &str       = "\x1b[90m";             // gray

// Banner
pub const BAN_BORDER: &str = "\x1b[34m";             // blue
pub const BAN_TITLE: &str  = "\x1b[1;36m";           // bold cyan
pub const BAN_INFO: &str   = "\x1b[39m";             // default foreground
pub const BAN_HELP: &str   = "\x1b[90m";             // gray

// Prompt
pub const PROMPT: &str     = "\x1b[36m";             // cyan
pub const PROMPT_CONT: &str= "\x1b[90m";             // gray

// Aliases for help text
pub const NORD3: &str      = "\x1b[90m";             // gray
pub const NORD7: &str      = "\x1b[36m";             // cyan
