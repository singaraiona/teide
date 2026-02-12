// Nord color palette — https://www.nordtheme.com
//
// Polar Night (structural grays)
// pub const NORD0: &str = "\x1b[38;2;46;52;64m";    // bg (unused)
// pub const NORD1: &str = "\x1b[38;2;59;66;82m";    // elevated bg
pub const NORD3: &str = "\x1b[38;2;76;86;106m";      // comments, dim
// Snow Storm (text)
pub const NORD4: &str = "\x1b[38;2;216;222;233m";    // primary text
// Frost (blues)
pub const NORD7: &str = "\x1b[38;2;143;188;187m";    // teal accent
pub const NORD8: &str = "\x1b[38;2;136;192;208m";    // bright frost
pub const NORD9: &str = "\x1b[38;2;129;161;193m";    // medium blue
pub const NORD10: &str = "\x1b[38;2;94;129;172m";    // dark blue
// Aurora (accents)
pub const NORD11: &str = "\x1b[38;2;191;97;106m";    // red (errors)
pub const NORD13: &str = "\x1b[38;2;235;203;139m";   // yellow (strings)
pub const NORD14: &str = "\x1b[38;2;163;190;140m";   // green (success)
pub const NORD15: &str = "\x1b[38;2;180;142;173m";   // purple (numbers)

pub const BOLD: &str = "\x1b[1m";
pub const ITALIC: &str = "\x1b[3m";
pub const R: &str = "\x1b[0m";

// Semantic aliases
pub const BORDER: &str = NORD3;
pub const HEADER: &str = NORD8;
pub const TYPE_DIM: &str = NORD3;
pub const TEXT: &str = NORD4;
pub const NULL_CLR: &str = NORD3;
pub const FOOTER: &str = NORD3;
pub const ERROR: &str = NORD11;
pub const SUCCESS: &str = NORD14;
pub const TIMER: &str = NORD3;

// Syntax highlighting
pub const KW: &str = NORD9;       // SQL keywords
pub const FN_CLR: &str = NORD8;   // aggregate functions
pub const STR: &str = NORD13;     // string literals
pub const NUM: &str = NORD15;     // number literals
pub const OP: &str = NORD9;       // operators
pub const DOT_CMD: &str = NORD7;  // dot commands
pub const HINT: &str = NORD3;     // history hints

// Banner
pub const BAN_BORDER: &str = NORD10;
pub const BAN_TITLE: &str = NORD8;
pub const BAN_INFO: &str = NORD4;
pub const BAN_HELP: &str = NORD3;

// Prompt
pub const PROMPT: &str = NORD8;
pub const PROMPT_CONT: &str = NORD3;
