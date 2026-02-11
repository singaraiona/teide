mod helper;

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "teide", version, about = "Fast SQL engine powered by Teide")]
struct Args {
    /// SQL query or CSV file path
    input: Option<String>,
    /// Execute SQL from file
    #[arg(short, long)]
    file: Option<PathBuf>,
}

#[derive(Clone, Copy)]
enum OutputFormat {
    Table,
    Csv,
    Json,
}

fn main() {
    let args = Args::parse();

    // Non-interactive: execute single query
    if let Some(ref input) = args.input {
        if !input.ends_with(".csv") {
            run_single_query(input);
            return;
        }
    }

    // Non-interactive: execute SQL from file
    if let Some(ref file) = args.file {
        run_sql_file(file);
        return;
    }

    // Interactive REPL (run synchronously; Context is !Send)
    run_repl(args.input.as_deref());
}

fn run_single_query(sql: &str) {
    let ctx = teide::Context::new().expect("Failed to init Teide engine");
    match teide_sql::execute_sql(&ctx, sql) {
        Ok(result) => print_result(&result, &OutputFormat::Table),
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    }
}

fn run_sql_file(path: &PathBuf) {
    let contents = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("Error reading {}: {e}", path.display());
        std::process::exit(1);
    });
    let mut session = teide_sql::Session::new().expect("Failed to init Teide engine");
    for stmt in contents.split(';') {
        let sql = stmt.trim();
        if sql.is_empty() {
            continue;
        }
        match session.execute(sql) {
            Ok(teide_sql::ExecResult::Query(result)) => {
                print_result(&result, &OutputFormat::Table);
            }
            Ok(teide_sql::ExecResult::Ddl(msg)) => println!("{msg}"),
            Err(e) => eprintln!("Error: {e}"),
        }
    }
}

fn run_repl(preload_csv: Option<&str>) {
    let mut session = teide_sql::Session::new().expect("Failed to init Teide engine");

    print_banner();

    if let Some(csv_path) = preload_csv {
        let sql = format!("CREATE TABLE t AS SELECT * FROM '{csv_path}'");
        match session.execute(&sql) {
            Ok(teide_sql::ExecResult::Ddl(msg)) => println!("{msg}"),
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error loading {csv_path}: {e}");
                return;
            }
        }
    }

    let config = rustyline::Config::builder()
        .max_history_size(1000)
        .unwrap()
        .completion_type(rustyline::config::CompletionType::List)
        .edit_mode(rustyline::config::EditMode::Emacs)
        .build();

    let helper = helper::SqlHelper::new();
    let mut editor = rustyline::Editor::with_config(config).expect("Failed to create editor");
    editor.set_helper(Some(helper));

    let history_path = dirs_or_home().join(".teide_history");
    let _ = editor.load_history(&history_path);

    let mut format = OutputFormat::Table;
    let mut show_timer = false;

    loop {
        // Validator handles multi-line: returns Incomplete until input ends with ';'
        // rustyline shows "teide ❯" for first line, "   ...> " for continuations
        match editor.readline("▸ ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                // Dot commands
                if trimmed.starts_with('.') {
                    editor.add_history_entry(trimmed).ok();
                    handle_dot_command(trimmed, &mut format, &mut show_timer, &session);
                    continue;
                }

                // SQL statement (Validator ensured it ends with ';')
                let sql = trimmed.trim_end_matches(';').trim();
                if sql.is_empty() {
                    continue;
                }

                // Normalize history: always exactly one trailing ';'
                let history_sql = format!("{sql};");
                editor.add_history_entry(&history_sql).ok();

                let start = std::time::Instant::now();
                match session.execute(sql) {
                    Ok(teide_sql::ExecResult::Query(result)) => {
                        // Update column cache for Tab-completion
                        if let Some(h) = editor.helper_mut() {
                            h.column_cache = result.columns.clone();
                        }
                        print_result(&result, &format);
                        if show_timer {
                            eprintln!("Run Time: {:.3}s", start.elapsed().as_secs_f64());
                        }
                    }
                    Ok(teide_sql::ExecResult::Ddl(msg)) => {
                        println!("{msg}");
                        // Update stored table names for Tab-completion
                        if let Some(h) = editor.helper_mut() {
                            h.table_names = session
                                .table_names()
                                .into_iter()
                                .map(|s| s.to_string())
                                .collect();
                        }
                        if show_timer {
                            eprintln!("Run Time: {:.3}s", start.elapsed().as_secs_f64());
                        }
                    }
                    Err(e) => eprintln!("Error: {e}"),
                }
            }
            Err(rustyline::error::ReadlineError::Eof) => break,
            Err(rustyline::error::ReadlineError::Interrupted) => continue,
            Err(e) => {
                eprintln!("Readline error: {e}");
                break;
            }
        }
    }

    let _ = editor.save_history(&history_path);
}

// ---------------------------------------------------------------------------
// Column index resolution
// ---------------------------------------------------------------------------

/// Resolve column names from SqlResult.columns to table column indices.
fn resolve_col_indices(result: &teide_sql::SqlResult) -> Vec<usize> {
    let table = &result.table;
    let ncols = table.ncols() as usize;

    let mut name_to_idx: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    for i in 0..ncols {
        let name = table.col_name_str(i).to_lowercase();
        if !name.is_empty() {
            name_to_idx.insert(name, i);
        }
    }

    let mut indices = Vec::new();
    for (pos, col_name) in result.columns.iter().enumerate() {
        if let Some(&idx) = name_to_idx.get(&col_name.to_lowercase()) {
            indices.push(idx);
        } else {
            // For GROUP BY results, columns are positional
            if pos < ncols {
                indices.push(pos);
            }
        }
    }

    // Fallback: if no columns matched, show all
    if indices.is_empty() {
        indices = (0..ncols).collect();
    }

    indices
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

fn print_result(result: &teide_sql::SqlResult, format: &OutputFormat) {
    match format {
        OutputFormat::Table => print_table(result),
        OutputFormat::Csv => print_csv(result),
        OutputFormat::Json => print_json(result),
    }
}

const HEAD_ROWS: usize = 20;
const TAIL_ROWS: usize = 20;

fn type_name(typ: i8) -> &'static str {
    match typ {
        1 => "bool",
        4 => "i16",
        5 => "i32",
        6 => "i64",
        7 => "f64",
        14 => "sym",
        15 => "enum",
        _ => "?",
    }
}

fn print_table(result: &teide_sql::SqlResult) {
    use std::fmt::Write;

    let table = &result.table;
    let nrows = table.nrows() as usize;
    let col_indices = resolve_col_indices(result);
    let ncols = col_indices.len();

    if ncols == 0 {
        println!("(empty result)");
        return;
    }

    // Column metadata
    let col_names: Vec<String> = col_indices
        .iter()
        .enumerate()
        .map(|(pos, &idx)| {
            if pos < result.columns.len() {
                result.columns[pos].clone()
            } else {
                table.col_name_str(idx).to_string()
            }
        })
        .collect();

    let col_types: Vec<&str> = col_indices
        .iter()
        .map(|&idx| type_name(table.col_type(idx)))
        .collect();

    let is_right: Vec<bool> = col_indices
        .iter()
        .map(|&idx| matches!(table.col_type(idx), 1 | 4 | 5 | 6 | 7))
        .collect();

    // Determine which rows to display
    let show_dots = nrows > HEAD_ROWS + TAIL_ROWS;
    let head_n = if show_dots { HEAD_ROWS } else { nrows };
    let tail_n = if show_dots { TAIL_ROWS } else { 0 };
    let shown = head_n + tail_n;

    // Format all cell values
    let mut cells: Vec<Vec<String>> = Vec::with_capacity(shown + 3);
    for r in 0..head_n {
        cells.push(
            col_indices
                .iter()
                .map(|&c| format_cell(table, c, r))
                .collect(),
        );
    }
    if show_dots {
        cells.push(
            (0..ncols)
                .map(|_| "\u{00b7}\u{00b7}\u{00b7}".to_string())
                .collect(),
        );
        for r in (nrows - tail_n)..nrows {
            cells.push(
                col_indices
                    .iter()
                    .map(|&c| format_cell(table, c, r))
                    .collect(),
            );
        }
    }

    // Footer text
    let footer_left = if show_dots {
        format!("{nrows} rows ({shown} shown)")
    } else {
        format!("{nrows} rows")
    };
    let footer_right = format!("{ncols} columns");
    let footer_min = footer_left.len() + footer_right.len() + 3;

    // Calculate column widths
    let mut w: Vec<usize> = (0..ncols)
        .map(|c| {
            let mut max = col_names[c].len().max(col_types[c].len());
            for row in &cells {
                max = max.max(row[c].len());
            }
            max
        })
        .collect();

    let mut inner_width: usize = w.iter().map(|x| x + 2).sum::<usize>() + ncols - 1;

    if inner_width < footer_min {
        let extra = footer_min - inner_width;
        w[ncols - 1] += extra;
        inner_width += extra;
    }
    let mut buf = String::with_capacity(inner_width + 4);

    // Top border
    buf.push('┌');
    for c in 0..ncols {
        if c > 0 {
            buf.push('┬');
        }
        for _ in 0..w[c] + 2 {
            buf.push('─');
        }
    }
    buf.push('┐');
    println!("{buf}");

    // Column names
    buf.clear();
    buf.push('│');
    for c in 0..ncols {
        if c > 0 {
            buf.push('│');
        }
        let _ = write!(buf, " {:^width$} ", col_names[c], width = w[c]);
    }
    buf.push('│');
    println!("{buf}");

    // Column types
    buf.clear();
    buf.push('│');
    for c in 0..ncols {
        if c > 0 {
            buf.push('│');
        }
        let _ = write!(buf, " {:^width$} ", col_types[c], width = w[c]);
    }
    buf.push('│');
    println!("{buf}");

    // Header/data separator
    buf.clear();
    buf.push('├');
    for c in 0..ncols {
        if c > 0 {
            buf.push('┼');
        }
        for _ in 0..w[c] + 2 {
            buf.push('─');
        }
    }
    buf.push('┤');
    println!("{buf}");

    // Data rows
    for row in &cells {
        buf.clear();
        buf.push('│');
        for c in 0..ncols {
            if c > 0 {
                buf.push('│');
            }
            if is_right[c] {
                let _ = write!(buf, " {:>width$} ", row[c], width = w[c]);
            } else {
                let _ = write!(buf, " {:<width$} ", row[c], width = w[c]);
            }
        }
        buf.push('│');
        println!("{buf}");
    }

    // Footer top border
    buf.clear();
    buf.push('├');
    for c in 0..ncols {
        if c > 0 {
            buf.push('┴');
        }
        for _ in 0..w[c] + 2 {
            buf.push('─');
        }
    }
    buf.push('┤');
    println!("{buf}");

    // Footer content
    let pad = inner_width - footer_left.len() - footer_right.len() - 2;
    buf.clear();
    let _ = write!(buf, "│ {footer_left}{:pad$}{footer_right} │", "");
    println!("{buf}");

    // Bottom border
    buf.clear();
    buf.push('└');
    for _ in 0..inner_width {
        buf.push('─');
    }
    buf.push('┘');
    println!("{buf}");
}

fn print_csv(result: &teide_sql::SqlResult) {
    let table = &result.table;
    let col_indices = resolve_col_indices(result);

    let headers: Vec<String> = col_indices
        .iter()
        .enumerate()
        .map(|(pos, &_idx)| {
            if pos < result.columns.len() {
                result.columns[pos].clone()
            } else {
                table.col_name_str(_idx).to_string()
            }
        })
        .collect();
    println!("{}", headers.join(","));

    for row in 0..table.nrows() as usize {
        let cells: Vec<String> = col_indices
            .iter()
            .map(|&col| format_cell(table, col, row))
            .collect();
        println!("{}", cells.join(","));
    }
}

fn print_json(result: &teide_sql::SqlResult) {
    let table = &result.table;
    let nrows = table.nrows() as usize;
    let col_indices = resolve_col_indices(result);

    let headers: Vec<String> = col_indices
        .iter()
        .enumerate()
        .map(|(pos, &_idx)| {
            if pos < result.columns.len() {
                result.columns[pos].clone()
            } else {
                table.col_name_str(_idx).to_string()
            }
        })
        .collect();

    println!("[");
    for row in 0..nrows {
        let pairs: Vec<String> = col_indices
            .iter()
            .enumerate()
            .map(|(i, &col)| {
                format!(
                    "\"{}\": {}",
                    headers[i],
                    format_json_value(table, col, row)
                )
            })
            .collect();
        let comma = if row + 1 < nrows { "," } else { "" };
        println!("  {{{}}}{comma}", pairs.join(", "));
    }
    println!("]");
}

/// Format a cell value for table/CSV display.
fn format_cell(table: &teide::Table, col: usize, row: usize) -> String {
    let typ = table.col_type(col);
    match typ {
        4 | 5 | 6 => match table.get_i64(col, row) {
            Some(v) => format!("{v}"),
            None => "NULL".to_string(),
        },
        7 => match table.get_f64(col, row) {
            Some(v) => {
                let s = format!("{v:.6}");
                let s = s.trim_end_matches('0');
                if s.ends_with('.') {
                    format!("{s}0")
                } else {
                    s.to_string()
                }
            }
            None => "NULL".to_string(),
        },
        1 => match table.get_i64(col, row) {
            Some(v) => {
                if v != 0 {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            None => "NULL".to_string(),
        },
        14 | 15 => match table.get_str(col, row) {
            Some(s) => s.to_string(),
            None => "NULL".to_string(),
        },
        _ => "<unsupported>".to_string(),
    }
}

/// Format a cell value for JSON output.
fn format_json_value(table: &teide::Table, col: usize, row: usize) -> String {
    let typ = table.col_type(col);
    match typ {
        4 | 5 | 6 => match table.get_i64(col, row) {
            Some(v) => format!("{v}"),
            None => "null".to_string(),
        },
        7 => match table.get_f64(col, row) {
            Some(v) => format!("{v}"),
            None => "null".to_string(),
        },
        1 => match table.get_i64(col, row) {
            Some(v) => {
                if v != 0 {
                    "true".to_string()
                } else {
                    "false".to_string()
                }
            }
            None => "null".to_string(),
        },
        14 | 15 => match table.get_str(col, row) {
            Some(s) => format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\"")),
            None => "null".to_string(),
        },
        _ => "null".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Dot commands
// ---------------------------------------------------------------------------

fn handle_dot_command(
    cmd: &str,
    format: &mut OutputFormat,
    timer: &mut bool,
    session: &teide_sql::Session,
) {
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    match parts[0] {
        ".mode" => {
            if parts.len() < 2 {
                println!("Usage: .mode table|csv|json");
                return;
            }
            match parts[1] {
                "table" => {
                    *format = OutputFormat::Table;
                    println!("Output mode: table");
                }
                "csv" => {
                    *format = OutputFormat::Csv;
                    println!("Output mode: csv");
                }
                "json" => {
                    *format = OutputFormat::Json;
                    println!("Output mode: json");
                }
                _ => println!("Unknown mode. Use: table, csv, json"),
            }
        }
        ".timer" => {
            if parts.len() < 2 {
                println!("Usage: .timer on|off");
                return;
            }
            match parts[1] {
                "on" => {
                    *timer = true;
                    println!("Timer: on");
                }
                "off" => {
                    *timer = false;
                    println!("Timer: off");
                }
                _ => println!("Usage: .timer on|off"),
            }
        }
        ".tables" => {
            let names = session.table_names();
            if names.is_empty() {
                println!("No stored tables.");
            } else {
                let mut sorted = names;
                sorted.sort();
                for name in sorted {
                    if let Some((nrows, ncols)) = session.table_info(name) {
                        println!("  {name:20} {nrows} rows, {ncols} cols");
                    }
                }
            }
        }
        ".help" => print_help(),
        ".quit" | ".exit" => std::process::exit(0),
        _ => println!("Unknown command: {}. Type .help for commands.", parts[0]),
    }
}

// ---------------------------------------------------------------------------
// Banner & help
// ---------------------------------------------------------------------------

fn print_banner() {
    println!("Teide v{} -- Fast SQL engine", env!("CARGO_PKG_VERSION"));
    println!("Type .help for commands, or enter SQL terminated with ;");
    println!();
}

fn print_help() {
    println!("Commands:");
    println!("  .mode table|csv|json  Set output format");
    println!("  .tables               List stored tables");
    println!("  .timer on|off         Show query execution time");
    println!("  .help                 Show this help");
    println!("  .quit                 Exit");
    println!();
    println!("SQL:");
    println!("  SELECT id1, SUM(v1) FROM 'data.csv' GROUP BY id1;");
    println!("  CREATE TABLE t AS SELECT * FROM 'data.csv' WHERE id1 = 'id016';");
    println!("  SELECT * FROM t GROUP BY id1;");
    println!("  DROP TABLE t;");
}

fn dirs_or_home() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}
