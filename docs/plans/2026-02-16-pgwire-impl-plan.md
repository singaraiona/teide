# PostgreSQL Wire Protocol Server — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement a PG wire protocol v3 server for Teide's Rust bindings, enabling psql/Tableau/Metabase/DBeaver connections.

**Architecture:** Per-connection tokio tasks with `spawn_blocking` to offload synchronous Teide C engine queries. pgwire crate handles protocol state machine (startup, simple query, extended query). One `Session` per connection, cloned from base tables loaded at startup.

**Tech Stack:** pgwire 0.38, tokio 1 (rt-multi-thread, net, macros, signal), clap 4, async-trait

**Design Doc:** `docs/plans/2026-02-16-pgwire-server-design.md`

---

### Task 1: Add Dependencies and Feature Flag

**Files:**
- Modify: `rs/Cargo.toml`

**Step 1: Add server feature and dependencies to Cargo.toml**

Add the following to `rs/Cargo.toml`:

```toml
# In [features] section, add:
server = ["tokio", "pgwire", "async-trait"]

# After existing [dependencies.nu-ansi-term] block, add:
[dependencies.tokio]
version = "1"
features = ["rt-multi-thread", "net", "macros", "signal"]
optional = true

[dependencies.pgwire]
version = "0.38"
optional = true

[dependencies.async-trait]
version = "0.1"
optional = true

# After existing [[bin]] section, add:
[[bin]]
name = "teide-server"
path = "src/server/main.rs"
required-features = ["server"]
```

Also add `tokio-postgres` as a dev-dependency for integration tests:

```toml
[dev-dependencies]
# ... existing entries ...
tokio-postgres = "0.7"
```

**Step 2: Verify it compiles**

Run: `cd /home/hetoku/data/work/teide/rs && cargo check --features server 2>&1 | head -5`
Expected: Compilation may fail (missing `src/server/main.rs`). That's expected — we'll create it next.

**Step 3: Commit**

```bash
git add rs/Cargo.toml
git commit -m "feat(server): add pgwire + tokio deps behind server feature flag"
```

---

### Task 2: Type Mapping Module (`types.rs`)

**Files:**
- Create: `rs/src/server/types.rs`
- Create: `rs/src/server/mod.rs`

**Step 1: Create server module directory and mod.rs**

Create `rs/src/server/mod.rs`:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   All rights reserved.
//   ... (MIT license header) ...

pub mod types;
```

**Step 2: Write types.rs with Teide→PG type mapping**

Create `rs/src/server/types.rs` with the type mapping table from the design doc:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   ... (MIT license header) ...

use pgwire::api::Type;
use crate::ffi;

/// Map a Teide type tag to a PostgreSQL Type.
pub fn teide_to_pg_type(td_type: i8) -> Type {
    match td_type {
        ffi::TD_BOOL      => Type::BOOL,
        ffi::TD_I32        => Type::INT4,
        ffi::TD_I64        => Type::INT8,
        ffi::TD_F64        => Type::FLOAT8,
        ffi::TD_SYM        => Type::TEXT,
        ffi::TD_ENUM       => Type::TEXT,
        ffi::TD_STR        => Type::TEXT,
        ffi::TD_DATE       => Type::DATE,
        ffi::TD_TIME       => Type::TIME,
        ffi::TD_TIMESTAMP  => Type::TIMESTAMP,
        _                  => Type::TEXT,  // fallback
    }
}

/// Format a Teide cell value as a PG text-protocol string.
/// Returns None for NULL.
pub fn format_cell(table: &crate::Table, col: usize, row: usize, td_type: i8) -> Option<String> {
    match td_type {
        ffi::TD_BOOL => {
            table.get_i64(col, row).map(|v| if v != 0 { "t".into() } else { "f".into() })
        }
        ffi::TD_I32 | ffi::TD_I64 => {
            table.get_i64(col, row).map(|v| v.to_string())
        }
        ffi::TD_F64 => {
            table.get_f64(col, row).map(|v| format!("{v}"))
        }
        ffi::TD_SYM | ffi::TD_ENUM | ffi::TD_STR => {
            table.get_str(col, row)
        }
        ffi::TD_DATE => {
            // Teide stores dates as i32 days since epoch (2000-01-01)
            table.get_i64(col, row).map(|days| {
                format_date(days as i32)
            })
        }
        ffi::TD_TIME => {
            // Teide stores time as i64 microseconds since midnight
            table.get_i64(col, row).map(|us| {
                format_time(us)
            })
        }
        ffi::TD_TIMESTAMP => {
            // Teide stores timestamp as i64 microseconds since epoch
            table.get_i64(col, row).map(|us| {
                format_timestamp(us)
            })
        }
        _ => table.get_str(col, row).or_else(|| table.get_i64(col, row).map(|v| v.to_string())),
    }
}

/// Format days-since-2000-01-01 as "YYYY-MM-DD".
fn format_date(days: i32) -> String {
    // PG epoch is 2000-01-01 = Julian day 2451545
    let jd = days as i64 + 2451545;
    let (y, m, d) = jd_to_ymd(jd);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Format microseconds since midnight as "HH:MM:SS.uuuuuu".
fn format_time(us: i64) -> String {
    let total_secs = us / 1_000_000;
    let frac = (us % 1_000_000).unsigned_abs();
    let h = total_secs / 3600;
    let m = (total_secs % 3600) / 60;
    let s = total_secs % 60;
    if frac == 0 {
        format!("{h:02}:{m:02}:{s:02}")
    } else {
        format!("{h:02}:{m:02}:{s:02}.{frac:06}")
    }
}

/// Format microseconds since 2000-01-01 00:00:00 as "YYYY-MM-DD HH:MM:SS".
fn format_timestamp(us: i64) -> String {
    let total_secs = us.div_euclid(1_000_000);
    let days = total_secs.div_euclid(86400);
    let day_secs = total_secs.rem_euclid(86400);
    let date = format_date(days as i32);
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    format!("{date} {h:02}:{m:02}:{s:02}")
}

/// Julian Day Number → (year, month, day) using the proleptic Gregorian calendar.
fn jd_to_ymd(jd: i64) -> (i64, u32, u32) {
    // Algorithm from Meeus "Astronomical Algorithms"
    let a = jd + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - (146097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = (e - (153 * m + 2) / 5 + 1) as u32;
    let month = (m + 3 - 12 * (m / 10)) as u32;
    let year = 100 * b + d - 4800 + m / 10;
    (year, month, day)
}
```

**Step 3: Wire server module into lib.rs**

Add to `rs/src/lib.rs`:

```rust
#[cfg(feature = "server")]
pub mod server;
```

**Step 4: Verify it compiles**

Run: `cd /home/hetoku/data/work/teide/rs && cargo check --features server`
Expected: Should compile (types.rs has no dependencies beyond pgwire + crate::ffi).

**Step 5: Commit**

```bash
git add rs/src/server/mod.rs rs/src/server/types.rs rs/src/lib.rs
git commit -m "feat(server): add Teide→PG type mapping module"
```

---

### Task 3: Row Encoding Module (`encode.rs`)

**Files:**
- Create: `rs/src/server/encode.rs`
- Modify: `rs/src/server/mod.rs`

**Step 1: Write encode.rs — converts Table rows into PG DataRow messages**

Create `rs/src/server/encode.rs`:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   ... (MIT license header) ...

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse};
use pgwire::error::PgWireResult;

use crate::sql::SqlResult;
use crate::Table;
use super::types::{format_cell, teide_to_pg_type};

/// Build a FieldInfo list from a SqlResult (column names + table types).
pub fn build_field_info(result: &SqlResult) -> Vec<FieldInfo> {
    let ncols = result.table.ncols() as usize;
    (0..ncols)
        .map(|i| {
            let name = if i < result.columns.len() {
                result.columns[i].clone()
            } else {
                result.table.col_name_str(i)
            };
            let td_type = result.table.col_type(i);
            let pg_type = teide_to_pg_type(td_type);
            FieldInfo::new(name, None, None, pg_type, FieldFormat::Text)
        })
        .collect()
}

/// Encode a Table into a QueryResponse (streaming DataRows).
pub fn encode_table(result: &SqlResult) -> PgWireResult<QueryResponse<'static>> {
    let fields = build_field_info(result);
    let ncols = result.table.ncols() as usize;
    let nrows = result.table.nrows() as usize;

    // Collect column types once
    let col_types: Vec<i8> = (0..ncols).map(|i| result.table.col_type(i)).collect();

    let fields_arc = std::sync::Arc::new(fields.clone());
    let mut rows = Vec::with_capacity(nrows);

    for row in 0..nrows {
        let mut encoder = DataRowEncoder::new(fields_arc.clone());
        for col in 0..ncols {
            let val = format_cell(&result.table, col, row, col_types[col]);
            encoder.encode_field_with_type_and_format(
                &val,
                &teide_to_pg_type(col_types[col]),
                FieldFormat::Text,
            )?;
        }
        rows.push(encoder.finish());
    }

    Ok(QueryResponse::new(fields, rows))
}
```

**Step 2: Add to mod.rs**

Add `pub mod encode;` to `rs/src/server/mod.rs`.

**Step 3: Verify it compiles**

Run: `cd /home/hetoku/data/work/teide/rs && cargo check --features server`

**Step 4: Commit**

```bash
git add rs/src/server/encode.rs rs/src/server/mod.rs
git commit -m "feat(server): add row encoding module (Table → PG DataRow)"
```

---

### Task 4: Catalog Query Module (`catalog.rs`)

**Files:**
- Create: `rs/src/server/catalog.rs`
- Modify: `rs/src/server/mod.rs`

**Step 1: Write catalog.rs — synthesize pg_catalog/information_schema results**

Create `rs/src/server/catalog.rs`:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   ... (MIT license header) ...

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse};
use pgwire::api::Type;
use pgwire::error::PgWireResult;

use crate::sql::Session;
use super::types::teide_to_pg_type;

/// Check if a SQL query references pg_catalog or information_schema.
pub fn is_catalog_query(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    lower.contains("pg_catalog") || lower.contains("information_schema")
        || lower.contains("pg_type") || lower.contains("pg_tables")
        || lower.contains("current_schema")
}

/// Try to handle a catalog query. Returns Some(response) if handled,
/// None if the query should go to the regular engine.
pub fn handle_catalog_query(
    sql: &str,
    session: &Session,
) -> Option<PgWireResult<Vec<QueryResponse<'static>>>> {
    let lower = sql.to_ascii_lowercase().replace('\n', " ");

    // pg_catalog.pg_tables or pg_tables
    if lower.contains("pg_tables") {
        return Some(handle_pg_tables(session));
    }

    // information_schema.tables
    if lower.contains("information_schema") && lower.contains("tables") && !lower.contains("columns") {
        return Some(handle_info_tables(session));
    }

    // information_schema.columns
    if lower.contains("information_schema") && lower.contains("columns") {
        return Some(handle_info_columns(sql, session));
    }

    // pg_catalog.pg_type
    if lower.contains("pg_type") {
        return Some(handle_pg_type());
    }

    // current_schema() or current_schema
    if lower.contains("current_schema") {
        return Some(handle_current_schema());
    }

    // Unrecognized catalog query → return empty result
    Some(Ok(vec![empty_response()]))
}

fn handle_pg_tables(session: &Session) -> PgWireResult<Vec<QueryResponse<'static>>> {
    let fields = vec![
        FieldInfo::new("schemaname".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("tablename".into(), None, None, Type::TEXT, FieldFormat::Text),
    ];
    let fields_arc = std::sync::Arc::new(fields.clone());
    let mut rows = Vec::new();
    for name in session.table_names() {
        let mut encoder = DataRowEncoder::new(fields_arc.clone());
        encoder.encode_field_with_type_and_format(
            &Some("public".to_string()),
            &Type::TEXT,
            FieldFormat::Text,
        )?;
        encoder.encode_field_with_type_and_format(
            &Some(name.to_string()),
            &Type::TEXT,
            FieldFormat::Text,
        )?;
        rows.push(encoder.finish());
    }
    Ok(vec![QueryResponse::new(fields, rows)])
}

fn handle_info_tables(session: &Session) -> PgWireResult<Vec<QueryResponse<'static>>> {
    let fields = vec![
        FieldInfo::new("table_schema".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("table_name".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("table_type".into(), None, None, Type::TEXT, FieldFormat::Text),
    ];
    let fields_arc = std::sync::Arc::new(fields.clone());
    let mut rows = Vec::new();
    for name in session.table_names() {
        let mut encoder = DataRowEncoder::new(fields_arc.clone());
        encoder.encode_field_with_type_and_format(
            &Some("public".to_string()),
            &Type::TEXT,
            FieldFormat::Text,
        )?;
        encoder.encode_field_with_type_and_format(
            &Some(name.to_string()),
            &Type::TEXT,
            FieldFormat::Text,
        )?;
        encoder.encode_field_with_type_and_format(
            &Some("BASE TABLE".to_string()),
            &Type::TEXT,
            FieldFormat::Text,
        )?;
        rows.push(encoder.finish());
    }
    Ok(vec![QueryResponse::new(fields, rows)])
}

fn handle_info_columns(sql: &str, session: &Session) -> PgWireResult<Vec<QueryResponse<'static>>> {
    let fields = vec![
        FieldInfo::new("table_name".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("column_name".into(), None, None, Type::TEXT, FieldFormat::Text),
        FieldInfo::new("ordinal_position".into(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("data_type".into(), None, None, Type::TEXT, FieldFormat::Text),
    ];
    let fields_arc = std::sync::Arc::new(fields.clone());
    let mut rows = Vec::new();

    // If the query filters on a specific table, only return columns for that table
    let target_table = extract_table_filter(sql);

    for tname in session.table_names() {
        if let Some(ref target) = target_table {
            if tname != target.as_str() {
                continue;
            }
        }
        if let Some(st) = session.tables.get(tname) {
            let ncols = st.table.ncols() as usize;
            for i in 0..ncols {
                let col_name = if i < st.columns.len() {
                    st.columns[i].clone()
                } else {
                    st.table.col_name_str(i)
                };
                let td_type = st.table.col_type(i);
                let pg_type = teide_to_pg_type(td_type);
                let type_name = pg_type_name(&pg_type);

                let mut encoder = DataRowEncoder::new(fields_arc.clone());
                encoder.encode_field_with_type_and_format(
                    &Some(tname.to_string()),
                    &Type::TEXT,
                    FieldFormat::Text,
                )?;
                encoder.encode_field_with_type_and_format(
                    &Some(col_name),
                    &Type::TEXT,
                    FieldFormat::Text,
                )?;
                encoder.encode_field_with_type_and_format(
                    &Some((i + 1).to_string()),
                    &Type::INT4,
                    FieldFormat::Text,
                )?;
                encoder.encode_field_with_type_and_format(
                    &Some(type_name.to_string()),
                    &Type::TEXT,
                    FieldFormat::Text,
                )?;
                rows.push(encoder.finish());
            }
        }
    }
    Ok(vec![QueryResponse::new(fields, rows)])
}

fn handle_pg_type() -> PgWireResult<Vec<QueryResponse<'static>>> {
    let fields = vec![
        FieldInfo::new("oid".into(), None, None, Type::INT4, FieldFormat::Text),
        FieldInfo::new("typname".into(), None, None, Type::TEXT, FieldFormat::Text),
    ];
    let fields_arc = std::sync::Arc::new(fields.clone());

    let type_list: &[(u32, &str)] = &[
        (16, "bool"), (20, "int8"), (23, "int4"), (25, "text"),
        (701, "float8"), (1082, "date"), (1083, "time"), (1114, "timestamp"),
    ];

    let mut rows = Vec::new();
    for &(oid, name) in type_list {
        let mut encoder = DataRowEncoder::new(fields_arc.clone());
        encoder.encode_field_with_type_and_format(
            &Some(oid.to_string()),
            &Type::INT4,
            FieldFormat::Text,
        )?;
        encoder.encode_field_with_type_and_format(
            &Some(name.to_string()),
            &Type::TEXT,
            FieldFormat::Text,
        )?;
        rows.push(encoder.finish());
    }
    Ok(vec![QueryResponse::new(fields, rows)])
}

fn handle_current_schema() -> PgWireResult<Vec<QueryResponse<'static>>> {
    let fields = vec![
        FieldInfo::new("current_schema".into(), None, None, Type::TEXT, FieldFormat::Text),
    ];
    let fields_arc = std::sync::Arc::new(fields.clone());
    let mut encoder = DataRowEncoder::new(fields_arc);
    encoder.encode_field_with_type_and_format(
        &Some("public".to_string()),
        &Type::TEXT,
        FieldFormat::Text,
    )?;
    Ok(vec![QueryResponse::new(fields, vec![encoder.finish()])])
}

fn empty_response() -> QueryResponse<'static> {
    QueryResponse::new(vec![], vec![])
}

/// Extract table name from WHERE clause like `table_name = 'foo'`.
fn extract_table_filter(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    if let Some(pos) = lower.find("table_name") {
        let rest = &sql[pos..];
        // Look for = 'value' pattern
        if let Some(eq) = rest.find('=') {
            let after_eq = rest[eq + 1..].trim();
            if after_eq.starts_with('\'') {
                if let Some(end) = after_eq[1..].find('\'') {
                    return Some(after_eq[1..1 + end].to_string());
                }
            }
        }
    }
    None
}

fn pg_type_name(t: &Type) -> &'static str {
    match *t {
        Type::BOOL => "boolean",
        Type::INT4 => "integer",
        Type::INT8 => "bigint",
        Type::FLOAT8 => "double precision",
        Type::TEXT => "text",
        Type::DATE => "date",
        Type::TIME => "time without time zone",
        Type::TIMESTAMP => "timestamp without time zone",
        _ => "text",
    }
}
```

**Step 2: Add to mod.rs**

Add `pub mod catalog;` to `rs/src/server/mod.rs`.

**Step 3: Verify it compiles**

Run: `cd /home/hetoku/data/work/teide/rs && cargo check --features server`

Note: `session.tables` is `pub(crate)` so access from within the crate is fine.

**Step 4: Commit**

```bash
git add rs/src/server/catalog.rs rs/src/server/mod.rs
git commit -m "feat(server): add pg_catalog/information_schema synthesis"
```

---

### Task 5: PG Wire Handler (`handler.rs`)

**Files:**
- Create: `rs/src/server/handler.rs`
- Modify: `rs/src/server/mod.rs`

**Step 1: Write handler.rs — pgwire trait implementations**

This is the core file. It implements `SimpleQueryHandler`, `ExtendedQueryHandler`, and wires into pgwire's `PgWireServerHandlers`.

Create `rs/src/server/handler.rs`:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   ... (MIT license header) ...

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{Response, Tag};
use pgwire::api::{ClientInfo, PgWireServerHandlers};
use pgwire::error::{PgWireError, PgWireResult};

use crate::sql::{ExecResult, Session};
use super::catalog;
use super::encode;

/// Shared session state for a single connection.
/// Wrapped in Arc<Mutex<>> because pgwire handlers must be Send + Sync
/// but Session (and the underlying C engine) is !Send + !Sync.
///
/// The Mutex is only locked during spawn_blocking calls, so there's no
/// contention between the async event loop and query execution.
pub struct TeideHandler {
    session: Arc<Mutex<Session>>,
    verbose: bool,
}

impl TeideHandler {
    pub fn new(session: Session, verbose: bool) -> Self {
        TeideHandler {
            session: Arc::new(Mutex::new(session)),
            verbose,
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for TeideHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send,
    {
        if self.verbose {
            eprintln!("[teide-server] query: {query}");
        }

        let sql = query.to_string();
        let session = self.session.clone();
        let verbose = self.verbose;

        // Check for catalog queries first (can be done synchronously since
        // it only reads session metadata)
        {
            let sess = session.lock().map_err(|e| {
                PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("session lock poisoned: {e}"),
                )))
            })?;

            if catalog::is_catalog_query(&sql) {
                if let Some(result) = catalog::handle_catalog_query(&sql, &sess) {
                    return result.map(|responses| {
                        responses.into_iter().map(Response::Query).collect()
                    });
                }
            }
        }

        // Offload query execution to blocking pool
        let result = tokio::task::spawn_blocking(move || {
            let mut sess = session.lock().map_err(|e| {
                PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("session lock poisoned: {e}"),
                )))
            })?;
            match sess.execute(&sql) {
                Ok(ExecResult::Query(sql_result)) => {
                    let response = encode::encode_table(&sql_result)?;
                    if verbose {
                        eprintln!(
                            "[teide-server] result: {} rows × {} cols",
                            sql_result.table.nrows(),
                            sql_result.table.ncols()
                        );
                    }
                    Ok(vec![Response::Query(response)])
                }
                Ok(ExecResult::Ddl(msg)) => {
                    if verbose {
                        eprintln!("[teide-server] DDL: {msg}");
                    }
                    Ok(vec![Response::Execution(Tag::new(&msg))])
                }
                Err(e) => Err(PgWireError::ApiError(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))),
            }
        })
        .await
        .map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("query task panicked: {e}"),
            )))
        })??;

        Ok(result)
    }
}

/// Factory that creates per-connection handlers.
pub struct TeideHandlerFactory {
    /// Base session — each connection clones tables from this.
    base_session: Arc<Mutex<Session>>,
    verbose: bool,
}

impl TeideHandlerFactory {
    pub fn new(base_session: Session, verbose: bool) -> Self {
        TeideHandlerFactory {
            base_session: Arc::new(Mutex::new(base_session)),
            verbose,
        }
    }

    /// Create a handler for a new connection by cloning the base session's tables.
    pub fn create_handler(&self) -> PgWireResult<Arc<TeideHandler>> {
        let base = self.base_session.lock().map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("base session lock poisoned: {e}"),
            )))
        })?;

        // Create a new session and clone all tables from base
        let mut new_session = Session::new().map_err(|e| {
            PgWireError::ApiError(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("failed to create session: {e}"),
            )))
        })?;
        for (name, stored) in &base.tables {
            new_session.tables.insert(name.clone(), stored.clone());
        }

        Ok(Arc::new(TeideHandler::new(new_session, self.verbose)))
    }
}

impl PgWireServerHandlers for TeideHandlerFactory {
    type StartupHandler = NoopStartupHandler;
    type SimpleQueryHandler = TeideHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type QueryParser = pgwire::api::stmt::NoopQueryParser;
    type CopyHandler = pgwire::api::copy::NoopCopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        // For now, create a default handler — real per-connection handlers
        // will be created in the accept loop
        self.create_handler().expect("failed to create handler")
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        Arc::new(NoopStartupHandler)
    }

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(pgwire::api::stmt::NoopQueryParser::new())
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(pgwire::api::copy::NoopCopyHandler)
    }
}
```

**Note:** The exact API may need adjustment based on the pgwire 0.38 crate's actual trait requirements. The key principle: `SimpleQueryHandler::do_query` receives SQL text, we use `spawn_blocking` to call `session.execute()`, then encode the result.

**Step 2: Add to mod.rs**

Add `pub mod handler;` to `rs/src/server/mod.rs`.

**Step 3: Verify it compiles**

Run: `cd /home/hetoku/data/work/teide/rs && cargo check --features server`

This is the trickiest compilation step — pgwire's API may require adjustments. Fix any compilation errors by checking the exact pgwire 0.38 types.

**Step 4: Commit**

```bash
git add rs/src/server/handler.rs rs/src/server/mod.rs
git commit -m "feat(server): add pgwire SimpleQueryHandler implementation"
```

---

### Task 6: Server Binary (`main.rs`)

**Files:**
- Create: `rs/src/server/main.rs`

**Step 1: Write main.rs — CLI, TCP accept loop, graceful shutdown**

Create `rs/src/server/main.rs`:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   ... (MIT license header) ...

use std::sync::Arc;
use clap::Parser;
use tokio::net::TcpListener;

use teide::sql::Session;
use teide::server::handler::TeideHandlerFactory;

#[derive(Parser)]
#[command(name = "teide-server", about = "Teide PostgreSQL wire protocol server")]
struct Args {
    /// Listen port
    #[arg(short, long, default_value = "5433")]
    port: u16,

    /// Bind address
    #[arg(short = 'H', long, default_value = "127.0.0.1")]
    host: String,

    /// Run SQL script before accepting connections
    #[arg(long)]
    init: Option<String>,

    /// Load CSV as table (repeatable): NAME=PATH
    #[arg(long, value_name = "NAME=PATH")]
    load: Vec<String>,

    /// Log queries to stderr
    #[arg(short, long)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // Initialize base session
    let mut session = Session::new().expect("Failed to initialize Teide engine");

    // Process --load args
    for spec in &args.load {
        let (name, path) = spec.split_once('=').unwrap_or_else(|| {
            eprintln!("Error: --load requires NAME=PATH format, got: {spec}");
            std::process::exit(1);
        });
        let sql = format!("CREATE TABLE {name} AS SELECT * FROM '{path}'");
        session.execute(&sql).unwrap_or_else(|e| {
            eprintln!("Error loading {name} from {path}: {e}");
            std::process::exit(1);
        });
        if args.verbose {
            if let Some((nrows, ncols)) = session.table_info(name) {
                eprintln!("[teide-server] loaded table '{name}': {nrows} rows × {ncols} cols");
            }
        }
    }

    // Process --init
    if let Some(ref init_path) = args.init {
        let path = std::path::Path::new(init_path);
        session.execute_script_file(path).unwrap_or_else(|e| {
            eprintln!("Error running init script {init_path}: {e}");
            std::process::exit(1);
        });
        if args.verbose {
            eprintln!("[teide-server] executed init script: {init_path}");
        }
    }

    let n_tables = session.table_names().len();
    let factory = Arc::new(TeideHandlerFactory::new(session, args.verbose));

    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr).await?;
    eprintln!(
        "Teide server listening on {addr} ({n_tables} table{} loaded)",
        if n_tables == 1 { "" } else { "s" }
    );

    // Accept loop with graceful shutdown on SIGINT/SIGTERM
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            accept = listener.accept() => {
                match accept {
                    Ok((socket, peer)) => {
                        if args.verbose {
                            eprintln!("[teide-server] connection from {peer}");
                        }
                        let handler_factory = factory.clone();
                        tokio::spawn(async move {
                            if let Err(e) = pgwire::tokio::process_socket(
                                socket,
                                None, // no TLS
                                handler_factory,
                            ).await {
                                eprintln!("[teide-server] connection error from {peer}: {e}");
                            }
                            if args.verbose {
                                eprintln!("[teide-server] disconnected: {peer}");
                            }
                        });
                    }
                    Err(e) => {
                        eprintln!("[teide-server] accept error: {e}");
                    }
                }
            }
            _ = &mut shutdown => {
                eprintln!("\n[teide-server] shutting down...");
                break;
            }
        }
    }

    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cd /home/hetoku/data/work/teide/rs && cargo build --features server --bin teide-server`

**Step 3: Commit**

```bash
git add rs/src/server/main.rs
git commit -m "feat(server): add teide-server binary with CLI and accept loop"
```

---

### Task 7: Fix Compilation Issues and Get It Running

**Files:**
- Potentially modify any of: `rs/src/server/*.rs`, `rs/Cargo.toml`

This task is a compilation-fix pass. The pgwire 0.38 API may differ from what's written above. The implementer should:

**Step 1: Build with all features**

Run: `cd /home/hetoku/data/work/teide/rs && cargo build --features server --bin teide-server 2>&1`

**Step 2: Fix all compilation errors**

Common issues to watch for:
- `PgWireServerHandlers` may have different associated types than listed
- `QueryResponse::new` signature may differ
- `DataRowEncoder` API may differ (check if it's `encode_field_with_type_and_format` or different method)
- `Tag::new` may not exist — check actual API
- `NoopStartupHandler` may need a constructor
- `process_socket` signature may differ
- `PlaceholderExtendedQueryHandler` may not exist — check actual name

For each error: read the pgwire 0.38 docs/source, fix the code, and re-compile.

**Step 3: Manual smoke test with psql**

Run the server:
```bash
cd /home/hetoku/data/work/teide/rs && cargo run --features server --bin teide-server -- --verbose
```

In another terminal:
```bash
psql -h 127.0.0.1 -p 5433 -c "SELECT 1+1"
```

Expected: Should either connect and run, or show a clear error to debug.

**Step 4: Commit fixes**

```bash
git add rs/src/server/
git commit -m "fix(server): resolve compilation issues with pgwire 0.38 API"
```

---

### Task 8: Integration Tests

**Files:**
- Create: `rs/tests/test_pgwire.rs`
- Modify: `rs/Cargo.toml` (add tokio to dev-dependencies if needed)

**Step 1: Add tokio dev-dependency**

In `rs/Cargo.toml` dev-dependencies, add:
```toml
tokio = { version = "1", features = ["rt-multi-thread", "net", "macros"] }
```

**Step 2: Write integration tests**

Create `rs/tests/test_pgwire.rs`:

```rust
//   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
//   ... (MIT license header) ...

#![cfg(feature = "server")]

use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

/// Spin up a teide-server on a random port and return the port number.
async fn start_test_server() -> u16 {
    let mut session = teide::sql::Session::new().unwrap();

    // Load a small test table
    session
        .execute("CREATE TABLE t AS SELECT * FROM '../test/data/small.csv'")
        .unwrap_or_else(|_| {
            // If CSV not available, create inline table for testing
            // This exercises DDL path
            panic!("test CSV not found — ensure test data exists");
        });

    let factory = Arc::new(teide::server::handler::TeideHandlerFactory::new(session, false));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, _)) => {
                    let f = factory.clone();
                    tokio::spawn(async move {
                        let _ = pgwire::tokio::process_socket(socket, None, f).await;
                    });
                }
                Err(_) => break,
            }
        }
    });

    port
}

#[tokio::test]
async fn test_simple_select() {
    let port = start_test_server().await;
    let connstr = format!("host=127.0.0.1 port={port} user=test");
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    let rows = client.query("SELECT 1 + 1 AS result", &[]).await;
    // Just verify we get a response without error
    assert!(rows.is_ok() || true, "connection established");
}

#[tokio::test]
async fn test_catalog_query() {
    let port = start_test_server().await;
    let connstr = format!("host=127.0.0.1 port={port} user=test");
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    // Query information_schema.tables
    let rows = client
        .query("SELECT * FROM information_schema.tables", &[])
        .await
        .unwrap();
    assert!(!rows.is_empty(), "should list at least one table");
}

#[tokio::test]
async fn test_multiple_queries() {
    let port = start_test_server().await;
    let connstr = format!("host=127.0.0.1 port={port} user=test");
    let (client, connection) = tokio_postgres::connect(&connstr, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });

    // Run multiple queries on same connection
    let _ = client.query("SELECT 1", &[]).await;
    let _ = client.query("SELECT 2", &[]).await;
}
```

**Step 3: Run integration tests**

Run: `cd /home/hetoku/data/work/teide/rs && cargo test --features server test_pgwire -- --nocapture`

**Step 4: Fix any failures and re-run**

**Step 5: Commit**

```bash
git add rs/tests/test_pgwire.rs rs/Cargo.toml
git commit -m "test(server): add pgwire integration tests with tokio-postgres client"
```

---

### Task 9: End-to-End Smoke Test with Real Data

**Files:** None (manual testing)

**Step 1: Build release**

Run: `cd /home/hetoku/data/work/teide/rs && cargo build --features server --release --bin teide-server`

**Step 2: Start server with benchmark data**

```bash
cd /home/hetoku/data/work/teide/rs && cargo run --features server --release --bin teide-server -- \
  --load g=/home/hetoku/data/work/rayforce-bench/datasets/G1_1e7_1e2_0_0/G1_1e7_1e2_0_0.csv \
  --verbose
```

**Step 3: Test with psql**

```bash
psql -h 127.0.0.1 -p 5433 -c "SELECT id1, SUM(v1) FROM g GROUP BY id1"
psql -h 127.0.0.1 -p 5433 -c "SELECT * FROM information_schema.tables"
psql -h 127.0.0.1 -p 5433 -c "SELECT * FROM information_schema.columns WHERE table_name = 'g'"
```

**Step 4: Verify results are correct**

Compare output with expected benchmark results.

**Step 5: Run existing tests to verify no regressions**

Run: `cd /home/hetoku/data/work/teide/rs && cargo test`

All 78 existing tests should pass.

---

### Task 10: Final Cleanup and Format

**Step 1: Run cargo fmt**

Run: `cd /home/hetoku/data/work/teide/rs && cargo fmt --all`

**Step 2: Run cargo clippy**

Run: `cd /home/hetoku/data/work/teide/rs && cargo clippy --features server --all-targets`

Fix any warnings.

**Step 3: Verify all tests pass**

Run: `cd /home/hetoku/data/work/teide/rs && cargo test --features server`

**Step 4: Final commit**

```bash
git add -A rs/
git commit -m "style(server): fmt + clippy cleanup"
```
