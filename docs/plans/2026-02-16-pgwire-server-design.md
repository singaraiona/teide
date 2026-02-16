# PostgreSQL Wire Protocol Server for Teide

**Date:** 2026-02-16
**Status:** Approved

## Summary

Implement a PostgreSQL wire protocol (v3) server for Teide's Rust bindings, enabling
seamless connections from analytics tools like Tableau, Metabase, DBeaver, and psql.
Full interactive mode: clients can run SELECT queries, CREATE/DROP tables, and load
CSVs over the wire.

## Architecture

```
┌─────────────┐     TCP/PG wire v3      ┌──────────────────────┐
│  Tableau /   │◄───────────────────────►│   teide-server       │
│  Metabase /  │                         │                      │
│  psql /      │     per-connection      │  ┌────────────────┐  │
│  DBeaver     │     tokio task          │  │ PgSession      │  │
└─────────────┘                         │  │  - Session      │  │
                                        │  │  - pgwire state │  │
                                        │  └────────┬───────┘  │
                                        │           │          │
                                        │  spawn_blocking()    │
                                        │           │          │
                                        │  ┌────────▼───────┐  │
                                        │  │ Teide C engine  │  │
                                        │  │ (thread pool)   │  │
                                        │  └────────────────┘  │
                                        └──────────────────────┘
```

- **One Session per connection** — each client gets an isolated table registry, matching
  PG semantics. Sessions are cheap (HashMap + Context handle).
- **spawn_blocking for queries** — Teide's C engine is synchronous and CPU-bound with
  its own internal thread pool. Queries are offloaded to tokio's blocking pool so the
  async reactor stays responsive for other connections.
- **pgwire handles protocol** — startup handshake, simple query flow, extended query flow
  (parse/bind/execute), error formatting, type OID mapping.

## Type Mapping (Teide → PostgreSQL)

All values sent as text strings (text protocol). Binary protocol is a future optimization.

| Teide Type    | PG Type   | OID  | Wire Format              |
|---------------|-----------|------|--------------------------|
| TD_BOOL       | BOOL      | 16   | "t" / "f"                |
| TD_I32        | INT4      | 23   | decimal text             |
| TD_I64        | INT8      | 20   | decimal text             |
| TD_F64        | FLOAT8    | 701  | decimal text             |
| TD_SYM        | TEXT      | 25   | UTF-8 string             |
| TD_ENUM       | TEXT      | 25   | UTF-8 string             |
| TD_STR        | TEXT      | 25   | UTF-8 string             |
| TD_DATE       | DATE      | 1082 | "YYYY-MM-DD"             |
| TD_TIME       | TIME      | 1083 | "HH:MM:SS.uuuuuu"       |
| TD_TIMESTAMP  | TIMESTAMP | 1114 | "YYYY-MM-DD HH:MM:SS"   |

NULL values use PG's -1 length marker. Unknown types fall back to TEXT.

## Connection Lifecycle

### Startup

1. Client sends StartupMessage (protocol v3.0, user/database params)
2. Server responds AuthenticationOk (trust mode, no password)
3. Server sends ParameterStatus messages:
   - `server_version` = `"15.0"`
   - `server_encoding` = `"UTF8"`
   - `client_encoding` = `"UTF8"`
   - `DateStyle` = `"ISO, MDY"`
4. Server sends ReadyForQuery with transaction status `I` (idle — no transactions)

### Simple Query Flow

1. Client sends Query message with SQL text
2. Server calls `spawn_blocking(|| session.execute(sql))`
3. On success: RowDescription → DataRow per row → CommandComplete → ReadyForQuery
4. On error: ErrorResponse (severity, code, message) → ReadyForQuery

### Extended Query Flow

1. Parse → Bind → Describe → Execute → Sync
2. pgwire handles the state machine; we implement ExtendedQueryHandler trait
3. No prepared statement caching in v1 — each Parse re-plans the query

### DDL Flow

CREATE TABLE, DROP TABLE return ExecResult::Ddl(msg) — send CommandComplete(msg)
with no row data.

## Catalog Queries (pg_catalog)

Analytics tools query pg_catalog system tables to discover schema. Strategy: intercept
SQL referencing pg_catalog or information_schema, synthesize results from Session
metadata.

### Minimum Viable Catalog

| Virtual Table                  | Key Columns                                           | Source                       |
|--------------------------------|-------------------------------------------------------|------------------------------|
| pg_catalog.pg_tables           | schemaname, tablename                                 | session.tables keys          |
| information_schema.tables      | table_schema, table_name, table_type                  | session.tables keys          |
| information_schema.columns     | table_name, column_name, ordinal_position, data_type  | table column metadata        |
| pg_catalog.pg_type             | oid, typname                                          | static list of mapped types  |

Unrecognized catalog queries return empty result sets rather than errors — tools handle
empty results gracefully.

## File Structure

```
rs/
├── Cargo.toml              # add pgwire, tokio deps + "server" feature
├── src/
│   ├── lib.rs              # unchanged
│   ├── engine.rs           # unchanged
│   ├── sql/                # unchanged
│   ├── cli/                # unchanged
│   └── server/             # NEW
│       ├── main.rs         # tokio::main, arg parsing, listener loop
│       ├── handler.rs      # pgwire trait impls (SimpleQuery, ExtendedQuery, Startup)
│       ├── catalog.rs      # pg_catalog / information_schema synthesis
│       ├── types.rs        # Teide type → PG OID/format mapping
│       └── encode.rs       # Row encoding: Table → DataRow stream
```

### Cargo.toml Additions

```toml
[features]
server = ["tokio", "pgwire"]

[dependencies.tokio]
version = "1"
features = ["rt-multi-thread", "net", "macros", "signal"]
optional = true

[dependencies.pgwire]
version = "0.25"
optional = true

[[bin]]
name = "teide-server"
path = "src/server/main.rs"
required-features = ["server"]
```

### Estimated Size

~800-1000 lines total across 5 files:
- main.rs (~100 lines) — arg parsing, TCP accept loop, graceful shutdown
- handler.rs (~300 lines) — pgwire trait implementations, spawn_blocking dispatch
- catalog.rs (~200 lines) — catalog query interception and synthesis
- types.rs (~100 lines) — type mapping table, OID constants
- encode.rs (~150 lines) — iterate Table rows, encode as PG DataRow messages

## CLI Interface

```
teide-server [OPTIONS]

Options:
  -p, --port <PORT>           Listen port [default: 5433]
  -H, --host <HOST>           Bind address [default: 127.0.0.1]
  --init <SQL_FILE>           Run SQL script before accepting connections
  --load <NAME=PATH>          Load CSV as table (repeatable)
  -w, --workers <N>           Tokio blocking pool size [default: num_cpus]
  -v, --verbose               Log queries to stderr
```

### Startup Sequence

1. Parse args via clap
2. Create Session::new()
3. Process --load args: session.execute("CREATE TABLE {name} AS '{path}'")
4. Process --init: session.execute_script_file(path)
5. Bind TcpListener on host:port
6. Print: "Teide server listening on 127.0.0.1:5433 (3 tables loaded)"
7. Accept loop: tokio::spawn per connection, clone base session's tables into new Session
8. SIGINT/SIGTERM → graceful shutdown (stop accepting, drain active queries)

### Session Cloning

When a client connects, they start with a copy of the tables loaded at startup.
DDL only affects their session. Table cloning is cheap — COW ref counting bumps
refcounts without copying data.

### Example Usage

```bash
teide-server --load sales=data/sales.csv --load orders=data/orders.csv --verbose

# From another terminal:
psql -h localhost -p 5433 -c "SELECT region, SUM(amount) FROM sales GROUP BY region"
```

## Testing Strategy

### Unit Tests

In-module `#[cfg(test)]` tests:
- types.rs — verify every Teide type maps to correct OID and format function
- catalog.rs — verify synthesized results match expected schemas
- encode.rs — verify row encoding (including NULLs, escaping)

### Integration Tests

`rs/tests/test_pgwire.rs` using tokio-postgres as dev-dependency:
- Spin up server on random port (bind 127.0.0.1:0)
- Connect with real PG client
- Test: CREATE TABLE, SELECT, aggregates, DDL, error handling
- Test: catalog discovery (information_schema.tables)
- Test: multiple queries on same connection
- Test: multiple concurrent connections

### Manual Smoke Tests

- psql interactive session
- Document Tableau and Metabase quickstart guides

## Future Work (not in v1)

- Binary protocol encoding for performance
- Prepared statement caching
- Password authentication (--password flag)
- SSL/TLS support
- Query cancellation via PG cancel protocol
- Multi-user with shared base tables + per-user overlays
- COPY protocol for bulk data import
