# Architecture Redesign

## Identity

Teide is a columnar computation engine written in C17 with zero dependencies.
Frontends make it accessible per language. SQL is one of several frontends,
not the primary interface.

## Two Layers

### Layer 1: Engine (C)

`libteide.so` — compute, memory, DAG, thread pool, IO (CSV).
Pure C17, no language-specific code. The API: build a graph of operations,
execute it, get columnar results.

The C engine is unchanged by this redesign. `exec.c` stays monolithic.
IO (csv.c) stays in the engine.

### Layer 2: Frontends

One package per language, each named "teide" in its ecosystem.
Frontends are equal citizens — none is primary.

- **Rust** (`rs/`) → `cargo install teide` / `cargo add teide`
- **Python** (`py/`) → `pip install teide`

## Directory Structure

```
teide/
├── include/teide/td.h       # C public API
├── src/                      # C engine source
│   ├── core/                 #   platform, types, block
│   ├── mem/                  #   buddy, arena, slab, sys
│   ├── vec/                  #   vectors, atoms, lists
│   ├── table/                #   tables, symbols
│   ├── ops/                  #   exec, graph, optimizer, pool
│   ├── store/                #   partitioned storage
│   └── io/                   #   CSV reader
├── test/                     # C tests
│
├── rs/                       # Rust frontend (single crate)
│   ├── Cargo.toml            #   name = "teide"
│   ├── build.rs              #   cmake + static link
│   └── src/
│       ├── lib.rs            #   public API surface
│       ├── ffi.rs            #   raw FFI bindings (was teide-sys)
│       ├── engine.rs         #   safe wrappers: Context, Table, Graph
│       ├── sql/
│       │   ├── mod.rs        #   Session, execute_sql
│       │   ├── planner.rs    #   SQL AST → DAG
│       │   └── expr.rs       #   expression planning
│       └── cli/
│           ├── main.rs       #   REPL entry (behind "cli" feature)
│           ├── completer.rs
│           ├── highlighter.rs
│           ├── prompt.rs
│           ├── theme.rs
│           └── validator.rs
│
├── py/                       # Python frontend (single package)
│   ├── pyproject.toml        #   name = "teide"
│   └── teide/
│       ├── __init__.py
│       └── api.py
│
├── Cargo.toml                # workspace root → members = ["rs"]
├── CMakeLists.txt            # C build
```

### Deleted

- `crates/` (teide-sys, teide, teide-db, teide-cli) → merged into `rs/`
- `bindings/` (python, rust) → `py/` moves to top level

## Rust Crate Design

Single crate, feature-gated:

```toml
[package]
name = "teide"

[features]
default = []
cli = ["reedline", "clap", "nu-ansi-term"]

[dependencies]
sqlparser = { version = "0.53", features = ["visitor"] }

[dependencies.reedline]
version = "0.45"
optional = true

[dependencies.clap]
version = "4"
features = ["derive"]
optional = true

[dependencies.nu-ansi-term]
version = "0.50"
optional = true

[[bin]]
name = "teide"
required-features = ["cli"]
```

- `cargo add teide` → library with engine API + SQL (sqlparser only)
- `cargo install teide` → REPL binary (adds reedline, clap, nu-ansi-term)

## Merge Map

| Old                          | New                    |
|------------------------------|------------------------|
| `crates/teide-sys/src/lib.rs`| `rs/src/ffi.rs`        |
| `crates/teide/src/lib.rs`   | `rs/src/engine.rs`     |
| `crates/teide-db/src/lib.rs`| `rs/src/sql/mod.rs`    |
| `crates/teide-db/src/planner.rs` | `rs/src/sql/planner.rs` |
| `crates/teide-db/src/expr.rs`| `rs/src/sql/expr.rs`  |
| `crates/teide-db/tests/sql.rs` | `rs/tests/sql.rs`   |
| `crates/teide/tests/integration.rs` | `rs/tests/integration.rs` |
| `crates/teide-cli/src/main.rs` | `rs/src/cli/main.rs` |
| `crates/teide-cli/src/*.rs` | `rs/src/cli/*.rs`      |
| `bindings/python/teide/`    | `py/teide/`            |

## CI/CD

GitHub Actions on tag push:

1. Build C engine + run C tests (linux/mac/windows matrix)
2. `cargo publish` from `rs/` → crates.io
3. `cibuildwheel` from `py/` → PyPI (manylinux, macOS, Windows wheels)

## Principles

- Logic flows downward: if useful to all languages, put it in C
- Frontends are thin: language-idiomatic wrappers, not logic layers
- One package per language, always named "teide"
- The C engine is the product; everything else is access
