# Teide — Developer Guide

Pure C17 zero-dependency columnar dataframe library. Lazy fusion API → operation DAG → optimizer → fused morsel-driven execution.

## Build & Test

```bash
# Debug (ASan + UBSan)
cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build

# Release (optimized, no sanitizers)
cmake -B build_release -DCMAKE_BUILD_TYPE=Release && cmake --build build_release

# Run all tests
cd build && ctest --output-on-failure

# Run a single test suite
./build/test_teide --suite /vec     # or /buddy, /cow, /df, /graph, etc.

# Python testing — MUST use release build (debug links ASan, ctypes can't load it)
TEIDE_LIB=build_release/libteide.so python -m pytest bindings/python/
```

## Architecture at a Glance

### Core Abstraction: `td_t` (32-byte block header)
Every object (atom, vector, list, table) is a `td_t`. Data follows immediately at byte 32.
- `type < 0` → atom, `type > 0` → vector, `type == 0` → LIST, `type == 13` → table
- `mmod`: 0=arena, 1=file-mmap, 2=direct-mmap (>1GiB)
- `rc`: COW ref count (`_Atomic(uint32_t)`, relaxed in sequential mode)
- Error encoding: valid `td_t*` always 32B-aligned → `TD_ERR_PTR(e)` encodes error in low bits, `TD_IS_ERR(p)` to check

### Memory: Buddy Allocator + COW
- Thread-local arenas (`td_tl_arena`), min order 5 (32B), max order 30 (1GiB)
- Slab cache for orders 5-9 (32B-512B) — LIFO stack, hottest path
- Direct mmap for >1GiB (`mmod=2`)
- COW: `td_cow(v)` returns same pointer if `rc==1`, else copies. Mutations main-thread-only.
- Cross-thread free via MPSC return queue, drained after `td_parallel_end()`

### Execution Pipeline
1. Build lazy DAG: `td_graph_new(df)` → `td_scan/td_add/td_filter/...` → `td_execute(g, root)`
2. Optimizer: type inference → constant fold → predicate pushdown → CSE → fusion → DCE
3. Fused executor: bytecode over register slots, morsel-by-morsel (1024 elements)
4. All processing through `td_morsel_t` iterators — never full-vector passes

### Extended Nodes
Ops with >2 inputs (group-by, multi-column sort, join) use `td_op_ext_t` (64B).

## Code Conventions

- **Prefix**: all public symbols `td_`, internal functions `static` with no prefix
- **Constants**: `TD_UPPER_SNAKE_CASE`
- **Types**: `td_name_t` (typedef'd structs)
- **Morsel-only processing**: all vector loops must chunk through `td_morsel_t` (1024 elements/tile). No full-vector iteration.
- **Error returns**: `td_t*` functions use `TD_ERR_PTR()` / `TD_IS_ERR()`; other functions return `td_err_t`
- **No external deps**: everything in pure C17, single public header `include/teide/td.h`

## Known Pitfalls

### Optimizer root pointer invalidation
`td_fuse_pass()` and `pass_constant_fold()` free old nodes and reallocate. After optimization, the original root pointer is stale. Always re-derive root via `g->nodes[root_id]` — `td_optimize()` returns the updated root.

### Cross-graph node references
Nodes from one `td_graph_t` cannot be resolved in another graph's execution context. For joins: right-table keys must use `td_const_vec(g, column_vector)`, NOT `td_scan()` from a separate graph.

### Fused executor BOOL path
`exec_fused()` needs explicit TD_BOOL handling using i64 registers for comparisons and logical ops. Without it, BOOL output falls through to default → all-zeros.

### Python ctypes + ASan incompatibility
Debug `libteide.so` links ASan — Python's ctypes can't load it without `LD_PRELOAD`. Always use the release build for Python: `TEIDE_LIB=build_release/libteide.so`.

### Sort API
`td_sort_op(g, df_node, keys, descs)` — the `df_node` argument must NOT be NULL. Use `td_const_df(g, df)` to create the DataFrame node first.

## Benchmark Data

Real CSVs live at `../rayforce-bench/datasets/` relative to project root:
- Groupby: `G1_1e7_1e2_0_0/G1_1e7_1e2_0_0.csv` (10M rows, 9 cols)
- Join: `h2oai_join_1e7/`
- Window join: `window_join_10m/`

**Never generate synthetic CSV data when these bench CSVs exist.**

## Project Status

Phases 0-11 complete (159+ C tests, 19/19 benchmark queries passing). Phase 12 (Rust bindings) pending. See SPEC.md for full implementation specification.
