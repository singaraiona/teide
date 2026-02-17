# C Optimizer Roadmap — Missing Passes & Architectural Gaps

**Date**: 2026-02-17
**Status**: Design / Future Work

## Current State

The C optimizer (`src/ops/opt.c`) has 4 passes:

1. **Type inference** — propagate output types through DAG
2. **Constant folding** — fold binary+unary ops on constant inputs
3. **Expression fusion** (`fuse.c`) — fuse elementwise chains into register bytecode
4. **Dead code elimination** — remove unreachable nodes

The executor (`src/ops/exec.c`) has ad-hoc pattern matching for:
- `HEAD(SORT)` → top-N partial sort (line ~9358)
- `HEAD(GROUP)` → limit hint to exec_group for early termination (line ~9381)

## Missing Optimizer Passes

### Pass A: Predicate Pushdown

**What**: Push `OP_FILTER` below `OP_GROUP`, `OP_SORT`, `OP_JOIN` when the predicate
references only input columns (not aggregated outputs).

**Why**: Currently `WHERE x > 5 GROUP BY id` creates FILTER→GROUP. The executor runs
GROUP on all rows, then filters. Pushdown would filter first, reducing rows entering GROUP.

**Complexity**: Medium. Must analyze predicate column references against GROUP key/agg
inputs to determine pushability. For JOINs, need to distinguish left/right predicates.

**Implementation sketch**:
```
pass_predicate_pushdown(g, root):
  for each FILTER node with child ∈ {GROUP, SORT, JOIN}:
    if predicate references only columns available below child:
      swap: FILTER becomes child of GROUP/SORT/JOIN's input
      update graph edges
```

**Note**: The Rust planner already pushes WHERE before GROUP BY at the SQL level
(line 888 in planner.rs), so this mainly helps Python/ctypes frontends that build
arbitrary graphs. Still valuable for the C library as a standalone product.

### Pass B: Projection Pushdown

**What**: Push `OP_SELECT` (column selection) below `OP_GROUP`, `OP_JOIN`, `OP_SORT` to
avoid materializing columns that are immediately discarded.

**Why**: `SELECT id1, sum(v1) FROM t GROUP BY id1` where `t` has 9 columns — the executor
currently materializes all 9 columns into the group result, then SELECT trims to 2. With
pushdown, only id1 and v1 are fed to GROUP.

**Complexity**: Medium-High. Must trace which columns are consumed by downstream nodes.
GROUP BY needs keys + agg inputs; JOIN needs join keys + output columns; SORT needs sort
keys + output columns.

**Implementation sketch**:
```
pass_projection_pushdown(g, root):
  for each SELECT node with child ∈ {GROUP, SORT, JOIN}:
    compute required_cols = columns referenced by SELECT + child's internal needs
    if required_cols ⊂ all_cols:
      insert SELECT(required_cols) below child
      remove or simplify the original SELECT
```

**Note**: The executor's `exec_group` already only reads columns referenced by key/agg
SCAN nodes, so this is partially handled implicitly. The main win is for JOIN and SORT
where unused columns are still gathered.

### Pass C: FILTER→GROUP Fusion (HAVING Pushdown)

**What**: When `OP_FILTER` sits above `OP_GROUP` and the predicate references aggregate
outputs, fuse the filter into GROUP's materialization phase (phase 3).

**Why**: Currently HAVING requires materializing the full GROUP result, then scanning it
again to filter. Fusing the predicate into phase 3 avoids the extra scan.

**Complexity**: Medium. Phase 3 already iterates all groups for result materialization.
Adding a predicate check per group during materialization is straightforward.

**Implementation options**:
1. **Optimizer rewrite**: Transform `FILTER(GROUP(...), pred)` into `GROUP(..., having=pred)`
   by storing the predicate in the GROUP ext node.
2. **Executor pattern match**: In `exec_node`, detect FILTER→GROUP and pass predicate to
   `exec_group` (similar to HEAD→GROUP pattern).

Option 2 is simpler and consistent with existing HEAD→GROUP and HEAD→SORT patterns.

### Pass D: SELECT→GROUP Fusion

**What**: When `OP_SELECT` sits above `OP_GROUP` and only reorders/trims output columns,
fuse into GROUP's phase 3 to output only the needed columns in the right order.

**Why**: Avoids a full-table copy just to rename or reorder columns.

**Complexity**: Low-Medium. Phase 3 already has `agg_out_t` descriptors. Adding a column
mapping that controls which columns are output is straightforward.

**Implementation**: Store output column mapping in GROUP ext node. Phase 3 uses it to
scatter directly into the final column order.

### Pass E: DISTINCT Without Dummy Aggregate

**What**: Native `OP_DISTINCT` that is GROUP BY with zero aggregates, returning only
unique key combinations.

**Why**: Currently DISTINCT requires GROUP BY + dummy COUNT + SELECT to strip the count
column. Three operations where one suffices.

**Complexity**: Low. `exec_group` with `n_aggs=0` should already work — the DA and HT
paths just need to skip accumulator allocation and emit only key columns. Need to verify
and add a `td_distinct()` graph builder API.

### Pass F: LIMIT Pushdown Through FILTER

**What**: Push `HEAD(n)` below `OP_FILTER` as `HEAD(n)` with early-termination semantics
in the filter loop.

**Why**: `SELECT * FROM t WHERE x > 5 LIMIT 10` currently scans all matching rows, then
takes 10. With pushdown, scanning stops after 10 matches.

**Complexity**: Low. The filter executor loop already processes morsel-by-morsel. Add a
running count and break when limit is reached.

## Missing Executor Features

### COUNT_DISTINCT (#18)

`OP_COUNT_DISTINCT` (opcode 58) is declared in `td.h` but not implemented in the executor.

**Implementation**: Per-group hash set of seen values. In the DA path, use a bitmap per
group. In the HT path, use a chained hash set per group slot.

**Complexity**: High. The per-group hash set state doesn't fit the current accumulator
model (fixed-size `da_val_t` per group). Needs either:
- A separate parallel hash structure, or
- Two-phase: phase 1 = GROUP BY (group_keys + distinct_col), phase 2 = COUNT per group

The two-phase approach reuses existing machinery and is how the Rust planner already does it.

### STDDEV/VAR Welford Merge (#9 partial)

Per-partition GROUP BY path needs parallel Welford merge for STDDEV:
```
M2_total = M2_a + M2_b + delta² × n_a × n_b / (n_a + n_b)
```

Requires 3 partial columns per STDDEV (count, sum, M2), similar to AVG's 2 (sum, count).

**Complexity**: Medium. Follow the AVG decomposition pattern — replace STDDEV with
SUM+COUNT+M2 per partition, merge with Welford formula, compute final stddev.

## Priority Order

Based on impact vs effort:

| Priority | Item | Impact | Effort |
|----------|------|--------|--------|
| 1 | Pass E: OP_DISTINCT | Fixes common SQL pattern | Low |
| 2 | Pass C: HAVING fusion (executor pattern) | Avoids extra scan | Low-Medium |
| 3 | Pass F: LIMIT through FILTER | Early termination | Low |
| 4 | Pass D: SELECT→GROUP fusion | Avoids copy | Low-Medium |
| 5 | Pass A: Predicate pushdown | Reduces GROUP/JOIN input | Medium |
| 6 | COUNT_DISTINCT (two-phase) | SQL completeness | Medium |
| 7 | STDDEV Welford merge | Partitioned STDDEV | Medium |
| 8 | Pass B: Projection pushdown | Reduces JOIN/SORT columns | Medium-High |

## Materialization Points (Rust Planner Reference)

The Rust planner creates 19 materialization points, 4 of which are multi-graph:

1. **GROUP BY → new graph for SELECT projection** (every GROUP BY query)
2. **DISTINCT → new graph to strip COUNT column** (every DISTINCT query)
3. **COUNT(DISTINCT) → two-phase GROUP BY** (any COUNT(DISTINCT))
4. **HEAD → skip_rows for OFFSET** (OFFSET queries)

Once the C optimizer handles SELECT→GROUP fusion (Pass D) and native DISTINCT (Pass E),
the Rust planner can build single-graph DAGs for these patterns, eliminating multi-graph overhead.
