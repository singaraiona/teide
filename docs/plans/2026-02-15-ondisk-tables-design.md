# On-Disk Tables: Larger-Than-RAM Execution

**Date**: 2026-02-15
**Status**: Design

## 1. Overview

Teide's current execution model loads entire datasets into memory. This design extends the architecture to support datasets larger than RAM by exploiting three existing strengths:

1. **mmap'd column vectors** — OS manages page residency; unused pages evicted automatically
2. **Morsel-driven execution** — 1024-element tiles stream through operators without full materialization
3. **Existing parallel infrastructure** — `td_pool_dispatch`, radix partitioning, batch-column gather

The approach: columns stay as mmap'd files (zero-copy, read-only). The OS is the buffer manager. When intermediate results exceed a configurable memory budget, external algorithms (merge sort, grace hash) spill to temp files that look identical to file-loaded vectors.

**Non-goals**: this is not a general-purpose database engine. No WAL, no transactions, no concurrent writers. The target workload is analytics on cold storage — read-heavy queries over append-only partitioned data.

## 2. On-Disk Format

The on-disk format IS the in-memory format. A column file is:

```
Bytes  0-15:  nullmap
Bytes 16-31:  mmod=0, order=0, type, attrs, rc=0, len
Bytes   32+:  raw element data
```

This is already implemented in `src/store/col.c`. The change: instead of `td_col_load` (mmap → memcpy into buddy block → munmap), we add `td_col_mmap` which keeps the mmap and returns it directly.

```c
td_t* td_col_mmap(const char* path);
```

Returns a `td_t*` with `mmod=1` (file-mmap). The OS manages which pages are resident. Unused pages get evicted under memory pressure. No buddy allocator involvement for the data — just the 32-byte header interpretation. Uses `MAP_PRIVATE` (COW): only the header page gets a private copy when we write mmod/rc; all data pages stay shared with the page cache.

**mmod values**:
- `0` — buddy-allocated (current behavior)
- `1` — file-mmap, zero-copy (td_col_mmap)
- `2` — direct-mmap for large buddy allocations (>1GiB, tracked via td_tl_direct_blocks)

**COW guard**: any mutation attempt on an mmod=1 vector triggers a buddy copy via `td_cow()` — allocate, memcpy, then write. Read-only analytics never hits this path.

**Release dispatch**: `td_release` → `td_free` checks mmod:
- `mmod=0` → buddy free (existing)
- `mmod=1` → compute mapped_size from header, `munmap`
- `mmod=2` → direct-mmap free (existing)

## 3. Symfile Management & Splayed Table Intersection

A partitioned table is a set of splayed tables sharing one symfile. The symfile provides consistent ENUM intern IDs across all partitions.

**Directory layout**:

```
db_root/
  sym                         <- global symbol intern table (shared)
  2024.01.15/
    quotes/                   <- splayed table (partition 1)
      .d                      <- schema (column names + types)
      exchange                <- ENUM column (uint32_t intern IDs)
      price                   <- F64 column
  2024.01.16/
    quotes/                   <- splayed table (partition 2)
      exchange                <- same intern IDs, same symfile
      price
```

**API with external sympath**:

```c
// sym_path = NULL  -> look for sym inside splay dir (standalone table)
// sym_path != NULL -> external symfile (partition of parted table)
td_t* td_splay_open(const char* path, const char* sym_path);

// Save: sym_path = NULL  -> write sym into splay dir
//       sym_path != NULL -> append new symbols to shared symfile
td_err_t td_splay_save(td_t* table, const char* path, const char* sym_path);
```

**Usage patterns**:

| Scenario | `sym_path` |
|----------|-----------|
| Standalone splayed table | `NULL` (sym inside splay dir) |
| Partition within parted table | `"db_root/sym"` (shared) |
| Extracting one partition for export | `NULL` (snapshot relevant symbols into export dir) |

`td_part_open` passes the shared sympath down to each partition's `td_splay_open` call.

**Symfile consistency during external algorithms**: spilled ENUM vectors preserve raw `uint32_t` intern IDs — no re-interning needed. The symfile is loaded once at `td_part_open` into the global `td_sym` table (arena-allocated, lives for the process lifetime).

**Appending new partitions**: new symbols get appended to `db_root/sym` (existing IDs stable, new IDs sequential). Existing partition files are never rewritten.

## 4. Memory Budget & Temp File Infrastructure

**Memory budget**: a configurable byte limit for intermediate allocations during query execution. Default: 75% of available RAM (queried via `sysinfo` or `/proc/meminfo`).

```c
typedef struct {
    int64_t budget_bytes;     // max intermediate memory
    int64_t used_bytes;       // current allocation tracking
    char    tmp_dir[256];     // mkdtemp path
    int     n_files;          // open temp file count
} td_spill_t;
```

**Temp file lifecycle**:

```c
td_spill_t* td_spill_new(int64_t budget_bytes);
td_t*       td_spill_write(td_spill_t* sp, td_t* vec);  // vec -> temp file, returns mmod=2 handle
void        td_spill_free(td_spill_t* sp);               // unlink all, munmap all
```

`td_spill_write` creates a temp file under `tmp_dir`, writes the vector (32B header + data), mmaps it back as mmod=2, and returns the handle. The spilled vector is indistinguishable from a file-loaded column — all existing code that reads `td_t*` vectors works unchanged.

Temp files live under `/tmp/teide-XXXXXX/` (mkdtemp). `td_spill_free` unlinks everything. Even on crash, OS cleans `/tmp`.

## 5. External Merge Sort

When sorted data exceeds the memory budget, external merge sort replaces the in-memory radix sort. Three phases:

### Phase 1: Sorted Run Generation

Read input in budget-sized chunks. Each chunk:
1. mmap the source columns for this row range
2. Run existing `radix_sort_run` on the chunk (in-memory radix sort — already optimized)
3. Gather sorted rows into contiguous vectors via `multi_gather_fn`
4. `td_spill_write` the sorted vectors → temp files
5. Release the in-memory sort buffers

Each spilled chunk is a "sorted run" — a sequence of temp files (one per column) containing rows in sort order.

### Phase 2: K-way Merge

Merge K sorted runs into the final result:
1. Open all runs (mmap'd temp files)
2. Maintain a min-heap of size K, keyed by the current head element of each run
3. Pop minimum, emit to output morsel (1024 rows at a time)
4. When output morsel is full, either:
   - Keep in memory if total output fits budget
   - Spill to temp file if output exceeds budget (produces a larger sorted run for recursive merge)
5. Advance the popped run's cursor

For ENUM sort keys, `build_enum_rank` (merge-sort of intern IDs by full string memcmp) provides the rank mapping — same as the in-memory path. The symfile must be loaded.

### Phase 3: Final Gather

Once the single merged run exists, gather all columns in sort order using the existing `multi_gather_fn` batch-column pattern (512 rows x 1 column). The result is a normal in-memory table.

**Complexity**: O(N log N) with O(budget) memory. Sequential I/O throughout — no random reads on spilled data.

## 6. External Group-By (Grace Hash)

The existing in-memory group-by already uses 256 radix partitions. Grace hash extends this naturally: partitions that exceed the budget get spilled to disk.

### Phase 1: Partition & Spill

Reuse the existing phase1 scatter — hash rows into 256 partitions with fat radix entries `[hash:8B][keys:n*8B][agg_vals:m*8B]`. The only change: after scatter, check each partition's size against `budget / 256`.

- Fits in memory → aggregate immediately (existing phase2)
- Exceeds budget → `td_spill_write` the partition's entries to a temp file

### Phase 2: Process Spilled Partitions

For each spilled partition:
1. mmap the temp file (OS pages it in)
2. Run existing phase2 aggregation (probe + accumulate) on the partition
3. If a single partition still exceeds budget: re-partition with a different hash seed (recursive grace hash), spill sub-partitions, process each

### Phase 3: Materialize Results

Collect aggregation results from all partitions (in-memory + spilled). Run existing phase3 single-pass materialization with `agg_out_t` descriptors.

**Key insight**: the 256-partition radix structure already exists. Grace hash is just "spill the ones that don't fit" — minimal new code. The row-layout HT (keys + accumulators inline) means spilled partitions are self-contained files with no external column references.

## 7. External Join (Grace Hash Join)

Same grace hash principle applied to joins: co-partition both sides, then join each partition pair independently.

### Phase 1: Co-Partition

Hash both left and right tables by join key into P partitions (P=256, matching existing radix infrastructure):
1. For each side: hash join key → partition ID, scatter rows to per-partition buffers
2. Spill each partition to temp file via `td_spill_write`

Both sides use the **same hash function** on the join key, guaranteeing matching rows land in the same partition.

### Phase 2: Per-Partition Join

For each partition pair (left_p, right_p):
1. mmap both temp files
2. Run existing parallel join: HT build on right_p (atomic CAS), 2-pass probe on left_p
3. Gather matched rows via existing `gather_fn` with prefetch
4. If a partition pair exceeds budget: recursive partitioning with different hash seed

### Phase 3: Concatenate Results

Concatenate per-partition join results into the final table. For left joins: track unmatched left rows per partition, emit with NULL right columns.

**Key insight**: the existing join already builds a HT on the right side and probes with the left. Grace hash just ensures each partition pair fits in memory. The parallel HT build (atomic CAS) and 2-pass probe work unchanged on the partition subsets.

## 8. Partition Pruning & Query Planning

The optimizer extends predicate pushdown to skip entire partitions before execution.

### Partition Metadata

When `td_part_open` scans partition directories, it builds a lightweight metadata index:

```c
typedef struct {
    int64_t n_parts;              // number of partitions
    char**  part_dirs;            // directory names (YYYY.MM.DD)
    struct {
        int64_t min, max;         // column value range
        int64_t count;            // row count
        int64_t null_count;       // null count
    } **col_stats;                // [n_parts][n_cols]
} td_part_meta_t;
```

Stats are derived from a single min/max scan on first access, cached to a `.meta` sidecar file alongside each partition directory.

### Pruning Pass

New optimizer pass `pass_partition_prune` runs after predicate pushdown:

1. Collect filter predicates referencing partition-key columns
2. For each partition, evaluate predicates against `col_stats[part][col].{min, max}`
3. `col > X` and partition `max < X` → skip
4. `col = X` and `X` outside `[min, max]` → skip
5. Build `bool pruned[]` bitvector — only non-pruned partitions enter execution

### Query Planner Decision

```
estimate = sum(non_pruned_partitions.row_count) * row_width

if estimate < memory_budget:
    -> mmap all non-pruned partitions, run existing in-memory executor
else:
    -> use external algorithm (sort/group/join as needed)
```

For queries touching a small date range, the planner automatically selects the fast in-memory path. External algorithms only activate when the working set genuinely exceeds the budget.

## 9. API Surface & Lifecycle

On-disk tables plug into the existing `td_graph` pipeline with zero API changes for query construction.

### Opening On-Disk Tables

```c
// Partitioned table — loads symfile, builds metadata, no column I/O
td_t* td_part_open(const char* db_root, const char* table_name);

// Single splayed table — sym_path=NULL for standalone, non-NULL for partition
td_t* td_splay_open(const char* path, const char* sym_path);
```

Both return a `td_t` table descriptor with `TD_ATTR_ONDISK` flag set. Columns are descriptors (file paths + stats), not loaded data.

### Graph Integration

```c
td_t* db = td_part_open("/data/trades", "quotes");
td_graph_t* g = td_graph_new(db);             // same API as in-memory
uint32_t scan = td_scan(g, "price");           // lazy — no I/O yet
uint32_t filt = td_filter(g, ...);             // predicate registered
uint32_t grp  = td_group(g, ...);              // group-by registered
td_t* result  = td_execute(g, grp);            // planner decides here
```

At `td_execute` time:
1. Run partition pruning against registered predicates
2. Estimate working set from non-pruned partition stats
3. Choose in-memory (mmap all, existing executor) or external path
4. Execute and return result as normal in-memory `td_t` table

### Column Materialization

```c
td_t* td_col_mmap(const char* path);  // returns mmod=2 vector
```

### Temp File Manager

```c
td_spill_t* td_spill_new(int64_t budget_bytes);
td_t*       td_spill_write(td_spill_t* sp, td_t* vec);
void        td_spill_free(td_spill_t* sp);
```

### Lifecycle Summary

```
td_part_open     -> lightweight descriptor (no I/O)
td_execute       -> planner decides path
  in-memory:     -> td_col_mmap per column -> existing executor -> result
  external:      -> td_spill_new -> external algorithm -> td_spill_free -> result
td_release       -> free result (normal buddy free)
```

## 10. Implementation Phases

Each phase ships a testable, benchmarkable unit. No phase depends on a later phase.

### Phase 1: mmap'd Column Vectors

- Add `mmod=2` (direct-mmap) to `td_t` header
- Implement `td_col_mmap(path)` — zero-copy mmod=2 vector
- `td_release` dispatch: mmod=0 → buddy free, mmod=2 → munmap
- COW guard: mutation on mmod=2 triggers buddy copy
- `td_splay_open(path, sym_path)` — uses `td_col_mmap` per column
- `td_splay_save(table, path, sym_path)` — respects external sympath

**Verify**: load 10M-row groupby CSV as splayed table, `td_splay_open`, run all 7 groupby queries. Identical results. RSS near-zero until columns touched.

### Phase 2: Partition Metadata & Pruning

- `td_part_meta_t` with per-partition per-column min/max/count
- `.meta` sidecar file: write on first scan, cache on subsequent opens
- `td_part_open(db_root, table_name)` — loads symfile, builds metadata
- `pass_partition_prune` optimizer pass
- Planner decision: estimate working set, choose in-memory vs external

**Verify**: 12-partition dataset, filter to 2, confirm only 2 partitions mmap'd.

### Phase 3: Temp File Infrastructure

- `td_spill_t` manager: mkdtemp, create/mmap temp files, cleanup
- `td_spill_write` / `td_spill_free`
- Memory budget tracking

**Verify**: spill + read back 1M-row vector. Spill 100 vectors, verify cleanup.

### Phase 4: External Merge Sort

- Sorted run generation with existing `radix_sort_run`
- K-way merge with min-heap
- ENUM rank via loaded symfile
- Multi-key composite sort keys

**Verify**: sort 10M rows with budget=1MB. Match in-memory result exactly.

### Phase 5: External Group-By

- Extend 256-partition scatter with spill threshold
- Per-partition aggregation on spilled data
- Recursive re-partitioning for oversized partitions

**Verify**: all 7 groupby queries with budget=1MB. Match in-memory exactly.

### Phase 6: External Join

- Co-partition both sides by join key hash
- Per-partition in-memory join (existing HT build + probe)
- Left-join unmatched row tracking

**Verify**: j1 (inner) and j2 (left) with budget=1MB. Match in-memory exactly.
