# Teide — Detailed Implementation Spec Plan

## Context

Teide is a pure C17 columnar database with **zero external dependencies**, designed to be ultra-tiny (fits in CPU cache) and consumed via bindings from Python. It is NOT a query language or parser — it exposes a **lazy fusion API** that builds an operation graph, optimizes it, and executes in a single fused pass.

The core differentiator is the tight coupling between a custom buddy allocator (32-byte cache-aligned block headers) and a lazy execution engine with a full query optimizer.

**Fundamental invariant**: All processing loops are chunked into fixed-size morsels (1024 elements). This is mandatory — it serves two purposes: (1) **auto-vectorization** — every fused/parallelized operation processes a contiguous, aligned, fixed-count array that the compiler reliably vectorizes into SIMD at `-O3`, and (2) **larger-than-RAM** — mmap'd vectors stream morsel-by-morsel, OS pages in/out transparently (4KB on x86_64 Linux/Windows, 16KB on ARM64 macOS), the executor never materializes an entire column.

---

## Key Design Decisions (Agreed)

| Area | Decision |
|------|----------|
| Language | C17, no extensions, no external deps |
| Platforms | x86_64 Linux, x86_64 Windows, ARM64 macOS |
| Allocator | Lockfree buddy allocator, thread-per-arena |
| Memory mgmt | COW ref counting: mutations main-thread-only, plain RC in sequential mode, atomic RC only when main thread sets parallel flag |
| Block header | 32 bytes: 16B nullable bitmask + 16B metadata/value |
| Types | 16 types: LIST(0)..ENUM(15). Atoms=negative tag, vectors=positive |
| Symbol | SYMBOL (i64 ID, 8B) for metadata. ENUM (u32 index, 4B) for data columns. Both reference global sym intern table. |
| Fusion API | Lazy: build DAG → optimize → fuse → execute |
| Optimizer | Full: predicate pushdown, CSE, join reorder, op fusion |
| Disk format | kdb+ style column-per-file, date-partitioned, mmap-native |
| Threading | Main thread owns data mutations; workers are read-only for data but can intern new symbols. Thread-per-arena. Plain RC in sequential mode, atomic only during parallel ops |
| Execution | **Universal morsel-driven**: ALL loops chunked, enabling larger-than-RAM mmap processing |

---

## 1. Directory Structure

```
teide/
├── CMakeLists.txt
├── include/teide/
│   └── td.h                    # Single public header (entire API)
├── src/
│   ├── core/
│   │   ├── platform.h          # Platform abstraction: OS detection, compiler macros
│   │   ├── platform.c          # Platform-specific implementations (vm_alloc, vm_free, vm_advise, tls)
│   │   ├── types.h             # Typedefs, type constants, type sizes
│   │   ├── err.h               # Error enum + TD_ERR_PTR macros
│   │   ├── err.c               # err_str[] static table (~30 lines)
│   │   ├── block.h             # 32-byte block header struct + accessor macros
│   │   └── block.c             # Block utility functions
│   ├── mem/
│   │   ├── buddy.h/.c          # Buddy allocator (split/coalesce/free-lists)
│   │   ├── arena.h/.c          # Thread-local arena management
│   │   └── cow.h/.c            # td_retain/td_release/td_cow, MPSC return queue
│   ├── vec/
│   │   ├── atom.h/.c           # Atom constructors (td_i64, td_f64, td_sym...)
│   │   ├── vec.h/.c            # Vector create/access/append, nullable bitmaps
│   │   ├── str.h/.c            # String type (SSO + long-string via CHAR vector)
│   │   └── list.h/.c           # LIST (heterogeneous vector of td_t* pointers)
│   ├── table/
│   │   ├── table.h/.c          # Table (named column collection)
│   │   └── sym.h/.c            # Global symbol intern table
│   ├── ops/
│   │   ├── morsel.h/.c       # Morsel iterator (td_morsel_t), madvise integration
│   │   ├── graph.h/.c          # Operation DAG: node structs, construction API
│   │   ├── plan.h/.c           # Linearize DAG into execution plan
│   │   ├── pipe.h/.c           # Pull-based morsel pipeline (td_pipe_t), spill-to-disk
│   │   ├── fuse.h/.c           # Fusion pass: merge element-wise chains → bytecode
│   │   ├── opt.h/.c            # Optimizer passes (pushdown, CSE, DCE, etc.)
│   │   └── exec.h/.c           # Top-level executor: build pipeline, run to completion
│   ├── store/
│   │   ├── col.h/.c            # Column file read/write + mmap (single vector)
│   │   ├── splay.h/.c          # Splayed table: save/load directory of column files + .d + sym interning
│   │   ├── part.h/.c           # Partitioned table: partition discovery, par.txt, query routing
│   │   └── meta.h/.c           # .d file serialization, schema inference from column headers
│   └── io/
│       └── csv.h/.c            # CSV parser: parallel chunked reading, type inference, symbol interning
├── test/
│   ├── munit.h/.c              # Vendored munit test framework (single file pair)
│   ├── test_main.c             # Test runner
│   └── test_*.c                # One file per module
├── bench/
│   └── bench_*.c               # Microbenchmarks
└── bindings/
    ├── python/
        ├── teide/__init__.py   # Python ctypes wrapper (low-level C API)
        ├── teide/api.py        # High-level API (Context, Table, Query, Expr)
        └── teide_adapter.py    # rayforce-bench Adapter implementation
```

---

## 2. Block Header (32 bytes)

```
Offset  Size  Field     Purpose
0       16    nullmap   Nullable bitmask (inline for ≤128 elements)
16      1     mmod      0=arena-managed, 1=file-mmap'd, 2=direct-mmap'd (large alloc)
17      1     order     Buddy order (block size = 2^order bytes)
18      1     type      Type tag: negative=atom, positive=vector, 0=LIST
19      1     attrs     Flags: SORTED|UNIQUE|PARTITIONED|GROUPED|SLICE|NULLMAP_EXT|HAS_NULLS
20      4     rc        Reference count (_Atomic(uint32_t), relaxed in sequential / acq_rel in parallel)
24      8     value     Union: inline atom value OR {len + raw[] for vectors}
```

### Nullable bitmask overflow (vectors > 128 elements)
- `attrs & NULLMAP_EXT == 0`: inline bitmap in bytes 0-15 (128 bits)
- `attrs & NULLMAP_EXT == 1`: bytes 0-7 = `td_t*` pointer to external U8 vector bitmap; bytes 8-15 unused
- No-null fast path: `attrs & HAS_NULLS == 0` → skip all null checks

### String storage (SSO)
- **Short strings (≤7 bytes)**: atom (type=-8), union = `{u8 slen; char sdata[7]}`
- **Long strings (>7 bytes)**: atom union = `td_t*` pointer to a CHAR vector
- **String vectors**: LIST of string atoms (uniform COW model)

### GUID (16 bytes > 8-byte union)
- GUID atom: union holds `td_t*` pointer to a U8 vector of length 16
- GUID vector: len in union, data region = packed `len × 16` bytes

### Symbol & Enum (interned IDs)

Two types, same intern table:

| | SYMBOL (type 14) | ENUM (type 15) |
|-|------------------|----------------|
| Storage | i64 intern ID (8 bytes) | u32 index (4 bytes) |
| Use case | Column names, metadata, small collections | Data columns in tables (millions of rows) |
| Atom tag | -14 | -15 |
| Vector tag | +14 | +15 |

Both reference the **global symbol intern table** (`sym.h/.c`):
- **ID → string**: O(1) array lookup (index into STRING vector)
- **string → ID**: hash table (string → u32)
- Monotonically growing: new symbols appended, never removed or reordered
- **Thread safety**: any thread can intern new symbols (see Section 6.5)
- On disk: the `sym` file IS the serialized intern table (a STRING vector)
- Comparison/hash/sort: integer operations on IDs (fastest possible)
- No string length limit — any length string maps to a 4-byte ENUM or 8-byte SYMBOL

**Saving to disk**: SYMBOL columns are automatically **enumerated** to ENUM columns (i64 → u32, 2x space savings). ENUM columns are saved as-is.
**Loading from disk**: ENUM columns stay as ENUM (efficient). De-enumerate to SYMBOL only when needed for display or general manipulation.

### Table internal layout
- `type = 13`, `len = column count` in union
- Data region (after header): `td_t* schema` (ENUM vector of column name IDs) + `td_t* columns[]` (array of column vector pointers)
- Column name lookup: enum ID → index in columns array (hash table or linear scan for small tables)

### Slice/View (zero-copy)
- `attrs |= SLICE`: bytes 0-7 of nullmap = `td_t* parent`, bytes 8-15 = `i64 offset`
- Element access: `parent->raw + (offset + i) * elem_size`
- COW on slice: copies only the slice range into a new independent block

---

## 3. Platform Abstraction (`platform.h/.c`)

All OS-specific and compiler-specific code is isolated behind a thin abstraction layer. No other file includes `<windows.h>`, `<sys/mman.h>`, etc. directly.

### 3.1 Virtual Memory

| Operation | Linux | macOS | Windows |
|-----------|-------|-------|---------|
| Allocate pages | `mmap(MAP_ANONYMOUS\|MAP_PRIVATE)` | `mmap(MAP_ANONYMOUS\|MAP_PRIVATE)` | `VirtualAlloc(MEM_RESERVE\|MEM_COMMIT)` |
| Free pages | `munmap()` | `munmap()` | `VirtualFree(MEM_RELEASE)` |
| Map file | `mmap(fd, PROT_READ, MAP_PRIVATE)` | `mmap(fd, PROT_READ, MAP_PRIVATE)` | `CreateFileMapping()` + `MapViewOfFile()` |
| Unmap file | `munmap()` | `munmap()` | `UnmapViewOfFile()` + `CloseHandle()` |
| Advise sequential | `madvise(MADV_SEQUENTIAL)` | `madvise(MADV_SEQUENTIAL)` | `PrefetchVirtualMemory()` (Win8.1+) |
| Release pages | `madvise(MADV_DONTNEED)` | `madvise(MADV_FREE)` | `DiscardVirtualMemory()` (Win8.1+) or `VirtualFree(MEM_DECOMMIT)` |

```c
// Unified API
void* td_vm_alloc(size_t size);                          // anonymous pages
void  td_vm_free(void* ptr, size_t size);                // release pages
void* td_vm_map_file(const char* path, size_t* out_size); // map file read-only
void  td_vm_unmap_file(void* ptr, size_t size);           // unmap file
void  td_vm_advise_seq(void* ptr, size_t size);           // hint: sequential access
void  td_vm_release(void* ptr, size_t size);              // hint: done with pages, can reclaim
```

Note: macOS uses `MADV_FREE` instead of `MADV_DONTNEED` — `MADV_FREE` is lazy (pages reclaimed only under pressure), which is actually better behavior for our use case.

### 3.2 Atomics

C17 `_Atomic` is supported by GCC and Clang on all three platforms. MSVC has limited C17 atomics support — use MSVC intrinsics via `#ifdef`:

```c
#if defined(_MSC_VER)
  #define td_atomic_inc(p)   _InterlockedIncrement((volatile long*)(p))
  #define td_atomic_dec(p)   _InterlockedDecrement((volatile long*)(p))
  #define td_atomic_load(p)  _InterlockedOr((volatile long*)(p), 0)
  // ... etc
#else
  #define td_atomic_inc(p)   atomic_fetch_add_explicit(p, 1, memory_order_relaxed)
  #define td_atomic_dec(p)   atomic_fetch_sub_explicit(p, 1, memory_order_acq_rel)
  #define td_atomic_load(p)  atomic_load_explicit(p, memory_order_acquire)
#endif
```

### 3.3 Thread-Local Storage

```c
#if defined(_MSC_VER)
  #define TD_TLS __declspec(thread)
#else
  #define TD_TLS _Thread_local
#endif
```

### 3.4 Compiler Hints

```c
#if defined(__GNUC__) || defined(__clang__)
  #define TD_LIKELY(x)   __builtin_expect(!!(x), 1)
  #define TD_UNLIKELY(x) __builtin_expect(!!(x), 0)
  #define TD_ALIGN(n)    __attribute__((aligned(n)))
  #define TD_INLINE      static inline __attribute__((always_inline))
#elif defined(_MSC_VER)
  #define TD_LIKELY(x)   (x)
  #define TD_UNLIKELY(x) (x)
  #define TD_ALIGN(n)    __declspec(align(n))
  #define TD_INLINE      static __forceinline
#endif
```

### 3.5 Threading

```c
#if defined(_WIN32)
  typedef void* td_thread_t;   // HANDLE
#else
  typedef unsigned long td_thread_t;  // pthread_t (opaque, but unsigned long on Linux/macOS)
#endif

typedef void (*td_thread_fn)(void* arg);
td_err_t td_thread_create(td_thread_t* t, td_thread_fn fn, void* arg);
td_err_t td_thread_join(td_thread_t t);
uint32_t td_thread_count(void);  // number of hardware threads (for parallel ops)
```

| Operation | Linux/macOS | Windows |
|-----------|-------------|---------|
| Create | `pthread_create()` | `CreateThread()` |
| Join | `pthread_join()` | `WaitForSingleObject()` |
| Hardware threads | `sysconf(_SC_NPROCESSORS_ONLN)` | `GetSystemInfo()` → `dwNumberOfProcessors` |

### 3.6 SIMD Strategy

- **No explicit SIMD intrinsics** — rely on compiler auto-vectorization via `-O3`
- All operations — fused chains, reductions, filters, comparisons — are chunked into morsel-sized loops (1024 elements, Section 8). This guarantees the properties compilers need for auto-vectorization: contiguous memory, fixed loop count, no aliasing, aligned data
- x86_64: compiler generates SSE2/AVX2 automatically with `-march=native` (GCC/Clang) or `/arch:AVX2` (MSVC)
- ARM64: compiler generates NEON automatically with `-O3` (Clang on macOS)
- If profiling shows auto-vectorization is insufficient, add explicit intrinsics behind `#ifdef` — but defer this

---

## 4. Core Type Definition (`td_t`)

The fundamental type. Every object in Teide — atom, vector, list, table — is a `td_t`. The 32-byte header IS the block header, data follows immediately after:

```c
typedef struct td_t {
    // Bytes 0-15: nullable bitmask (inline for ≤128 elements)
    // Overloaded for slices: [0-7]=parent ptr, [8-15]=offset
    // Overloaded for ext nullmap: [0-7]=bitmap ptr, [8-15]=unused
    union {
        uint8_t  nullmap[16];
        struct { struct td_t* slice_parent; int64_t slice_offset; };
        struct { struct td_t* ext_nullmap;  int64_t _reserved; };
    };
    // Bytes 16-31: metadata
    uint8_t  mmod;       // 0=arena-managed, 1=file-mmap'd, 2=direct-mmap'd (large alloc)
    uint8_t  order;      // buddy order (block size = 2^order)
    int8_t   type;       // type tag: negative=atom, positive=vector, 0=LIST
    uint8_t  attrs;      // SORTED|UNIQUE|PARTITIONED|GROUPED|SLICE|NULLMAP_EXT|HAS_NULLS
    _Atomic(uint32_t) rc; // reference count (always _Atomic for type safety; plain ops via relaxed ordering in sequential mode)
    union {
        uint8_t  b8;    // BOOL atom
        uint8_t  u8;    // U8 atom
        char     c8;    // CHAR atom
        int16_t  i16;   // I16 atom
        int32_t  i32;   // I32 atom
        uint32_t u32;   // ENUM atom (intern index)
        int64_t  i64;   // I64 atom, SYMBOL atom (intern ID), DATE/TIME/TIMESTAMP
        double   f64;   // F64 atom
        struct td_t* obj;   // pointer to child (long strings, GUID, etc.)
        struct { uint8_t slen; char sdata[7]; }; // SSO string atom (≤7 bytes)
        int64_t  len;   // vector: element count (data follows header at offset 32)
    };
} td_t;  // 32 bytes (header only; vector data follows immediately after at byte 32)
```

Note on the `len` field: vectors store their element count in the `len` union member. The raw data starts at byte 32 (immediately after the header), accessed via the `td_data()` macro below. This avoids a flexible array member inside a union, which is a non-standard extension. The `_Atomic(uint32_t) rc` field is always atomic-typed for C17 type safety — in sequential mode (`td_parallel_flag == 0`), we use `memory_order_relaxed` which compiles to plain loads/stores on x86_64 and ARM64 (zero overhead).

**Accessors** (macros/inline functions):
```c
#define td_type(v)      ((v)->type)
#define td_is_atom(v)   ((v)->type < 0)
#define td_is_vec(v)    ((v)->type > 0)
#define td_len(v)       ((v)->len)
#define td_data(v)      ((void*)((char*)(v) + 32))   // pointer to vector data (byte 32 onward)
#define td_elem_size(t) (td_type_sizes[(t)])          // lookup from static table in types.h
```

---

## 5. Buddy Allocator

> **SOTA reference**: mimalloc (Microsoft, 2019+) uses free-list sharding per size class with thread-local pages — our buddy approach mirrors this with per-thread arenas. jemalloc's slab-class approach is the other mainstream alternative. Our buddy scheme is chosen for simplicity and because all allocations are power-of-2 aligned (a natural fit for the 32-byte block header). For the MPSC return queue, Treiber stack (lock-free LIFO) is the standard; Michael & Scott queue for FIFO if ordering matters.

- **Min order**: 5 (2^5 = 32 bytes — one header, enough for atoms)
- **Max order**: 30 (1 GiB)
- **Free lists**: array of heads, one per order, indexed by `order - ORDER_MIN`
- **Linkage**: intrusive doubly-linked list in the free block's data region (bytes 32-47)
- **Bitmaps**: `split_bits` (has block been split?) + `buddy_bits` (XOR of buddy free status) for O(1) coalesce detection

### Allocation algorithm
1. Compute needed order `k` = `ceil(log2(N + 32))`, clamped to `≥ ORDER_MIN`
2. Search `free[k]`. If non-empty → pop head, return
3. If empty → search `free[k+1]`, `free[k+2]`, ... until found at order `j`
4. Split order-j block repeatedly: create two buddies, put one on free list, keep splitting the other until order `k`
5. Return the final block

### Deallocation algorithm
1. Compute buddy address: `buddy_addr = block_addr XOR (1 << k)` (relative to arena base)
2. Toggle `buddy_bits`. If result = 0 (buddy also free) → remove buddy from its free list, clear `split_bits`, recurse at `k+1`
3. If buddy not free (result = 1) → push block onto `free[k]`

### Arena model
```c
_Thread_local td_arena_t* td_tl_arena = NULL;
```
- Default arena: 64 MiB via `td_vm_alloc()`
- Growth: double size on exhaustion, cap at 1 GiB, arenas linked per-thread
- No locks on hot path (thread-exclusive ownership)

### Cross-arena free (MPSC return queue)
- Each arena has `_Atomic(td_t*) return_queue` — lock-free stack
- Worker threads push freed blocks onto owning arena's return queue
- **Main thread drains all return queues after `td_parallel_end()`** — batch reintegration into free lists, no contention
- Arena identification: each arena occupies a contiguous power-of-2 aligned region → arena base = `addr & ~(arena_size - 1)` → O(1) lookup via hash of base address
- Between parallel operations (sequential mode), no return queue activity — plain free to own arena

### Slab Cache (orders 5-9, 32B-512B)

Small allocations (32-512B) are the hottest path — every `td_t` header, every graph node. A LIFO stack cache per order avoids full buddy split/coalesce for these sizes:

```c
#define TD_SLAB_CACHE_SIZE 64   /* max cached blocks per order */
#define TD_SLAB_ORDERS     5    /* orders 5,6,7,8,9 (32B..512B) */

typedef struct {
    int64_t count;
    td_t*   stack[TD_SLAB_CACHE_SIZE];
} td_slab_cache_t;
```

- Each `td_arena_t` contains `slabs[TD_SLAB_ORDERS]` — cache per order
- **Alloc fast path**: check slab cache before buddy walk; pop from LIFO stack
- **Free fast path**: push to slab cache before buddy coalesce; falls through to buddy when cache is full
- Zeroed automatically by `memset` in `td_arena_init`; destroyed with the arena mmap

### Direct mmap for Large Allocations (>1 GiB)

When `td_order_for_size()` returns `k > TD_ORDER_MAX (30)`, bypass buddy entirely:

```c
typedef struct td_direct_block {
    void*   ptr;           /* mmap base (= td_t header) */
    size_t  mapped_size;   /* total mmap'd bytes */
    struct td_direct_block* next;
} td_direct_block_t;
```

- `td_alloc()` calls `td_vm_alloc()` directly, sets `mmod=2`, `order=0`
- Tracker struct allocated from buddy (small block), linked in TLS list `td_tl_direct_blocks`
- `td_free()` with `mmod==2` looks up tracker, calls `td_vm_free()`, frees tracker to buddy
- `td_scratch_alloc/realloc` handle `mmod==2` by looking up `mapped_size` from tracker
- `td_arena_destroy_all()` cleans up all direct blocks before arena destruction

### Memory Statistics

Thread-local counters (no atomics needed):

```c
typedef struct {
    size_t alloc_count;     /* td_alloc calls */
    size_t free_count;      /* td_free calls */
    size_t bytes_allocated; /* currently allocated */
    size_t peak_bytes;      /* high-water mark */
    size_t slab_hits;       /* slab cache hits */
    size_t direct_count;    /* active direct mmaps */
    size_t direct_bytes;    /* bytes in direct mmaps */
} td_mem_stats_t;
```

- Updated in `td_alloc()` and `td_free()` on all paths (buddy, slab, direct)
- `td_mem_stats(td_mem_stats_t* out)` copies TLS stats to caller
- Reset in `td_arena_destroy_all()`

### Mmap integration
- `mmod = 1` marks file-mmap'd blocks (not managed by buddy free lists)
- `mmod = 2` marks direct-mmap'd blocks (large allocations bypassing buddy)
- `td_release()` on file-mmap'd block → `td_vm_unmap_file()` when rc hits 0
- COW on mmap'd data → copy into buddy arena, original stays read-only
- File-mmap'd blocks tracked in per-thread external blocks list:
  ```c
  typedef struct td_ext_block {
      td_t* ptr; size_t mapped_size; struct td_ext_block* next;
  } td_ext_block_t;
  ```

---

## 6. Threading Model & COW

### 6.1 Thread Ownership Model

**One main thread owns all object mutations.** Worker threads are read-only for data objects but **can add new symbols** to the global intern table:

| | Main thread | Worker threads |
|-|------------|----------------|
| Mutate data objects | Yes (via COW) | **Never** — produce new objects only |
| Intern new symbols | Yes | **Yes** — e.g., during parallel CSV parsing |
| Allocate | Own arena | Own per-worker arenas |
| Free | Own arena + drain return queues | Push to owning arena's return queue |
| Spawn workers | Yes (orchestrates parallel ops) | No |
| Coalesce blocks | Yes (during alloc/free) | No |
| Access shared data | Read + write (after COW) | **Read-only** (except sym intern) |

The main thread orchestrates all parallel operations:
1. Partition work into morsels
2. Spawn workers with read-only references to input data
3. Workers produce new output morsels in their own arenas (and may intern new symbols)
4. Main thread collects results, combines them
5. Worker arenas can be bulk-freed or reused for next parallel op

### 6.2 RC Mode Switching

When no workers are running (the common case), RC operations use `memory_order_relaxed` — compiles to plain loads/stores on x86_64 and ARM64, zero overhead vs non-atomic:

```c
static uint32_t td_parallel_flag = 0;  // 0 = sequential, 1 = parallel. Main thread only writes.

void td_retain(td_t* v) {
    if (TD_LIKELY(!td_parallel_flag)) {
        atomic_fetch_add_explicit(&v->rc, 1, memory_order_relaxed);  // plain on x86/ARM
    } else {
        atomic_fetch_add_explicit(&v->rc, 1, memory_order_relaxed);  // same for retain
    }
}

void td_release(td_t* v) {
    uint32_t prev;
    if (TD_LIKELY(!td_parallel_flag)) {
        prev = atomic_fetch_sub_explicit(&v->rc, 1, memory_order_relaxed);  // plain on x86/ARM
    } else {
        prev = atomic_fetch_sub_explicit(&v->rc, 1, memory_order_acq_rel);  // full barrier for cross-thread
    }
    if (prev == 1) td_free(v);      // was 1, now 0 → deallocate
}
```

The `rc` field is `_Atomic(uint32_t)` (see Section 4), so all operations are type-safe. In sequential mode, `memory_order_relaxed` compiles to identical machine code as plain `uint32++`/`uint32--` on all target platforms. In parallel mode, `acq_rel` on release provides the necessary barrier for safe deallocation across threads.

**Mode transitions** (main thread only, called before/after spawning workers):
```c
void td_parallel_begin(void) { td_parallel_flag = 1; }  // set before td_thread_create()
void td_parallel_end(void)   { td_parallel_flag = 0; }  // set after td_thread_join()
```

The flag is a plain variable, not atomic. The main thread sets it **before** creating worker threads (thread creation is a memory barrier — workers see the updated flag). The main thread clears it **after** joining all workers (thread join is a memory barrier — subsequent sequential code sees the cleared flag). No nesting needed — the main thread is the sole driver of all map/reduce operations.

### 6.3 COW Semantics

COW is **only called from the main thread** (data mutations are main-thread-only):

```c
td_t* td_cow(td_t* v) {
    if (v->rc == 1) return v;       // sole owner → mutate in place
    td_t* copy = td_alloc_copy(v);  // deep copy header + data
    copy->rc = 1;
    td_release(v);                  // decrement original
    return copy;
}
```

- Every mutating API function calls `td_cow()` first, returns (possibly new) pointer
- Callers must use the returned pointer (same pattern as `realloc()`)
- Since COW is main-thread-only and workers never mutate data objects, there is no race between COW copy and concurrent reads — workers only see the immutable original

### 6.4 Main Thread Optimizations

Because the main thread has full control over data objects:

- **Block coalescing**: only happens during `td_free()` on the main thread — no concurrent modification of free lists
- **Return queue draining**: main thread drains worker return queues after `td_parallel_end()`, not during parallel execution
- **Arena management**: main thread can resize arenas, create new arenas, or bulk-free worker arenas between parallel operations — no coordination needed
- **Allocator fast path**: when sequential (`td_parallel_flag == 0`), the entire allocator is single-threaded — no CAS loops, no retry, no contention

### 6.5 Concurrent Symbol Intern Table

The symbol intern table is the **one shared mutable structure** that all threads can write to. This is required for parallel CSV parsing, parallel enumeration, and any operation that encounters new string values needing interning.

> **SOTA reference**: Unchained hash table (Birler et al., DaMoN 2024) achieves highest throughput for concurrent string interning — open addressing with robin hood probing and SIMD-accelerated tag matching (one metadata byte per slot, 16 tags compared in a single SSE/NEON instruction). Swiss Table (Google Abseil) is the same concept widely deployed. Our packed `(hash32 | id32)` bucket layout is a simplified variant of this approach — the high 32 bits serve as the tag for fast rejection. For the append-only ID→string array, a seqlock or epoch-based reclamation handles concurrent growth; we use the simpler approach of atomic pointer swap + deferred free since resize is rare.

**Design**: lock-free concurrent hash map + append-only array.

```c
// Hash map state — swapped atomically as a single pointer during resize
typedef struct td_sym_map {
    _Atomic(uint64_t)* buckets;       // packed (hash32 | id32) entries
    uint32_t           bucket_mask;  // power-of-2 mask for bucket count
} td_sym_map_t;

typedef struct td_sym_table {
    // ID → string: append-only array (read lock-free, grow via atomic swap)
    _Atomic(td_t**)  strings;        // array of td_t* string pointers
    _Atomic(uint32_t) count;          // current symbol count
    _Atomic(uint32_t) capacity;       // current array capacity (atomic for concurrent readers)

    // string → ID: concurrent hash map (indirection for atomic resize)
    _Atomic(td_sym_map_t*) map;       // pointer to current map (swapped atomically during resize)

    // Resize lock (rare path only)
    _Atomic(uint32_t) resize_lock;    // 0=unlocked, 1=resizing
} td_sym_table_t;
```

The `map` indirection ensures that `buckets` and `bucket_mask` are always read consistently — readers load `map` once (atomic pointer read), then access `map->buckets` and `map->bucket_mask` from that snapshot. During resize, the new `td_sym_map_t` is allocated, populated, and atomically swapped into `td_sym_table.map`.

**Lookup** (`td_sym_find`, any thread, lock-free):
1. Load `map = atomic_load(&table->map)` — snapshot of current hash map
2. Hash the string → 32-bit hash
3. Probe `map->buckets[hash & map->bucket_mask]` with linear/robin-hood probing
4. Each bucket stores `(hash32 << 32) | id` atomically — compare hash first (cheap), then full string compare via `strings[id]`
5. On match → return existing ID (no mutation needed)
6. On empty slot → string not found

**Insert** (`td_sym_intern`, any thread):
1. Try `td_sym_find()` first — if found, return existing ID
2. Allocate string copy in calling thread's arena
3. Claim next ID via `atomic_fetch_add(&count, 1)` — gives unique monotonic ID
4. Store string pointer: `strings[new_id] = str_copy` (release store)
5. Load current `map`, CAS the bucket entry: `CAS(&map->buckets[slot], EMPTY, (hash32 << 32) | new_id)`
   - If CAS fails → another thread inserted a different entry at this slot → reprobe and retry
   - If during reprobe we find the same string was inserted by another thread → discard our copy, return their ID
6. If load factor > 0.7 → trigger resize (acquire `resize_lock`, allocate new map, rehash, swap `map` pointer)

**Resize** (rare, serialized by `resize_lock`):
1. `CAS(&resize_lock, 0, 1)` — only one thread resizes
2. Allocate new `td_sym_map_t` with new `buckets` array (2x size) and updated `bucket_mask`
3. Rehash all existing entries from old map → new map (other threads still read old map during this)
4. `atomic_store(&table->map, new_map)` — single pointer swap makes both buckets + mask visible atomically
5. Old map freed after a safe grace period (or by main thread after `td_parallel_end()`)
6. Release `resize_lock`

**Strings array growth**:
- Initial capacity: 4096 entries
- When `count` approaches `capacity`: allocate new array (2x), copy pointers, atomic swap `strings` pointer
- Old array freed after grace period (same as bucket resize)

**Thread safety guarantees**:
- `td_sym_find()`: fully lock-free, safe from any thread
- `td_sym_intern()`: lock-free on the fast path (no resize needed). Only blocks briefly during resize (rare)
- ID assignment is globally ordered (monotonic `atomic_fetch_add`)
- The same string always gets the same ID regardless of which thread interns it first

**Sequential mode optimization**: when `td_parallel_flag == 0`, skip atomics — use plain hash table operations with no CAS overhead. The concurrent structure is only exercised during parallel operations.

### 6.6 Thread Pool & Auto-Parallelization

> **SOTA reference**: Morsel-driven parallelism (Leis et al., HyPer 2014) is the standard for modern OLAP query engines — a fixed thread pool pulls fixed-size work units ("morsels") from a shared queue, achieving near-perfect load balancing without static partitioning. DuckDB, Umbra/CedarDB, and DataFusion all use variants of this model.

#### Thread Pool

A persistent pool of worker threads avoids the overhead of thread creation/destruction per operation:

```c
typedef struct td_pool {
    td_thread_t*      threads;       // worker thread handles
    uint32_t           n_workers;     // number of workers (hw_threads - 1)
    _Atomic(uint32_t)  shutdown;      // 0=running, 1=shutting down

    // Work queue (morsel dispatch)
    td_task_t*         tasks;         // circular task buffer
    uint32_t           task_cap;      // buffer capacity (power of 2)
    _Atomic(uint32_t)  task_head;     // producer index (main thread)
    _Atomic(uint32_t)  task_tail;     // consumer index (workers)

    // Synchronization
    td_sem_t           work_ready;    // signaled when new tasks available
    _Atomic(uint32_t)  active_count;  // workers currently executing (for join/barrier)
    td_sem_t          all_done;      // signaled when active_count hits 0
} td_pool_t;

typedef struct td_task {
    void (*fn)(void* arg, uint32_t worker_id);  // task function
    void*   arg;                              // task argument
} td_task_t;
```

- **Pool size**: `td_thread_count() - 1` (reserve one core for main thread)
- **Lifecycle**: created in `td_pool_init()` at library startup, destroyed in `td_pool_destroy()` at shutdown
- Each worker has its own thread-local arena (already in Section 5) — no contention on allocation
- Workers spin-wait on `work_ready` semaphore — zero CPU when idle, instant wake on new work

**Worker loop**:
```c
static void worker_loop(void* arg) {
    td_pool_t* pool = (td_pool_t*)arg;
    while (!atomic_load(&pool->shutdown)) {
        td_sem_wait(&pool->work_ready);
        // Pull tasks from queue until empty
        uint32_t tail;
        while ((tail = atomic_fetch_add(&pool->task_tail, 1)) <
               atomic_load(&pool->task_head)) {
            td_task_t* t = &pool->tasks[tail & (pool->task_cap - 1)];
            t->fn(t->arg, worker_id);
        }
        // Signal completion
        if (atomic_fetch_sub(&pool->active_count, 1) == 1)
            td_sem_signal(&pool->all_done);
    }
}
```

#### Auto-Parallelization Policy

Not every operation benefits from parallelism. The executor decides automatically based on estimated work:

```c
#define TD_PARALLEL_THRESHOLD  (64 * TD_MORSEL_ELEMS)  // 64K rows = 64 morsels
```

**Decision rule**: parallelize an operation when `est_rows > TD_PARALLEL_THRESHOLD` **and** the operation is parallelizable. Below this threshold, single-threaded execution avoids synchronization overhead.

| Operation | Parallelizable? | Strategy |
|-----------|-----------------|----------|
| **Fused element-wise chain** | Yes | Partition morsel range across workers. Each worker processes a contiguous slice of morsels independently. No synchronization needed — output morsels are independent. |
| **Filter** | Yes | Same as fused chain — each worker evaluates predicate on its morsel range, produces compacted output morsels. Main thread concatenates. |
| **Reduction** (sum, count...) | Yes | Each worker accumulates a partial result over its morsel range. Main thread combines partials (single pass, trivial). |
| **Sort** | Yes | Phase 1: workers sort morsel-sized runs in parallel. Phase 2: parallel k-way merge using Merge Path (balanced work distribution). |
| **Group-by** | Yes | Phase 1: workers radix-partition input into cache-sized buckets (thread-local partition buffers → no contention). Phase 2: workers aggregate one partition each. |
| **Hash Join** | Yes | Build phase: workers partition build side in parallel. Probe phase: workers probe their partition independently. |
| **Window Join** | Partial | Sort phase parallelizes. Merge-scan is sequential (ordered output). |
| **Scan** (mmap'd) | Yes | Each worker gets a morsel range. With `madvise(MADV_SEQUENTIAL)` per range, OS prefetches for each worker independently. |

#### Morsel-Driven Work Distribution

For parallelizable operations, the executor submits morsel ranges to the pool:

```c
typedef struct td_dispatch {
    int64_t  start;     // first element index
    int64_t  end;       // one-past-last element index
    void*  context;   // operation-specific state (input vector, output buffer, etc.)
} td_dispatch_t;
```

**Dispatch range**: `TD_DISPATCH_MORSELS = 8` (8 morsels = 8192 elements = 64 KiB for f64). Each task covers this many morsels. Large enough to amortize task queue overhead, small enough for load balancing across workers. Within each dispatch range, the worker iterates morsel-by-morsel (1024 elements each), with per-morsel `madvise` page lifecycle.

**Dispatch flow** (e.g., parallel fused chain):
1. Main thread computes `n_tasks = ceil(nrows / (TD_MORSEL_ELEMS * TD_DISPATCH_MORSELS))`
2. Submits `n_tasks` tasks to the pool, each processing a range of morsels
3. Main thread also processes morsels (participates as a worker)
4. Waits on `all_done` semaphore
5. `td_parallel_end()` → drain return queues, clear parallel flag

For pipeline breakers (sort, group, join), the two-phase approach means:
- **Phase 1** (partition/sort runs): morsel-driven, embarrassingly parallel
- **Phase 2** (merge/combine): either sequential or parallel depending on the number of partitions

#### Integration with Executor Pipeline

The pool integrates with the pipeline model (Section 8.5):

```
[Parallel Scan] → morsels → [Parallel Fused Chain] → morsels → [Parallel Group-by]
                                                                   Phase 1: parallel partition
                                                                   Phase 2: parallel aggregate
                                                                 → [Sequential Output]
```

Between pipeline stages, the main thread orchestrates: checks auto-parallel threshold, dispatches to pool or runs single-threaded, collects results. The pool is reused across all stages and across multiple `td_execute()` calls — no per-query setup cost.

#### Python API: Thread Count Control

```python
with Context(threads=4) as ctx:     # limit to 4 threads (3 workers + main)
    ...
with Context(threads=1) as ctx:     # force single-threaded (no pool)
    ...
with Context() as ctx:              # default: all hardware threads
    ...
```

---

## 7. Fusion API (Lazy Execution)

### Usage pattern
```c
td_graph_t *g = td_graph_new(df);   // bind to table — columns resolved by name
td_op_t *price = td_scan(g, "price");
td_op_t *qty   = td_scan(g, "qty");
td_op_t *total = td_mul(g, price, qty);
td_op_t *big   = td_filter(g, total, td_gt(g, price, td_const_f64(g, 100.0)));
td_op_t *result = td_sum(g, big);
td_t *val = td_execute(g, result);  // optimize → fuse → run
```

### Operation graph (`td_graph_t`)
```c
typedef struct td_graph {
    td_op_t*  nodes;      // arena-allocated array of op nodes
    uint32_t  node_count; // number of nodes in the graph
    uint32_t  node_cap;   // allocated capacity
    td_t*     df;         // bound table (provides columns for OP_SCAN)
} td_graph_t;
```
- `td_graph_new(df)` creates a new graph bound to a table (columns are resolved by name via `OP_SCAN`)
- All nodes are allocated from the thread-local arena — graph is freed when the arena is freed or explicitly via `td_graph_free()`

### Operation graph nodes (32 bytes, fits one cache line)
```c
typedef struct td_op {
    uint16_t opcode;     // OP_ADD, OP_SCAN, OP_FILTER, etc.
    uint8_t  arity;      // 0, 1, or 2
    uint8_t  flags;      // FUSED, DEAD, MATERIALIZED
    int8_t   out_type;   // inferred output type
    uint8_t  pad[3];
    uint32_t id;         // unique node ID
    uint32_t est_rows;   // estimated row count (for cost model)
    struct td_op* inputs[2];  // NULL if unused
} td_op_t;               // 32 bytes
```
- Extended node (64 bytes) adds union: `td_t* literal`, `int64_t sym` (column name), or extra inputs for N-ary ops (see group-by encoding below)

### Opcodes
- **Sources** (arity 0): `OP_SCAN`, `OP_CONST`
- **Unary element-wise** (fuseable): `OP_NEG`, `OP_ABS`, `OP_NOT`, `OP_SQRT`, `OP_LOG`, `OP_EXP`, `OP_CEIL`, `OP_FLOOR`, `OP_ISNULL`, `OP_CAST`
- **Binary element-wise** (fuseable): `OP_ADD`, `OP_SUB`, `OP_MUL`, `OP_DIV`, `OP_MOD`, `OP_EQ`, `OP_NE`, `OP_LT`, `OP_LE`, `OP_GT`, `OP_GE`, `OP_AND`, `OP_OR`, `OP_MIN2`, `OP_MAX2`
- **Reductions** (pipeline breakers): `OP_SUM`, `OP_PROD`, `OP_MIN`, `OP_MAX`, `OP_COUNT`, `OP_AVG`, `OP_FIRST`, `OP_LAST`
- **Structural** (pipeline breakers): `OP_FILTER`, `OP_SORT`, `OP_GROUP`, `OP_JOIN`, `OP_WINDOW_JOIN`, `OP_PROJECT`, `OP_SELECT`, `OP_HEAD`, `OP_TAIL`
- **Misc**: `OP_ALIAS`, `OP_MATERIALIZE`

### N-ary operations (group-by, multi-column sort, join)

Operations needing more than 2 inputs use **extended nodes** (64 bytes). The extra 32 bytes contain:

```c
typedef struct td_op_ext {
    td_op_t    base;           // 32 bytes (standard node)
    union {
        td_t*  literal;        // OP_CONST: inline literal value
        int64_t  sym;            // OP_SCAN: column name symbol ID
        struct {                 // OP_GROUP: group-by specification
            td_op_t**  keys;     //   array of key-column op nodes
            uint8_t    n_keys;   //   number of group keys
            uint8_t    n_aggs;   //   number of aggregations
            uint16_t*  agg_ops;  //   array of aggregation opcodes (OP_SUM, OP_AVG, ...)
            td_op_t**  agg_ins;  //   array of aggregation input op nodes
        };
        struct {                 // OP_SORT: multi-column sort
            td_op_t**  columns;  //   array of sort-key op nodes
            uint8_t*   desc;     //   per-column descending flags
            uint8_t    n_cols;   //   number of sort columns
        };
    };
} td_op_ext_t;  // 64 bytes
```

**Group-by DAG encoding** — `sum(v1), avg(v3) group by id1, id2`:
```
OP_SCAN("id1") ─┐
OP_SCAN("id2") ─┤── OP_GROUP(keys=[id1,id2], aggs=[SUM,AVG], agg_inputs=[v1,v3])
OP_SCAN("v1")  ─┤
OP_SCAN("v3")  ─┘
```
The `OP_GROUP` extended node stores arrays of key/agg pointers in the arena. The `inputs[0]` of the base node points to the first key (for type inference traversal), `inputs[1]` is unused.

---

## 8. Universal Morsel Execution Model

**This is the central architectural invariant of Teide.** Every operation — fused or not — processes data in fixed-size chunks (morsels). No operation ever reads or writes an entire vector in a single pass. This enables:

1. **Larger-than-RAM processing**: Mmap'd 100GB column on 16GB machine works transparently — OS pages in current morsel, evicts old ones under pressure
2. **Cache efficiency**: Morsel fits in L1/L2 cache, maximizing compute/memory ratio
3. **Auto-vectorization**: All fused and parallelized operations are chunked into morsel-sized loops — contiguous, aligned, fixed-count, no aliasing. The compiler emits SIMD (SSE2/AVX2 on x86_64, NEON on ARM64) automatically at `-O3` without explicit intrinsics
4. **Predictable memory usage**: Intermediate results never exceed morsel size × pipeline width

### 8.1 Morsel Constants

```c
#define TD_MORSEL_ELEMS  1024   // elements per morsel (power of 2 for alignment)
#define TD_MORSEL_REGS   8      // register slots for intermediate values
```

- 1024 × 8 bytes (f64) = 8 KiB per morsel slot → 8 slots = 64 KiB → fits L1 cache
- For smaller types (i32, i16, bool): same element count, less memory per slot
- Morsel size is a compile-time constant, not configurable at runtime

### 8.2 Morsel Iterator (td_morsel_t)

Every vector access goes through a morsel iterator — there is **no** `td_vec_get_all()` that returns the entire data:

```c
typedef struct {
    td_t*    vec;          // source vector
    int64_t  offset;       // current position (element index)
    int64_t  len;          // total length of vector
    uint32_t elem_size;    // bytes per element (from td_elem_size(vec->type))
    int64_t  morsel_len;   // elements in current morsel (≤ TD_MORSEL_ELEMS)
    void*    morsel_ptr;   // pointer to current morsel's data start
    uint8_t* null_bits;    // pointer to current morsel's null bitmap (or NULL)
} td_morsel_t;
```

```c
void td_morsel_init(td_morsel_t* m, td_t* vec);       // start at offset 0
bool td_morsel_next(td_morsel_t* m);                    // advance to next morsel, return false if done
// td_morsel_next sets morsel_ptr, morsel_len, null_bits for the current chunk
```

For mmap'd vectors, `morsel_ptr` points directly into the mmap'd region — no copy. The OS will page-fault the relevant pages on first access (4KB on x86_64, 16KB on ARM64 macOS) and can evict them once the morsel iterator moves past.

### 8.3 Mmap Memory Hints

The kernel detects sequential access patterns automatically (readahead + drop-behind). We set `MADV_SEQUENTIAL` **once** per mmap'd vector at scan init — not per morsel:

```c
void td_morsel_init(td_morsel_t* m, td_t* vec) {
    m->vec = vec;
    m->offset = 0;
    m->len = td_len(vec);
    m->elem_size = td_elem_size(vec->type);
    // One-time hint: sequential access → kernel does readahead + drop-behind
    if (vec->mmod == 1) {
        td_vm_advise_seq(td_data(vec), m->len * m->elem_size);
    }
}

bool td_morsel_next(td_morsel_t* m) {
    m->offset += m->morsel_len;
    if (m->offset >= m->len) return false;
    m->morsel_len = min(TD_MORSEL_ELEMS, m->len - m->offset);
    m->morsel_ptr = (uint8_t*)td_data(m->vec) + m->offset * m->elem_size;
    return true;
    // No per-morsel madvise — kernel handles page lifecycle via MADV_SEQUENTIAL
}
```

- `MADV_SEQUENTIAL` tells the kernel: aggressive readahead, drop pages behind access point
- Linux readahead state machine detects the linear pattern after a few page faults
- No syscall overhead per morsel — the inner loop stays in userspace
- `MADV_DONTNEED` reserved as a last resort if profiling shows memory pressure under extreme larger-than-RAM workloads (not the default path)

### 8.4 All Operations Are Morsel-Driven

Every operation type processes data morsel-by-morsel:

| Operation | Morsel Strategy | SOTA Algorithm |
|-----------|-----------------|----------------|
| **Element-wise** (add, mul, eq...) | Input morsels → compute → output morsel. No materialization of full vectors. | Auto-vectorized tight loops; InkFuse-style (ICDE 2024) interpreted bytecode over register slots |
| **Fused chains** | Multiple element-wise ops on same morsel in one pass (bytecode interpreter over register slots). | Adaptive fusion: interpreted bytecode for short chains, optional JIT for hot paths (Umbra/CedarDB approach) |
| **Reduction** (sum, count, avg...) | Accumulate partial result per morsel. Final combine after all morsels. | Standard morsel accumulation; Kahan summation for f64 precision if needed |
| **Filter** | Read morsel, evaluate predicate morsel, compact output morsel. Output may be shorter than input. | SIMD-accelerated selection vectors (DuckDB), branchless compaction |
| **Sort** | **Normalized-key sort**: encode multi-column keys as binary-comparable byte strings → radix sort / pdqsort on morsels → parallel k-way merge sorted runs with Merge Path (DuckDB 2025). Loser tree for merge. | vqsort (Google, 2022) for in-morsel SIMD sort; IPS4o for parallel in-memory; DuckDB's normalized-key + parallel merge for multi-column |
| **Group-by** | **Partitioned pre-aggregation**: radix-partition by hash into cache-sized partitions, aggregate within each partition. For larger-than-RAM: spill partitions to disk. | DuckDB/DataFusion radix-partitioned aggregation; software write-combine buffers (Umbra) for scatter-heavy partitioning |
| **Hash Join** | **Adaptive partitioned hash join**: partition both sides by join key hash with fine-grained spilling. If a partition exceeds memory, recursively partition it. | DuckDB adaptive hash join (VLDB 2025) with per-partition spill; Grace hash join as fallback; bloom filters for semi-join reduction |
| **Window Join** | **Sort-merge AsOf join**: both sides sorted by time key, merge with window bounds. | DuckDB sort-merge AsOf join; IEJoin (Khayyat et al.) for general range predicates |
| **Scan** (mmap'd) | Morsel iterator directly over mmap'd memory. Zero copy. OS pages in/out transparently. | Standard mmap with madvise hints |

### 8.5 Executor Pipeline

The executor runs as a **pull-based morsel pipeline**:

```
[OP_SCAN] → morsel → [OP_MUL (fused)] → morsel → [OP_FILTER] → morsel → [OP_SUM]
```

Each pipeline stage:
1. Requests next morsel from its input(s)
2. Processes the morsel, producing an output morsel
3. Passes output morsel to the next stage

Pipeline breakers (sort, group, join) consume ALL input morsels before producing any output:
- They accumulate results internally (using temp files for larger-than-RAM)
- Then emit output morsels from the accumulated result

```c
typedef struct td_pipe {
    td_op_t*          op;           // operation node
    struct td_pipe*   inputs[2];    // upstream pipes
    td_morsel_t       state;        // current morsel state
    // For pipeline breakers:
    td_t*             materialized; // fully materialized intermediate (or NULL)
    int               spill_fd;     // file descriptor for spill-to-disk (-1 if not spilling)
} td_pipe_t;
```

### 8.6 Spill-to-Disk for Larger-than-RAM Intermediates

When a pipeline breaker (sort, group, join) accumulates more data than a configurable threshold (`TD_SPILL_THRESHOLD`, default 25% of available RAM), it spills partitions to temp files:

```c
#define TD_SPILL_THRESHOLD_RATIO  0.25  // fraction of available RAM before spilling
```

**Sort spill**: Write sorted morsel-runs to temp file (same column file format). Parallel k-way merge from disk using Merge Path (Odeh et al.) for balanced work distribution across threads. Use loser tree for efficient k-way selection.

**Group-by spill**: Radix-partition by hash into `N` temp files using software write-combine buffers (64-byte cache-line-sized staging buffers per partition to avoid random-write cache misses). Process each partition file independently. Each partition fits in memory.

**Join spill (adaptive partitioned hash join)**: Partition both build and probe sides by join key hash into `N` bucket files with fine-grained spilling — only oversized partitions spill, small ones stay in memory (DuckDB VLDB 2025 approach). For each spilled bucket pair, load build side into hash table, stream probe side in morsels. Recursive partitioning if a bucket still exceeds memory.

All spill files use the native column file format (32-byte header + raw data), so they can be mmap'd back for reading — getting the same morsel-driven page-in/page-out behavior.

### 8.7 COW Is Also Morsel-Aware

When `td_cow()` is triggered on a large mmap'd vector, it does NOT allocate a full copy:

```c
td_t* td_cow(td_t* v) {
    if (v->rc == 1) return v;
    if (v->mmod == 1 && td_is_vec(v) && td_len(v) > TD_MORSEL_ELEMS) {
        // Large mmap'd vector: defer copy, mark as COW-pending
        // Actual copy happens morsel-by-morsel during execution
        return td_cow_lazy(v);
    }
    // Small or arena-allocated: full copy (fast for small data)
    td_t* copy = td_alloc_copy(v);
    copy->rc = 1;
    td_release(v);
    return copy;
}
```

`td_cow_lazy()` creates a "lazy COW wrapper" that copies data morsel-by-morsel as the executor reads through it. This avoids the pathological case of `memcpy(100GB)`.

---

## 9. Optimizer Passes (in order)

> Note: Section 8 (Morsel Execution Model) is the architectural foundation. All passes below produce plans that execute via the morsel pipeline.

| # | Pass | Purpose |
|---|------|---------|
| 1 | Type Inference | Bottom-up: infer `out_type`, insert implicit casts. Promotion: BOOL < U8 < I16 < I32 < I64 < F64 |
| 2 | Constant Folding | All-const inputs → evaluate immediately, replace with OP_CONST |
| 3 | Predicate Pushdown | Move OP_FILTER below joins/projects, closer to OP_SCAN sources |
| 4 | Projection Pushdown | Track used columns, modify OP_SCAN to load only required columns |
| 5 | CSE | Hash-based dedup: `(opcode, input_ids, literal_hash)` → merge identical nodes |
| 6 | Op Reordering | Canonicalize commutative ops (lower ID left) for better CSE |
| 7 | Join Optimization | Greedy reorder: smallest join first by est_rows |
| 8 | Fusion | Merge element-wise chains into single fused bytecode node |
| 9 | DCE | Remove nodes with zero consumers |

### Fusion detail

> **SOTA reference**: InkFuse (ICDE 2024) demonstrates that a bytecode interpreter over register slots can match JIT-compiled code for short fusion chains (≤10 ops), with zero compile latency. Umbra/CedarDB uses adaptive JIT with a custom IR for longer chains. Our approach: bytecode interpreter (v1), with optional JIT via a tiny custom IR if profiling shows hot fusion chains exceeding ~10 ops.

- Detect maximal chains of element-wise ops where each intermediate has exactly one consumer
- Replace chain with fused node containing bytecode: `[(opcode, src_slot, dst_slot), ...]`
- Executor processes fused node morsel-by-morsel (Section 8): read input morsel → apply bytecode sequence over register slots → emit output morsel. Each bytecode step is a tight loop over 1024 contiguous elements — auto-vectorized by the compiler into SIMD
- Register slots: 8 × `f64[TD_MORSEL_ELEMS]` arrays on thread stack ≈ 64 KiB → all slots L1-resident during the inner loop
- Parallel dispatch (Section 6.6): each pool worker gets a range of morsels to process through the same fused bytecode — embarrassingly parallel, no synchronization

### Staged rollout
- **v1**: Type Inference + Constant Folding + Fusion + DCE (minimum viable optimizer)
- **v2**: Predicate/Projection Pushdown + CSE
- **v3**: Op Reordering + Join Optimization

> **SOTA reference for optimizer**: Cascades/Columbia-style top-down optimization is the gold standard (DuckDB, CockroachDB), but overkill for our scope. Our fixed-pass pipeline is sufficient for the benchmark suite and matches what DataFusion uses. For join reordering, DPccp (connected-subgraph complement pairs) is optimal for ≤~18 tables; our greedy smallest-first suffices for typical 2-3 table joins.

---

## 10. Storage (kdb+/Rayforce style)

Two on-disk table formats, same as kdb+ and Rayforce:

### 10.1 Column File Format

Each column is stored as a raw block image — the on-disk format IS the in-memory format:

```
Bytes 0-15:  nullmap (16 bytes)
Bytes 16-31: mmod=0, order=0, type, attrs, rc=0, len
Bytes 32+:   raw element data
```

- **Load**: `td_vm_map_file(path)` → set `mmod=1`, `rc=1` → use directly as `td_t*` (zero deserialization)
- After map: `td_vm_advise_seq(ptr, size)` for streaming access pattern
- **Save**: `write(header)` + `write(data)` with `mmod=0`, `rc=0`
- All access to mmap'd vectors goes through `td_morsel_t` iterator (Section 8) — enables larger-than-RAM columns

**Numeric types** (I64, F64, etc.): directly mmap-able, values are portable.
**Symbol columns**: stored as **enumerated integer indices** (see 10.4 below), not raw symbol values — enables mmap.

### 10.2 Splayed Table (leaf format)

A splayed table is the **atomic on-disk unit**: a flat directory containing a `.d` file and column files. Nothing else — no sub-tables, no nesting. It is the leaf structure of all on-disk storage.

```
trades/                          # Splayed table = flat directory
  .d                             # Column names in order (ENUM vector of name IDs)
  time                           # Column file: TIMESTAMP vector
  ticker                         # Column file: ENUM vector (u32 indices into sym)
  price                          # Column file: F64 vector
  size                           # Column file: I64 vector
```

**`.d` file**: a serialized ENUM vector listing column name IDs in their canonical order. Types are inferred from the column files themselves (the `type` byte in each column's block header). Same as kdb+ and Rayforce.

A standalone (non-partitioned) database is just splayed tables at the root:

```
db_root/
  sym                            # Global symbol intern table (STRING vector)
  trades/                        # Splayed table
    .d
    time, ticker, price, size
  quotes/                        # Another splayed table
    .d
    time, ticker, bid, ask
```

**One `sym` file per database** (at `db_root/sym`). All tables share it. All ENUM columns reference it.

**Loading a splayed table**:
1. Load `db_root/sym` → initialize the intern table (string → ID, ID → string)
2. Read `.d` → get column name IDs and order → resolve to strings via intern table
3. `td_vm_map_file()` each column file → get `td_t*` vectors (zero deserialization)
4. ENUM columns stay as ENUM — no de-enumeration needed unless display is required
5. Construct `td_t*` table from column vectors

### 10.3 Partitioned Tables (HDB)

A partitioned table distributes a logical table across partition directories (typically by date). Each partition directory contains a splayed table — the same flat `.d` + column files format. **One `sym` file at database root, shared by all partitions and all tables.**

```
db_root/
  sym                              # Global symbol intern table (ONE per database)
  par.txt                          # Optional: multi-disk segment paths
  2024.01.15/                      # Partition directory (date value)
    trades/                        # Splayed table (flat: .d + columns)
      .d                           # Column names: [`time, `ticker, `price, `size]
      time                         # TIMESTAMP vector
      ticker                       # ENUM vector (u32 indices into db_root/sym)
      price                        # F64 vector
      size                         # I64 vector
    quotes/                        # Another splayed table (flat: .d + columns)
      .d
      time, ticker, bid, ask
  2024.01.16/                      # Next partition
    trades/                        # Same schema as 2024.01.15/trades/
      .d
      time, ticker, price, size
    quotes/
      .d
      time, ticker, bid, ask
```

The logical table "trades" spans all partitions: `2024.01.15/trades/`, `2024.01.16/trades/`, etc. Each is a splayed table with identical schema.

**Key rules** (same as kdb+/Rayforce):
- **One `sym` file per database** — all ENUM columns across all tables and all partitions reference this single intern table
- **Partition column is virtual** — its value is the directory name, NOT stored as a column file
- **Schema must be consistent** across all partitions (same columns, same types, same `.d` order)
- **Adding a new partition** = creating a new directory containing splayed tables
- **Each partition** can contain multiple tables (each a flat splayed table)
- **New symbols** encountered when saving a partition are appended to the shared `sym` file

**Partition discovery**: scan `db_root/` for directories matching the partition type pattern (dates, months, years, or integers). Partitions are sorted and presented as a single logical table with a virtual partition column.

### 10.4 Symbol Intern Table & Enumeration

The `sym` file is a STRING vector — the global intern table for the entire database:

```
sym file = STRING vector: ["AAPL", "GOOG", "MSFT", ...]
                            ID=0    ID=1    ID=2
```

**ENUM columns on disk** store u32 indices into this table:
```
Column "ticker" (ENUM type, u32[]):  [0, 1, 2, 0, 2]
Meaning:                              [AAPL, GOOG, MSFT, AAPL, MSFT]
```

**Saving** (`td_splay_save()`):
1. Load existing `sym` file (intern table)
2. SYMBOL columns (type 14, i64 IDs) are **enumerated** → ENUM columns (type 15, u32 indices). 2x space savings.
3. New symbol strings not yet in the intern table are appended, assigned next monotonic ID
4. Save updated `sym` file
5. ENUM columns saved as-is (already u32 indices)

**Loading** (`td_splay_load()`):
1. Load `sym` file → initialize intern table
2. `td_vm_map_file()` ENUM columns → direct mmap, zero deserialization
3. ENUM values can be resolved to strings on demand (display) via intern table lookup
4. De-enumeration (ENUM → SYMBOL) only when needed for general manipulation

**Why one shared `sym` file**: The same string always gets the same ID across all tables and all partitions. `"GOOG"` is always ID 1 everywhere. Without this, the same string could get different IDs in different partitions, breaking cross-partition queries.

### 10.5 Multi-Disk Segmentation (`par.txt`)

For databases spanning multiple disks, `par.txt` lists segment paths:

```
# db_root/par.txt
/disk1/db
/disk2/db
/disk3/db
```

Each segment directory is itself partitioned:
```
db_root/             /disk1/db/           /disk2/db/           /disk3/db/
├── par.txt          ├── 2024.01.15/      ├── 2024.01.16/      ├── 2024.01.17/
└── sym              │   ├── trades/      │   ├── trades/      │   ├── trades/
                     │   └── quotes/      │   └── quotes/      │   └── quotes/
```

- All segments share the single `sym` file at `db_root/`
- Partitions are distributed across segments (round-robin or manual assignment)
- Query executor can read partitions from different disks in parallel

### 10.6 Query Routing on Partitioned Tables

When executing a query against a partitioned table:
1. **Partition pruning**: analyze WHERE clause for partition column filters → load only matching partition directories
2. **Projection pushdown**: load only column files referenced in the query (via `td_vm_map_file()`)
3. **Per-partition execution**: run the query pipeline on each partition independently (morsel-by-morsel)
4. **Result combination**: concatenate or reduce results across partitions

This integrates directly with the optimizer (Section 9): `OP_SCAN` on a partitioned table resolves to multiple partition scans, each producing morsels from their mmap'd column files.

---

## 11. Python API (`bindings/python/teide/api.py`)

High-level API built on top of the low-level ctypes wrapper (`__init__.py`):

### Usage

```python
from teide.api import Context, col, lit

with Context() as ctx:
    df = ctx.read_csv("data.csv")
    result = (
        df.filter(col("v1") > 0)
          .group_by("id1")
          .agg(col("v1").sum(), col("v3").mean())
          .sort("v1_sum", descending=True)
          .collect()
    )
    print(result.head(10))
    print(ctx.mem_stats())
```

### Classes

| Class | Purpose |
|-------|---------|
| `Context` | Manages `TeideLib` lifecycle (sym_init/destroy, arena_destroy_all). Context manager. `read_csv()`, `mem_stats()` |
| `Table` | Materialized data. `.columns`, `.shape`, `[name]→Series`, `.head(n)`, `.to_dict()`, `.to_pandas()`. Lazy entry points: `.filter()`, `.group_by()`, `.sort()` return `Query` |
| `Query` | Lazy computation builder. `.filter(expr)`, `.group_by(*cols)`, `.sort(*cols)`. `.collect()→Table` |
| `Expr` | Column expression tree. `col("x")`, `lit(42)`. Operator overloads: `+`, `-`, `*`, `/`, `==`, `!=`, `<`, `<=`, `>`, `>=`, `&`, `\|`. Agg methods: `.sum()`, `.mean()`, `.min()`, `.max()`, `.count()`, `.first()`, `.last()` |
| `GroupBy` | Intermediate state from `.group_by()`. `.agg(*exprs)→Query` |
| `Series` | Single column. `.to_list()`, `.to_numpy()` (zero-copy for I64/F64/I32) |

`Table` is always materialized — no internal state dispatch. `Query` is always lazy — records ops, executes on `collect()`. Table's `.filter()/.group_by()/.sort()` create a Query implicitly (no explicit `.lazy()` needed).

### Graph Building

`Query.collect()` builds a `td_graph_t`, walks the `_ops` list to emit nodes:

- `("filter", expr)` → resolve expr to predicate node → `td_filter(g, input, pred)`
- `("group", key_cols, agg_exprs)` → scan keys, decompose aggs → `td_group(g, keys, ops, inputs)`
- `("sort", cols, desc)` → use pipeline node (or `td_const_df(g, df)` if first op) + scan keys → `td_sort(g, df_node, keys, desc)`

### Opcode Mapping

| Method | Teide opcode | Value |
|---|---|---|
| `.sum()` | `OP_SUM` | 50 |
| `.mean()` | `OP_AVG` | 55 |
| `.min()` | `OP_MIN` | 52 |
| `.max()` | `OP_MAX` | 53 |
| `.count()` | `OP_COUNT` | 54 |
| `.first()` | `OP_FIRST` | 56 |
| `.last()` | `OP_LAST` | 57 |

---

## 12. Error Handling (tiny)

```c
typedef enum {
    TD_OK=0, TD_ERR_OOM, TD_ERR_TYPE, TD_ERR_RANGE, TD_ERR_LENGTH,
    TD_ERR_RANK, TD_ERR_DOMAIN, TD_ERR_NYI, TD_ERR_IO,
    TD_ERR_SCHEMA, TD_ERR_CORRUPT
} td_err_t;
```
- Functions returning `td_t*`: encode errors in low pointer bits
  - Valid `td_t*` always 32-byte aligned → low 5 bits always zero
  - `#define TD_ERR_PTR(e) ((td_t*)(uintptr_t)(e))`
  - `#define TD_IS_ERR(p) ((uintptr_t)(p) < 32)`
  - `#define TD_ERR_CODE(p) ((td_err_t)(uintptr_t)(p))`
- Functions not returning `td_t*`: return `td_err_t` directly
- Error strings: static `const char* td_err_str[]` — ~60 bytes total

---

## 13. Build System

- **CMake 3.15+** with C17 standard
- **Test framework**: munit (vendored single .c/.h pair, ~2000 lines, pure C)
- Outputs: `libteide.a` (static) + `libteide.so/.dylib/.dll` (shared)
- Single public header: `include/teide/td.h`

### Platform-specific compiler settings

| Setting | GCC/Clang (Linux) | Clang (macOS ARM64) | MSVC (Windows) |
|---------|-------------------|---------------------|----------------|
| Warnings | `-Wall -Wextra -Wpedantic -Werror` | same | `/W4 /WX` |
| Release | `-O3 -march=native -DNDEBUG` | `-O3 -mcpu=apple-m1 -DNDEBUG` | `/O2 /DNDEBUG /arch:AVX2` |
| Debug | `-O0 -g -fsanitize=address,undefined` | `-O0 -g -fsanitize=address,undefined` | `/Od /Zi /fsanitize=address` |
| Links | `-lm -lpthread` | (none needed) | (none needed) |
| C17 | `-std=c17` | `-std=c17` | `/std:c17` |

### CI matrix
- Linux x86_64: GCC 12+ and Clang 15+
- macOS ARM64: Apple Clang (Xcode 15+)
- Windows x86_64: MSVC 2022 (17.x)

### Code Style
- Prefix all public symbols with `td_`
- Internal (file-scope) functions: `static` with no prefix
- Constants: `TD_UPPER_SNAKE_CASE`
- Struct typedefs: `td_name_t`
- No `_t` suffix on non-typedef names (POSIX reserves `_t`)

---

## 14. Implementation Phases

| Phase | What | Key files | Depends on | Status |
|-------|------|-----------|------------|--------|
| **0** | Foundation | `platform.h/.c`, `types.h`, `err.h/.c`, `block.h/.c`, `CMakeLists.txt`, `td.h` | — | **DONE** |
| **1** | Buddy allocator | `buddy.h/.c`, `arena.h/.c` | Phase 0 | **DONE** |
| **2** | COW + Atoms | `cow.h/.c`, `atom.h/.c` | Phase 1 | **DONE** |
| **3** | Vectors + Strings + Lists | `vec.h/.c`, `str.h/.c`, `list.h/.c` | Phase 2 | **DONE** |
| **4** | Symbols + Table | `sym.h/.c`, `df.h/.c` | Phase 3 | **DONE** |
| **5** | Morsel infrastructure | `morsel.h/.c`, `pipe.h/.c` | Phase 3 | **DONE** |
| **6** | Op graph + Basic executor | `graph.h/.c`, `plan.h/.c`, `exec.h/.c` | Phases 4+5 | **DONE** |
| **7** | Fusion pass | `fuse.h/.c` | Phase 6 | **DONE** |
| **8** | Optimizer passes | `opt.h/.c` | Phase 7 | **DONE** |
| **9** | Storage: splayed + partitioned | `col.h/.c`, `splay.h/.c`, `part.h/.c`, `meta.h/.c` | Phases 4+5 | **DONE** |
| **9a** | CSV parser | `csv.h/.c` | Phase 4 | **DONE** |
| **10** | Joins + Advanced ops | `opt.c`, `exec.c`, `pipe.c` | Phases 8+9 | **DONE** |
| **11** | Python bindings (low-level) + Benchmark adapter | `bindings/python/teide/__init__.py`, `teide_adapter.py` | Phase 10 | **DONE** (172 C tests, 19/19 bench queries) |
| **11a** | Allocator: slab cache + direct mmap + mem stats | `buddy.h`, `arena.h/.c`, `td.h` | Phase 1 | **DONE** (4 new tests: slab_cache, direct_mmap, direct_mmap_realloc, mem_stats) |
| **11b** | Python API | `bindings/python/teide/api.py` | Phase 11 | **DONE** (Context, Table, Query, Expr, GroupBy, Series) |
| **11c** | Python API integration tests (Q1-Q7) | `test/test_api.py` | Phases 11a+11b | **TODO** |

### Algorithm notes for key phases

**Phase 9a — CSV parser**: DuckDB's parallel CSV parser (2023+) is the SOTA: a parallel state-machine approach that splits the file into chunks, uses speculative parsing with dialect sniffing, and resolves chunk boundary ambiguity via a finite automaton that tracks quote/escape state. SIMD-accelerated delimiters (simdcsv / Sep library, Nietfeld 2023) can parse at >2 GB/s per core. Our approach: parallel chunked reader with per-chunk type inference, newline-boundary resolution via small overlapping reads, and symbol interning into the concurrent intern table (Section 6.5) during parse.

**Phase 10 — Radix partitioning** (used by sort, group-by, join): Software write-combine buffers (Umbra/CedarDB) are critical for scatter-heavy partitioning — buffer 64 bytes (one cache line) per target partition before flushing, avoiding random write misses. DuckDB's adaptive partitioning selects partition fan-out based on available memory to minimize passes.

---

## Verification Plan

### Unit & Integration Tests

1. **Build**: `cmake -B build -DCMAKE_BUILD_TYPE=Debug && cmake --build build`
2. **Tests**: `cd build && ctest --output-on-failure` (runs all test_*.c via munit)
3. **ASan/UBSan**: Debug builds include sanitizers — all tests must pass clean
4. **Round-trip test**: Create table → save to disk → mmap load → verify identical
5. **Fusion correctness**: For every query, run with optimizer disabled and enabled — results must match
6. **Cross-thread test**: Spawn threads, share `td_t*` objects, verify COW + refcount correctness
7. **Larger-than-RAM test**: Create a column file larger than available RAM (e.g., use `ulimit -v` to simulate), mmap it, run a full pipeline (scan → filter → sum). Verify: peak RSS stays bounded to morsel size × pipeline width, not the full column size. Use `/proc/self/status` VmRSS to measure.
8. **Spill-to-disk test**: Sort / group-by / join on data exceeding `TD_SPILL_THRESHOLD`. Verify correct results and temp file cleanup.
9. **Microbenchmarks**: `bench_alloc` (alloc/free throughput), `bench_vec` (vectorized ops), `bench_exec` (end-to-end queries), `bench_mmap` (morsel throughput on mmap'd vs arena data)

### Acceptance Criteria: rayforce-benchmark Suite

**Readiness is defined as: all benchmarks from `../rayforce-bench/` (adapted to Teide's Python bindings) pass with correct results.**

The rayforce-benchmark framework is a Python-based benchmark suite using an adapter pattern. Teide must implement a Python adapter (`teide_adapter.py`) conforming to the `Adapter` base class.

#### Python Adapter Interface

Teide's adapter must implement:

```python
class TeideAdapter(Adapter):
    name = "teide"
    def setup(self, schema: dict) -> None          # Initialize tables from schema
    def load_csv(self, csv_paths, table_name) -> None  # Load CSV data into Teide tables
    def run(self, task: str, params: dict) -> AdapterResult  # Execute benchmark task, return timing + row_count
    def close(self) -> None                         # Cleanup resources
```

The adapter returns `AdapterResult` with `execution_time_ns`, `row_count`, and optional `checksum` for validation.

#### Required Benchmark Suites (4 suites, 19 queries total)

**1. Group-by (H2OAI standard) — 10 queries, 10M rows**

| Query | Operation |
|-------|-----------|
| Q1 | `sum(v1) group by id1` (low cardinality) |
| Q2 | `sum(v1) group by id1, id2` (low cardinality pair) |
| Q3 | `sum(v1), avg(v3) group by id3` (high cardinality) |
| Q4 | `avg(v1), avg(v2), avg(v3) group by id4` (low cardinality) |
| Q5 | `sum(v1), sum(v2), sum(v3) group by id6` (high cardinality) |
| Q6 | `max(v1) - min(v2) group by id3` (expression in aggregation) |
| Q7 | `sum(v3), count group by id1..id6` (wide group-by) |
| Q8 | `filter(v1>=3) then sum(v3) group by id2` (filter + group) |
| Q9 | `filter(v1>=2 AND v2<=8) then sum(v1,v2,v3) group by id3` (multi-predicate filter + group) |
| Q10 | `filter(v3>0) then sum(v1), sum(v2) group by id1..id4` (filter + wide group) |

Required capabilities: group-by with sum/avg/min/max/count aggregations, multi-column group keys, pre-filter + group-by fusion, expression evaluation inside aggregation.

**2. Join — 2 queries, 10M rows**

| Query | Operation |
|-------|-----------|
| Q1 | Left join on (id1, id2) |
| Q2 | Inner join on (id1, id2) |

Required capabilities: hash join (inner + left), multi-column join keys.

**3. Sort — 6 queries, 10M rows**

| Query | Operation |
|-------|-----------|
| Q1 | Sort by single low-cardinality column (id1) |
| Q2 | Sort by single high-cardinality column (id3) |
| Q3 | Sort by integer column (id4) |
| Q4 | Sort by float column descending (v3) |
| Q5 | Sort by two columns (id1, id2) |
| Q6 | Sort by three columns (id1, id2, id3) |

Required capabilities: single/multi-column sort, ascending/descending, sort on string/int/float types.

**4. Window Join — 1 query, 10M rows**

| Query | Operation |
|-------|-----------|
| Q1 | Window join (+/- 10 seconds): min(Bid), max(Ask) within time window |

Required capabilities: time-series window join (wj1), aggregations within time windows, keyed by symbol + timestamp.

#### Operations Required by Benchmarks

The benchmark suite collectively requires these operations to be functional in the lazy fusion API:

- **Scan**: `OP_SCAN` — load CSV / bind columns
- **Filter**: `OP_FILTER` — predicate evaluation (`>=`, `<=`, `>`, `AND`)
- **Group-by**: `OP_GROUP` — with `sum`, `avg` (`mean`), `min`, `max`, `count` aggregations
- **Join**: `OP_JOIN` — inner join, left join, multi-column keys
- **Sort**: `OP_SORT` — single/multi-column, ascending/descending
- **Window Join**: extension op — time-window-bounded aggregation join
- **Arithmetic**: `OP_ADD`, `OP_SUB`, `OP_MUL` — for expressions like `max(v1) - min(v2)`
- **Comparison**: `OP_GE`, `OP_LE`, `OP_GT`, `OP_AND` — for filter predicates

#### Acceptance Pass Criteria

- All 19 queries across 4 suites produce **correct results** (validated by row count, optionally checksum)
- All queries complete **without errors** (`AdapterResult.success == True`)
- No memory leaks under ASan (run benchmarks under Debug build)
- Python bindings load `libteide.so` via ctypes and expose the full lazy API

---

## Open Design Points for Future Discussion

1. **String vectors**: Currently LIST of string atoms (simple, uniform COW). Arrow-style offset arrays would have better cache locality for string-heavy workloads — revisit if profiling shows this matters.
2. ~~Global symbol intern table~~: **Resolved** — concurrent lock-free intern table (Section 6.5). Any thread can intern new symbols (e.g., parallel CSV parsing). Lock-free hash map + append-only array with sequential mode optimization.
3. **Large null bitmaps**: External bitmap allocation for vectors > 128 elements with nulls — profile to see if the indirection matters.
4. **Morsel size tuning**: 1024 elements is a starting point. May need per-type tuning (e.g., 2048 for booleans, 512 for GUIDs) based on L1 cache size. Could also be runtime-configurable per arena.
5. ~~madvise portability~~: **Resolved** — abstracted behind `td_vm_release()` / `td_vm_advise_seq()` in `platform.h` (Section 3).
6. **Lazy COW granularity**: `td_cow_lazy()` for large mmap'd vectors — what triggers the copy? Options: (a) copy morsel on first write to that morsel's range, (b) copy entire vector on first write but morsel-by-morsel. Option (a) is more complex but avoids copying untouched regions.
7. ~~Small alloc performance~~: **Resolved** — slab cache (Section 5) for orders 5-9 (32B-512B). LIFO stack avoids buddy walk for the hottest allocation sizes.
8. ~~Large alloc OOM~~: **Resolved** — direct mmap (Section 5) for allocations > 1 GiB. `mmod=2` blocks bypass buddy entirely. Enables Q7 6-key groupby on 10M rows.
9. **VM reserve+commit**: Rayforce-style `mmap_reserve` + `mmap_commit` avoids wasting physical pages on arena over-allocation. Deferred.
10. **Huge pages**: `MAP_HUGETLB` for large arenas to improve TLB coverage. Deferred.
11. **`MAP_NORESERVE` on arena mmap**: ngn/k's allocator uses `MAP_NORESERVE|MAP_PRIVATE|MAP_ANON`. Adding `MAP_NORESERVE` to `td_vm_alloc()` tells Linux not to check swap/RAM at mmap time — useful since arenas allocate up to 1GiB virtual but may only touch a fraction. Prevents premature OOM on overcommit-disabled systems. One-line change in `platform.c`.
12. **Byte-indexed grouping for low-cardinality keys**: ngn/k's `grp()` uses direct array indexing when value range is small, hash-based for wide distributions. For group-by with known small cardinality (e.g. `id1` with 100 groups), a direct-indexed accumulator array avoids all hashing overhead. Add cardinality estimation to group-by executor, branch to array-indexed path when `est_groups < threshold` (e.g. 4096).
13. **Float sign-bit-flip for radix sort**: ngn/k's `of_()` encodes floats as unsigned integers preserving sort order (IEEE 754 trick: flip all bits if negative, flip only sign bit if positive). Enables radix sort on F64 columns without comparison-based sorting. Specific encoding technique for the normalized-key sort described in Section 8.4.
14. **Insertion sort cutoff**: ngn/k's merge sort falls back to insertion sort at n<17. Standard optimization — merge sort has high constant factors for small N. Apply to our sort executor's partition-level sort if not already present.
