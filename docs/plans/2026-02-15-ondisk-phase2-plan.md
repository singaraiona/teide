# Phase 2: Parted Types & td_part_open — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement parted column types and `td_part_open` so a multi-partition table appears as a single table with zero-copy mmap'd segments.

**Architecture:** A parted column wraps N mmap'd segment vectors (one per partition) behind a composite type tag (`TD_PARTED_BASE + base_type`). A virtual `TD_MAPCOMMON` column stores partition key values and row counts with zero per-row storage. `td_part_open` builds this structure from on-disk partitions. Inner loops never see parted types — they're unwrapped at the executor's outer dispatch level (Phase 3+).

**Tech Stack:** C17, no external deps, buddy allocator, munit test framework.

---

### Task 1: Add parted type constants and helper macros to td.h

**Files:**
- Modify: `include/teide/td.h:73-119` (type constants section)

**Step 1: Add the type constants and macros**

After `#define TD_ENUM 15` and before `/* Atom variants */`, add:

```c
/* Parted types: composite of TD_PARTED_BASE + base type */
#define TD_PARTED_BASE   32
#define TD_MAPCOMMON     48   /* virtual partition column */

#define TD_IS_PARTED(t)       ((t) >= TD_PARTED_BASE && (t) < TD_MAPCOMMON)
#define TD_PARTED_BASETYPE(t) ((t) - TD_PARTED_BASE)
```

**Step 2: Add `td_parted_nrows` declaration to the public API**

In the Table API section (near line 547), add:

```c
int64_t     td_parted_nrows(td_t* parted_col);
```

**Step 3: Add `td_part_open` declaration**

Replace the existing `td_part_load` declaration with both:

```c
td_t*    td_part_load(const char* db_root, const char* table_name);
td_t*    td_part_open(const char* db_root, const char* table_name);
```

**Step 4: Build to verify no syntax errors**

Run: `cmake --build build 2>&1 | tail -20`
Expected: Compiles cleanly (new symbols declared but not yet defined).

**Step 5: Commit**

```bash
git add include/teide/td.h
git commit -m "feat: add parted type constants TD_PARTED_BASE, TD_MAPCOMMON, TD_IS_PARTED macros"
```

---

### Task 2: Implement td_parted_nrows in table.c

**Files:**
- Modify: `src/table/table.c` (add function after `td_table_nrows`)

**Step 1: Write the failing test**

Add to `test/test_store.c` before the suite definition (`store_tests[]`):

```c
/* ---- test_parted_nrows ------------------------------------------------- */

static MunitResult test_parted_nrows(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build a parted column with 3 segments: 100, 200, 300 rows */
    int64_t lens[] = {100, 200, 300};
    td_t* segs[3];
    for (int i = 0; i < 3; i++) {
        segs[i] = td_vec_new(TD_I64, lens[i]);
        munit_assert_false(TD_IS_ERR(segs[i]));
        segs[i]->len = lens[i];
    }

    /* Allocate parted column: type = TD_PARTED_BASE + TD_I64, len = 3 */
    td_t* parted = td_alloc(3 * sizeof(td_t*));
    munit_assert_ptr_not_null(parted);
    parted->type = TD_PARTED_BASE + TD_I64;
    parted->len = 3;
    parted->attrs = 0;
    memset(parted->nullmap, 0, 16);

    td_t** seg_ptrs = (td_t**)td_data(parted);
    for (int i = 0; i < 3; i++) {
        td_retain(segs[i]);
        seg_ptrs[i] = segs[i];
    }

    /* td_parted_nrows should return 600 */
    int64_t total = td_parted_nrows(parted);
    munit_assert_int64(total, ==, 600);

    /* Cleanup */
    for (int i = 0; i < 3; i++) td_release(segs[i]);
    /* Release parted col — we'll handle segment release manually here */
    for (int i = 0; i < 3; i++) td_release(seg_ptrs[i]);
    td_free(parted);

    return MUNIT_OK;
}
```

Register it in `store_tests[]`:
```c
    { "/parted_nrows",         test_parted_nrows,         store_setup, store_teardown, 0, NULL },
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /store/parted_nrows`
Expected: FAIL — `td_parted_nrows` is declared but not defined (linker error).

**Step 3: Implement td_parted_nrows**

In `src/table/table.c`, after `td_table_nrows`:

```c
/* --------------------------------------------------------------------------
 * td_parted_nrows — total rows across all segments of a parted column
 * -------------------------------------------------------------------------- */

int64_t td_parted_nrows(td_t* v) {
    if (!v || TD_IS_ERR(v)) return 0;
    if (!TD_IS_PARTED(v->type) && v->type != TD_MAPCOMMON) return v->len;

    if (v->type == TD_MAPCOMMON) {
        /* MAPCOMMON: [key_values, row_counts] — sum row_counts */
        td_t** ptrs = (td_t**)td_data(v);
        td_t* counts = ptrs[1];
        if (!counts || TD_IS_ERR(counts)) return 0;
        int64_t total = 0;
        int64_t* cdata = (int64_t*)td_data(counts);
        for (int64_t i = 0; i < counts->len; i++)
            total += cdata[i];
        return total;
    }

    /* Parted column: sum segment lengths */
    int64_t n_segs = v->len;
    td_t** segs = (td_t**)td_data(v);
    int64_t total = 0;
    for (int64_t i = 0; i < n_segs; i++) {
        if (segs[i] && !TD_IS_ERR(segs[i]))
            total += segs[i]->len;
    }
    return total;
}
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build && ./build/test_teide --suite /store/parted_nrows`
Expected: PASS

**Step 5: Commit**

```bash
git add src/table/table.c test/test_store.c
git commit -m "feat: implement td_parted_nrows — sum segment lengths for parted columns"
```

---

### Task 3: Update td_table_nrows for parted columns

**Files:**
- Modify: `src/table/table.c:186-195` (td_table_nrows function)

**Step 1: Write the failing test**

Add to `test/test_store.c` before the suite definition:

```c
/* ---- test_table_nrows_parted ------------------------------------------- */

static MunitResult test_table_nrows_parted(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build 2 segment vectors of I64 with 50 and 75 rows */
    td_t* seg0 = td_vec_new(TD_I64, 50);
    td_t* seg1 = td_vec_new(TD_I64, 75);
    munit_assert_false(TD_IS_ERR(seg0));
    munit_assert_false(TD_IS_ERR(seg1));
    seg0->len = 50;
    seg1->len = 75;

    /* Build parted column */
    td_t* parted = td_alloc(2 * sizeof(td_t*));
    munit_assert_ptr_not_null(parted);
    parted->type = TD_PARTED_BASE + TD_I64;
    parted->len = 2;
    parted->attrs = 0;
    memset(parted->nullmap, 0, 16);

    td_t** seg_ptrs = (td_t**)td_data(parted);
    td_retain(seg0); seg_ptrs[0] = seg0;
    td_retain(seg1); seg_ptrs[1] = seg1;

    /* Build table containing this parted column */
    td_t* tbl = td_table_new(2);
    munit_assert_false(TD_IS_ERR(tbl));
    int64_t name_id = td_sym_intern("x", 1);
    tbl = td_table_add_col(tbl, name_id, parted);
    munit_assert_false(TD_IS_ERR(tbl));

    /* td_table_nrows should return 125 */
    int64_t nrows = td_table_nrows(tbl);
    munit_assert_int64(nrows, ==, 125);

    /* Cleanup */
    td_release(tbl);
    td_release(seg0);
    td_release(seg1);
    /* parted col owns refs to segments via table */

    return MUNIT_OK;
}
```

Register in `store_tests[]`:
```c
    { "/table_nrows_parted",   test_table_nrows_parted,   store_setup, store_teardown, 0, NULL },
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /store/table_nrows_parted`
Expected: FAIL — `td_table_nrows` returns `parted->len` (= 2, the segment count), not 125.

**Step 3: Update td_table_nrows**

In `src/table/table.c`, replace the `td_table_nrows` body:

```c
int64_t td_table_nrows(td_t* tbl) {
    if (!tbl || TD_IS_ERR(tbl)) return 0;
    if (tbl->len <= 0) return 0;

    td_t** cols = tbl_col_slots(tbl);
    td_t* first_col = cols[0];
    if (!first_col || TD_IS_ERR(first_col)) return 0;

    /* Parted or MAPCOMMON column: delegate to td_parted_nrows */
    if (TD_IS_PARTED(first_col->type) || first_col->type == TD_MAPCOMMON)
        return td_parted_nrows(first_col);

    return first_col->len;
}
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build && ./build/test_teide --suite /store/table_nrows_parted`
Expected: PASS

**Step 5: Run full test suite to check no regressions**

Run: `cd build && ctest --output-on-failure`
Expected: All existing tests pass.

**Step 6: Commit**

```bash
git add src/table/table.c test/test_store.c
git commit -m "feat: update td_table_nrows to handle parted columns via td_parted_nrows"
```

---

### Task 4: Update td_free (arena.c) and td_release_owned_refs for parted types

**Files:**
- Modify: `src/mem/arena.c:312-350` (td_release_owned_refs) and `src/mem/arena.c:460-468` (mmod==1 block in td_free)

**Step 1: Write the failing test**

Add to `test/test_store.c`:

```c
/* ---- test_parted_release ----------------------------------------------- */

static MunitResult test_parted_release(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Build 2 segment vectors */
    int64_t raw0[] = {1, 2, 3};
    int64_t raw1[] = {4, 5};
    td_t* seg0 = td_vec_from_raw(TD_I64, raw0, 3);
    td_t* seg1 = td_vec_from_raw(TD_I64, raw1, 2);
    munit_assert_false(TD_IS_ERR(seg0));
    munit_assert_false(TD_IS_ERR(seg1));

    /* Build parted column */
    td_t* parted = td_alloc(2 * sizeof(td_t*));
    munit_assert_ptr_not_null(parted);
    parted->type = TD_PARTED_BASE + TD_I64;
    parted->len = 2;
    parted->attrs = 0;
    memset(parted->nullmap, 0, 16);

    td_t** seg_ptrs = (td_t**)td_data(parted);
    td_retain(seg0); seg_ptrs[0] = seg0;
    td_retain(seg1); seg_ptrs[1] = seg1;

    /* Verify segments have rc=2 (our ref + parted ref) */
    munit_assert_uint(atomic_load_explicit(&seg0->rc, memory_order_relaxed), ==, 2);
    munit_assert_uint(atomic_load_explicit(&seg1->rc, memory_order_relaxed), ==, 2);

    /* Release parted col — should release each segment */
    td_release(parted);

    /* Segments should now have rc=1 (our ref only) */
    munit_assert_uint(atomic_load_explicit(&seg0->rc, memory_order_relaxed), ==, 1);
    munit_assert_uint(atomic_load_explicit(&seg1->rc, memory_order_relaxed), ==, 1);

    td_release(seg0);
    td_release(seg1);

    return MUNIT_OK;
}
```

Register in `store_tests[]`:
```c
    { "/parted_release",       test_parted_release,       store_setup, store_teardown, 0, NULL },
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /store/parted_release`
Expected: FAIL — segments still have rc=2 after releasing parted col (segments not released).

**Step 3: Add parted type handling to td_release_owned_refs**

In `src/mem/arena.c`, in `td_release_owned_refs`, add a parted-column path **before** the TABLE path. Find the section after the `TD_ATTR_NULLMAP_EXT` block and the TABLE check. Add:

```c
    /* Parted column: release all segment vectors */
    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++) {
            if (segs[i] && !TD_IS_ERR(segs[i]))
                td_release(segs[i]);
        }
        return;
    }

    /* MAPCOMMON: release key_values and row_counts vectors */
    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        if (ptrs[0] && !TD_IS_ERR(ptrs[0])) td_release(ptrs[0]);
        if (ptrs[1] && !TD_IS_ERR(ptrs[1])) td_release(ptrs[1]);
        return;
    }
```

**Step 4: Update the mmod==1 block in td_free for parted types**

The existing `mmod==1` path in `td_free` computes `mapped_size = 32 + len * elem_size`. This won't work for parted types (type >= 32 is outside `td_type_sizes[]`). Add a guard:

In `td_free`, the `mmod == 1` block currently reads:
```c
    if (v->mmod == 1) {
        if (v->type > 0 && v->type < TD_TYPE_COUNT) {
```

This is already safe — parted columns have `mmod=0` (buddy-allocated wrapper), and their segment vectors have `mmod=1` with base types (< TD_TYPE_COUNT). So no change needed here. The check `v->type < TD_TYPE_COUNT` already guards against parted types accidentally entering this path.

**Step 5: Also add parted handling to td_retain_owned_refs**

In `td_retain_owned_refs`, add the same pattern (before the TABLE check):

```c
    /* Parted column: retain all segment vectors */
    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++) {
            if (segs[i] && !TD_IS_ERR(segs[i]))
                td_retain(segs[i]);
        }
        return;
    }

    /* MAPCOMMON: retain key_values and row_counts vectors */
    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        if (ptrs[0] && !TD_IS_ERR(ptrs[0])) td_retain(ptrs[0]);
        if (ptrs[1] && !TD_IS_ERR(ptrs[1])) td_retain(ptrs[1]);
        return;
    }
```

**Step 6: Also update td_alloc_copy for parted types**

In `td_alloc_copy` (arena.c ~line 503), the `data_size` calculation uses `td_elem_size(t)` which indexes `td_type_sizes[t]` — would be out-of-bounds for parted types. Add a guard before the existing vector path:

```c
    } else if (TD_IS_PARTED(v->type) || v->type == TD_MAPCOMMON) {
        /* Parted/MAPCOMMON: data is pointer array */
        int64_t n_ptrs = v->len;
        if (v->type == TD_MAPCOMMON) n_ptrs = 2;
        data_size = (size_t)n_ptrs * sizeof(td_t*);
    } else {
```

**Step 7: Run test to verify it passes**

Run: `cmake --build build && ./build/test_teide --suite /store/parted_release`
Expected: PASS

**Step 8: Run full test suite**

Run: `cd build && ctest --output-on-failure`
Expected: All tests pass.

**Step 9: Commit**

```bash
git add src/mem/arena.c test/test_store.c
git commit -m "feat: handle parted/MAPCOMMON types in release, retain, and alloc_copy"
```

---

### Task 5: Implement td_part_open in part.c

**Files:**
- Modify: `src/store/part.c` (add `td_part_open` function)

**Step 1: Write the failing test**

Add to `test/test_store.c`:

```c
#define TMP_PART_DB "/tmp/teide_test_parted_db"
#define TMP_TABLE_NAME "test_tbl"

/* ---- test_part_open ---------------------------------------------------- */

static MunitResult test_part_open(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Setup: create a 2-partition db with 2 columns each */
    (void)!system("rm -rf " TMP_PART_DB);
    (void)!system("mkdir -p " TMP_PART_DB "/2024.01.01/" TMP_TABLE_NAME);
    (void)!system("mkdir -p " TMP_PART_DB "/2024.01.02/" TMP_TABLE_NAME);

    /* Partition 1: 3 rows */
    int64_t raw_a1[] = {10, 20, 30};
    double  raw_b1[] = {1.1, 2.2, 3.3};
    td_t* a1 = td_vec_from_raw(TD_I64, raw_a1, 3);
    td_t* b1 = td_vec_from_raw(TD_F64, raw_b1, 3);

    td_t* tbl1 = td_table_new(3);
    int64_t name_a = td_sym_intern("a", 1);
    int64_t name_b = td_sym_intern("b", 1);
    tbl1 = td_table_add_col(tbl1, name_a, a1);
    tbl1 = td_table_add_col(tbl1, name_b, b1);
    td_err_t err = td_splay_save(tbl1, TMP_PART_DB "/2024.01.01/" TMP_TABLE_NAME);
    munit_assert_int(err, ==, TD_OK);

    /* Partition 2: 5 rows */
    int64_t raw_a2[] = {40, 50, 60, 70, 80};
    double  raw_b2[] = {4.4, 5.5, 6.6, 7.7, 8.8};
    td_t* a2 = td_vec_from_raw(TD_I64, raw_a2, 5);
    td_t* b2 = td_vec_from_raw(TD_F64, raw_b2, 5);

    td_t* tbl2 = td_table_new(3);
    tbl2 = td_table_add_col(tbl2, name_a, a2);
    tbl2 = td_table_add_col(tbl2, name_b, b2);
    err = td_splay_save(tbl2, TMP_PART_DB "/2024.01.02/" TMP_TABLE_NAME);
    munit_assert_int(err, ==, TD_OK);

    /* Save symfile */
    err = td_sym_save(TMP_PART_DB "/sym");
    munit_assert_int(err, ==, TD_OK);

    /* Cleanup in-memory tables */
    td_release(a1); td_release(b1); td_release(tbl1);
    td_release(a2); td_release(b2); td_release(tbl2);

    /* Open via td_part_open */
    td_t* parted = td_part_open(TMP_PART_DB, TMP_TABLE_NAME);
    munit_assert_ptr_not_null(parted);
    munit_assert_false(TD_IS_ERR(parted));

    /* Should have 3 columns: a (parted I64), b (parted F64), __part (MAPCOMMON) */
    int64_t ncols = td_table_ncols(parted);
    munit_assert_int64(ncols, ==, 3);

    /* Total rows should be 8 */
    int64_t nrows = td_table_nrows(parted);
    munit_assert_int64(nrows, ==, 8);

    /* Verify first column is parted I64 */
    td_t* col_a = td_table_get_col_idx(parted, 0);
    munit_assert_ptr_not_null(col_a);
    munit_assert_true(TD_IS_PARTED(col_a->type));
    munit_assert_int(TD_PARTED_BASETYPE(col_a->type), ==, TD_I64);
    munit_assert_int64(col_a->len, ==, 2); /* 2 segments */

    /* Verify segment 0 has 3 rows, mmod=1 */
    td_t** segs_a = (td_t**)td_data(col_a);
    munit_assert_int64(segs_a[0]->len, ==, 3);
    munit_assert_uint(segs_a[0]->mmod, ==, 1);
    munit_assert_int64(segs_a[1]->len, ==, 5);
    munit_assert_uint(segs_a[1]->mmod, ==, 1);

    /* Verify data in segment 0 */
    int64_t* data_a0 = (int64_t*)td_data(segs_a[0]);
    munit_assert_int64(data_a0[0], ==, 10);
    munit_assert_int64(data_a0[2], ==, 30);

    /* Verify MAPCOMMON column (last column) */
    td_t* mapcommon = td_table_get_col_idx(parted, ncols - 1);
    munit_assert_ptr_not_null(mapcommon);
    munit_assert_int(mapcommon->type, ==, TD_MAPCOMMON);

    /* MAPCOMMON: [key_values, row_counts] */
    td_t** mc_ptrs = (td_t**)td_data(mapcommon);
    td_t* row_counts = mc_ptrs[1];
    munit_assert_int64(row_counts->len, ==, 2);
    int64_t* rc_data = (int64_t*)td_data(row_counts);
    munit_assert_int64(rc_data[0], ==, 3);
    munit_assert_int64(rc_data[1], ==, 5);

    /* Release — should unmap all segments */
    td_release(parted);

    (void)!system("rm -rf " TMP_PART_DB);
    return MUNIT_OK;
}
```

Register in `store_tests[]`:
```c
    { "/part_open",            test_part_open,            store_setup, store_teardown, 0, NULL },
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build && ./build/test_teide --suite /store/part_open`
Expected: FAIL — linker error, `td_part_open` not defined.

**Step 3: Implement td_part_open**

In `src/store/part.c`, add after `td_part_load`:

```c
/* --------------------------------------------------------------------------
 * td_part_open — open a partitioned table with zero-copy parted columns
 *
 * Builds a table where each column is a TD_PARTED_* wrapper holding
 * mmap'd segment vectors (one per partition). Adds a TD_MAPCOMMON
 * virtual partition column as the last column.
 * -------------------------------------------------------------------------- */

td_t* td_part_open(const char* db_root, const char* table_name) {
    if (!db_root || !table_name) return TD_ERR_PTR(TD_ERR_IO);

    /* Validate table_name: no path separators or traversal */
    if (strchr(table_name, '/') || strchr(table_name, '\\') ||
        strstr(table_name, "..") || table_name[0] == '.')
        return TD_ERR_PTR(TD_ERR_IO);

    /* Load symfile */
    char sym_path[1024];
    snprintf(sym_path, sizeof(sym_path), "%s/sym", db_root);
    td_err_t err = td_sym_load(sym_path);
    if (err != TD_OK) return TD_ERR_PTR(err);

    /* Scan db_root for partition directories (digits + dots only) */
    DIR* d = opendir(db_root);
    if (!d) return TD_ERR_PTR(TD_ERR_IO);

    char** part_dirs = NULL;
    int64_t part_count = 0;
    int64_t part_cap = 0;

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        if (ent->d_name[0] == '.') continue;
        bool valid = false;
        for (const char* c = ent->d_name; *c; c++) {
            if (*c == '.') { valid = true; continue; }
            if (*c >= '0' && *c <= '9') continue;
            valid = false; break;
        }
        if (!valid) continue;

        if (part_count >= part_cap) {
            part_cap = part_cap == 0 ? 16 : part_cap * 2;
            char** tmp = (char**)td_sys_realloc(part_dirs, (size_t)part_cap * sizeof(char*));
            if (!tmp) break;
            part_dirs = tmp;
        }
        char* dup = td_sys_strdup(ent->d_name);
        if (!dup) break;
        part_dirs[part_count++] = dup;
    }
    closedir(d);

    if (part_count == 0) {
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_IO);
    }

    /* Sort partition names */
    for (int64_t i = 0; i < part_count - 1; i++)
        for (int64_t j = i + 1; j < part_count; j++)
            if (strcmp(part_dirs[i], part_dirs[j]) > 0) {
                char* tmp = part_dirs[i];
                part_dirs[i] = part_dirs[j];
                part_dirs[j] = tmp;
            }

    /* Open each partition via td_splay_open (zero-copy mmap) */
    td_t** part_tables = (td_t**)td_sys_alloc((size_t)part_count * sizeof(td_t*));
    if (!part_tables) {
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    char path[1024];
    for (int64_t p = 0; p < part_count; p++) {
        snprintf(path, sizeof(path), "%s/%s/%s", db_root, part_dirs[p], table_name);
        part_tables[p] = td_splay_open(path, sym_path);
        if (!part_tables[p] || TD_IS_ERR(part_tables[p])) {
            /* Cleanup on failure */
            for (int64_t k = 0; k < p; k++) td_release(part_tables[k]);
            td_sys_free(part_tables);
            for (int64_t k = 0; k < part_count; k++) td_sys_free(part_dirs[k]);
            td_sys_free(part_dirs);
            return TD_ERR_PTR(TD_ERR_IO);
        }
    }

    /* Get schema from first partition */
    int64_t ncols = td_table_ncols(part_tables[0]);
    if (ncols <= 0) {
        for (int64_t p = 0; p < part_count; p++) td_release(part_tables[p]);
        td_sys_free(part_tables);
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_SCHEMA);
    }

    /* Build result table: ncols parted columns + 1 MAPCOMMON */
    td_t* result = td_table_new(ncols + 2);
    if (!result || TD_IS_ERR(result)) {
        for (int64_t p = 0; p < part_count; p++) td_release(part_tables[p]);
        td_sys_free(part_tables);
        for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
        td_sys_free(part_dirs);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    /* For each column: build a parted wrapper */
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = td_table_col_name(part_tables[0], c);
        td_t* first_seg = td_table_get_col_idx(part_tables[0], c);
        int8_t base_type = first_seg->type;

        /* Allocate parted column */
        td_t* parted_col = td_alloc((size_t)part_count * sizeof(td_t*));
        if (!parted_col) goto fail;
        parted_col->type = TD_PARTED_BASE + base_type;
        parted_col->len = part_count;
        parted_col->attrs = 0;
        memset(parted_col->nullmap, 0, 16);

        td_t** seg_ptrs = (td_t**)td_data(parted_col);
        for (int64_t p = 0; p < part_count; p++) {
            td_t* seg = td_table_get_col_idx(part_tables[p], c);
            td_retain(seg);
            seg_ptrs[p] = seg;
        }

        result = td_table_add_col(result, name_id, parted_col);
        td_release(parted_col);
        if (TD_IS_ERR(result)) goto fail;
    }

    /* Build MAPCOMMON virtual partition column */
    {
        /* key_values: SYM vector of partition directory names (as symbol IDs) */
        td_t* key_values = td_vec_new(TD_SYM, part_count);
        if (!key_values || TD_IS_ERR(key_values)) goto fail;
        int64_t* kv_data = (int64_t*)td_data(key_values);
        for (int64_t p = 0; p < part_count; p++) {
            kv_data[p] = td_sym_intern(part_dirs[p], strlen(part_dirs[p]));
        }
        key_values->len = part_count;

        /* row_counts: I64 vector of per-partition row counts */
        td_t* row_counts = td_vec_new(TD_I64, part_count);
        if (!row_counts || TD_IS_ERR(row_counts)) {
            td_release(key_values);
            goto fail;
        }
        int64_t* rc_data = (int64_t*)td_data(row_counts);
        for (int64_t p = 0; p < part_count; p++) {
            rc_data[p] = td_table_nrows(part_tables[p]);
        }
        row_counts->len = part_count;

        /* Allocate MAPCOMMON column: 2 pointers */
        td_t* mapcommon = td_alloc(2 * sizeof(td_t*));
        if (!mapcommon) {
            td_release(key_values);
            td_release(row_counts);
            goto fail;
        }
        mapcommon->type = TD_MAPCOMMON;
        mapcommon->len = 2;
        mapcommon->attrs = 0;
        memset(mapcommon->nullmap, 0, 16);

        td_t** mc_ptrs = (td_t**)td_data(mapcommon);
        mc_ptrs[0] = key_values;    /* owned */
        mc_ptrs[1] = row_counts;    /* owned */

        int64_t part_name_id = td_sym_intern("__part", 6);
        result = td_table_add_col(result, part_name_id, mapcommon);
        td_release(mapcommon);
        if (TD_IS_ERR(result)) goto fail;
    }

    /* Cleanup temporaries (partition sub-tables still alive via segment refs) */
    for (int64_t p = 0; p < part_count; p++) td_release(part_tables[p]);
    td_sys_free(part_tables);
    for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
    td_sys_free(part_dirs);

    return result;

fail:
    if (result && !TD_IS_ERR(result)) td_release(result);
    for (int64_t p = 0; p < part_count; p++) {
        if (part_tables[p] && !TD_IS_ERR(part_tables[p]))
            td_release(part_tables[p]);
    }
    td_sys_free(part_tables);
    for (int64_t i = 0; i < part_count; i++) td_sys_free(part_dirs[i]);
    td_sys_free(part_dirs);
    return TD_ERR_PTR(TD_ERR_OOM);
}
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build && ./build/test_teide --suite /store/part_open`
Expected: PASS

**Step 5: Run full test suite**

Run: `cd build && ctest --output-on-failure`
Expected: All tests pass.

**Step 6: Commit**

```bash
git add src/store/part.c test/test_store.c
git commit -m "feat: implement td_part_open — zero-copy parted table from mmap'd partitions"
```

---

### Task 6: Add Python binding and update bench_query_parted.py

**Files:**
- Modify: `py/teide/__init__.py` (add `part_open` binding)
- Modify: `bench_query_parted.py` (use `td_part_open` instead of per-partition iteration)

**Step 1: Add part_open binding to Python**

In `py/teide/__init__.py`, in `_setup_signatures`:
```python
lib.td_part_open.argtypes = [ctypes.c_char_p, ctypes.c_char_p]
lib.td_part_open.restype = c_td_p
```

Add convenience method:
```python
def part_open(self, db_root, table_name):
    return self._lib.td_part_open(
        db_root.encode() if isinstance(db_root, str) else db_root,
        table_name.encode() if isinstance(table_name, str) else table_name
    )
```

**Step 2: Update bench_query_parted.py**

Replace the per-partition query loop with a single `td_part_open` call and run all queries on the unified parted table. The script should:

1. `td_part_open(db_root, TABLE_NAME)` — returns parted table
2. Print nrows (should be 10M total)
3. Run all 7 groupby queries on the parted table (for now they won't produce correct merged results — Phase 3 is the partition-aware executor. But verifying that `td_part_open` + `td_table_nrows` works end-to-end is the goal.)
4. Also keep the per-partition mode as a fallback for comparison

**Step 3: Test manually**

Run:
```bash
TEIDE_LIB=build_release/libteide.so python bench_query_parted.py --db /tmp/teide_db
```
Expected: Opens parted table, reports 10M total rows, 3 columns per data column + 1 MAPCOMMON. Per-partition queries still work.

**Step 4: Commit**

```bash
git add py/teide/__init__.py bench_query_parted.py
git commit -m "feat: add td_part_open Python binding, update bench_query_parted.py"
```

---

### Task 7: Final verification — full test suite + bench

**Step 1: Rebuild both debug and release**

```bash
cmake --build build && cmake --build build_release
```

**Step 2: Run full C test suite**

```bash
cd build && ctest --output-on-failure
```
Expected: All tests pass (existing 159+ tests + new parted tests).

**Step 3: Run bench to verify no perf regressions**

```bash
TEIDE_LIB=build_release/libteide.so python bench_run.py
```
Expected: All 21 queries produce correct results, timings within normal variance.

**Step 4: Run parted bench**

```bash
TEIDE_LIB=build_release/libteide.so python bench_query_parted.py --db /tmp/teide_db
```
Expected: Parted table opens with 10M total rows, zero-copy segments.
