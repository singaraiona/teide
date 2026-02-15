/*
 *   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
 *   All rights reserved.

 *   Permission is hereby granted, free of charge, to any person obtaining a copy
 *   of this software and associated documentation files (the "Software"), to deal
 *   in the Software without restriction, including without limitation the rights
 *   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *   copies of the Software, and to permit persons to whom the Software is
 *   furnished to do so, subject to the following conditions:

 *   The above copyright notice and this permission notice shall be included in all
 *   copies or substantial portions of the Software.

 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *   SOFTWARE.
 */

#include "munit.h"
#include <teide/td.h>
#include <stdatomic.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

#define TMP_COL_PATH  "/tmp/teide_test_col.dat"
#define TMP_SPLAY_DIR "/tmp/teide_test_splay"

/* ---- Setup / Teardown -------------------------------------------------- */

static void* store_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_arena_init();
    td_sym_init();
    return NULL;
}

static void store_teardown(void* fixture) {
    (void)fixture;
    td_sym_destroy();
    td_arena_destroy_all();
}

/* ---- test_col_mmap_i64 ------------------------------------------------- */

static MunitResult test_col_mmap_i64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[] = {10, 20, 30, 40, 50};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 5);
    munit_assert_ptr_not_null(vec);
    munit_assert_false(TD_IS_ERR(vec));

    /* Save to file */
    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    /* Load via mmap */
    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_ptr_not_null(mapped);
    munit_assert_false(TD_IS_ERR(mapped));

    /* Verify mmod==1 */
    munit_assert_uint(mapped->mmod, ==, 1);

    /* Verify type, len, data */
    munit_assert_int(mapped->type, ==, TD_I64);
    munit_assert_int(mapped->len, ==, 5);

    int64_t* data = (int64_t*)td_data(mapped);
    for (int i = 0; i < 5; i++) {
        munit_assert_int(data[i], ==, raw[i]);
    }

    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_f64 ------------------------------------------------- */

static MunitResult test_col_mmap_f64(const void* params, void* fixture) {
    (void)params; (void)fixture;

    double raw[] = {1.1, 2.2, 3.3, 4.4};
    td_t* vec = td_vec_from_raw(TD_F64, raw, 4);
    munit_assert_ptr_not_null(vec);
    munit_assert_false(TD_IS_ERR(vec));

    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_ptr_not_null(mapped);
    munit_assert_false(TD_IS_ERR(mapped));

    munit_assert_uint(mapped->mmod, ==, 1);
    munit_assert_int(mapped->type, ==, TD_F64);
    munit_assert_int(mapped->len, ==, 4);

    double* data = (double*)td_data(mapped);
    for (int i = 0; i < 4; i++) {
        munit_assert_double(data[i], ==, raw[i]);
    }

    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_cow ------------------------------------------------- */

static MunitResult test_col_mmap_cow(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[] = {100, 200, 300};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 3);
    munit_assert_false(TD_IS_ERR(vec));

    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_false(TD_IS_ERR(mapped));
    munit_assert_uint(mapped->mmod, ==, 1);

    /* Retain so rc==2, forcing td_cow to make a real copy */
    td_retain(mapped);
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 2);

    /* COW: td_cow should produce a buddy-allocated copy */
    td_t* copy = td_cow(mapped);
    munit_assert_ptr_not_null(copy);
    munit_assert_false(TD_IS_ERR(copy));
    munit_assert_uint(copy->mmod, ==, 0);

    /* td_cow called td_release on mapped (rc 2->1), so mapped still alive */

    /* Verify data in copy */
    int64_t* data = (int64_t*)td_data(copy);
    for (int i = 0; i < 3; i++) {
        munit_assert_int(data[i], ==, raw[i]);
    }

    td_release(copy);
    td_release(mapped);
    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_refcount -------------------------------------------- */

static MunitResult test_col_mmap_refcount(const void* params, void* fixture) {
    (void)params; (void)fixture;

    int64_t raw[] = {7, 8, 9};
    td_t* vec = td_vec_from_raw(TD_I64, raw, 3);
    munit_assert_false(TD_IS_ERR(vec));

    td_err_t err = td_col_save(vec, TMP_COL_PATH);
    munit_assert_int(err, ==, TD_OK);

    td_t* mapped = td_col_mmap(TMP_COL_PATH);
    munit_assert_false(TD_IS_ERR(mapped));
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 1);

    /* Retain: rc should be 2 */
    td_retain(mapped);
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 2);

    /* Release once: rc==1, still readable */
    td_release(mapped);
    munit_assert_uint(atomic_load_explicit(&mapped->rc, memory_order_relaxed), ==, 1);

    int64_t* data = (int64_t*)td_data(mapped);
    munit_assert_int(data[0], ==, 7);
    munit_assert_int(data[1], ==, 8);
    munit_assert_int(data[2], ==, 9);

    /* Release again: munmap */
    td_release(mapped);

    td_release(vec);
    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_corrupt --------------------------------------------- */

static MunitResult test_col_mmap_corrupt(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Write a 16-byte file (too small for a valid column header) */
    FILE* f = fopen(TMP_COL_PATH, "wb");
    munit_assert_ptr_not_null(f);
    uint8_t junk[16] = {0};
    fwrite(junk, 1, 16, f);
    fclose(f);

    td_t* result = td_col_mmap(TMP_COL_PATH);
    munit_assert_true(TD_IS_ERR(result));
    munit_assert_int(TD_ERR_CODE(result), ==, TD_ERR_CORRUPT);

    unlink(TMP_COL_PATH);
    return MUNIT_OK;
}

/* ---- test_col_mmap_nofile ---------------------------------------------- */

static MunitResult test_col_mmap_nofile(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* result = td_col_mmap("/tmp/teide_nonexistent_file_xyz.dat");
    munit_assert_true(TD_IS_ERR(result));
    munit_assert_int(TD_ERR_CODE(result), ==, TD_ERR_IO);

    return MUNIT_OK;
}

/* ---- test_splay_open_roundtrip ----------------------------------------- */

static MunitResult test_splay_open_roundtrip(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Clean up any leftover splay dir */
    (void)!system("rm -rf " TMP_SPLAY_DIR);

    /* Build a 3-column table: I64, F64, I32 */
    td_t* tbl = td_table_new(4);
    munit_assert_ptr_not_null(tbl);
    munit_assert_false(TD_IS_ERR(tbl));

    int64_t id_a = td_sym_intern("col_a", 5);
    int64_t id_b = td_sym_intern("col_b", 5);
    int64_t id_c = td_sym_intern("col_c", 5);

    int64_t raw_a[] = {1, 2, 3, 4, 5};
    double  raw_b[] = {1.5, 2.5, 3.5, 4.5, 5.5};
    int32_t raw_c[] = {10, 20, 30, 40, 50};

    td_t* col_a = td_vec_from_raw(TD_I64, raw_a, 5);
    td_t* col_b = td_vec_from_raw(TD_F64, raw_b, 5);
    td_t* col_c = td_vec_from_raw(TD_I32, raw_c, 5);
    munit_assert_false(TD_IS_ERR(col_a));
    munit_assert_false(TD_IS_ERR(col_b));
    munit_assert_false(TD_IS_ERR(col_c));

    tbl = td_table_add_col(tbl, id_a, col_a);
    munit_assert_false(TD_IS_ERR(tbl));
    tbl = td_table_add_col(tbl, id_b, col_b);
    munit_assert_false(TD_IS_ERR(tbl));
    tbl = td_table_add_col(tbl, id_c, col_c);
    munit_assert_false(TD_IS_ERR(tbl));

    /* Save to splay directory */
    td_err_t err = td_splay_save(tbl, TMP_SPLAY_DIR, NULL);
    munit_assert_int(err, ==, TD_OK);

    /* Open via mmap (zero-copy) */
    td_t* loaded = td_splay_open(TMP_SPLAY_DIR, NULL);
    munit_assert_ptr_not_null(loaded);
    munit_assert_false(TD_IS_ERR(loaded));

    /* Verify ncols and nrows */
    munit_assert_int(td_table_ncols(loaded), ==, 3);
    munit_assert_int(td_table_nrows(loaded), ==, 5);

    /* Verify column mmod==1 (mmap'd) */
    td_t* la = td_table_get_col(loaded, id_a);
    td_t* lb = td_table_get_col(loaded, id_b);
    td_t* lc = td_table_get_col(loaded, id_c);
    munit_assert_ptr_not_null(la);
    munit_assert_ptr_not_null(lb);
    munit_assert_ptr_not_null(lc);

    munit_assert_uint(la->mmod, ==, 1);
    munit_assert_uint(lb->mmod, ==, 1);
    munit_assert_uint(lc->mmod, ==, 1);

    /* Verify data */
    int64_t* da = (int64_t*)td_data(la);
    double*  db = (double*)td_data(lb);
    int32_t* dc = (int32_t*)td_data(lc);

    for (int i = 0; i < 5; i++) {
        munit_assert_int(da[i], ==, raw_a[i]);
        munit_assert_double(db[i], ==, raw_b[i]);
        munit_assert_int(dc[i], ==, raw_c[i]);
    }

    td_release(loaded);
    td_release(col_a);
    td_release(col_b);
    td_release(col_c);
    td_release(tbl);

    /* Cleanup */
    (void)!system("rm -rf " TMP_SPLAY_DIR);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest store_tests[] = {
    { "/col_mmap_i64",         test_col_mmap_i64,         store_setup, store_teardown, 0, NULL },
    { "/col_mmap_f64",         test_col_mmap_f64,         store_setup, store_teardown, 0, NULL },
    { "/col_mmap_cow",         test_col_mmap_cow,         store_setup, store_teardown, 0, NULL },
    { "/col_mmap_refcount",    test_col_mmap_refcount,    store_setup, store_teardown, 0, NULL },
    { "/col_mmap_corrupt",     test_col_mmap_corrupt,     store_setup, store_teardown, 0, NULL },
    { "/col_mmap_nofile",      test_col_mmap_nofile,      store_setup, store_teardown, 0, NULL },
    { "/splay_open_roundtrip", test_splay_open_roundtrip, store_setup, store_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_store_suite = {
    "/store",
    store_tests,
    NULL,
    0,
    0,
};
