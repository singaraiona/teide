#include "munit.h"
#include <teide/td.h>
#include <string.h>

/* ---- Setup / Teardown -------------------------------------------------- */

static void* df_setup(const void* params, void* user_data) {
    (void)params; (void)user_data;
    td_arena_init();
    td_sym_init();
    return NULL;
}

static void df_teardown(void* fixture) {
    (void)fixture;
    td_sym_destroy();
    td_arena_destroy_all();
}

/* ---- df_new ------------------------------------------------------------ */

static MunitResult test_df_new(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    munit_assert_ptr_not_null(df);
    munit_assert_false(TD_IS_ERR(df));
    munit_assert_int(df->type, ==, TD_TABLE);
    munit_assert_int(td_df_ncols(df), ==, 0);

    td_release(df);
    return MUNIT_OK;
}

/* ---- df_add_col -------------------------------------------------------- */

static MunitResult test_df_add_col(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    munit_assert_ptr_not_null(df);

    int64_t name_id = td_sym_intern("x", 1);
    int64_t raw[] = {10, 20, 30};
    td_t* col = td_vec_from_raw(TD_I64, raw, 3);
    munit_assert_ptr_not_null(col);

    df = td_df_add_col(df, name_id, col);
    munit_assert_false(TD_IS_ERR(df));
    munit_assert_int(td_df_ncols(df), ==, 1);

    td_release(col);
    td_release(df);
    return MUNIT_OK;
}

/* ---- df_get_col_by_name ------------------------------------------------ */

static MunitResult test_df_get_col_by_name(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    int64_t name_id = td_sym_intern("price", 5);
    double raw[] = {1.5, 2.5, 3.5};
    td_t* col = td_vec_from_raw(TD_F64, raw, 3);

    df = td_df_add_col(df, name_id, col);
    munit_assert_false(TD_IS_ERR(df));

    td_t* got = td_df_get_col(df, name_id);
    munit_assert_ptr_not_null(got);
    munit_assert_ptr_equal(got, col);

    /* Non-existent column returns NULL */
    int64_t other_id = td_sym_intern("missing", 7);
    td_t* missing = td_df_get_col(df, other_id);
    munit_assert_null(missing);

    td_release(col);
    td_release(df);
    return MUNIT_OK;
}

/* ---- df_get_col_by_idx ------------------------------------------------- */

static MunitResult test_df_get_col_by_idx(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    int64_t name_id = td_sym_intern("val", 3);
    int64_t raw[] = {100, 200};
    td_t* col = td_vec_from_raw(TD_I64, raw, 2);

    df = td_df_add_col(df, name_id, col);

    td_t* got = td_df_get_col_idx(df, 0);
    munit_assert_ptr_not_null(got);
    munit_assert_ptr_equal(got, col);

    /* Out of range */
    td_t* oob = td_df_get_col_idx(df, 1);
    munit_assert_null(oob);
    oob = td_df_get_col_idx(df, -1);
    munit_assert_null(oob);

    td_release(col);
    td_release(df);
    return MUNIT_OK;
}

/* ---- df_col_name ------------------------------------------------------- */

static MunitResult test_df_col_name(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    int64_t id_a = td_sym_intern("alpha", 5);
    int64_t id_b = td_sym_intern("beta", 4);
    int64_t raw[] = {1, 2, 3};
    td_t* col_a = td_vec_from_raw(TD_I64, raw, 3);
    td_t* col_b = td_vec_from_raw(TD_I64, raw, 3);

    df = td_df_add_col(df, id_a, col_a);
    df = td_df_add_col(df, id_b, col_b);

    munit_assert_int(td_df_col_name(df, 0), ==, id_a);
    munit_assert_int(td_df_col_name(df, 1), ==, id_b);

    /* Out of range */
    munit_assert_int(td_df_col_name(df, 2), ==, -1);

    td_release(col_a);
    td_release(col_b);
    td_release(df);
    return MUNIT_OK;
}

/* ---- df_nrows ---------------------------------------------------------- */

static MunitResult test_df_nrows(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    /* Empty df has 0 rows */
    munit_assert_int(td_df_nrows(df), ==, 0);

    int64_t name_id = td_sym_intern("col1", 4);
    int64_t raw[] = {10, 20, 30, 40, 50};
    td_t* col = td_vec_from_raw(TD_I64, raw, 5);

    df = td_df_add_col(df, name_id, col);
    munit_assert_int(td_df_nrows(df), ==, 5);

    td_release(col);
    td_release(df);
    return MUNIT_OK;
}

/* ---- df_schema --------------------------------------------------------- */

static MunitResult test_df_schema(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(4);
    td_t* schema = td_df_schema(df);
    munit_assert_ptr_not_null(schema);
    munit_assert_int(schema->type, ==, TD_I64);
    munit_assert_int(schema->len, ==, 0);  /* no columns yet */

    int64_t id_x = td_sym_intern("x", 1);
    int64_t raw[] = {1};
    td_t* col = td_vec_from_raw(TD_I64, raw, 1);
    df = td_df_add_col(df, id_x, col);

    schema = td_df_schema(df);
    munit_assert_int(schema->len, ==, 1);
    int64_t* ids = (int64_t*)td_data(schema);
    munit_assert_int(ids[0], ==, id_x);

    td_release(col);
    td_release(df);
    return MUNIT_OK;
}

/* ---- df_multiple_cols -------------------------------------------------- */

static MunitResult test_df_multiple_cols(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t* df = td_df_new(8);

    int64_t id_a = td_sym_intern("a", 1);
    int64_t id_b = td_sym_intern("b", 1);
    int64_t id_c = td_sym_intern("c", 1);

    int64_t raw_a[] = {1, 2, 3};
    double  raw_b[] = {1.1, 2.2, 3.3};
    uint8_t raw_c[] = {1, 0, 1};

    td_t* col_a = td_vec_from_raw(TD_I64, raw_a, 3);
    td_t* col_b = td_vec_from_raw(TD_F64, raw_b, 3);
    td_t* col_c = td_vec_from_raw(TD_BOOL, raw_c, 3);

    df = td_df_add_col(df, id_a, col_a);
    munit_assert_false(TD_IS_ERR(df));
    df = td_df_add_col(df, id_b, col_b);
    munit_assert_false(TD_IS_ERR(df));
    df = td_df_add_col(df, id_c, col_c);
    munit_assert_false(TD_IS_ERR(df));

    munit_assert_int(td_df_ncols(df), ==, 3);
    munit_assert_int(td_df_nrows(df), ==, 3);

    /* Verify by name */
    munit_assert_ptr_equal(td_df_get_col(df, id_a), col_a);
    munit_assert_ptr_equal(td_df_get_col(df, id_b), col_b);
    munit_assert_ptr_equal(td_df_get_col(df, id_c), col_c);

    /* Verify by index */
    munit_assert_ptr_equal(td_df_get_col_idx(df, 0), col_a);
    munit_assert_ptr_equal(td_df_get_col_idx(df, 1), col_b);
    munit_assert_ptr_equal(td_df_get_col_idx(df, 2), col_c);

    /* Verify column names */
    munit_assert_int(td_df_col_name(df, 0), ==, id_a);
    munit_assert_int(td_df_col_name(df, 1), ==, id_b);
    munit_assert_int(td_df_col_name(df, 2), ==, id_c);

    /* Verify schema */
    td_t* schema = td_df_schema(df);
    munit_assert_int(schema->len, ==, 3);
    int64_t* ids = (int64_t*)td_data(schema);
    munit_assert_int(ids[0], ==, id_a);
    munit_assert_int(ids[1], ==, id_b);
    munit_assert_int(ids[2], ==, id_c);

    /* Verify data integrity */
    int64_t* data_a = (int64_t*)td_data(col_a);
    munit_assert_int(data_a[0], ==, 1);
    munit_assert_int(data_a[2], ==, 3);

    double* data_b = (double*)td_data(col_b);
    munit_assert_double(data_b[1], ==, 2.2);

    td_release(col_a);
    td_release(col_b);
    td_release(col_c);
    td_release(df);
    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest df_tests[] = {
    { "/new",              test_df_new,              df_setup, df_teardown, 0, NULL },
    { "/add_col",          test_df_add_col,          df_setup, df_teardown, 0, NULL },
    { "/get_col_by_name",  test_df_get_col_by_name,  df_setup, df_teardown, 0, NULL },
    { "/get_col_by_idx",   test_df_get_col_by_idx,   df_setup, df_teardown, 0, NULL },
    { "/col_name",         test_df_col_name,         df_setup, df_teardown, 0, NULL },
    { "/nrows",            test_df_nrows,            df_setup, df_teardown, 0, NULL },
    { "/schema",           test_df_schema,           df_setup, df_teardown, 0, NULL },
    { "/multiple_cols",    test_df_multiple_cols,    df_setup, df_teardown, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_df_suite = {
    "/df",
    df_tests,
    NULL,
    0,
    0,
};
