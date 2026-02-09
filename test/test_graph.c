#include "munit.h"
#include <teide/td.h>
#include <string.h>
#include <math.h>

/* --------------------------------------------------------------------------
 * Helper: create a test DataFrame with columns id1(I64), v1(I64), v3(F64)
 * -------------------------------------------------------------------------- */

static td_t* make_test_df(void) {
    td_sym_init();

    int64_t n = 10;
    int64_t id1_data[] = {1, 1, 2, 2, 3, 3, 1, 2, 3, 1};
    int64_t v1_data[]  = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100};
    double  v3_data[]  = {1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5};

    td_t* id1_vec = td_vec_from_raw(TD_I64, id1_data, n);
    td_t* v1_vec  = td_vec_from_raw(TD_I64, v1_data, n);
    td_t* v3_vec  = td_vec_from_raw(TD_F64, v3_data, n);

    int64_t name_id1 = td_sym_intern("id1", 3);
    int64_t name_v1  = td_sym_intern("v1", 2);
    int64_t name_v3  = td_sym_intern("v3", 2);

    td_t* df = td_table_new(3);
    df = td_table_add_col(df, name_id1, id1_vec);
    df = td_table_add_col(df, name_v1, v1_vec);
    df = td_table_add_col(df, name_v3, v3_vec);

    td_release(id1_vec);
    td_release(v1_vec);
    td_release(v3_vec);

    return df;
}

/* --------------------------------------------------------------------------
 * Test: scan + sum
 * -------------------------------------------------------------------------- */

static MunitResult test_scan_sum(const void* params, void* data) {
    (void)params; (void)data;
    td_arena_init();

    td_t* df = make_test_df();
    munit_assert_ptr_not_null(df);

    td_graph_t* g = td_graph_new(df);
    munit_assert_ptr_not_null(g);

    td_op_t* v1 = td_scan(g, "v1");
    munit_assert_ptr_not_null(v1);

    td_op_t* result_op = td_sum(g, v1);
    td_t* result = td_execute(g, result_op);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_ATOM_I64);
    munit_assert_int(result->i64, ==, 550);  /* 10+20+...+100 */

    td_release(result);
    td_graph_free(g);
    td_release(df);
    td_sym_destroy();
    td_arena_destroy_all();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: scan + filter + count
 * -------------------------------------------------------------------------- */

static MunitResult test_filter_count(const void* params, void* data) {
    (void)params; (void)data;
    td_arena_init();

    td_t* df = make_test_df();
    td_graph_t* g = td_graph_new(df);

    td_op_t* v1 = td_scan(g, "v1");
    td_op_t* threshold = td_const_i64(g, 50);
    td_op_t* pred = td_ge(g, v1, threshold);
    td_op_t* filtered = td_filter(g, v1, pred);
    td_op_t* cnt = td_count(g, filtered);

    td_t* result = td_execute(g, cnt);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->i64, ==, 6);  /* 50,60,70,80,90,100 */

    td_release(result);
    td_graph_free(g);
    td_release(df);
    td_sym_destroy();
    td_arena_destroy_all();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: arithmetic + reduction
 * -------------------------------------------------------------------------- */

static MunitResult test_arithmetic(const void* params, void* data) {
    (void)params; (void)data;
    td_arena_init();

    td_t* df = make_test_df();
    td_graph_t* g = td_graph_new(df);

    td_op_t* v3 = td_scan(g, "v3");
    td_op_t* two = td_const_f64(g, 2.0);
    td_op_t* doubled = td_mul(g, v3, two);
    td_op_t* total = td_sum(g, doubled);

    td_t* result = td_execute(g, total);
    munit_assert_false(TD_IS_ERR(result));
    /* sum(v3) = 60.0, doubled = 120.0 */
    munit_assert_double_equal(result->f64, 120.0, 6);

    td_release(result);
    td_graph_free(g);
    td_release(df);
    td_sym_destroy();
    td_arena_destroy_all();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: group by id1, sum(v1)
 * -------------------------------------------------------------------------- */

static MunitResult test_group_sum(const void* params, void* data) {
    (void)params; (void)data;
    td_arena_init();

    td_t* df = make_test_df();
    td_graph_t* g = td_graph_new(df);

    td_op_t* key = td_scan(g, "id1");
    td_op_t* val = td_scan(g, "v1");

    td_op_t* keys[] = { key };
    td_op_t* agg_ins[] = { val };
    uint16_t agg_ops[] = { OP_SUM };

    td_op_t* grp = td_group(g, keys, 1, agg_ops, agg_ins, 1);
    td_t* result = td_execute(g, grp);
    munit_assert_false(TD_IS_ERR(result));
    munit_assert_int(result->type, ==, TD_TABLE);

    /* Should have 3 groups (id1=1,2,3) */
    munit_assert_int(td_table_ncols(result), ==, 2);
    int64_t nrows = td_table_nrows(result);
    munit_assert_int(nrows, ==, 3);

    /* Verify sums: id1=1: 10+20+70+100=200, id1=2: 30+40+80=150, id1=3: 50+60+90=200 */
    td_t* sum_col = td_table_get_col_idx(result, 1);
    munit_assert_ptr_not_null(sum_col);

    int64_t sum1 = 0, sum2 = 0, sum3 = 0;
    td_t* id_col = td_table_get_col_idx(result, 0);
    for (int64_t i = 0; i < nrows; i++) {
        int64_t id = ((int64_t*)td_data(id_col))[i];
        int64_t s = ((int64_t*)td_data(sum_col))[i];
        if (id == 1) sum1 = s;
        else if (id == 2) sum2 = s;
        else if (id == 3) sum3 = s;
    }
    munit_assert_int(sum1, ==, 200);
    munit_assert_int(sum2, ==, 150);
    munit_assert_int(sum3, ==, 200);

    td_release(result);
    td_graph_free(g);
    td_release(df);
    td_sym_destroy();
    td_arena_destroy_all();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Test: graph new/free
 * -------------------------------------------------------------------------- */

static MunitResult test_graph_lifecycle(const void* params, void* data) {
    (void)params; (void)data;
    td_arena_init();

    td_t* df = make_test_df();
    td_graph_t* g = td_graph_new(df);
    munit_assert_ptr_not_null(g);
    munit_assert_uint(g->node_count, ==, 0);

    td_op_t* v1 = td_scan(g, "v1");
    munit_assert_ptr_not_null(v1);
    munit_assert_uint(v1->opcode, ==, OP_SCAN);
    munit_assert_uint(g->node_count, ==, 1);

    td_graph_free(g);
    td_release(df);
    td_sym_destroy();
    td_arena_destroy_all();
    return MUNIT_OK;
}

/* --------------------------------------------------------------------------
 * Suite
 * -------------------------------------------------------------------------- */

static MunitTest tests[] = {
    { "/lifecycle",    test_graph_lifecycle, NULL, NULL, 0, NULL },
    { "/scan_sum",     test_scan_sum,        NULL, NULL, 0, NULL },
    { "/filter_count", test_filter_count,    NULL, NULL, 0, NULL },
    { "/arithmetic",   test_arithmetic,      NULL, NULL, 0, NULL },
    { "/group_sum",    test_group_sum,       NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL }
};

MunitSuite test_graph_suite = {
    "/graph", tests, NULL, 1, 0
};
