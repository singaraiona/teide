#include "munit.h"
#include <teide/td.h>
#include "core/block.h"

/* ---- Accessor macro tests ---------------------------------------------- */

static MunitResult test_type_macros(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t atom;
    memset(&atom, 0, sizeof(atom));
    atom.type = -TD_I64;  /* atom */
    munit_assert_int(td_type(&atom), ==, -TD_I64);
    munit_assert_true(td_is_atom(&atom));
    munit_assert_false(td_is_vec(&atom));

    td_t vec;
    memset(&vec, 0, sizeof(vec));
    vec.type = TD_F64;    /* vector */
    vec.len  = 100;
    munit_assert_int(td_type(&vec), ==, TD_F64);
    munit_assert_false(td_is_atom(&vec));
    munit_assert_true(td_is_vec(&vec));
    munit_assert_int(td_len(&vec), ==, 100);

    td_t list;
    memset(&list, 0, sizeof(list));
    list.type = TD_LIST;  /* neither atom nor vec */
    munit_assert_false(td_is_atom(&list));
    munit_assert_false(td_is_vec(&list));

    return MUNIT_OK;
}

static MunitResult test_td_data(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t block;
    memset(&block, 0, sizeof(block));
    void* data = td_data(&block);
    /* Data should be exactly 32 bytes past the start of the block */
    munit_assert_int((char*)data - (char*)&block, ==, 32);

    return MUNIT_OK;
}

static MunitResult test_elem_size(const void* params, void* fixture) {
    (void)params; (void)fixture;

    munit_assert_int(td_elem_size(TD_BOOL), ==, 1);
    munit_assert_int(td_elem_size(TD_U8),   ==, 1);
    munit_assert_int(td_elem_size(TD_CHAR), ==, 1);
    munit_assert_int(td_elem_size(TD_I16),  ==, 2);
    munit_assert_int(td_elem_size(TD_I32),  ==, 4);
    munit_assert_int(td_elem_size(TD_I64),  ==, 8);
    munit_assert_int(td_elem_size(TD_F64),  ==, 8);
    munit_assert_int(td_elem_size(TD_ENUM), ==, 4);
    munit_assert_int(td_elem_size(TD_SYMBOL), ==, 8);
    munit_assert_int(td_elem_size(TD_GUID), ==, 16);

    return MUNIT_OK;
}

/* ---- td_block_size tests ----------------------------------------------- */

static MunitResult test_block_size_atom(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t atom;
    memset(&atom, 0, sizeof(atom));
    atom.type = -TD_F64;  /* atom */
    atom.f64  = 3.14;

    size_t sz = td_block_size(&atom);
    munit_assert_size(sz, ==, 32);

    return MUNIT_OK;
}

static MunitResult test_block_size_vec(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t vec;
    memset(&vec, 0, sizeof(vec));
    vec.type = TD_I64;
    vec.len  = 10;

    size_t sz = td_block_size(&vec);
    /* 32 header + 10 * 8 bytes = 112 */
    munit_assert_size(sz, ==, 112);

    return MUNIT_OK;
}

static MunitResult test_block_size_vec_bool(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t vec;
    memset(&vec, 0, sizeof(vec));
    vec.type = TD_BOOL;
    vec.len  = 1024;

    size_t sz = td_block_size(&vec);
    /* 32 header + 1024 * 1 = 1056 */
    munit_assert_size(sz, ==, 1056);

    return MUNIT_OK;
}

static MunitResult test_block_size_empty_vec(const void* params, void* fixture) {
    (void)params; (void)fixture;

    td_t vec;
    memset(&vec, 0, sizeof(vec));
    vec.type = TD_F64;
    vec.len  = 0;

    size_t sz = td_block_size(&vec);
    munit_assert_size(sz, ==, 32);

    return MUNIT_OK;
}

/* ---- td_t struct size check -------------------------------------------- */

static MunitResult test_td_t_size(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* td_t must be exactly 32 bytes */
    munit_assert_size(sizeof(td_t), ==, 32);

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest block_tests[] = {
    { "/type_macros",      test_type_macros,         NULL, NULL, 0, NULL },
    { "/td_data",          test_td_data,             NULL, NULL, 0, NULL },
    { "/elem_size",        test_elem_size,           NULL, NULL, 0, NULL },
    { "/block_size_atom",  test_block_size_atom,     NULL, NULL, 0, NULL },
    { "/block_size_vec",   test_block_size_vec,      NULL, NULL, 0, NULL },
    { "/block_size_bool",  test_block_size_vec_bool, NULL, NULL, 0, NULL },
    { "/block_size_empty", test_block_size_empty_vec, NULL, NULL, 0, NULL },
    { "/td_t_size",        test_td_t_size,           NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_block_suite = {
    "/block",
    block_tests,
    NULL,
    0,
    0,
};
