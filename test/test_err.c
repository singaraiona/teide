#include "munit.h"
#include "core/err.h"
#include "core/types.h"

/* ---- td_err_str tests -------------------------------------------------- */

static MunitResult test_err_str_ok(const void* params, void* fixture) {
    (void)params; (void)fixture;
    munit_assert_string_equal(td_err_str(TD_OK), "ok");
    return MUNIT_OK;
}

static MunitResult test_err_str_all(const void* params, void* fixture) {
    (void)params; (void)fixture;
    munit_assert_string_equal(td_err_str(TD_ERR_OOM),     "out of memory");
    munit_assert_string_equal(td_err_str(TD_ERR_TYPE),    "type error");
    munit_assert_string_equal(td_err_str(TD_ERR_RANGE),   "range error");
    munit_assert_string_equal(td_err_str(TD_ERR_LENGTH),  "length mismatch");
    munit_assert_string_equal(td_err_str(TD_ERR_RANK),    "rank error");
    munit_assert_string_equal(td_err_str(TD_ERR_DOMAIN),  "domain error");
    munit_assert_string_equal(td_err_str(TD_ERR_NYI),     "not yet implemented");
    munit_assert_string_equal(td_err_str(TD_ERR_IO),      "I/O error");
    munit_assert_string_equal(td_err_str(TD_ERR_SCHEMA),  "schema error");
    munit_assert_string_equal(td_err_str(TD_ERR_CORRUPT), "corrupt data");
    return MUNIT_OK;
}

static MunitResult test_err_str_unknown(const void* params, void* fixture) {
    (void)params; (void)fixture;
    munit_assert_string_equal(td_err_str((td_err_t)99), "unknown error");
    return MUNIT_OK;
}

/* ---- TD_ERR_PTR / TD_IS_ERR / TD_ERR_CODE macro tests ------------------ */

static MunitResult test_err_ptr_encoding(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* Error pointers should be detected as errors */
    td_t* err_oom = TD_ERR_PTR(TD_ERR_OOM);
    munit_assert_true(TD_IS_ERR(err_oom));
    munit_assert_int(TD_ERR_CODE(err_oom), ==, TD_ERR_OOM);

    td_t* err_type = TD_ERR_PTR(TD_ERR_TYPE);
    munit_assert_true(TD_IS_ERR(err_type));
    munit_assert_int(TD_ERR_CODE(err_type), ==, TD_ERR_TYPE);

    td_t* err_corrupt = TD_ERR_PTR(TD_ERR_CORRUPT);
    munit_assert_true(TD_IS_ERR(err_corrupt));
    munit_assert_int(TD_ERR_CODE(err_corrupt), ==, TD_ERR_CORRUPT);

    /* NULL is also detected as error (value 0 < 32) */
    munit_assert_true(TD_IS_ERR(NULL));
    munit_assert_int(TD_ERR_CODE(NULL), ==, TD_OK);

    return MUNIT_OK;
}

static MunitResult test_err_valid_ptr_not_error(const void* params, void* fixture) {
    (void)params; (void)fixture;

    /* A properly aligned td_t on the stack: address will be >= 32 */
    td_t block;
    memset(&block, 0, sizeof(block));
    munit_assert_false(TD_IS_ERR(&block));

    return MUNIT_OK;
}

/* ---- Suite definition -------------------------------------------------- */

static MunitTest err_tests[] = {
    { "/str_ok",            test_err_str_ok,            NULL, NULL, 0, NULL },
    { "/str_all",           test_err_str_all,           NULL, NULL, 0, NULL },
    { "/str_unknown",       test_err_str_unknown,       NULL, NULL, 0, NULL },
    { "/ptr_encoding",      test_err_ptr_encoding,      NULL, NULL, 0, NULL },
    { "/valid_ptr_not_err", test_err_valid_ptr_not_error, NULL, NULL, 0, NULL },
    { NULL, NULL, NULL, NULL, 0, NULL },
};

MunitSuite test_err_suite = {
    "/err",
    err_tests,
    NULL,
    0,
    0,
};
