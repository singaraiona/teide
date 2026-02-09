#include "err.h"

static const char* err_strings[] = {
    [TD_OK]          = "ok",
    [TD_ERR_OOM]     = "out of memory",
    [TD_ERR_TYPE]    = "type error",
    [TD_ERR_RANGE]   = "range error",
    [TD_ERR_LENGTH]  = "length mismatch",
    [TD_ERR_RANK]    = "rank error",
    [TD_ERR_DOMAIN]  = "domain error",
    [TD_ERR_NYI]     = "not yet implemented",
    [TD_ERR_IO]      = "I/O error",
    [TD_ERR_SCHEMA]  = "schema error",
    [TD_ERR_CORRUPT] = "corrupt data",
};

#define ERR_STRING_COUNT (sizeof(err_strings) / sizeof(err_strings[0]))

const char* td_err_str(td_err_t e) {
    if ((unsigned)e >= ERR_STRING_COUNT) return "unknown error";
    return err_strings[e];
}
