#include "meta.h"
#include <string.h>
#include <stdio.h>

/* --------------------------------------------------------------------------
 * .d file: serialized I64 vector of column name symbol IDs
 *
 * td_meta_save_d: write schema vector to .d file
 * td_meta_load_d: read .d file back as I64 vector
 * -------------------------------------------------------------------------- */

td_err_t td_meta_save_d(td_t* schema, const char* path) {
    if (!schema || TD_IS_ERR(schema)) return TD_ERR_TYPE;
    return td_col_save(schema, path);
}

td_t* td_meta_load_d(const char* path) {
    return td_col_load(path);
}
