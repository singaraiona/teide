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

#include "table.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Data layout helpers
 *
 * Data region of a TABLE block:
 *   [0]                          = td_t* schema (I64 vector of name IDs)
 *   [sizeof(td_t*)]              = td_t* col_0
 *   [sizeof(td_t*) * 2]          = td_t* col_1
 *   ...
 *   [sizeof(td_t*) * (ncols)]    = td_t* col_{ncols-1}
 *
 * tbl->len = current column count
 * -------------------------------------------------------------------------- */

static td_t** tbl_schema_slot(td_t* tbl) {
    return (td_t**)td_data(tbl);
}

static td_t** tbl_col_slots(td_t* tbl) {
    return (td_t**)((char*)td_data(tbl) + sizeof(td_t*));
}

/* --------------------------------------------------------------------------
 * td_table_new
 * -------------------------------------------------------------------------- */

td_t* td_table_new(int64_t ncols) {
    /* Allocate: 1 schema pointer + ncols column pointers */
    size_t data_size = (size_t)(1 + ncols) * sizeof(td_t*);

    td_t* tbl = td_alloc(data_size);
    if (!tbl || TD_IS_ERR(tbl)) return tbl;

    tbl->type = TD_TABLE;
    tbl->len = 0;  /* no columns yet */
    tbl->attrs = 0;
    memset(tbl->nullmap, 0, 16);

    /* Zero the data region */
    memset(td_data(tbl), 0, data_size);

    /* Create schema: I64 vector with capacity = ncols */
    td_t* schema = td_vec_new(TD_I64, ncols);
    if (!schema || TD_IS_ERR(schema)) {
        td_free(tbl);
        return schema;
    }
    *tbl_schema_slot(tbl) = schema;

    return tbl;
}

/* --------------------------------------------------------------------------
 * td_table_add_col
 * -------------------------------------------------------------------------- */

td_t* td_table_add_col(td_t* tbl, int64_t name_id, td_t* col_vec) {
    if (!tbl || TD_IS_ERR(tbl)) return tbl;
    if (!col_vec || TD_IS_ERR(col_vec)) return TD_ERR_PTR(TD_ERR_TYPE);

    /* COW the tbl */
    tbl = td_cow(tbl);
    if (!tbl || TD_IS_ERR(tbl)) return tbl;

    int64_t idx = tbl->len;

    /* Check capacity: we need (1 + idx + 1) pointers in data region */
    size_t block_size = (size_t)1 << tbl->order;
    size_t data_space = block_size - 32;
    int64_t max_cols = (int64_t)(data_space / sizeof(td_t*)) - 1;  /* minus schema slot */

    if (idx >= max_cols) {
        /* Need to grow the tbl block */
        size_t new_data_size = (size_t)(1 + (idx + 1) * 2) * sizeof(td_t*);
        td_t* new_df = td_scratch_realloc(tbl, new_data_size);
        if (!new_df || TD_IS_ERR(new_df)) return new_df;
        tbl = new_df;
    }

    /* Append name_id to schema vector */
    td_t* schema = *tbl_schema_slot(tbl);
    schema = td_vec_append(schema, &name_id);
    if (!schema || TD_IS_ERR(schema)) return TD_ERR_PTR(TD_ERR_OOM);

    /* vec_append returns the owned schema reference (possibly moved). */
    *tbl_schema_slot(tbl) = schema;

    /* Store column vector pointer and retain it */
    td_t** cols = tbl_col_slots(tbl);
    cols[idx] = col_vec;
    td_retain(col_vec);

    tbl->len = idx + 1;

    return tbl;
}

/* --------------------------------------------------------------------------
 * td_table_get_col
 * -------------------------------------------------------------------------- */

td_t* td_table_get_col(td_t* tbl, int64_t name_id) {
    if (!tbl || TD_IS_ERR(tbl)) return NULL;

    td_t* schema = *tbl_schema_slot(tbl);
    if (!schema || TD_IS_ERR(schema)) return NULL;

    int64_t* ids = (int64_t*)td_data(schema);
    int64_t ncols = tbl->len;

    for (int64_t i = 0; i < ncols; i++) {
        if (ids[i] == name_id) {
            td_t** cols = tbl_col_slots(tbl);
            return cols[i];
        }
    }

    return NULL;  /* column not found */
}

/* --------------------------------------------------------------------------
 * td_table_get_col_idx
 * -------------------------------------------------------------------------- */

td_t* td_table_get_col_idx(td_t* tbl, int64_t idx) {
    if (!tbl || TD_IS_ERR(tbl)) return NULL;
    if (idx < 0 || idx >= tbl->len) return NULL;

    td_t** cols = tbl_col_slots(tbl);
    return cols[idx];
}

/* --------------------------------------------------------------------------
 * td_table_col_name
 * -------------------------------------------------------------------------- */

int64_t td_table_col_name(td_t* tbl, int64_t idx) {
    if (!tbl || TD_IS_ERR(tbl)) return -1;
    if (idx < 0 || idx >= tbl->len) return -1;

    td_t* schema = *tbl_schema_slot(tbl);
    if (!schema || TD_IS_ERR(schema)) return -1;

    int64_t* ids = (int64_t*)td_data(schema);
    return ids[idx];
}

/* --------------------------------------------------------------------------
 * td_table_ncols
 * -------------------------------------------------------------------------- */

int64_t td_table_ncols(td_t* tbl) {
    if (!tbl || TD_IS_ERR(tbl)) return 0;
    return tbl->len;
}

/* --------------------------------------------------------------------------
 * td_table_nrows
 * -------------------------------------------------------------------------- */

int64_t td_table_nrows(td_t* tbl) {
    if (!tbl || TD_IS_ERR(tbl)) return 0;
    if (tbl->len <= 0) return 0;

    td_t** cols = tbl_col_slots(tbl);
    td_t* first_col = cols[0];
    if (!first_col || TD_IS_ERR(first_col)) return 0;

    return first_col->len;
}

/* --------------------------------------------------------------------------
 * td_table_schema
 * -------------------------------------------------------------------------- */

td_t* td_table_schema(td_t* tbl) {
    if (!tbl || TD_IS_ERR(tbl)) return NULL;
    return *tbl_schema_slot(tbl);
}
