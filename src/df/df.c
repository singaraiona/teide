#include "df.h"
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
 * df->len = current column count
 * -------------------------------------------------------------------------- */

static td_t** df_schema_slot(td_t* df) {
    return (td_t**)td_data(df);
}

static td_t** df_col_slots(td_t* df) {
    return (td_t**)((char*)td_data(df) + sizeof(td_t*));
}

/* --------------------------------------------------------------------------
 * td_df_new
 * -------------------------------------------------------------------------- */

td_t* td_df_new(int64_t ncols) {
    /* Allocate: 1 schema pointer + ncols column pointers */
    size_t data_size = (size_t)(1 + ncols) * sizeof(td_t*);

    td_t* df = td_alloc(data_size);
    if (!df || TD_IS_ERR(df)) return df;

    df->type = TD_TABLE;
    df->len = 0;  /* no columns yet */
    df->attrs = 0;
    memset(df->nullmap, 0, 16);

    /* Zero the data region */
    memset(td_data(df), 0, data_size);

    /* Create schema: I64 vector with capacity = ncols */
    td_t* schema = td_vec_new(TD_I64, ncols);
    if (!schema || TD_IS_ERR(schema)) {
        td_free(df);
        return schema;
    }
    td_retain(schema);
    *df_schema_slot(df) = schema;

    return df;
}

/* --------------------------------------------------------------------------
 * td_df_add_col
 * -------------------------------------------------------------------------- */

td_t* td_df_add_col(td_t* df, int64_t name_id, td_t* col_vec) {
    if (!df || TD_IS_ERR(df)) return df;
    if (!col_vec || TD_IS_ERR(col_vec)) return TD_ERR_PTR(TD_ERR_TYPE);

    /* COW the df */
    df = td_cow(df);
    if (!df || TD_IS_ERR(df)) return df;

    int64_t idx = df->len;

    /* Check capacity: we need (1 + idx + 1) pointers in data region */
    size_t block_size = (size_t)1 << df->order;
    size_t data_space = block_size - 32;
    int64_t max_cols = (int64_t)(data_space / sizeof(td_t*)) - 1;  /* minus schema slot */

    if (idx >= max_cols) {
        /* Need to grow the df block */
        size_t new_data_size = (size_t)(1 + (idx + 1) * 2) * sizeof(td_t*);
        td_t* new_df = td_scratch_realloc(df, new_data_size);
        if (!new_df || TD_IS_ERR(new_df)) return new_df;
        df = new_df;
    }

    /* Append name_id to schema vector */
    td_t* schema = *df_schema_slot(df);
    schema = td_vec_append(schema, &name_id);
    if (!schema || TD_IS_ERR(schema)) return TD_ERR_PTR(TD_ERR_OOM);

    /* Update schema pointer (vec_append may realloc) */
    /* Release old schema ref, retain new */
    td_t* old_schema = *df_schema_slot(df);
    if (old_schema != schema) {
        td_retain(schema);
        td_release(old_schema);
    }
    *df_schema_slot(df) = schema;

    /* Store column vector pointer and retain it */
    td_t** cols = df_col_slots(df);
    cols[idx] = col_vec;
    td_retain(col_vec);

    df->len = idx + 1;

    return df;
}

/* --------------------------------------------------------------------------
 * td_df_get_col
 * -------------------------------------------------------------------------- */

td_t* td_df_get_col(td_t* df, int64_t name_id) {
    if (!df || TD_IS_ERR(df)) return NULL;

    td_t* schema = *df_schema_slot(df);
    if (!schema || TD_IS_ERR(schema)) return NULL;

    int64_t* ids = (int64_t*)td_data(schema);
    int64_t ncols = df->len;

    for (int64_t i = 0; i < ncols; i++) {
        if (ids[i] == name_id) {
            td_t** cols = df_col_slots(df);
            return cols[i];
        }
    }

    return NULL;  /* column not found */
}

/* --------------------------------------------------------------------------
 * td_df_get_col_idx
 * -------------------------------------------------------------------------- */

td_t* td_df_get_col_idx(td_t* df, int64_t idx) {
    if (!df || TD_IS_ERR(df)) return NULL;
    if (idx < 0 || idx >= df->len) return NULL;

    td_t** cols = df_col_slots(df);
    return cols[idx];
}

/* --------------------------------------------------------------------------
 * td_df_col_name
 * -------------------------------------------------------------------------- */

int64_t td_df_col_name(td_t* df, int64_t idx) {
    if (!df || TD_IS_ERR(df)) return -1;
    if (idx < 0 || idx >= df->len) return -1;

    td_t* schema = *df_schema_slot(df);
    if (!schema || TD_IS_ERR(schema)) return -1;

    int64_t* ids = (int64_t*)td_data(schema);
    return ids[idx];
}

/* --------------------------------------------------------------------------
 * td_df_ncols
 * -------------------------------------------------------------------------- */

int64_t td_df_ncols(td_t* df) {
    if (!df || TD_IS_ERR(df)) return 0;
    return df->len;
}

/* --------------------------------------------------------------------------
 * td_df_nrows
 * -------------------------------------------------------------------------- */

int64_t td_df_nrows(td_t* df) {
    if (!df || TD_IS_ERR(df)) return 0;
    if (df->len <= 0) return 0;

    td_t** cols = df_col_slots(df);
    td_t* first_col = cols[0];
    if (!first_col || TD_IS_ERR(first_col)) return 0;

    return first_col->len;
}

/* --------------------------------------------------------------------------
 * td_df_schema
 * -------------------------------------------------------------------------- */

td_t* td_df_schema(td_t* df) {
    if (!df || TD_IS_ERR(df)) return NULL;
    return *df_schema_slot(df);
}
