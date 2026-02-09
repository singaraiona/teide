#include "splay.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <errno.h>
#include <dirent.h>

/* --------------------------------------------------------------------------
 * Splayed table: directory of column files + .d schema file
 *
 * Format:
 *   dir/.d        — I64 vector of column name symbol IDs
 *   dir/<colname> — column file per column
 * -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
 * td_splay_save — save a DataFrame to a splayed table directory
 * -------------------------------------------------------------------------- */

td_err_t td_splay_save(td_t* df, const char* dir) {
    if (!df || TD_IS_ERR(df)) return TD_ERR_TYPE;
    if (!dir) return TD_ERR_IO;

    /* Create directory */
    if (mkdir(dir, 0755) != 0 && errno != EEXIST) return TD_ERR_IO;

    int64_t ncols = td_table_ncols(df);

    /* Save .d schema file */
    td_t* schema = td_table_schema(df);
    if (schema) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/.d", dir);
        td_err_t err = td_col_save(schema, path);
        if (err != TD_OK) return err;
    }

    /* Save each column */
    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(df, c);
        int64_t name_id = td_table_col_name(df, c);
        if (!col) continue;

        /* Get column name string */
        td_t* name_atom = td_sym_str(name_id);
        if (!name_atom) continue;

        const char* name = td_str_ptr(name_atom);
        size_t name_len = td_str_len(name_atom);

        /* Reject names with path separators, traversal, or starting with '.' */
        if (name_len == 0 || name[0] == '.' ||
            memchr(name, '/', name_len) || memchr(name, '\\', name_len) ||
            memchr(name, '\0', name_len))
            continue;

        char path[1024];
        int path_len = snprintf(path, sizeof(path), "%s/%.*s", dir, (int)name_len, name);
        if (path_len < 0 || (size_t)path_len >= sizeof(path)) continue;

        td_err_t err = td_col_save(col, path);
        if (err != TD_OK) return err;
    }

    return TD_OK;
}

/* --------------------------------------------------------------------------
 * td_splay_load — load a splayed table from a directory
 * -------------------------------------------------------------------------- */

td_t* td_splay_load(const char* dir) {
    if (!dir) return TD_ERR_PTR(TD_ERR_IO);

    /* Load .d schema */
    char path[1024];
    snprintf(path, sizeof(path), "%s/.d", dir);
    td_t* schema = td_col_load(path);
    if (!schema || TD_IS_ERR(schema)) return schema;

    int64_t ncols = schema->len;
    int64_t* name_ids = (int64_t*)td_data(schema);

    td_t* df = td_table_new(ncols);
    if (!df || TD_IS_ERR(df)) {
        td_release(schema);
        return df;
    }

    /* Load each column */
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = name_ids[c];
        td_t* name_atom = td_sym_str(name_id);
        if (!name_atom) continue;

        const char* name = td_str_ptr(name_atom);
        size_t name_len = td_str_len(name_atom);

        /* Reject names with path separators, traversal, or starting with '.' */
        if (name_len == 0 || name[0] == '.' ||
            memchr(name, '/', name_len) || memchr(name, '\\', name_len) ||
            memchr(name, '\0', name_len))
            continue;

        int path_len = snprintf(path, sizeof(path), "%s/%.*s", dir, (int)name_len, name);
        if (path_len < 0 || (size_t)path_len >= sizeof(path)) continue;

        td_t* col = td_col_load(path);
        if (!col || TD_IS_ERR(col)) continue;

        td_t* new_df = td_table_add_col(df, name_id, col);
        if (!new_df || TD_IS_ERR(new_df)) continue;
        df = new_df;
    }

    td_release(schema);
    return df;
}
