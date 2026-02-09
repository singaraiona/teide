#define _POSIX_C_SOURCE 200809L
#include "part.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>

/* --------------------------------------------------------------------------
 * Partitioned table: date-partitioned directory of splayed tables
 *
 * Format:
 *   db_root/sym              — global symbol intern table
 *   db_root/YYYY.MM.DD/      — partition directories
 *   db_root/YYYY.MM.DD/table — splayed table per partition
 * -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
 * td_part_load — load a partitioned table
 *
 * Discovers partition directories, loads each splayed table, and
 * concatenates columns across partitions.
 * -------------------------------------------------------------------------- */

td_t* td_part_load(const char* db_root, const char* table_name) {
    if (!db_root || !table_name) return TD_ERR_PTR(TD_ERR_IO);

    /* Scan db_root for partition directories (YYYY.MM.DD format) */
    DIR* d = opendir(db_root);
    if (!d) return TD_ERR_PTR(TD_ERR_IO);

    /* Collect partition directory names */
    char** part_dirs = NULL;
    int64_t part_count = 0;
    int64_t part_cap = 0;

    struct dirent* ent;
    while ((ent = readdir(d)) != NULL) {
        /* Skip . and .. and non-directories */
        if (ent->d_name[0] == '.') continue;

        /* Check if it looks like a partition (contains a dot) */
        if (strchr(ent->d_name, '.') == NULL) continue;

        if (part_count >= part_cap) {
            part_cap = part_cap == 0 ? 16 : part_cap * 2;
            part_dirs = (char**)realloc(part_dirs, (size_t)part_cap * sizeof(char*));
        }
        part_dirs[part_count] = strdup(ent->d_name);
        part_count++;
    }
    closedir(d);

    if (part_count == 0) {
        free(part_dirs);
        return TD_ERR_PTR(TD_ERR_IO);
    }

    /* Sort partition names for deterministic order */
    for (int64_t i = 0; i < part_count - 1; i++) {
        for (int64_t j = i + 1; j < part_count; j++) {
            if (strcmp(part_dirs[i], part_dirs[j]) > 0) {
                char* tmp = part_dirs[i];
                part_dirs[i] = part_dirs[j];
                part_dirs[j] = tmp;
            }
        }
    }

    /* Load first partition to get schema */
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s/%s", db_root, part_dirs[0], table_name);
    td_t* first = td_splay_load(path);
    if (!first || TD_IS_ERR(first)) {
        for (int64_t i = 0; i < part_count; i++) free(part_dirs[i]);
        free(part_dirs);
        return first;
    }

    if (part_count == 1) {
        for (int64_t i = 0; i < part_count; i++) free(part_dirs[i]);
        free(part_dirs);
        return first;
    }

    /* Load remaining partitions and concatenate */
    int64_t ncols = td_table_ncols(first);
    int64_t total_rows = td_table_nrows(first);

    /* Accumulate rows from all partitions */
    td_t** all_dfs = (td_t**)malloc((size_t)part_count * sizeof(td_t*));
    all_dfs[0] = first;

    for (int64_t p = 1; p < part_count; p++) {
        snprintf(path, sizeof(path), "%s/%s/%s", db_root, part_dirs[p], table_name);
        all_dfs[p] = td_splay_load(path);
        if (all_dfs[p] && !TD_IS_ERR(all_dfs[p])) {
            total_rows += td_table_nrows(all_dfs[p]);
        }
    }

    /* Build combined DataFrame by concatenating columns */
    td_t* result = td_table_new(ncols);
    for (int64_t c = 0; c < ncols; c++) {
        int64_t name_id = td_table_col_name(first, c);
        td_t* combined = td_table_get_col_idx(first, c);
        if (!combined) continue;
        td_retain(combined);

        for (int64_t p = 1; p < part_count; p++) {
            if (!all_dfs[p] || TD_IS_ERR(all_dfs[p])) continue;
            td_t* part_col = td_table_get_col_idx(all_dfs[p], c);
            if (part_col) {
                td_t* new_combined = td_vec_concat(combined, part_col);
                td_release(combined);
                combined = new_combined;
            }
        }

        result = td_table_add_col(result, name_id, combined);
        td_release(combined);
    }

    /* Cleanup */
    for (int64_t p = 0; p < part_count; p++) {
        if (all_dfs[p] && !TD_IS_ERR(all_dfs[p]))
            td_release(all_dfs[p]);
        free(part_dirs[p]);
    }
    free(all_dfs);
    free(part_dirs);

    return result;
}
