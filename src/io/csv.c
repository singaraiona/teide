#if defined(__linux__)
  #define _POSIX_C_SOURCE 200809L
#endif
#include "csv.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>

/* --------------------------------------------------------------------------
 * CSV parser: reads CSV files into DataFrames
 *
 * Features:
 *   - Auto-detect delimiter (comma, tab)
 *   - Optional header row
 *   - Type inference (I64, F64, STR/ENUM)
 *   - Symbol interning for string columns
 * -------------------------------------------------------------------------- */

/* Max columns, max field length */
#define CSV_MAX_COLS  256
#define CSV_MAX_FIELD 8192
#define CSV_INIT_ROWS 1024

/* Type inference state per column */
typedef enum {
    CSV_TYPE_UNKNOWN = 0,
    CSV_TYPE_BOOL,
    CSV_TYPE_I64,
    CSV_TYPE_F64,
    CSV_TYPE_STR
} csv_type_t;

/* --------------------------------------------------------------------------
 * Helper: read entire file into memory
 * -------------------------------------------------------------------------- */

static char* read_file(const char* path, size_t* out_size) {
    FILE* f = fopen(path, "rb");
    if (!f) return NULL;

    /* Use fstat for >2GB file safety on 32-bit systems */
    struct stat st;
    if (fstat(fileno(f), &st) != 0 || st.st_size <= 0) {
        fclose(f); return NULL;
    }
    size_t sz = (size_t)st.st_size;

    char* buf = (char*)malloc(sz + 1);
    if (!buf) { fclose(f); return NULL; }

    size_t rd = fread(buf, 1, sz, f);
    fclose(f);
    buf[rd] = '\0';
    *out_size = rd;
    return buf;
}

/* --------------------------------------------------------------------------
 * Helper: parse a field from CSV, handling quoting
 * -------------------------------------------------------------------------- */

static const char* parse_field(const char* p, char delim, char* out, size_t out_cap, size_t* out_len) {
    size_t len = 0;

    if (*p == '"') {
        /* Quoted field */
        p++;
        while (*p) {
            if (*p == '"') {
                if (*(p + 1) == '"') {
                    /* Escaped quote */
                    if (len < out_cap - 1) out[len++] = '"';
                    p += 2;
                } else {
                    /* End of quoted field */
                    p++;
                    break;
                }
            } else {
                if (len < out_cap - 1) out[len++] = *p;
                p++;
            }
        }
        /* Skip delimiter or newline after closing quote */
        if (*p == delim) p++;
        else if (*p == '\r') { p++; if (*p == '\n') p++; }
        else if (*p == '\n') p++;
    } else {
        /* Unquoted field */
        while (*p && *p != delim && *p != '\n' && *p != '\r') {
            if (len < out_cap - 1) out[len++] = *p;
            p++;
        }
        if (*p == delim) p++;
        else if (*p == '\r') { p++; if (*p == '\n') p++; }
        else if (*p == '\n') p++;
    }

    out[len] = '\0';
    *out_len = len;
    return p;
}

/* --------------------------------------------------------------------------
 * Helper: detect type of a field value
 * -------------------------------------------------------------------------- */

static csv_type_t detect_type(const char* field, size_t len) {
    if (len == 0) return CSV_TYPE_UNKNOWN;

    /* Check for boolean */
    if ((len == 4 && strncmp(field, "true", 4) == 0) ||
        (len == 5 && strncmp(field, "false", 5) == 0) ||
        (len == 4 && strncmp(field, "TRUE", 4) == 0) ||
        (len == 5 && strncmp(field, "FALSE", 5) == 0)) {
        return CSV_TYPE_BOOL;
    }

    /* Try integer */
    const char* p = field;
    if (*p == '-' || *p == '+') p++;
    bool all_digit = true;
    bool has_dot = false;
    bool has_e = false;
    while (*p) {
        if (isdigit((unsigned char)*p)) { p++; continue; }
        if (*p == '.' && !has_dot) { has_dot = true; p++; all_digit = false; continue; }
        if ((*p == 'e' || *p == 'E') && !has_e) { has_e = true; p++; all_digit = false; if (*p == '-' || *p == '+') p++; continue; }
        all_digit = false;
        break;
    }

    if (*p == '\0' && p > field) {
        /* Require at least one digit (bare +/- is not numeric) */
        bool has_digit = false;
        for (const char* d = field; *d; d++) {
            if (isdigit((unsigned char)*d)) { has_digit = true; break; }
        }
        if (has_digit) {
            if (!has_dot && !has_e && all_digit) return CSV_TYPE_I64;
            if (has_dot || has_e) return CSV_TYPE_F64;
        }
    }

    return CSV_TYPE_STR;
}

/* Promote type based on new observation */
static csv_type_t promote_csv_type(csv_type_t current, csv_type_t observed) {
    if (current == CSV_TYPE_UNKNOWN) return observed;
    if (observed == CSV_TYPE_UNKNOWN) return current;
    if (current == CSV_TYPE_STR || observed == CSV_TYPE_STR) return CSV_TYPE_STR;
    if (current == CSV_TYPE_F64 || observed == CSV_TYPE_F64) return CSV_TYPE_F64;
    if (current == CSV_TYPE_I64 || observed == CSV_TYPE_I64) return CSV_TYPE_I64;
    return current;
}

/* --------------------------------------------------------------------------
 * td_csv_read_opts — main CSV parser
 * -------------------------------------------------------------------------- */

td_t* td_csv_read_opts(const char* path, char delimiter, bool header) {
    size_t file_size = 0;
    char* buf = read_file(path, &file_size);
    if (!buf) return TD_ERR_PTR(TD_ERR_IO);

    /* Detect delimiter if not specified */
    if (delimiter == 0) {
        /* Count commas and tabs in first line */
        int commas = 0, tabs = 0;
        for (const char* p = buf; *p && *p != '\n'; p++) {
            if (*p == ',') commas++;
            if (*p == '\t') tabs++;
        }
        delimiter = (tabs > commas) ? '\t' : ',';
    }

    /* Count columns from first line */
    int ncols = 1;
    for (const char* p = buf; *p && *p != '\n' && *p != '\r'; p++) {
        if (*p == delimiter) ncols++;
        if (*p == '"') {
            p++;
            while (*p && !(*p == '"' && *(p + 1) != '"')) p++;
        }
    }
    if (ncols > CSV_MAX_COLS) ncols = CSV_MAX_COLS;

    /* Parse header row */
    const char* p = buf;
    char field[CSV_MAX_FIELD];
    size_t field_len;

    int64_t* col_name_ids = (int64_t*)calloc((size_t)ncols, sizeof(int64_t));
    if (!col_name_ids) { free(buf); return TD_ERR_PTR(TD_ERR_OOM); }

    if (header) {
        for (int c = 0; c < ncols; c++) {
            p = parse_field(p, delimiter, field, sizeof(field), &field_len);
            col_name_ids[c] = td_sym_intern(field, field_len);
        }
        /* Ensure we're past the newline */
        while (*p == '\r' || *p == '\n') p++;
    } else {
        for (int c = 0; c < ncols; c++) {
            char name[32];
            snprintf(name, sizeof(name), "V%d", c + 1);
            col_name_ids[c] = td_sym_intern(name, strlen(name));
        }
    }

    /* First pass: type inference + count rows */
    int64_t row_count = 0;
    csv_type_t* col_types = (csv_type_t*)calloc((size_t)ncols, sizeof(csv_type_t));
    if (!col_types) { free(col_name_ids); free(buf); return TD_ERR_PTR(TD_ERR_OOM); }

    const char* data_start = p;
    while (*p) {
        /* Skip blank lines */
        if (*p == '\r' || *p == '\n') {
            while (*p == '\r' || *p == '\n') p++;
            continue;
        }
        for (int c = 0; c < ncols && *p; c++) {
            p = parse_field(p, delimiter, field, sizeof(field), &field_len);
            csv_type_t t = detect_type(field, field_len);
            col_types[c] = promote_csv_type(col_types[c], t);
        }
        while (*p == '\r' || *p == '\n') p++;
        row_count++;
    }

    /* Allocate column vectors */
    td_t** col_vecs = (td_t**)calloc((size_t)ncols, sizeof(td_t*));
    if (!col_vecs) {
        free(col_types); free(col_name_ids); free(buf);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    for (int c = 0; c < ncols; c++) {
        int8_t type;
        switch (col_types[c]) {
            case CSV_TYPE_BOOL: type = TD_BOOL; break;
            case CSV_TYPE_I64:  type = TD_I64; break;
            case CSV_TYPE_F64:  type = TD_F64; break;
            case CSV_TYPE_STR:  type = TD_ENUM; break;
            default:            type = TD_ENUM; break;
        }
        col_vecs[c] = td_vec_new(type, row_count);
        if (!col_vecs[c] || TD_IS_ERR(col_vecs[c])) {
            for (int j = 0; j < c; j++) td_release(col_vecs[j]);
            free(col_vecs); free(col_types); free(col_name_ids); free(buf);
            return TD_ERR_PTR(TD_ERR_OOM);
        }
        col_vecs[c]->len = row_count;
    }

    /* Second pass: parse values */
    p = data_start;
    for (int64_t row = 0; row < row_count; row++) {
        for (int c = 0; c < ncols && *p; c++) {
            p = parse_field(p, delimiter, field, sizeof(field), &field_len);

            switch (col_types[c]) {
                case CSV_TYPE_BOOL: {
                    uint8_t val = (field[0] == 't' || field[0] == 'T' || field[0] == '1') ? 1 : 0;
                    ((uint8_t*)td_data(col_vecs[c]))[row] = val;
                    break;
                }
                case CSV_TYPE_I64: {
                    int64_t val = 0;
                    if (field_len > 0) val = strtoll(field, NULL, 10);
                    ((int64_t*)td_data(col_vecs[c]))[row] = val;
                    break;
                }
                case CSV_TYPE_F64: {
                    double val = 0.0;
                    if (field_len > 0) val = strtod(field, NULL);
                    ((double*)td_data(col_vecs[c]))[row] = val;
                    break;
                }
                case CSV_TYPE_STR:
                default: {
                    /* Intern string as ENUM */
                    int64_t sym_id = td_sym_intern(field, field_len);
                    if (sym_id < 0) sym_id = 0; /* fallback to empty-string id on OOM */
                    ((uint32_t*)td_data(col_vecs[c]))[row] = (uint32_t)sym_id;
                    break;
                }
            }
        }
        while (*p == '\r' || *p == '\n') p++;
    }

    /* Build DataFrame */
    td_t* df = td_table_new(ncols);
    if (!df || TD_IS_ERR(df)) {
        for (int c = 0; c < ncols; c++) td_release(col_vecs[c]);
        free(col_vecs); free(col_types); free(col_name_ids); free(buf);
        return df;
    }

    for (int c = 0; c < ncols; c++) {
        df = td_table_add_col(df, col_name_ids[c], col_vecs[c]);
        td_release(col_vecs[c]);
    }

    free(col_vecs);
    free(col_types);
    free(col_name_ids);
    free(buf);

    return df;
}

/* --------------------------------------------------------------------------
 * td_csv_read — convenience wrapper with default options
 * -------------------------------------------------------------------------- */

td_t* td_csv_read(const char* path) {
    return td_csv_read_opts(path, 0, true);
}
