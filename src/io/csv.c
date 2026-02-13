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

/* ============================================================================
 * csv.c — Fast parallel CSV reader
 *
 * Design:
 *   1. mmap + MAP_POPULATE for zero-copy file access
 *   2. memchr-based newline scan for row offset discovery
 *   3. Single-pass: sample-based type inference, then parallel value parsing
 *   4. Inline integer/float parsers (bypass strtoll/strtod overhead)
 *   5. Parallel row parsing via td_pool_dispatch
 *   6. Per-worker local sym tables, merged post-parse on main thread
 * ============================================================================ */

#if defined(__linux__)
  #define _GNU_SOURCE
#endif

#include "csv.h"
#include "ops/pool.h"

#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>

/* --------------------------------------------------------------------------
 * Constants
 * -------------------------------------------------------------------------- */

#define CSV_MAX_COLS      256
#define CSV_SAMPLE_ROWS   100

/* --------------------------------------------------------------------------
 * mmap flags
 * -------------------------------------------------------------------------- */

#ifdef __linux__
  #define MMAP_FLAGS (MAP_PRIVATE | MAP_POPULATE)
#else
  #define MMAP_FLAGS MAP_PRIVATE
#endif

/* --------------------------------------------------------------------------
 * Scratch memory helpers (same pattern as exec.c).
 * Uses td_alloc/td_free (buddy allocator) instead of malloc/free.
 * -------------------------------------------------------------------------- */

static inline void* scratch_alloc(td_t** hdr_out, size_t nbytes) {
    td_t* h = td_alloc(nbytes);
    if (!h) { *hdr_out = NULL; return NULL; }
    *hdr_out = h;
    return td_data(h);
}

static inline void* scratch_realloc(td_t** hdr_out, size_t old_bytes, size_t new_bytes) {
    td_t* old_h = *hdr_out;
    td_t* new_h = td_alloc(new_bytes);
    if (!new_h) return NULL;
    void* new_p = td_data(new_h);
    if (old_h) {
        memcpy(new_p, td_data(old_h), old_bytes < new_bytes ? old_bytes : new_bytes);
        td_free(old_h);
    }
    *hdr_out = new_h;
    return new_p;
}

static inline void scratch_free(td_t* hdr) {
    if (hdr) td_free(hdr);
}

/* --------------------------------------------------------------------------
 * FNV-1a 32-bit hash
 * -------------------------------------------------------------------------- */

static uint32_t fnv1a(const char* data, size_t len) {
    uint32_t h = 0x811c9dc5u;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint8_t)data[i];
        h *= 0x01000193u;
    }
    return h;
}

/* --------------------------------------------------------------------------
 * Per-worker local symbol table
 *
 * Workers must NOT call td_sym_intern() because it allocates string atoms
 * on the calling thread's arena. Worker arenas are destroyed when the pool
 * shuts down, but the global sym table outlives workers — creating dangling
 * pointers. Instead, each worker gets one local_sym_t per string column.
 * Strings are interned locally (no locks). After the parallel parse, the
 * main thread merges local tables into the global sym table.
 *
 * Workers init their local_sym on first use (same-thread alloc via scratch).
 * Main thread frees after merge (cross-thread free → return queue).
 * -------------------------------------------------------------------------- */

#define LSYM_INIT_BUCKETS 128
#define LSYM_INIT_STRS    64
#define LSYM_INIT_ARENA   4096
#define LSYM_LOAD_FACTOR  0.7

/* Pack worker_id + local_id into uint32_t for string columns.
 * Upper 8 bits = worker_id (max 256 workers).
 * Lower 24 bits = local_id (max 16M unique strings per worker per column). */
#define PACK_SYM(wid, lid) (((uint32_t)(wid) << 24) | (lid))
#define UNPACK_WID(packed) ((packed) >> 24)
#define UNPACK_LID(packed) ((packed) & 0x00FFFFFFu)

typedef struct {
    td_t*      buckets_hdr;
    uint64_t*  buckets;      /* (hash<<32) | (id+1), 0 = empty */
    uint32_t   bucket_cap;   /* power of 2 */
    td_t*      offsets_hdr;
    uint32_t*  offsets;      /* offsets[id] = byte offset into arena */
    td_t*      lens_hdr;
    uint32_t*  lens;         /* lens[id] = string length */
    uint32_t   count;
    uint32_t   cap;
    td_t*      arena_hdr;
    char*      arena;        /* growable buffer for string copies */
    size_t     arena_used;
    size_t     arena_cap;
} local_sym_t;

static void local_sym_init(local_sym_t* ls) {
    ls->bucket_cap = LSYM_INIT_BUCKETS;
    ls->buckets = (uint64_t*)scratch_alloc(&ls->buckets_hdr,
                                            ls->bucket_cap * sizeof(uint64_t));
    if (ls->buckets) memset(ls->buckets, 0, ls->bucket_cap * sizeof(uint64_t));
    ls->cap = LSYM_INIT_STRS;
    ls->offsets = (uint32_t*)scratch_alloc(&ls->offsets_hdr,
                                            ls->cap * sizeof(uint32_t));
    ls->lens = (uint32_t*)scratch_alloc(&ls->lens_hdr,
                                         ls->cap * sizeof(uint32_t));
    ls->count = 0;
    ls->arena_cap = LSYM_INIT_ARENA;
    ls->arena = (char*)scratch_alloc(&ls->arena_hdr, ls->arena_cap);
    ls->arena_used = 0;
}

static void local_sym_free(local_sym_t* ls) {
    scratch_free(ls->buckets_hdr);
    scratch_free(ls->offsets_hdr);
    scratch_free(ls->lens_hdr);
    scratch_free(ls->arena_hdr);
    memset(ls, 0, sizeof(*ls));
}

static void local_sym_rehash(local_sym_t* ls) {
    uint32_t new_cap = ls->bucket_cap * 2;
    td_t* new_hdr = NULL;
    uint64_t* new_buckets = (uint64_t*)scratch_alloc(&new_hdr,
                                                      new_cap * sizeof(uint64_t));
    if (!new_buckets) return;
    memset(new_buckets, 0, new_cap * sizeof(uint64_t));

    uint32_t new_mask = new_cap - 1;
    for (uint32_t i = 0; i < ls->bucket_cap; i++) {
        uint64_t e = ls->buckets[i];
        if (e == 0) continue;
        uint32_t h = (uint32_t)(e >> 32);
        uint32_t slot = h & new_mask;
        while (new_buckets[slot] != 0) slot = (slot + 1) & new_mask;
        new_buckets[slot] = e;
    }
    scratch_free(ls->buckets_hdr);
    ls->buckets_hdr = new_hdr;
    ls->buckets = new_buckets;
    ls->bucket_cap = new_cap;
}

static uint32_t local_sym_intern(local_sym_t* ls, const char* str, size_t len) {
    uint32_t hash = fnv1a(str, len);
    uint32_t mask = ls->bucket_cap - 1;
    uint32_t slot = hash & mask;

    for (;;) {
        uint64_t e = ls->buckets[slot];
        if (e == 0) break;
        uint32_t e_hash = (uint32_t)(e >> 32);
        if (e_hash == hash) {
            uint32_t e_id = (uint32_t)(e & 0xFFFFFFFF) - 1;
            if (ls->lens[e_id] == (uint32_t)len &&
                memcmp(ls->arena + ls->offsets[e_id], str, len) == 0)
                return e_id;
        }
        slot = (slot + 1) & mask;
    }

    uint32_t new_id = ls->count;

    if (new_id >= ls->cap) {
        uint32_t new_cap = ls->cap * 2;
        ls->offsets = (uint32_t*)scratch_realloc(&ls->offsets_hdr,
            ls->cap * sizeof(uint32_t), new_cap * sizeof(uint32_t));
        ls->lens = (uint32_t*)scratch_realloc(&ls->lens_hdr,
            ls->cap * sizeof(uint32_t), new_cap * sizeof(uint32_t));
        ls->cap = new_cap;
    }

    if (ls->arena_used + len > ls->arena_cap) {
        size_t new_acap = ls->arena_cap * 2;
        while (new_acap < ls->arena_used + len) new_acap *= 2;
        ls->arena = (char*)scratch_realloc(&ls->arena_hdr,
            ls->arena_cap, new_acap);
        ls->arena_cap = new_acap;
    }
    memcpy(ls->arena + ls->arena_used, str, len);
    ls->offsets[new_id] = (uint32_t)ls->arena_used;
    ls->lens[new_id] = (uint32_t)len;
    ls->arena_used += len;
    ls->count++;

    uint64_t entry = ((uint64_t)hash << 32) | ((uint64_t)(new_id + 1));
    ls->buckets[slot] = entry;

    if ((double)ls->count / (double)ls->bucket_cap > LSYM_LOAD_FACTOR) {
        local_sym_rehash(ls);
    }

    return new_id;
}

/* --------------------------------------------------------------------------
 * Type inference
 * -------------------------------------------------------------------------- */

typedef enum {
    CSV_TYPE_UNKNOWN = 0,
    CSV_TYPE_BOOL,
    CSV_TYPE_I64,
    CSV_TYPE_F64,
    CSV_TYPE_STR
} csv_type_t;

static csv_type_t detect_type(const char* f, size_t len) {
    if (len == 0) return CSV_TYPE_UNKNOWN;

    /* Boolean */
    if ((len == 4 && memcmp(f, "true", 4) == 0) ||
        (len == 5 && memcmp(f, "false", 5) == 0) ||
        (len == 4 && memcmp(f, "TRUE", 4) == 0) ||
        (len == 5 && memcmp(f, "FALSE", 5) == 0))
        return CSV_TYPE_BOOL;

    /* Numeric scan */
    const char* p = f;
    const char* end = f + len;
    if (*p == '-' || *p == '+') p++;
    bool has_dot = false, has_e = false, has_digit = false;
    while (p < end) {
        unsigned char c = (unsigned char)*p;
        if (c >= '0' && c <= '9') { has_digit = true; p++; continue; }
        if (c == '.' && !has_dot) { has_dot = true; p++; continue; }
        if ((c == 'e' || c == 'E') && !has_e) {
            has_e = true; p++;
            if (p < end && (*p == '-' || *p == '+')) p++;
            continue;
        }
        break;
    }
    if (p == end && has_digit) {
        if (!has_dot && !has_e) return CSV_TYPE_I64;
        return CSV_TYPE_F64;
    }
    return CSV_TYPE_STR;
}

static csv_type_t promote_csv_type(csv_type_t cur, csv_type_t obs) {
    if (cur == CSV_TYPE_UNKNOWN) return obs;
    if (obs == CSV_TYPE_UNKNOWN) return cur;
    if (cur == CSV_TYPE_STR || obs == CSV_TYPE_STR) return CSV_TYPE_STR;
    if (cur == CSV_TYPE_F64 || obs == CSV_TYPE_F64) return CSV_TYPE_F64;
    if (cur == CSV_TYPE_I64 || obs == CSV_TYPE_I64) return CSV_TYPE_I64;
    return cur;
}

/* --------------------------------------------------------------------------
 * Zero-copy field scanner
 *
 * Returns pointer past the field's trailing delimiter (or at newline/end).
 * Sets *out and *out_len to the field content. For unquoted fields, *out
 * points directly into the mmap buffer. For quoted fields with escaped
 * quotes, content is unescaped into esc_buf.
 * -------------------------------------------------------------------------- */

static const char* scan_field_quoted(const char* p, const char* buf_end,
                                     char delim,
                                     const char** out, size_t* out_len,
                                     char* esc_buf) {
    p++; /* skip opening quote */
    const char* fld_start = p;
    bool has_escape = false;

    while (p < buf_end) {
        if (*p == '"') {
            if (p + 1 < buf_end && *(p + 1) == '"') {
                has_escape = true;
                p += 2;
            } else {
                break; /* closing quote */
            }
        } else {
            p++;
        }
    }
    size_t raw_len = (size_t)(p - fld_start);
    if (p < buf_end && *p == '"') p++; /* skip closing quote */

    if (has_escape) {
        size_t olen = 0;
        for (const char* s = fld_start; s < fld_start + raw_len; s++) {
            if (*s == '"' && s + 1 < fld_start + raw_len && *(s + 1) == '"') {
                esc_buf[olen++] = '"';
                s++;
            } else {
                esc_buf[olen++] = *s;
            }
        }
        *out = esc_buf;
        *out_len = olen;
    } else {
        *out = fld_start;
        *out_len = raw_len;
    }

    /* Advance past delimiter */
    if (p < buf_end && *p == delim) p++;
    /* Don't advance past newline — caller handles row boundaries */
    return p;
}

TD_INLINE const char* scan_field(const char* p, const char* buf_end,
                                  char delim,
                                  const char** out, size_t* out_len,
                                  char* esc_buf) {
    if (TD_UNLIKELY(p >= buf_end)) {
        *out = p;
        *out_len = 0;
        return p;
    }

    if (TD_LIKELY(*p != '"')) {
        /* Unquoted field — fast path */
        const char* s = p;
        while (p < buf_end && *p != delim && *p != '\n' && *p != '\r') p++;
        *out = s;
        *out_len = (size_t)(p - s);
        if (p < buf_end && *p == delim) return p + 1;
        return p;
    }

    return scan_field_quoted(p, buf_end, delim, out, out_len, esc_buf);
}

/* --------------------------------------------------------------------------
 * Fast inline integer parser (replaces strtoll)
 * -------------------------------------------------------------------------- */

TD_INLINE int64_t fast_i64(const char* p, size_t len) {
    if (TD_UNLIKELY(len == 0)) return 0;

    const char* end = p + len;
    int64_t sign = 1;
    if (*p == '-') { sign = -1; p++; }
    else if (*p == '+') { p++; }

    uint64_t val = 0;
    while (p < end) {
        val = val * 10 + (uint64_t)((unsigned char)*p - '0');
        p++;
    }
    return sign * (int64_t)val;
}

/* --------------------------------------------------------------------------
 * Fast inline float parser (replaces strtod)
 *
 * Handles: [+-]digits[.digits][eE[+-]digits]
 * Uses pow10 lookup table for exponents up to +/-22.
 * -------------------------------------------------------------------------- */

static const double g_pow10[] = {
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,
    1e8,  1e9,  1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
    1e16, 1e17, 1e18, 1e19, 1e20, 1e21, 1e22
};

TD_INLINE double fast_f64(const char* p, size_t len) {
    if (TD_UNLIKELY(len == 0)) return 0.0;

    const char* end = p + len;
    int negative = 0;
    if (*p == '-') { negative = 1; p++; }
    else if (*p == '+') { p++; }

    /* Integer part */
    uint64_t int_part = 0;
    while (p < end && (unsigned)(*p - '0') < 10) {
        int_part = int_part * 10 + (uint64_t)(*p - '0');
        p++;
    }
    double val = (double)int_part;

    /* Fractional part */
    if (p < end && *p == '.') {
        p++;
        uint64_t frac = 0;
        int frac_digits = 0;
        while (p < end && (unsigned)(*p - '0') < 10) {
            frac = frac * 10 + (uint64_t)(*p - '0');
            frac_digits++;
            p++;
        }
        if (frac_digits > 0 && frac_digits <= 22) {
            val += (double)frac / g_pow10[frac_digits];
        } else if (frac_digits > 0) {
            double f = (double)frac;
            int d = frac_digits;
            while (d > 22) { f /= 1e22; d -= 22; }
            f /= g_pow10[d];
            val += f;
        }
    }

    /* Exponent */
    if (p < end && (*p == 'e' || *p == 'E')) {
        p++;
        int exp_neg = 0;
        if (p < end) {
            if (*p == '-') { exp_neg = 1; p++; }
            else if (*p == '+') { p++; }
        }
        int exp_val = 0;
        while (p < end && (unsigned)(*p - '0') < 10) {
            exp_val = exp_val * 10 + (*p - '0');
            p++;
        }
        if (exp_val <= 22) {
            if (exp_neg) val /= g_pow10[exp_val];
            else         val *= g_pow10[exp_val];
        } else {
            int e = exp_val;
            if (exp_neg) {
                while (e > 22) { val /= 1e22; e -= 22; }
                val /= g_pow10[e];
            } else {
                while (e > 22) { val *= 1e22; e -= 22; }
                val *= g_pow10[e];
            }
        }
    }

    return negative ? -val : val;
}

/* --------------------------------------------------------------------------
 * Row offsets builder — memchr-accelerated
 *
 * Uses memchr (glibc: SIMD-accelerated ~15-20 GB/s) for newline scanning.
 * Fast path for quote-free files; falls back to byte-by-byte for quoted
 * fields with embedded newlines. Returns exact row count.
 *
 * Allocates offsets via scratch_alloc. Caller frees with scratch_free.
 * -------------------------------------------------------------------------- */

static int64_t build_row_offsets(const char* buf, size_t buf_size,
                                  size_t data_offset,
                                  int64_t** offsets_out, td_t** hdr_out) {
    const char* p = buf + data_offset;
    const char* end = buf + buf_size;

    /* Skip leading blank lines */
    while (p < end && (*p == '\r' || *p == '\n')) p++;
    if (p >= end) { *offsets_out = NULL; *hdr_out = NULL; return 0; }

    /* Estimate capacity: ~40 bytes per row + headroom */
    size_t remaining = (size_t)(end - p);
    int64_t est = (int64_t)(remaining / 40) + 16;
    td_t* hdr = NULL;
    int64_t* offs = (int64_t*)scratch_alloc(&hdr, (size_t)est * sizeof(int64_t));
    if (!offs) { *offsets_out = NULL; *hdr_out = NULL; return 0; }

    int64_t n = 0;
    offs[n++] = (int64_t)(p - buf);

    /* Check if file has any quotes — determines fast vs slow path */
    bool has_quotes = (memchr(p, '"', remaining) != NULL);

    if (TD_LIKELY(!has_quotes)) {
        /* Fast path: no quotes, use memchr for newlines */
        for (;;) {
            const char* nl = (const char*)memchr(p, '\n', (size_t)(end - p));
            if (!nl) break;
            p = nl + 1;
            /* Skip \r and consecutive blank lines */
            while (p < end && (*p == '\r' || *p == '\n')) p++;
            if (p >= end) break;

            if (n >= est) {
                est *= 2;
                offs = (int64_t*)scratch_realloc(&hdr,
                    (size_t)n * sizeof(int64_t),
                    (size_t)est * sizeof(int64_t));
                if (!offs) { *offsets_out = NULL; *hdr_out = NULL; return 0; }
            }
            offs[n++] = (int64_t)(p - buf);
        }
    } else {
        /* Slow path: track quote parity, byte-by-byte */
        bool in_quote = false;
        while (p < end) {
            char c = *p;
            if (c == '"') {
                in_quote = !in_quote;
                p++;
            } else if (!in_quote && (c == '\n' || c == '\r')) {
                if (c == '\r' && p + 1 < end && *(p + 1) == '\n') p++;
                p++;
                while (p < end && (*p == '\r' || *p == '\n')) p++;
                if (p < end) {
                    if (n >= est) {
                        est *= 2;
                        offs = (int64_t*)scratch_realloc(&hdr,
                            (size_t)n * sizeof(int64_t),
                            (size_t)est * sizeof(int64_t));
                        if (!offs) { *offsets_out = NULL; *hdr_out = NULL; return 0; }
                    }
                    offs[n++] = (int64_t)(p - buf);
                }
            } else {
                p++;
            }
        }
    }

    *offsets_out = offs;
    *hdr_out = hdr;
    return n;
}

/* --------------------------------------------------------------------------
 * Merge per-worker local sym tables into global sym table and fix up
 * the packed (worker_id, local_id) values in string columns.
 *
 * Runs on the main thread so td_sym_intern allocates on the main arena
 * (which outlives workers). Uses VLAs for small arrays.
 * -------------------------------------------------------------------------- */

static void merge_local_syms(local_sym_t* local_syms, uint32_t n_workers,
                              int n_cols, const csv_type_t* col_types,
                              void** col_data, int64_t n_rows) {
    for (int c = 0; c < n_cols; c++) {
        if (col_types[c] != CSV_TYPE_STR) continue;

        /* Build per-worker mappings: local_id → global sym_id (VLA) */
        int64_t* mappings[CSV_MAX_COLS]; /* reuse for n_workers, which is < 256 */
        td_t* map_hdrs[CSV_MAX_COLS];
        for (uint32_t w = 0; w < n_workers; w++) {
            mappings[w] = NULL;
            map_hdrs[w] = NULL;
        }

        for (uint32_t w = 0; w < n_workers; w++) {
            local_sym_t* ls = &local_syms[(size_t)w * (size_t)n_cols + (size_t)c];
            if (ls->count == 0) continue;

            mappings[w] = (int64_t*)scratch_alloc(&map_hdrs[w],
                                                    ls->count * sizeof(int64_t));
            if (!mappings[w]) continue;
            for (uint32_t i = 0; i < ls->count; i++) {
                mappings[w][i] = td_sym_intern(
                    ls->arena + ls->offsets[i], ls->lens[i]);
                if (mappings[w][i] < 0) mappings[w][i] = 0;
            }
        }

        /* Fix up column data: unpack (wid, lid) → global sym_id */
        uint32_t* data = (uint32_t*)col_data[c];
        for (int64_t r = 0; r < n_rows; r++) {
            uint32_t packed = data[r];
            uint32_t wid = UNPACK_WID(packed);
            uint32_t lid = UNPACK_LID(packed);
            if (mappings[wid])
                data[r] = (uint32_t)mappings[wid][lid];
            else
                data[r] = 0;
        }

        for (uint32_t w = 0; w < n_workers; w++) scratch_free(map_hdrs[w]);
    }
}

/* --------------------------------------------------------------------------
 * Parallel parse context and callback
 * -------------------------------------------------------------------------- */

typedef struct {
    const char*       buf;
    size_t            buf_size;
    const int64_t*    row_offsets;
    int               n_cols;
    char              delim;
    const csv_type_t* col_types;
    void**            col_data;
    local_sym_t*      local_syms;   /* [n_workers * n_cols] */
    uint32_t          n_workers;
} csv_par_ctx_t;

static void csv_parse_fn(void* arg, uint32_t worker_id,
                          int64_t start, int64_t end_row) {
    csv_par_ctx_t* ctx = (csv_par_ctx_t*)arg;
    char esc_buf[8192];
    const char* buf_end = ctx->buf + ctx->buf_size;
    local_sym_t* my_syms = ctx->local_syms
                           ? &ctx->local_syms[(size_t)worker_id * (size_t)ctx->n_cols]
                           : NULL;

    /* Lazy init: workers allocate their own local_sym buffers (same-thread) */
    if (my_syms) {
        for (int c = 0; c < ctx->n_cols; c++) {
            if (ctx->col_types[c] == CSV_TYPE_STR && my_syms[c].buckets == NULL)
                local_sym_init(&my_syms[c]);
        }
    }

    for (int64_t row = start; row < end_row; row++) {
        const char* p = ctx->buf + ctx->row_offsets[row];

        for (int c = 0; c < ctx->n_cols; c++) {
            const char* fld;
            size_t flen;
            p = scan_field(p, buf_end, ctx->delim, &fld, &flen, esc_buf);

            switch (ctx->col_types[c]) {
                case CSV_TYPE_BOOL: {
                    uint8_t v = (flen > 0 && (fld[0] == 't' || fld[0] == 'T' || fld[0] == '1')) ? 1 : 0;
                    ((uint8_t*)ctx->col_data[c])[row] = v;
                    break;
                }
                case CSV_TYPE_I64:
                    ((int64_t*)ctx->col_data[c])[row] = fast_i64(fld, flen);
                    break;
                case CSV_TYPE_F64:
                    ((double*)ctx->col_data[c])[row] = fast_f64(fld, flen);
                    break;
                case CSV_TYPE_STR: {
                    uint32_t lid = local_sym_intern(&my_syms[c], fld, flen);
                    ((uint32_t*)ctx->col_data[c])[row] = PACK_SYM(worker_id, lid);
                    break;
                }
                default:
                    break;
            }
        }
    }
}

/* --------------------------------------------------------------------------
 * Serial parse fallback (small files or no thread pool)
 * -------------------------------------------------------------------------- */

static void csv_parse_serial(const char* buf, size_t buf_size,
                              const int64_t* row_offsets, int64_t n_rows,
                              int n_cols, char delim,
                              const csv_type_t* col_types, void** col_data) {
    char esc_buf[8192];
    const char* buf_end = buf + buf_size;

    for (int64_t row = 0; row < n_rows; row++) {
        const char* p = buf + row_offsets[row];

        for (int c = 0; c < n_cols; c++) {
            const char* fld;
            size_t flen;
            p = scan_field(p, buf_end, delim, &fld, &flen, esc_buf);

            switch (col_types[c]) {
                case CSV_TYPE_BOOL: {
                    uint8_t v = (flen > 0 && (fld[0] == 't' || fld[0] == 'T' || fld[0] == '1')) ? 1 : 0;
                    ((uint8_t*)col_data[c])[row] = v;
                    break;
                }
                case CSV_TYPE_I64:
                    ((int64_t*)col_data[c])[row] = fast_i64(fld, flen);
                    break;
                case CSV_TYPE_F64:
                    ((double*)col_data[c])[row] = fast_f64(fld, flen);
                    break;
                case CSV_TYPE_STR: {
                    int64_t sym_id = td_sym_intern(fld, flen);
                    if (sym_id < 0) sym_id = 0;
                    ((uint32_t*)col_data[c])[row] = (uint32_t)sym_id;
                    break;
                }
                default:
                    break;
            }
        }
    }
}

/* --------------------------------------------------------------------------
 * td_csv_read_opts — main CSV parser
 * -------------------------------------------------------------------------- */

td_t* td_csv_read_opts(const char* path, char delimiter, bool header) {
    /* ---- 1. Open file and get size ---- */
    int fd = open(path, O_RDONLY);
    if (fd < 0) return TD_ERR_PTR(TD_ERR_IO);

    struct stat st;
    if (fstat(fd, &st) != 0 || st.st_size <= 0) {
        close(fd);
        return TD_ERR_PTR(TD_ERR_IO);
    }
    size_t file_size = (size_t)st.st_size;

    /* ---- 2. mmap the file ---- */
    char* buf = (char*)mmap(NULL, file_size, PROT_READ, MMAP_FLAGS, fd, 0);
    close(fd);
    if (buf == MAP_FAILED) return TD_ERR_PTR(TD_ERR_IO);

#ifdef __APPLE__
    madvise(buf, file_size, MADV_SEQUENTIAL);
#endif

    const char* buf_end = buf + file_size;
    td_t* result = NULL;

    /* ---- 3. Detect delimiter ---- */
    if (delimiter == 0) {
        int commas = 0, tabs = 0;
        for (const char* p = buf; p < buf_end && *p != '\n'; p++) {
            if (*p == ',') commas++;
            if (*p == '\t') tabs++;
        }
        delimiter = (tabs > commas) ? '\t' : ',';
    }

    /* ---- 4. Count columns from first line ---- */
    int ncols = 1;
    {
        const char* p = buf;
        bool in_quote = false;
        while (p < buf_end && (in_quote || (*p != '\n' && *p != '\r'))) {
            if (*p == '"') in_quote = !in_quote;
            else if (!in_quote && *p == delimiter) ncols++;
            p++;
        }
    }
    if (ncols > CSV_MAX_COLS) ncols = CSV_MAX_COLS;

    /* ---- 5. Parse header row ---- */
    const char* p = buf;
    char esc_buf[8192];
    int64_t col_name_ids[CSV_MAX_COLS];

    if (header) {
        for (int c = 0; c < ncols; c++) {
            const char* fld;
            size_t flen;
            p = scan_field(p, buf_end, delimiter, &fld, &flen, esc_buf);
            col_name_ids[c] = td_sym_intern(fld, flen);
        }
        while (p < buf_end && (*p == '\r' || *p == '\n')) p++;
    } else {
        for (int c = 0; c < ncols; c++) {
            char name[32];
            snprintf(name, sizeof(name), "V%d", c + 1);
            col_name_ids[c] = td_sym_intern(name, strlen(name));
        }
    }

    size_t data_offset = (size_t)(p - buf);

    /* ---- 6. Build row offsets (memchr-accelerated) ---- */
    td_t* row_offsets_hdr = NULL;
    int64_t* row_offsets = NULL;
    int64_t n_rows = build_row_offsets(buf, file_size, data_offset,
                                        &row_offsets, &row_offsets_hdr);

    if (n_rows == 0) {
        /* Empty file → empty DataFrame */
        td_t* df = td_table_new(ncols);
        if (!df || TD_IS_ERR(df)) goto fail_unmap;
        for (int c = 0; c < ncols; c++) {
            td_t* empty_vec = td_vec_new(TD_F64, 0);
            if (empty_vec && !TD_IS_ERR(empty_vec)) {
                df = td_table_add_col(df, col_name_ids[c], empty_vec);
                td_release(empty_vec);
            }
        }
        munmap(buf, file_size);
        return df;
    }

    /* ---- 7. Sample-based type inference ---- */
    csv_type_t col_types[CSV_MAX_COLS];
    memset(col_types, 0, (size_t)ncols * sizeof(csv_type_t));
    {
        int64_t sample_n = (n_rows < CSV_SAMPLE_ROWS) ? n_rows : CSV_SAMPLE_ROWS;
        for (int64_t r = 0; r < sample_n; r++) {
            const char* rp = buf + row_offsets[r];
            for (int c = 0; c < ncols; c++) {
                const char* fld;
                size_t flen;
                rp = scan_field(rp, buf_end, delimiter, &fld, &flen, esc_buf);
                csv_type_t t = detect_type(fld, flen);
                col_types[c] = promote_csv_type(col_types[c], t);
            }
        }
    }

    /* ---- 8. Allocate column vectors ---- */
    td_t* col_vecs[CSV_MAX_COLS];
    void* col_data[CSV_MAX_COLS];

    for (int c = 0; c < ncols; c++) {
        int8_t type;
        switch (col_types[c]) {
            case CSV_TYPE_BOOL: type = TD_BOOL; break;
            case CSV_TYPE_I64:  type = TD_I64;  break;
            case CSV_TYPE_F64:  type = TD_F64;  break;
            case CSV_TYPE_STR:  type = TD_ENUM; break;
            default:            type = TD_ENUM; break;
        }
        col_vecs[c] = td_vec_new(type, n_rows);
        if (!col_vecs[c] || TD_IS_ERR(col_vecs[c])) {
            for (int j = 0; j < c; j++) td_release(col_vecs[j]);
            goto fail_offsets;
        }
        col_vecs[c]->len = n_rows;
        col_data[c] = td_data(col_vecs[c]);
    }

    /* ---- 9. Parse data ---- */
    {
        /* Check if any string columns exist */
        int has_str_cols = 0;
        for (int c = 0; c < ncols; c++) {
            if (col_types[c] == CSV_TYPE_STR) { has_str_cols = 1; break; }
        }

        td_pool_t* pool = td_pool_get();
        bool use_parallel = pool && n_rows > 8192;

        if (use_parallel) {
            uint32_t n_workers = td_pool_total_workers(pool);

            /* Allocate per-worker local sym tables for string columns.
             * Zero-init so workers can lazy-init on first use. */
            td_t* local_syms_hdr = NULL;
            local_sym_t* local_syms = NULL;
            if (has_str_cols) {
                size_t lsym_sz = (size_t)n_workers * (size_t)ncols * sizeof(local_sym_t);
                local_syms = (local_sym_t*)scratch_alloc(&local_syms_hdr, lsym_sz);
                if (local_syms) {
                    memset(local_syms, 0, lsym_sz);
                } else {
                    use_parallel = false;
                }
            }

            if (use_parallel) {
                csv_par_ctx_t ctx = {
                    .buf         = buf,
                    .buf_size    = file_size,
                    .row_offsets = row_offsets,
                    .n_cols      = ncols,
                    .delim       = delimiter,
                    .col_types   = col_types,
                    .col_data    = col_data,
                    .local_syms  = local_syms,
                    .n_workers   = n_workers,
                };

                td_pool_dispatch(pool, csv_parse_fn, &ctx, n_rows);

                /* Merge local sym tables into global (main thread — safe) */
                if (has_str_cols && local_syms) {
                    merge_local_syms(local_syms, n_workers, ncols,
                                     col_types, col_data, n_rows);

                    for (uint32_t w = 0; w < n_workers; w++) {
                        for (int c = 0; c < ncols; c++) {
                            if (col_types[c] == CSV_TYPE_STR)
                                local_sym_free(&local_syms[(size_t)w * (size_t)ncols + (size_t)c]);
                        }
                    }
                }
                scratch_free(local_syms_hdr);
            }
        }

        if (!use_parallel) {
            csv_parse_serial(buf, file_size, row_offsets, n_rows,
                             ncols, delimiter, col_types, col_data);
        }
    }

    /* ---- 11. Build DataFrame ---- */
    {
        td_t* df = td_table_new(ncols);
        if (!df || TD_IS_ERR(df)) {
            for (int c = 0; c < ncols; c++) td_release(col_vecs[c]);
            goto fail_offsets;
        }

        for (int c = 0; c < ncols; c++) {
            df = td_table_add_col(df, col_name_ids[c], col_vecs[c]);
            td_release(col_vecs[c]);
        }

        result = df;
    }

    /* ---- 12. Cleanup ---- */
    scratch_free(row_offsets_hdr);
    munmap(buf, file_size);
    return result;

    /* Error paths */
fail_offsets:
    scratch_free(row_offsets_hdr);
fail_unmap:
    munmap(buf, file_size);
    return TD_ERR_PTR(TD_ERR_OOM);
}

/* --------------------------------------------------------------------------
 * td_csv_read — convenience wrapper with default options
 * -------------------------------------------------------------------------- */

td_t* td_csv_read(const char* path) {
    return td_csv_read_opts(path, 0, true);
}
