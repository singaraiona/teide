#include "exec.h"
#include "hash.h"
#include "pool.h"
#include <string.h>
#include <math.h>
#include <float.h>

/* --------------------------------------------------------------------------
 * Arena-based scratch allocation helpers
 *
 * All temporary buffers use the buddy allocator instead of malloc/free.
 * td_alloc() returns a td_t* header; data starts at td_data(hdr).
 * -------------------------------------------------------------------------- */

/* Allocate zero-initialized scratch buffer, returns data pointer.
 * *hdr_out receives the td_t* header for later td_free(). */
static inline void* scratch_calloc(td_t** hdr_out, size_t nbytes) {
    td_t* h = td_alloc(nbytes);
    if (!h) { *hdr_out = NULL; return NULL; }
    void* p = td_data(h);
    memset(p, 0, nbytes);
    *hdr_out = h;
    return p;
}

/* Allocate uninitialized scratch buffer. */
static inline void* scratch_alloc(td_t** hdr_out, size_t nbytes) {
    td_t* h = td_alloc(nbytes);
    if (!h) { *hdr_out = NULL; return NULL; }
    *hdr_out = h;
    return td_data(h);
}

/* Reallocate: alloc new, copy old, free old. Returns new data pointer. */
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

/* Free a scratch buffer (NULL-safe). */
static inline void scratch_free(td_t* hdr) {
    if (hdr) td_free(hdr);
}

/* --------------------------------------------------------------------------
 * Helper: find the extended node for a given base node ID
 * -------------------------------------------------------------------------- */

static td_op_ext_t* find_ext(td_graph_t* g, uint32_t node_id) {
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i]->base.id == node_id)
            return g->ext_nodes[i];
    }
    return NULL;
}

/* ============================================================================
 * Element-wise execution
 * ============================================================================ */

static td_t* exec_elementwise_unary(td_graph_t* g, td_op_t* op, td_t* input) {
    (void)g;
    if (!input || TD_IS_ERR(input)) return input;
    int64_t len = input->len;
    int8_t in_type = input->type;
    int8_t out_type = op->out_type;

    td_t* result = td_vec_new(out_type, len);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = len;

    td_morsel_t m;
    td_morsel_init(&m, input);
    int64_t out_off = 0;

    while (td_morsel_next(&m)) {
        int64_t n = m.morsel_len;
        void* dst = (char*)td_data(result) + out_off * td_elem_size(out_type);

        if (in_type == TD_F64 || in_type == TD_I64) {
            for (int64_t i = 0; i < n; i++) {
                if (in_type == TD_F64) {
                    double v = ((double*)m.morsel_ptr)[i];
                    double r;
                    switch (op->opcode) {
                        case OP_NEG:   r = -v; break;
                        case OP_ABS:   r = fabs(v); break;
                        case OP_SQRT:  r = sqrt(v); break;
                        case OP_LOG:   r = log(v); break;
                        case OP_EXP:   r = exp(v); break;
                        case OP_CEIL:  r = ceil(v); break;
                        case OP_FLOOR: r = floor(v); break;
                        default:       r = v; break;
                    }
                    if (out_type == TD_F64) ((double*)dst)[i] = r;
                    else if (out_type == TD_I64) ((int64_t*)dst)[i] = (int64_t)r;
                } else {
                    int64_t v = ((int64_t*)m.morsel_ptr)[i];
                    if (out_type == TD_I64) {
                        int64_t r;
                        switch (op->opcode) {
                            case OP_NEG: r = -v; break;
                            case OP_ABS: r = v < 0 ? -v : v; break;
                            default:     r = v; break;
                        }
                        ((int64_t*)dst)[i] = r;
                    } else if (out_type == TD_F64) {
                        double r;
                        switch (op->opcode) {
                            case OP_NEG:   r = -(double)v; break;
                            case OP_SQRT:  r = sqrt((double)v); break;
                            case OP_LOG:   r = log((double)v); break;
                            case OP_EXP:   r = exp((double)v); break;
                            default:       r = (double)v; break;
                        }
                        ((double*)dst)[i] = r;
                    } else if (out_type == TD_BOOL) {
                        /* ISNULL: for non-null vecs, always false */
                        ((uint8_t*)dst)[i] = 0;
                    }
                }
            }
        } else if (in_type == TD_BOOL && op->opcode == OP_NOT) {
            for (int64_t i = 0; i < n; i++) {
                ((uint8_t*)dst)[i] = !((uint8_t*)m.morsel_ptr)[i];
            }
        }

        out_off += n;
    }

    return result;
}

/* Inner loop for binary element-wise over a range [start, end) */
static void binary_range(td_op_t* op, int8_t out_type,
                         td_t* lhs, td_t* rhs, td_t* result,
                         bool l_scalar, bool r_scalar,
                         double l_f64, double r_f64,
                         int64_t l_i64, int64_t r_i64,
                         int64_t start, int64_t end) {
    uint8_t out_esz = td_elem_size(out_type);
    void* dst = (char*)td_data(result) + start * out_esz;
    int64_t n = end - start;

    /* Pointers into source data at offset start */
    double* lp_f64 = NULL; int64_t* lp_i64 = NULL; uint8_t* lp_bool = NULL;
    double* rp_f64 = NULL; int64_t* rp_i64 = NULL; uint8_t* rp_bool = NULL;

    if (!l_scalar) {
        char* lbase = (char*)td_data(lhs) + start * td_elem_size(lhs->type);
        if (lhs->type == TD_F64) lp_f64 = (double*)lbase;
        else if (lhs->type == TD_I64) lp_i64 = (int64_t*)lbase;
        else if (lhs->type == TD_BOOL) lp_bool = (uint8_t*)lbase;
    }
    if (!r_scalar) {
        char* rbase = (char*)td_data(rhs) + start * td_elem_size(rhs->type);
        if (rhs->type == TD_F64) rp_f64 = (double*)rbase;
        else if (rhs->type == TD_I64) rp_i64 = (int64_t*)rbase;
        else if (rhs->type == TD_BOOL) rp_bool = (uint8_t*)rbase;
    }

    for (int64_t i = 0; i < n; i++) {
        double lv, rv;
        if (lp_f64)       lv = lp_f64[i];
        else if (lp_i64)  lv = (double)lp_i64[i];
        else if (lp_bool) lv = (double)lp_bool[i];
        else if (l_scalar && (lhs->type == TD_ATOM_F64 || lhs->type == -TD_F64)) lv = l_f64;
        else              lv = (double)l_i64;

        if (rp_f64)       rv = rp_f64[i];
        else if (rp_i64)  rv = (double)rp_i64[i];
        else if (rp_bool) rv = (double)rp_bool[i];
        else if (r_scalar && (rhs->type == TD_ATOM_F64 || rhs->type == -TD_F64)) rv = r_f64;
        else              rv = (double)r_i64;

        if (out_type == TD_F64) {
            double r;
            switch (op->opcode) {
                case OP_ADD: r = lv + rv; break;
                case OP_SUB: r = lv - rv; break;
                case OP_MUL: r = lv * rv; break;
                case OP_DIV: r = rv != 0.0 ? lv / rv : 0.0; break;
                case OP_MOD: r = rv != 0.0 ? fmod(lv, rv) : 0.0; break;
                case OP_MIN2: r = lv < rv ? lv : rv; break;
                case OP_MAX2: r = lv > rv ? lv : rv; break;
                default: r = 0.0; break;
            }
            ((double*)dst)[i] = r;
        } else if (out_type == TD_I64) {
            int64_t li = (int64_t)lv, ri = (int64_t)rv;
            int64_t r;
            switch (op->opcode) {
                case OP_ADD: r = li + ri; break;
                case OP_SUB: r = li - ri; break;
                case OP_MUL: r = li * ri; break;
                case OP_DIV: r = ri != 0 ? li / ri : 0; break;
                case OP_MOD: r = ri != 0 ? li % ri : 0; break;
                case OP_MIN2: r = li < ri ? li : ri; break;
                case OP_MAX2: r = li > ri ? li : ri; break;
                default: r = 0; break;
            }
            ((int64_t*)dst)[i] = r;
        } else if (out_type == TD_BOOL) {
            uint8_t r;
            switch (op->opcode) {
                case OP_EQ:  r = lv == rv; break;
                case OP_NE:  r = lv != rv; break;
                case OP_LT:  r = lv < rv; break;
                case OP_LE:  r = lv <= rv; break;
                case OP_GT:  r = lv > rv; break;
                case OP_GE:  r = lv >= rv; break;
                case OP_AND: r = (uint8_t)lv && (uint8_t)rv; break;
                case OP_OR:  r = (uint8_t)lv || (uint8_t)rv; break;
                default: r = 0; break;
            }
            ((uint8_t*)dst)[i] = r;
        }
    }
}

/* Context for parallel binary dispatch */
typedef struct {
    td_op_t* op;
    int8_t   out_type;
    td_t*    lhs;
    td_t*    rhs;
    td_t*    result;
    bool     l_scalar;
    bool     r_scalar;
    double   l_f64, r_f64;
    int64_t  l_i64, r_i64;
} par_binary_ctx_t;

static void par_binary_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    par_binary_ctx_t* c = (par_binary_ctx_t*)ctx;
    binary_range(c->op, c->out_type, c->lhs, c->rhs, c->result,
                 c->l_scalar, c->r_scalar,
                 c->l_f64, c->r_f64, c->l_i64, c->r_i64,
                 start, end);
}

static td_t* exec_elementwise_binary(td_graph_t* g, td_op_t* op, td_t* lhs, td_t* rhs) {
    (void)g;
    if (!lhs || TD_IS_ERR(lhs)) return lhs;
    if (!rhs || TD_IS_ERR(rhs)) return rhs;

    int64_t len = lhs->len;
    bool l_scalar = td_is_atom(lhs);
    bool r_scalar = td_is_atom(rhs);
    if (l_scalar) len = rhs->len;
    if (r_scalar && !l_scalar) len = lhs->len;

    int8_t out_type = op->out_type;
    td_t* result = td_vec_new(out_type, len);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = len;

    double l_f64_val = 0, r_f64_val = 0;
    int64_t l_i64_val = 0, r_i64_val = 0;
    if (l_scalar) {
        if (lhs->type == TD_ATOM_F64 || lhs->type == -TD_F64) l_f64_val = lhs->f64;
        else l_i64_val = lhs->i64;
    }
    if (r_scalar) {
        if (rhs->type == TD_ATOM_F64 || rhs->type == -TD_F64) r_f64_val = rhs->f64;
        else r_i64_val = rhs->i64;
    }

    td_pool_t* pool = td_pool_get();
    if (pool && len >= TD_PARALLEL_THRESHOLD && !l_scalar && !r_scalar) {
        par_binary_ctx_t ctx = {
            .op = op, .out_type = out_type,
            .lhs = lhs, .rhs = rhs, .result = result,
            .l_scalar = l_scalar, .r_scalar = r_scalar,
            .l_f64 = l_f64_val, .r_f64 = r_f64_val,
            .l_i64 = l_i64_val, .r_i64 = r_i64_val,
        };
        td_pool_dispatch(pool, par_binary_fn, &ctx, len);
        return result;
    }

    /* Sequential fallback */
    binary_range(op, out_type, lhs, rhs, result,
                 l_scalar, r_scalar,
                 l_f64_val, r_f64_val, l_i64_val, r_i64_val,
                 0, len);
    return result;
}

/* ============================================================================
 * Reduction execution
 * ============================================================================ */

typedef struct {
    double sum_f, min_f, max_f, prod_f, first_f, last_f;
    int64_t sum_i, min_i, max_i, prod_i, first_i, last_i;
    int64_t cnt;
    bool has_first;
} reduce_acc_t;

static void reduce_acc_init(reduce_acc_t* acc) {
    acc->sum_f = 0; acc->min_f = DBL_MAX; acc->max_f = -DBL_MAX;
    acc->prod_f = 1.0; acc->first_f = 0; acc->last_f = 0;
    acc->sum_i = 0; acc->min_i = INT64_MAX; acc->max_i = INT64_MIN;
    acc->prod_i = 1; acc->first_i = 0; acc->last_i = 0;
    acc->cnt = 0; acc->has_first = false;
}

static void reduce_range(td_t* input, int64_t start, int64_t end, reduce_acc_t* acc) {
    int8_t in_type = input->type;
    char* base = (char*)td_data(input);

    for (int64_t row = start; row < end; row++) {
        if (in_type == TD_F64) {
            double v = ((double*)base)[row];
            acc->sum_f += v;
            acc->prod_f *= v;
            if (v < acc->min_f) acc->min_f = v;
            if (v > acc->max_f) acc->max_f = v;
            if (!acc->has_first) { acc->first_f = v; acc->has_first = true; }
            acc->last_f = v;
        } else {
            int64_t v;
            if (in_type == TD_I64 || in_type == TD_SYM)
                v = ((int64_t*)base)[row];
            else if (in_type == TD_I32)
                v = ((int32_t*)base)[row];
            else if (in_type == TD_I16)
                v = ((int16_t*)base)[row];
            else if (in_type == TD_BOOL || in_type == TD_U8)
                v = ((uint8_t*)base)[row];
            else
                v = ((int64_t*)base)[row];
            acc->sum_i += v;
            acc->prod_i *= v;
            if (v < acc->min_i) acc->min_i = v;
            if (v > acc->max_i) acc->max_i = v;
            if (!acc->has_first) { acc->first_i = v; acc->has_first = true; }
            acc->last_i = v;
        }
        acc->cnt++;
    }
}

/* Context for parallel reduction */
typedef struct {
    td_t*         input;
    reduce_acc_t* accs;   /* one per worker */
} par_reduce_ctx_t;

static void par_reduce_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    par_reduce_ctx_t* c = (par_reduce_ctx_t*)ctx;
    reduce_range(c->input, start, end, &c->accs[worker_id]);
}

static void reduce_merge(reduce_acc_t* dst, const reduce_acc_t* src, int8_t in_type) {
    if (in_type == TD_F64) {
        dst->sum_f += src->sum_f;
        dst->prod_f *= src->prod_f;
        if (src->min_f < dst->min_f) dst->min_f = src->min_f;
        if (src->max_f > dst->max_f) dst->max_f = src->max_f;
    } else {
        dst->sum_i += src->sum_i;
        dst->prod_i *= src->prod_i;
        if (src->min_i < dst->min_i) dst->min_i = src->min_i;
        if (src->max_i > dst->max_i) dst->max_i = src->max_i;
    }
    dst->cnt += src->cnt;
    /* first/last: keep lowest-index first and highest-index last
     * Since workers process sequential ranges, worker 0's first is the global first,
     * and the last worker's last is the global last. We handle this after merge. */
}

static td_t* exec_reduction(td_graph_t* g, td_op_t* op, td_t* input) {
    (void)g;
    if (!input || TD_IS_ERR(input)) return input;

    int8_t in_type = input->type;
    int64_t len = input->len;

    td_pool_t* pool = td_pool_get();
    if (pool && len >= TD_PARALLEL_THRESHOLD) {
        uint32_t nw = td_pool_total_workers(pool);
        reduce_acc_t accs[nw];
        memset(accs, 0, nw * sizeof(reduce_acc_t));
        for (uint32_t i = 0; i < nw; i++) reduce_acc_init(&accs[i]);

        par_reduce_ctx_t ctx = { .input = input, .accs = accs };
        td_pool_dispatch(pool, par_reduce_fn, &ctx, len);

        /* Merge: worker 0 is the base, merge the rest in order */
        reduce_acc_t merged;
        reduce_acc_init(&merged);
        merged = accs[0];
        for (uint32_t i = 1; i < nw; i++) {
            if (!accs[i].has_first) continue;
            reduce_merge(&merged, &accs[i], in_type);
        }
        /* first = accs[first worker with data], last = accs[last worker with data] */
        for (uint32_t i = 0; i < nw; i++) {
            if (accs[i].has_first) {
                if (in_type == TD_F64) merged.first_f = accs[i].first_f;
                else merged.first_i = accs[i].first_i;
                break;
            }
        }
        for (int32_t i = (int32_t)nw - 1; i >= 0; i--) {
            if (accs[i].has_first) {
                if (in_type == TD_F64) merged.last_f = accs[i].last_f;
                else merged.last_i = accs[i].last_i;
                break;
            }
        }

        switch (op->opcode) {
            case OP_SUM:   return in_type == TD_F64 ? td_f64(merged.sum_f) : td_i64(merged.sum_i);
            case OP_PROD:  return in_type == TD_F64 ? td_f64(merged.prod_f) : td_i64(merged.prod_i);
            case OP_MIN:   return in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.min_f : 0.0) : td_i64(merged.cnt > 0 ? merged.min_i : 0);
            case OP_MAX:   return in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.max_f : 0.0) : td_i64(merged.cnt > 0 ? merged.max_i : 0);
            case OP_COUNT: return td_i64(merged.cnt);
            case OP_AVG:   return in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.sum_f / merged.cnt : 0.0) : td_f64(merged.cnt > 0 ? (double)merged.sum_i / merged.cnt : 0.0);
            case OP_FIRST: return in_type == TD_F64 ? td_f64(merged.first_f) : td_i64(merged.first_i);
            case OP_LAST:  return in_type == TD_F64 ? td_f64(merged.last_f) : td_i64(merged.last_i);
            default:       return TD_ERR_PTR(TD_ERR_NYI);
        }
    }

    reduce_acc_t acc;
    reduce_acc_init(&acc);
    reduce_range(input, 0, len, &acc);

    switch (op->opcode) {
        case OP_SUM:   return in_type == TD_F64 ? td_f64(acc.sum_f) : td_i64(acc.sum_i);
        case OP_PROD:  return in_type == TD_F64 ? td_f64(acc.prod_f) : td_i64(acc.prod_i);
        case OP_MIN:   return in_type == TD_F64 ? td_f64(acc.cnt > 0 ? acc.min_f : 0.0) : td_i64(acc.cnt > 0 ? acc.min_i : 0);
        case OP_MAX:   return in_type == TD_F64 ? td_f64(acc.cnt > 0 ? acc.max_f : 0.0) : td_i64(acc.cnt > 0 ? acc.max_i : 0);
        case OP_COUNT: return td_i64(acc.cnt);
        case OP_AVG:   return in_type == TD_F64 ? td_f64(acc.cnt > 0 ? acc.sum_f / acc.cnt : 0.0) : td_f64(acc.cnt > 0 ? (double)acc.sum_i / acc.cnt : 0.0);
        case OP_FIRST: return in_type == TD_F64 ? td_f64(acc.first_f) : td_i64(acc.first_i);
        case OP_LAST:  return in_type == TD_F64 ? td_f64(acc.last_f) : td_i64(acc.last_i);
        default:       return TD_ERR_PTR(TD_ERR_NYI);
    }
}

/* ============================================================================
 * Filter execution
 * ============================================================================ */

static td_t* exec_filter(td_graph_t* g, td_op_t* op, td_t* input, td_t* pred) {
    (void)g;
    (void)op;
    if (!input || TD_IS_ERR(input)) return input;
    if (!pred || TD_IS_ERR(pred)) return pred;
    uint8_t esz = td_elem_size(input->type);

    /* Count passing elements */
    int64_t pass_count = 0;
    td_morsel_t mp;
    td_morsel_init(&mp, pred);
    while (td_morsel_next(&mp)) {
        uint8_t* bits = (uint8_t*)mp.morsel_ptr;
        for (int64_t i = 0; i < mp.morsel_len; i++)
            if (bits[i]) pass_count++;
    }

    td_t* result = td_vec_new(input->type, pass_count);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = pass_count;

    /* Copy passing elements */
    td_morsel_t mi, mf;
    td_morsel_init(&mi, input);
    td_morsel_init(&mf, pred);
    int64_t out_idx = 0;

    while (td_morsel_next(&mi) && td_morsel_next(&mf)) {
        uint8_t* bits = (uint8_t*)mf.morsel_ptr;
        char* src = (char*)mi.morsel_ptr;
        char* dst = (char*)td_data(result);
        for (int64_t i = 0; i < mi.morsel_len; i++) {
            if (bits[i]) {
                memcpy(dst + out_idx * esz, src + i * esz, esz);
                out_idx++;
            }
        }
    }

    return result;
}

/* ============================================================================
 * Sort execution (simple insertion sort)
 * ============================================================================ */

static td_t* exec_sort(td_graph_t* g, td_op_t* op, td_t* df) {
    if (!df || TD_IS_ERR(df)) return df;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t nrows = td_table_nrows(df);
    int64_t ncols = td_table_ncols(df);
    uint8_t n_sort = ext->sort.n_cols;

    td_t* indices_hdr;
    int64_t* indices = (int64_t*)scratch_alloc(&indices_hdr, (size_t)nrows * sizeof(int64_t));
    if (!indices) return TD_ERR_PTR(TD_ERR_OOM);
    for (int64_t i = 0; i < nrows; i++) indices[i] = i;

    td_t* sort_vecs[n_sort];
    memset(sort_vecs, 0, n_sort * sizeof(td_t*));

    for (uint8_t k = 0; k < n_sort; k++) {
        td_op_t* key_op = ext->sort.columns[k];
        td_op_ext_t* key_ext = find_ext(g, key_op->id);
        if (key_ext && key_ext->base.opcode == OP_SCAN) {
            sort_vecs[k] = td_table_get_col(df, key_ext->sym);
        } else {
            sort_vecs[k] = NULL;
        }
    }

    for (int64_t i = 1; i < nrows; i++) {
        int64_t key = indices[i];
        int64_t j = i - 1;
        while (j >= 0) {
            int cmp = 0;
            int64_t ij = indices[j];
            for (uint8_t k = 0; k < n_sort && cmp == 0; k++) {
                td_t* col = sort_vecs[k];
                if (!col) continue;
                int desc = ext->sort.desc ? ext->sort.desc[k] : 0;

                if (col->type == TD_F64) {
                    double a = ((double*)td_data(col))[ij];
                    double b = ((double*)td_data(col))[key];
                    if (a < b) cmp = -1;
                    else if (a > b) cmp = 1;
                } else if (col->type == TD_I64 || col->type == TD_SYM || col->type == TD_TIMESTAMP) {
                    int64_t a = ((int64_t*)td_data(col))[ij];
                    int64_t b = ((int64_t*)td_data(col))[key];
                    if (a < b) cmp = -1;
                    else if (a > b) cmp = 1;
                } else if (col->type == TD_I32) {
                    int32_t a = ((int32_t*)td_data(col))[ij];
                    int32_t b = ((int32_t*)td_data(col))[key];
                    if (a < b) cmp = -1;
                    else if (a > b) cmp = 1;
                } else if (col->type == TD_ENUM) {
                    uint32_t a = ((uint32_t*)td_data(col))[ij];
                    uint32_t b = ((uint32_t*)td_data(col))[key];
                    td_t* sa = td_sym_str((int64_t)a);
                    td_t* sb = td_sym_str((int64_t)b);
                    if (sa && sb) cmp = td_str_cmp(sa, sb);
                }

                if (desc) cmp = -cmp;
            }
            if (cmp <= 0) break;
            indices[j + 1] = indices[j];
            j--;
        }
        indices[j + 1] = key;
    }

    td_t* result = td_table_new(ncols);
    if (!result || TD_IS_ERR(result)) {
        scratch_free(indices_hdr); return result;
    }

    for (int64_t c = 0; c < ncols; c++) {
        td_t* col = td_table_get_col_idx(df, c);
        int64_t name_id = td_table_col_name(df, c);
        if (!col) continue;

        uint8_t esz = td_elem_size(col->type);
        td_t* new_col = td_vec_new(col->type, nrows);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = nrows;

        char* src = (char*)td_data(col);
        char* dst = (char*)td_data(new_col);
        for (int64_t i = 0; i < nrows; i++) {
            memcpy(dst + i * esz, src + indices[i] * esz, esz);
        }

        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }

    scratch_free(indices_hdr);
    return result;
}

/* ============================================================================
 * Group-by execution — with parallel local hash tables + merge
 * ============================================================================ */

/* Hash using td_t** (used by join code) */
static uint64_t hash_row_keys(td_t** key_vecs, uint8_t n_keys, int64_t row) {
    uint64_t h = 0;
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* col = key_vecs[k];
        if (!col) continue;
        uint64_t kh;
        if (col->type == TD_I64 || col->type == TD_SYM || col->type == TD_TIMESTAMP)
            kh = td_hash_i64(((int64_t*)td_data(col))[row]);
        else if (col->type == TD_F64)
            kh = td_hash_f64(((double*)td_data(col))[row]);
        else if (col->type == TD_I32)
            kh = td_hash_i64((int64_t)((int32_t*)td_data(col))[row]);
        else if (col->type == TD_ENUM)
            kh = td_hash_i64((int64_t)((uint32_t*)td_data(col))[row]);
        else
            kh = 0;
        h = (k == 0) ? kh : td_hash_combine(h, kh);
    }
    return h;
}

/* --- Pre-computed key metadata for the group-by hot path --- */

/* Hash with pre-computed key_data/key_types arrays (eliminates td_data() calls) */
static inline uint64_t hash_row_fast(void** key_data, int8_t* key_types,
                                     uint8_t n_keys, int64_t row) {
    uint64_t h = 0;
    for (uint8_t k = 0; k < n_keys; k++) {
        uint64_t kh;
        int8_t t = key_types[k];
        if (t == TD_I64 || t == TD_SYM || t == TD_TIMESTAMP)
            kh = td_hash_i64(((int64_t*)key_data[k])[row]);
        else if (t == TD_F64)
            kh = td_hash_f64(((double*)key_data[k])[row]);
        else if (t == TD_I32)
            kh = td_hash_i64((int64_t)((int32_t*)key_data[k])[row]);
        else if (t == TD_ENUM)
            kh = td_hash_i64((int64_t)((uint32_t*)key_data[k])[row]);
        else
            kh = 0;
        h = (k == 0) ? kh : td_hash_combine(h, kh);
    }
    return h;
}

static inline bool keys_equal_fast(void** key_data, int8_t* key_types,
                                   uint8_t n_keys, int64_t a, int64_t b) {
    for (uint8_t k = 0; k < n_keys; k++) {
        int8_t t = key_types[k];
        if (t == TD_I64 || t == TD_SYM || t == TD_TIMESTAMP) {
            if (((int64_t*)key_data[k])[a] != ((int64_t*)key_data[k])[b]) return false;
        } else if (t == TD_F64) {
            if (((double*)key_data[k])[a] != ((double*)key_data[k])[b]) return false;
        } else if (t == TD_I32) {
            if (((int32_t*)key_data[k])[a] != ((int32_t*)key_data[k])[b]) return false;
        } else if (t == TD_ENUM) {
            if (((uint32_t*)key_data[k])[a] != ((uint32_t*)key_data[k])[b]) return false;
        }
    }
    return true;
}

/* Extract salt from hash (upper 16 bits) for fast mismatch rejection */
#define HT_SALT(h) ((uint16_t)((h) >> 48))

typedef struct {
    int64_t  first_row;
    int64_t  count;
} group_entry_t;

/* Per-worker local hash table for parallel group-by */
typedef struct {
    int64_t*       ht_rows;    /* hash table: representative row indices */
    int32_t*       ht_gids;    /* hash table: group IDs */
    uint16_t*      ht_salts;   /* hash table: salt (upper 16 bits of hash) */
    uint32_t       ht_cap;
    group_entry_t* groups;
    uint32_t       grp_count;
    uint32_t       grp_cap;
    uint8_t        n_aggs;     /* cached for indexing flat arrays */
    /* Flat accumulator arrays indexed by [gid * n_aggs + agg_idx] */
    double*        all_agg_f64;
    int64_t*       all_agg_i64;
    double*        all_agg_min_f64;
    double*        all_agg_max_f64;
    int64_t*       all_agg_min_i64;
    int64_t*       all_agg_max_i64;
    /* Arena headers for scratch_free */
    td_t* _h_ht_rows;
    td_t* _h_ht_gids;
    td_t* _h_ht_salts;
    td_t* _h_groups;
    td_t* _h_agg_f64;
    td_t* _h_agg_i64;
    td_t* _h_min_f64;
    td_t* _h_max_f64;
    td_t* _h_min_i64;
    td_t* _h_max_i64;
} group_ht_t;

static void group_ht_init(group_ht_t* ht, uint32_t cap, uint8_t n_aggs) {
    ht->ht_cap = cap;
    ht->ht_rows  = (int64_t*)scratch_alloc(&ht->_h_ht_rows, cap * sizeof(int64_t));
    ht->ht_gids  = (int32_t*)scratch_alloc(&ht->_h_ht_gids, cap * sizeof(int32_t));
    ht->ht_salts = (uint16_t*)scratch_alloc(&ht->_h_ht_salts, cap * sizeof(uint16_t));
    memset(ht->ht_rows, -1, cap * sizeof(int64_t));
    ht->grp_cap = 256;
    ht->grp_count = 0;
    ht->n_aggs = n_aggs;
    ht->groups = (group_entry_t*)scratch_calloc(&ht->_h_groups, ht->grp_cap * sizeof(group_entry_t));

    size_t total = (size_t)ht->grp_cap * n_aggs;
    ht->all_agg_f64     = (double*)scratch_calloc(&ht->_h_agg_f64, total * sizeof(double));
    ht->all_agg_i64     = (int64_t*)scratch_calloc(&ht->_h_agg_i64, total * sizeof(int64_t));
    ht->all_agg_min_f64 = (double*)scratch_alloc(&ht->_h_min_f64, total * sizeof(double));
    ht->all_agg_max_f64 = (double*)scratch_alloc(&ht->_h_max_f64, total * sizeof(double));
    ht->all_agg_min_i64 = (int64_t*)scratch_alloc(&ht->_h_min_i64, total * sizeof(int64_t));
    ht->all_agg_max_i64 = (int64_t*)scratch_alloc(&ht->_h_max_i64, total * sizeof(int64_t));
    for (size_t i = 0; i < total; i++) {
        ht->all_agg_min_f64[i] = DBL_MAX;
        ht->all_agg_max_f64[i] = -DBL_MAX;
        ht->all_agg_min_i64[i] = INT64_MAX;
        ht->all_agg_max_i64[i] = INT64_MIN;
    }
}

static void group_ht_free(group_ht_t* ht) {
    scratch_free(ht->_h_groups);
    scratch_free(ht->_h_agg_f64);
    scratch_free(ht->_h_agg_i64);
    scratch_free(ht->_h_min_f64);
    scratch_free(ht->_h_max_f64);
    scratch_free(ht->_h_min_i64);
    scratch_free(ht->_h_max_i64);
    scratch_free(ht->_h_ht_rows);
    scratch_free(ht->_h_ht_gids);
    scratch_free(ht->_h_ht_salts);
}

static void group_ht_grow(group_ht_t* ht) {
    uint32_t old_cap = ht->grp_cap;
    uint32_t new_cap = old_cap * 2;
    uint8_t n_aggs = ht->n_aggs;
    size_t old_total = (size_t)old_cap * n_aggs;
    size_t new_total = (size_t)new_cap * n_aggs;

    group_entry_t* new_groups = (group_entry_t*)scratch_realloc(
        &ht->_h_groups, old_cap * sizeof(group_entry_t), new_cap * sizeof(group_entry_t));
    if (!new_groups) return;
    ht->groups = new_groups;
    memset(&ht->groups[old_cap], 0, (new_cap - old_cap) * sizeof(group_entry_t));

    ht->all_agg_f64     = (double*)scratch_realloc(&ht->_h_agg_f64, old_total * sizeof(double), new_total * sizeof(double));
    ht->all_agg_i64     = (int64_t*)scratch_realloc(&ht->_h_agg_i64, old_total * sizeof(int64_t), new_total * sizeof(int64_t));
    ht->all_agg_min_f64 = (double*)scratch_realloc(&ht->_h_min_f64, old_total * sizeof(double), new_total * sizeof(double));
    ht->all_agg_max_f64 = (double*)scratch_realloc(&ht->_h_max_f64, old_total * sizeof(double), new_total * sizeof(double));
    ht->all_agg_min_i64 = (int64_t*)scratch_realloc(&ht->_h_min_i64, old_total * sizeof(int64_t), new_total * sizeof(int64_t));
    ht->all_agg_max_i64 = (int64_t*)scratch_realloc(&ht->_h_max_i64, old_total * sizeof(int64_t), new_total * sizeof(int64_t));

    /* Zero-init sum/count arrays for new slots; set sentinel values for min/max */
    memset(ht->all_agg_f64 + old_total, 0, (new_total - old_total) * sizeof(double));
    memset(ht->all_agg_i64 + old_total, 0, (new_total - old_total) * sizeof(int64_t));
    for (size_t i = old_total; i < new_total; i++) {
        ht->all_agg_min_f64[i] = DBL_MAX;
        ht->all_agg_max_f64[i] = -DBL_MAX;
        ht->all_agg_min_i64[i] = INT64_MAX;
        ht->all_agg_max_i64[i] = INT64_MIN;
    }

    ht->grp_cap = new_cap;
}

/* Rehash: double the HT bucket capacity and re-insert all existing entries */
static void group_ht_rehash(group_ht_t* ht, void** key_data, int8_t* key_types,
                             uint8_t n_keys) {
    uint32_t new_cap = ht->ht_cap * 2;
    scratch_free(ht->_h_ht_rows);
    scratch_free(ht->_h_ht_gids);
    scratch_free(ht->_h_ht_salts);
    ht->ht_rows  = (int64_t*)scratch_alloc(&ht->_h_ht_rows, new_cap * sizeof(int64_t));
    ht->ht_gids  = (int32_t*)scratch_alloc(&ht->_h_ht_gids, new_cap * sizeof(int32_t));
    ht->ht_salts = (uint16_t*)scratch_alloc(&ht->_h_ht_salts, new_cap * sizeof(uint16_t));
    memset(ht->ht_rows, -1, new_cap * sizeof(int64_t));
    ht->ht_cap = new_cap;

    uint32_t mask = new_cap - 1;
    for (uint32_t gi = 0; gi < ht->grp_count; gi++) {
        int64_t rep_row = ht->groups[gi].first_row;
        uint64_t h = hash_row_fast(key_data, key_types, n_keys, rep_row);
        uint32_t slot = (uint32_t)(h & mask);
        while (ht->ht_rows[slot] != -1)
            slot = (slot + 1) & mask;
        ht->ht_rows[slot]  = rep_row;
        ht->ht_gids[slot]  = (int32_t)gi;
        ht->ht_salts[slot] = HT_SALT(h);
    }
}

/* Probe + accumulate a single row into the HT. Returns updated mask. */
static inline uint32_t group_probe_row(group_ht_t* ht, void** key_data,
                                        int8_t* key_types, uint8_t n_keys,
                                        td_t** agg_vecs, uint8_t n_aggs,
                                        int64_t row, uint64_t h, uint32_t slot,
                                        uint32_t mask) {
    uint16_t salt = HT_SALT(h);
    int32_t gid = -1;
    for (;;) {
        if (ht->ht_rows[slot] == -1) {
            if (ht->grp_count >= ht->grp_cap)
                group_ht_grow(ht);
            gid = (int32_t)ht->grp_count++;
            ht->groups[gid].first_row = row;
            ht->groups[gid].count = 0;
            ht->ht_rows[slot]  = row;
            ht->ht_gids[slot]  = gid;
            ht->ht_salts[slot] = salt;
            break;
        }
        if (ht->ht_salts[slot] == salt &&
            keys_equal_fast(key_data, key_types, n_keys, ht->ht_rows[slot], row)) {
            gid = ht->ht_gids[slot];
            break;
        }
        slot = (slot + 1) & mask;
    }

    if (gid < 0) return mask;
    ht->groups[gid].count++;

    size_t base = (size_t)gid * n_aggs;
    for (uint8_t a = 0; a < n_aggs; a++) {
        td_t* agg_col = agg_vecs[a];
        if (!agg_col) continue;
        size_t idx = base + a;

        if (agg_col->type == TD_F64) {
            double v = ((double*)td_data(agg_col))[row];
            ht->all_agg_f64[idx] += v;
            if (v < ht->all_agg_min_f64[idx]) ht->all_agg_min_f64[idx] = v;
            if (v > ht->all_agg_max_f64[idx]) ht->all_agg_max_f64[idx] = v;
        } else {
            int64_t v;
            if (agg_col->type == TD_I64 || agg_col->type == TD_SYM)
                v = ((int64_t*)td_data(agg_col))[row];
            else if (agg_col->type == TD_I32)
                v = ((int32_t*)td_data(agg_col))[row];
            else
                v = ((uint8_t*)td_data(agg_col))[row];
            ht->all_agg_i64[idx] += v;
            if (v < ht->all_agg_min_i64[idx]) ht->all_agg_min_i64[idx] = v;
            if (v > ht->all_agg_max_i64[idx]) ht->all_agg_max_i64[idx] = v;
        }
    }

    /* Rehash when load factor > 0.5 */
    if (ht->grp_count * 2 > ht->ht_cap) {
        group_ht_rehash(ht, key_data, key_types, n_keys);
        mask = ht->ht_cap - 1;
    }
    return mask;
}

/* Process rows [start, end) into a local hash table.
 * Uses software prefetching in batches to hide HT lookup latency. */
#define GROUP_PREFETCH_BATCH 16

static void group_rows_range(group_ht_t* ht, void** key_data, int8_t* key_types,
                              uint8_t n_keys,
                              td_t** agg_vecs, uint8_t n_aggs, uint16_t* agg_ops,
                              int64_t start, int64_t end) {
    (void)agg_ops;
    uint32_t mask = ht->ht_cap - 1;

    uint64_t batch_hashes[GROUP_PREFETCH_BATCH];
    uint32_t batch_slots[GROUP_PREFETCH_BATCH];

    int64_t row = start;
    for (; row + GROUP_PREFETCH_BATCH <= end; row += GROUP_PREFETCH_BATCH) {
        /* Phase 1: hash + prefetch HT slots */
        for (int i = 0; i < GROUP_PREFETCH_BATCH; i++) {
            uint64_t h = hash_row_fast(key_data, key_types, n_keys, row + i);
            uint32_t slot = (uint32_t)(h & mask);
            batch_hashes[i] = h;
            batch_slots[i] = slot;
            __builtin_prefetch(&ht->ht_rows[slot], 0, 1);
            __builtin_prefetch(&ht->ht_salts[slot], 0, 1);
        }
        /* Phase 2: probe + accumulate */
        for (int i = 0; i < GROUP_PREFETCH_BATCH; i++) {
            mask = group_probe_row(ht, key_data, key_types, n_keys,
                                   agg_vecs, n_aggs, row + i,
                                   batch_hashes[i], batch_slots[i], mask);
        }
    }
    /* Handle remaining rows without prefetching */
    for (; row < end; row++) {
        uint64_t h = hash_row_fast(key_data, key_types, n_keys, row);
        uint32_t slot = (uint32_t)(h & mask);
        mask = group_probe_row(ht, key_data, key_types, n_keys,
                               agg_vecs, n_aggs, row, h, slot, mask);
    }
}

/* ============================================================================
 * Radix-partitioned parallel group-by
 *
 * Phase 1 (parallel): Each worker hashes rows in its range and writes
 *         (row_idx, hash) into thread-local per-partition buffers.
 * Phase 2 (parallel): Each partition is aggregated independently by one
 *         worker — no merge needed since partitions are disjoint.
 * Phase 3: Concatenate partition HT results into the final DataFrame.
 * ============================================================================ */

/* Number of radix partitions — must be power of 2 */
#define RADIX_BITS  8
#define RADIX_P     (1u << RADIX_BITS)   /* 256 partitions */
#define RADIX_MASK  (RADIX_P - 1)

/* Partition ID from hash: use bits [16..23] (above HT probe bits, below salt) */
#define RADIX_PART(h) (((uint32_t)((h) >> 16)) & RADIX_MASK)

/* (row_idx, hash) pair written during partitioning phase */
typedef struct {
    int64_t  row;
    uint64_t hash;
} radix_entry_t;

/* Per-worker, per-partition buffer */
typedef struct {
    radix_entry_t* entries;
    uint32_t       count;
    uint32_t       cap;
    td_t*          _hdr;    /* arena header for scratch_free */
} radix_buf_t;

/* Phase 1 context: partition rows by hash into per-worker-per-partition buffers.
 * Each worker has a pre-allocated slab; per-partition buffers are slices of it. */
typedef struct {
    void**       key_data;
    int8_t*      key_types;
    uint8_t      n_keys;
    uint32_t     n_workers;
    radix_buf_t* bufs;    /* [n_workers * RADIX_P] */
} radix_phase1_ctx_t;

static inline void radix_buf_push(radix_buf_t* buf, int64_t row, uint64_t hash) {
    if (__builtin_expect(buf->count >= buf->cap, 0)) {
        uint32_t old_cap = buf->cap;
        buf->cap *= 2;
        buf->entries = (radix_entry_t*)scratch_realloc(
            &buf->_hdr, (size_t)old_cap * sizeof(radix_entry_t),
            (size_t)buf->cap * sizeof(radix_entry_t));
    }
    buf->entries[buf->count].row  = row;
    buf->entries[buf->count].hash = hash;
    buf->count++;
}

static void radix_phase1_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    radix_phase1_ctx_t* c = (radix_phase1_ctx_t*)ctx;
    radix_buf_t* my_bufs = &c->bufs[(size_t)worker_id * RADIX_P];

    for (int64_t row = start; row < end; row++) {
        uint64_t h = hash_row_fast(c->key_data, c->key_types, c->n_keys, row);
        uint32_t part = RADIX_PART(h);
        radix_buf_push(&my_bufs[part], row, h);
    }
}

/* Process rows from indirect index array with pre-computed hashes into a HT */
/* Process pre-partitioned rows (with pre-computed hashes) into a HT.
 * Uses the same prefetch batching as group_rows_range. */
static void group_rows_indirect(group_ht_t* ht, void** key_data, int8_t* key_types,
                                 uint8_t n_keys,
                                 td_t** agg_vecs, uint8_t n_aggs,
                                 radix_entry_t* entries, uint32_t n_entries) {
    uint32_t mask = ht->ht_cap - 1;

    uint32_t batch_slots[GROUP_PREFETCH_BATCH];
    uint32_t i = 0;
    for (; i + GROUP_PREFETCH_BATCH <= n_entries; i += GROUP_PREFETCH_BATCH) {
        /* Phase 1: compute slots + prefetch */
        for (int b = 0; b < GROUP_PREFETCH_BATCH; b++) {
            uint32_t slot = (uint32_t)(entries[i + b].hash & mask);
            batch_slots[b] = slot;
            __builtin_prefetch(&ht->ht_rows[slot], 0, 1);
            __builtin_prefetch(&ht->ht_salts[slot], 0, 1);
        }
        /* Phase 2: probe + accumulate */
        for (int b = 0; b < GROUP_PREFETCH_BATCH; b++) {
            mask = group_probe_row(ht, key_data, key_types, n_keys,
                                   agg_vecs, n_aggs, entries[i + b].row,
                                   entries[i + b].hash, batch_slots[b], mask);
        }
    }
    /* Remainder */
    for (; i < n_entries; i++) {
        uint32_t slot = (uint32_t)(entries[i].hash & mask);
        mask = group_probe_row(ht, key_data, key_types, n_keys,
                               agg_vecs, n_aggs, entries[i].row,
                               entries[i].hash, slot, mask);
    }
}

/* Phase 2 context: aggregate each partition independently */
typedef struct {
    void**       key_data;
    int8_t*      key_types;
    td_t**       agg_vecs;
    uint8_t      n_keys;
    uint8_t      n_aggs;
    uint32_t     n_workers;
    radix_buf_t* bufs;         /* [n_workers * RADIX_P] */
    group_ht_t*  part_hts;     /* [RADIX_P] — one HT per partition */
} radix_phase2_ctx_t;

static void radix_phase2_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    radix_phase2_ctx_t* c = (radix_phase2_ctx_t*)ctx;

    for (int64_t p = start; p < end; p++) {
        /* Count total entries in this partition across all workers */
        uint32_t total = 0;
        for (uint32_t w = 0; w < c->n_workers; w++) {
            total += c->bufs[(size_t)w * RADIX_P + p].count;
        }
        if (total == 0) continue;

        /* Size the per-partition HT */
        uint32_t part_ht_cap = 256;
        {
            uint64_t target = (uint64_t)total;
            if (target < 256) target = 256;
            while (part_ht_cap < target) part_ht_cap *= 2;
        }
        group_ht_init(&c->part_hts[p], part_ht_cap, c->n_aggs);

        /* Process all workers' entries for this partition */
        for (uint32_t w = 0; w < c->n_workers; w++) {
            radix_buf_t* buf = &c->bufs[(size_t)w * RADIX_P + p];
            if (buf->count == 0) continue;
            group_rows_indirect(&c->part_hts[p], c->key_data, c->key_types,
                                c->n_keys, c->agg_vecs, c->n_aggs,
                                buf->entries, buf->count);
        }
    }
}

/* ============================================================================
 * Parallel direct-array accumulation for low-cardinality single integer key
 * ============================================================================ */

/* Parallel min/max scan for direct-array key range detection */
typedef struct {
    const void* key_data;
    int8_t      key_type;
    int64_t*    per_worker_min;  /* [n_workers] */
    int64_t*    per_worker_max;  /* [n_workers] */
    uint32_t    n_workers;
} minmax_ctx_t;

static void minmax_scan_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    minmax_ctx_t* c = (minmax_ctx_t*)ctx;
    uint32_t wid = worker_id % c->n_workers;
    int64_t kmin = INT64_MAX, kmax = INT64_MIN;
    int8_t t = c->key_type;
    if (t == TD_I64 || t == TD_SYM) {
        const int64_t* kd = (const int64_t*)c->key_data;
        for (int64_t r = start; r < end; r++) {
            if (kd[r] < kmin) kmin = kd[r];
            if (kd[r] > kmax) kmax = kd[r];
        }
    } else if (t == TD_ENUM) {
        const uint32_t* kd = (const uint32_t*)c->key_data;
        for (int64_t r = start; r < end; r++) {
            int64_t v = (int64_t)kd[r];
            if (v < kmin) kmin = v;
            if (v > kmax) kmax = v;
        }
    } else { /* TD_I32 */
        const int32_t* kd = (const int32_t*)c->key_data;
        for (int64_t r = start; r < end; r++) {
            int64_t v = (int64_t)kd[r];
            if (v < kmin) kmin = v;
            if (v > kmax) kmax = v;
        }
    }
    /* Merge with existing per-worker values (a worker may process multiple morsels) */
    if (kmin < c->per_worker_min[wid]) c->per_worker_min[wid] = kmin;
    if (kmax > c->per_worker_max[wid]) c->per_worker_max[wid] = kmax;
}

typedef struct {
    double*  f64;
    int64_t* i64;
    double*  min_f64;
    double*  max_f64;
    int64_t* min_i64;
    int64_t* max_i64;
    int64_t* count;
    /* Arena headers */
    td_t* _h_f64;
    td_t* _h_i64;
    td_t* _h_min_f64;
    td_t* _h_max_f64;
    td_t* _h_min_i64;
    td_t* _h_max_i64;
    td_t* _h_count;
} da_accum_t;

static inline void da_accum_free(da_accum_t* a) {
    scratch_free(a->_h_f64);    scratch_free(a->_h_i64);
    scratch_free(a->_h_min_f64); scratch_free(a->_h_max_f64);
    scratch_free(a->_h_min_i64); scratch_free(a->_h_max_i64);
    scratch_free(a->_h_count);
}

/* Unified agg result emitter — used by both DA and HT paths.
 * Arrays indexed by [gi * n_aggs + a], counts by [gi]. */
static void emit_agg_columns(td_t** result, td_graph_t* g, td_op_ext_t* ext,
                              td_t** agg_vecs, uint32_t grp_count, uint8_t n_keys,
                              uint8_t n_aggs,
                              double*  sum_f64,  int64_t* sum_i64,
                              double*  min_f64,  double*  max_f64,
                              int64_t* min_i64,  int64_t* max_i64,
                              int64_t* counts) {
    for (uint8_t a = 0; a < n_aggs; a++) {
        uint16_t agg_op = ext->agg_ops[a];
        td_t* agg_col = agg_vecs[a];
        bool is_f64 = agg_col && agg_col->type == TD_F64;
        int8_t out_type;
        switch (agg_op) {
            case OP_AVG:   out_type = TD_F64; break;
            case OP_COUNT: out_type = TD_I64; break;
            case OP_SUM: case OP_PROD:
                out_type = is_f64 ? TD_F64 : TD_I64; break;
            default:
                out_type = agg_col ? agg_col->type : TD_I64; break;
        }
        td_t* new_col = td_vec_new(out_type, (int64_t)grp_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = (int64_t)grp_count;
        for (uint32_t gi = 0; gi < grp_count; gi++) {
            size_t idx = (size_t)gi * n_aggs + a;
            if (out_type == TD_F64) {
                double v;
                switch (agg_op) {
                    case OP_SUM: v = is_f64 ? sum_f64[idx] : (double)sum_i64[idx]; break;
                    case OP_AVG: v = is_f64 ? sum_f64[idx] / counts[gi] : (double)sum_i64[idx] / counts[gi]; break;
                    case OP_MIN: v = is_f64 ? min_f64[idx] : (double)min_i64[idx]; break;
                    case OP_MAX: v = is_f64 ? max_f64[idx] : (double)max_i64[idx]; break;
                    default:     v = 0.0; break;
                }
                ((double*)td_data(new_col))[gi] = v;
            } else {
                int64_t v;
                switch (agg_op) {
                    case OP_SUM:   v = sum_i64[idx]; break;
                    case OP_COUNT: v = counts[gi]; break;
                    case OP_MIN:   v = min_i64[idx]; break;
                    case OP_MAX:   v = max_i64[idx]; break;
                    case OP_FIRST: v = sum_i64[idx]; break;
                    default:       v = 0; break;
                }
                ((int64_t*)td_data(new_col))[gi] = v;
            }
        }
        td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
        int64_t name_id = agg_ext ? agg_ext->sym : (int64_t)(n_keys + a);
        *result = td_table_add_col(*result, name_id, new_col);
        td_release(new_col);
    }
}

/* Bitmask for which accumulator arrays are actually needed */
#define DA_NEED_SUM   0x01  /* f64 + i64 arrays */
#define DA_NEED_MIN   0x02  /* min_f64 + min_i64 */
#define DA_NEED_MAX   0x04  /* max_f64 + max_i64 */
#define DA_NEED_COUNT 0x08  /* count array */

typedef struct {
    da_accum_t*    accums;
    uint32_t       n_accums;     /* number of accumulator sets (may < pool workers) */
    void**         key_ptrs;     /* key data pointers [n_keys] */
    int8_t*        key_types;    /* key type codes [n_keys] */
    int64_t*       key_mins;     /* per-key minimum [n_keys] */
    int64_t*       key_strides;  /* per-key stride [n_keys] */
    uint8_t        n_keys;
    void**         agg_ptrs;
    int8_t*        agg_types;
    uint16_t*      agg_ops;      /* per-agg operation code */
    uint8_t        n_aggs;
    uint8_t        need_flags;   /* DA_NEED_* bitmask */
    uint32_t       n_slots;
} da_ctx_t;

static inline int32_t da_composite_gid(da_ctx_t* c, int64_t r) {
    int32_t gid = 0;
    for (uint8_t k = 0; k < c->n_keys; k++) {
        int64_t val;
        int8_t t = c->key_types[k];
        if (t == TD_I64 || t == TD_SYM)
            val = ((const int64_t*)c->key_ptrs[k])[r];
        else if (t == TD_ENUM)
            val = (int64_t)((const uint32_t*)c->key_ptrs[k])[r];
        else /* TD_I32 */
            val = (int64_t)((const int32_t*)c->key_ptrs[k])[r];
        gid += (int32_t)((val - c->key_mins[k]) * c->key_strides[k]);
    }
    return gid;
}

static inline void da_read_val(const void* ptr, int8_t type, int64_t r,
                               double* out_f64, int64_t* out_i64) {
    if (type == TD_F64) {
        *out_f64 = ((const double*)ptr)[r];
        *out_i64 = (int64_t)*out_f64;
    } else if (type == TD_I64 || type == TD_SYM) {
        *out_i64 = ((const int64_t*)ptr)[r];
        *out_f64 = (double)*out_i64;
    } else if (type == TD_ENUM) {
        *out_i64 = (int64_t)((const uint32_t*)ptr)[r];
        *out_f64 = (double)*out_i64;
    } else if (type == TD_I32) {
        *out_i64 = (int64_t)((const int32_t*)ptr)[r];
        *out_f64 = (double)*out_i64;
    } else {
        *out_i64 = ((const uint8_t*)ptr)[r];
        *out_f64 = (double)*out_i64;
    }
}

static void da_accum_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    da_ctx_t* c = (da_ctx_t*)ctx;
    da_accum_t* acc = &c->accums[worker_id % c->n_accums];
    uint8_t n_aggs = c->n_aggs;
    uint8_t n_keys = c->n_keys;

    /* Fast path: single key — avoid composite GID loop overhead */
    if (n_keys == 1) {
        const void* kptr = c->key_ptrs[0];
        int8_t kt = c->key_types[0];
        int64_t kmin = c->key_mins[0];
        for (int64_t r = start; r < end; r++) {
            int64_t kv;
            if (kt == TD_I64 || kt == TD_SYM)
                kv = ((const int64_t*)kptr)[r];
            else if (kt == TD_ENUM)
                kv = (int64_t)((const uint32_t*)kptr)[r];
            else
                kv = (int64_t)((const int32_t*)kptr)[r];
            int32_t gid = (int32_t)(kv - kmin);
            acc->count[gid]++;
            size_t base = (size_t)gid * n_aggs;
            for (uint8_t a = 0; a < n_aggs; a++) {
                if (!c->agg_ptrs[a]) continue;
                size_t idx = base + a;
                double fv; int64_t iv;
                da_read_val(c->agg_ptrs[a], c->agg_types[a], r, &fv, &iv);
                uint16_t op = c->agg_ops[a];
                if (op == OP_SUM || op == OP_AVG) {
                    acc->f64[idx] += fv;
                    acc->i64[idx] += iv;
                } else if (op == OP_MIN) {
                    if (fv < acc->min_f64[idx]) acc->min_f64[idx] = fv;
                    if (iv < acc->min_i64[idx]) acc->min_i64[idx] = iv;
                } else if (op == OP_MAX) {
                    if (fv > acc->max_f64[idx]) acc->max_f64[idx] = fv;
                    if (iv > acc->max_i64[idx]) acc->max_i64[idx] = iv;
                }
            }
        }
        return;
    }

    /* Multi-key composite GID path */
    for (int64_t r = start; r < end; r++) {
        int32_t gid = da_composite_gid(c, r);
        acc->count[gid]++;
        size_t base = (size_t)gid * n_aggs;
        for (uint8_t a = 0; a < n_aggs; a++) {
            if (!c->agg_ptrs[a]) continue;
            size_t idx = base + a;
            double fv; int64_t iv;
            da_read_val(c->agg_ptrs[a], c->agg_types[a], r, &fv, &iv);
            uint16_t op = c->agg_ops[a];
            if (op == OP_SUM || op == OP_AVG) {
                acc->f64[idx] += fv;
                acc->i64[idx] += iv;
            } else if (op == OP_MIN) {
                if (fv < acc->min_f64[idx]) acc->min_f64[idx] = fv;
                if (iv < acc->min_i64[idx]) acc->min_i64[idx] = iv;
            } else if (op == OP_MAX) {
                if (fv > acc->max_f64[idx]) acc->max_f64[idx] = fv;
                if (iv > acc->max_i64[idx]) acc->max_i64[idx] = iv;
            }
        }
    }
}

static td_t* exec_group(td_graph_t* g, td_op_t* op, td_t* df) {
    if (!df || TD_IS_ERR(df)) return df;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t nrows = td_table_nrows(df);
    uint8_t n_keys = ext->n_keys;
    uint8_t n_aggs = ext->n_aggs;

    if (n_keys > 8 || n_aggs > 8) return TD_ERR_PTR(TD_ERR_NYI);

    /* Resolve key columns (VLA — n_keys ≤ 8) */
    td_t* key_vecs[n_keys];
    memset(key_vecs, 0, n_keys * sizeof(td_t*));

    for (uint8_t k = 0; k < n_keys; k++) {
        td_op_t* key_op = ext->keys[k];
        td_op_ext_t* key_ext = find_ext(g, key_op->id);
        if (key_ext && key_ext->base.opcode == OP_SCAN) {
            key_vecs[k] = td_table_get_col(df, key_ext->sym);
        }
    }

    /* Resolve agg input columns (VLA — n_aggs ≤ 8) */
    td_t* agg_vecs[n_aggs];
    memset(agg_vecs, 0, n_aggs * sizeof(td_t*));

    for (uint8_t a = 0; a < n_aggs; a++) {
        td_op_t* agg_op = ext->agg_ins[a];
        td_op_ext_t* agg_ext = find_ext(g, agg_op->id);
        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            agg_vecs[a] = td_table_get_col(df, agg_ext->sym);
        }
    }

    /* Pre-compute key metadata (VLA — n_keys ≤ 8) */
    void* key_data[n_keys];
    int8_t key_types[n_keys];
    for (uint8_t k = 0; k < n_keys; k++) {
        if (key_vecs[k]) {
            key_data[k]  = td_data(key_vecs[k]);
            key_types[k] = key_vecs[k]->type;
        } else {
            key_data[k]  = NULL;
            key_types[k] = 0;
        }
    }

    /* ---- Direct-array fast path for low-cardinality integer keys ---- */
    /* Supports multi-key via composite index: product of ranges <= MAX */
    #define DA_MAX_COMPOSITE_SLOTS 262144  /* 256K slots max */
    #define DA_MEM_BUDGET      (256ULL << 20)  /* 256 MB total across all workers */
    #define DA_PER_WORKER_MAX  (6ULL << 20)    /* 6 MB per-worker max */
    {
        bool da_eligible = (nrows > 0 && n_keys > 0 && n_keys <= 8);
        for (uint8_t k = 0; k < n_keys && da_eligible; k++) {
            if (!key_data[k]) { da_eligible = false; break; }
            int8_t t = key_types[k];
            if (t != TD_I64 && t != TD_SYM && t != TD_I32 && t != TD_ENUM)
                da_eligible = false;
        }

        int64_t da_key_min[8], da_key_range[8], da_key_stride[8];
        uint64_t total_slots = 1;
        bool da_fits = false;

        if (da_eligible) {
            da_fits = true;
            td_pool_t* mm_pool = td_pool_get();
            uint32_t mm_n = (mm_pool && nrows >= TD_PARALLEL_THRESHOLD)
                            ? td_pool_total_workers(mm_pool) : 1;
            int64_t mm_mins[mm_n], mm_maxs[mm_n];
            for (uint8_t k = 0; k < n_keys && da_fits; k++) {
                int64_t kmin, kmax;
                for (uint32_t w = 0; w < mm_n; w++) {
                    mm_mins[w] = INT64_MAX;
                    mm_maxs[w] = INT64_MIN;
                }
                minmax_ctx_t mm_ctx = {
                    .key_data       = key_data[k],
                    .key_type       = key_types[k],
                    .per_worker_min = mm_mins,
                    .per_worker_max = mm_maxs,
                    .n_workers      = mm_n,
                };
                if (mm_n > 1) {
                    td_pool_dispatch(mm_pool, minmax_scan_fn, &mm_ctx, nrows);
                } else {
                    minmax_scan_fn(&mm_ctx, 0, 0, nrows);
                }
                kmin = INT64_MAX; kmax = INT64_MIN;
                for (uint32_t w = 0; w < mm_n; w++) {
                    if (mm_mins[w] < kmin) kmin = mm_mins[w];
                    if (mm_maxs[w] > kmax) kmax = mm_maxs[w];
                }
                da_key_min[k]   = kmin;
                da_key_range[k] = kmax - kmin + 1;
                if (da_key_range[k] <= 0) { da_fits = false; break; }
                total_slots *= (uint64_t)da_key_range[k];
                if (total_slots > DA_MAX_COMPOSITE_SLOTS) da_fits = false;
            }
        }

        if (da_fits) {
            /* Compute which accumulator arrays we actually need */
            uint8_t need_flags = DA_NEED_COUNT; /* always need count */
            for (uint8_t a = 0; a < n_aggs; a++) {
                uint16_t op = ext->agg_ops[a];
                if (op == OP_SUM || op == OP_AVG) need_flags |= DA_NEED_SUM;
                else if (op == OP_MIN) need_flags |= DA_NEED_MIN;
                else if (op == OP_MAX) need_flags |= DA_NEED_MAX;
            }

            /* Compute per-worker memory: only count arrays we'll allocate */
            uint32_t arrays_per_agg = 0;
            if (need_flags & DA_NEED_SUM) arrays_per_agg += 2; /* f64 + i64 */
            if (need_flags & DA_NEED_MIN) arrays_per_agg += 2; /* min_f64 + min_i64 */
            if (need_flags & DA_NEED_MAX) arrays_per_agg += 2; /* max_f64 + max_i64 */
            uint64_t per_worker = total_slots * (arrays_per_agg * n_aggs + 1u) * 8u;
            if (per_worker > DA_PER_WORKER_MAX)
                da_fits = false;
        }

        if (da_fits) {
            /* Recompute need_flags (da_fits may have changed scope) */
            uint8_t need_flags = DA_NEED_COUNT;
            for (uint8_t a = 0; a < n_aggs; a++) {
                uint16_t op = ext->agg_ops[a];
                if (op == OP_SUM || op == OP_AVG) need_flags |= DA_NEED_SUM;
                else if (op == OP_MIN) need_flags |= DA_NEED_MIN;
                else if (op == OP_MAX) need_flags |= DA_NEED_MAX;
            }

            /* Compute strides: stride[k] = product of ranges[k+1..n_keys-1] */
            for (uint8_t k = 0; k < n_keys; k++) {
                int64_t s = 1;
                for (uint8_t j = k + 1; j < n_keys; j++)
                    s *= da_key_range[j];
                da_key_stride[k] = s;
            }

            uint32_t n_slots = (uint32_t)total_slots;
            size_t total = (size_t)n_slots * n_aggs;

            void* agg_ptrs[n_aggs];
            int8_t agg_types[n_aggs];
            for (uint8_t a = 0; a < n_aggs; a++) {
                if (agg_vecs[a]) {
                    agg_ptrs[a]  = td_data(agg_vecs[a]);
                    agg_types[a] = agg_vecs[a]->type;
                } else {
                    agg_ptrs[a]  = NULL;
                    agg_types[a] = 0;
                }
            }

            td_pool_t* da_pool = td_pool_get();
            uint32_t da_n_workers = (da_pool && nrows >= TD_PARALLEL_THRESHOLD)
                                    ? td_pool_total_workers(da_pool) : 1;

            /* Limit workers so total memory stays within budget */
            uint32_t arrays_per_agg = 0;
            if (need_flags & DA_NEED_SUM) arrays_per_agg += 2;
            if (need_flags & DA_NEED_MIN) arrays_per_agg += 2;
            if (need_flags & DA_NEED_MAX) arrays_per_agg += 2;
            uint64_t per_worker_bytes = (uint64_t)n_slots * (arrays_per_agg * n_aggs + 1u) * 8u;
            while (da_n_workers > 1 && (uint64_t)da_n_workers * per_worker_bytes > DA_MEM_BUDGET)
                da_n_workers /= 2;

            td_t* accums_hdr;
            da_accum_t* accums = (da_accum_t*)scratch_calloc(&accums_hdr,
                da_n_workers * sizeof(da_accum_t));
            if (!accums) goto ht_path;

            bool alloc_ok = true;
            for (uint32_t w = 0; w < da_n_workers; w++) {
                if (need_flags & DA_NEED_SUM) {
                    accums[w].f64 = (double*)scratch_calloc(&accums[w]._h_f64, total * sizeof(double));
                    accums[w].i64 = (int64_t*)scratch_calloc(&accums[w]._h_i64, total * sizeof(int64_t));
                    if (!accums[w].f64 || !accums[w].i64) { alloc_ok = false; break; }
                }
                if (need_flags & DA_NEED_MIN) {
                    accums[w].min_f64 = (double*)scratch_alloc(&accums[w]._h_min_f64, total * sizeof(double));
                    accums[w].min_i64 = (int64_t*)scratch_alloc(&accums[w]._h_min_i64, total * sizeof(int64_t));
                    if (!accums[w].min_f64 || !accums[w].min_i64) { alloc_ok = false; break; }
                    for (size_t i = 0; i < total; i++) {
                        accums[w].min_f64[i] = DBL_MAX;
                        accums[w].min_i64[i] = INT64_MAX;
                    }
                }
                if (need_flags & DA_NEED_MAX) {
                    accums[w].max_f64 = (double*)scratch_alloc(&accums[w]._h_max_f64, total * sizeof(double));
                    accums[w].max_i64 = (int64_t*)scratch_alloc(&accums[w]._h_max_i64, total * sizeof(int64_t));
                    if (!accums[w].max_f64 || !accums[w].max_i64) { alloc_ok = false; break; }
                    for (size_t i = 0; i < total; i++) {
                        accums[w].max_f64[i] = -DBL_MAX;
                        accums[w].max_i64[i] = INT64_MIN;
                    }
                }
                accums[w].count = (int64_t*)scratch_calloc(&accums[w]._h_count, n_slots * sizeof(int64_t));
                if (!accums[w].count) { alloc_ok = false; break; }
            }
            if (!alloc_ok) {
                for (uint32_t w = 0; w < da_n_workers; w++)
                    da_accum_free(&accums[w]);
                scratch_free(accums_hdr);
                goto ht_path;
            }

            da_ctx_t da_ctx = {
                .accums      = accums,
                .n_accums    = da_n_workers,
                .key_ptrs    = key_data,
                .key_types   = key_types,
                .key_mins    = da_key_min,
                .key_strides = da_key_stride,
                .n_keys      = n_keys,
                .agg_ptrs    = agg_ptrs,
                .agg_types   = agg_types,
                .agg_ops     = ext->agg_ops,
                .n_aggs      = n_aggs,
                .need_flags  = need_flags,
                .n_slots     = n_slots,
            };

            if (da_n_workers > 1)
                td_pool_dispatch(da_pool, da_accum_fn, &da_ctx, nrows);
            else
                da_accum_fn(&da_ctx, 0, 0, nrows);

            /* Merge per-worker accumulators into accums[0] */
            da_accum_t* merged = &accums[0];
            for (uint32_t w = 1; w < da_n_workers; w++) {
                da_accum_t* wa = &accums[w];
                for (uint32_t s = 0; s < n_slots; s++) {
                    merged->count[s] += wa->count[s];
                }
                if (need_flags & DA_NEED_SUM) {
                    for (size_t i = 0; i < total; i++) {
                        merged->f64[i] += wa->f64[i];
                        merged->i64[i] += wa->i64[i];
                    }
                }
                if (need_flags & DA_NEED_MIN) {
                    for (size_t i = 0; i < total; i++) {
                        if (wa->min_f64[i] < merged->min_f64[i])
                            merged->min_f64[i] = wa->min_f64[i];
                        if (wa->min_i64[i] < merged->min_i64[i])
                            merged->min_i64[i] = wa->min_i64[i];
                    }
                }
                if (need_flags & DA_NEED_MAX) {
                    for (size_t i = 0; i < total; i++) {
                        if (wa->max_f64[i] > merged->max_f64[i])
                            merged->max_f64[i] = wa->max_f64[i];
                        if (wa->max_i64[i] > merged->max_i64[i])
                            merged->max_i64[i] = wa->max_i64[i];
                    }
                }
            }
            for (uint32_t w = 1; w < da_n_workers; w++)
                da_accum_free(&accums[w]);

            double*  da_f64     = merged->f64;     /* may be NULL if !DA_NEED_SUM */
            int64_t* da_i64     = merged->i64;
            double*  da_min_f64 = merged->min_f64; /* may be NULL if !DA_NEED_MIN */
            double*  da_max_f64 = merged->max_f64;
            int64_t* da_min_i64 = merged->min_i64;
            int64_t* da_max_i64 = merged->max_i64;
            int64_t* da_count   = merged->count;

            uint32_t grp_count = 0;
            for (uint32_t s = 0; s < n_slots; s++)
                if (da_count[s] > 0) grp_count++;

            int64_t total_cols = n_keys + n_aggs;
            td_t* result = td_table_new(total_cols);
            if (!result || TD_IS_ERR(result)) {
                da_accum_free(&accums[0]); scratch_free(accums_hdr);
                return result ? result : TD_ERR_PTR(TD_ERR_OOM);
            }

            /* Key columns — decompose composite slot back to per-key values */
            for (uint8_t k = 0; k < n_keys; k++) {
                td_t* src_col = key_vecs[k];
                if (!src_col) continue;
                td_t* key_col = td_vec_new(src_col->type, (int64_t)grp_count);
                if (!key_col || TD_IS_ERR(key_col)) continue;
                key_col->len = (int64_t)grp_count;
                uint32_t gi = 0;
                for (uint32_t s = 0; s < n_slots; s++) {
                    if (da_count[s] == 0) continue;
                    int64_t offset = ((int64_t)s / da_key_stride[k]) % da_key_range[k];
                    int64_t key_val = da_key_min[k] + offset;
                    if (src_col->type == TD_I32)
                        ((int32_t*)td_data(key_col))[gi] = (int32_t)key_val;
                    else if (src_col->type == TD_ENUM)
                        ((uint32_t*)td_data(key_col))[gi] = (uint32_t)key_val;
                    else
                        ((int64_t*)td_data(key_col))[gi] = key_val;
                    gi++;
                }
                td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
                int64_t name_id = key_ext ? key_ext->sym : (int64_t)k;
                result = td_table_add_col(result, name_id, key_col);
                td_release(key_col);
            }

            /* Agg columns — compact sparse DA arrays into dense, then emit */
            size_t dense_total = (size_t)grp_count * n_aggs;
            td_t *_h_df64 = NULL, *_h_di64 = NULL, *_h_dminf = NULL;
            td_t *_h_dmaxf = NULL, *_h_dmini = NULL, *_h_dmaxi = NULL, *_h_dcnt = NULL;
            double*  dense_f64     = da_f64     ? (double*)scratch_alloc(&_h_df64, dense_total * sizeof(double)) : NULL;
            int64_t* dense_i64     = da_i64     ? (int64_t*)scratch_alloc(&_h_di64, dense_total * sizeof(int64_t)) : NULL;
            double*  dense_min_f64 = da_min_f64 ? (double*)scratch_alloc(&_h_dminf, dense_total * sizeof(double)) : NULL;
            double*  dense_max_f64 = da_max_f64 ? (double*)scratch_alloc(&_h_dmaxf, dense_total * sizeof(double)) : NULL;
            int64_t* dense_min_i64 = da_min_i64 ? (int64_t*)scratch_alloc(&_h_dmini, dense_total * sizeof(int64_t)) : NULL;
            int64_t* dense_max_i64 = da_max_i64 ? (int64_t*)scratch_alloc(&_h_dmaxi, dense_total * sizeof(int64_t)) : NULL;
            int64_t* dense_counts  = (int64_t*)scratch_alloc(&_h_dcnt, grp_count * sizeof(int64_t));

            uint32_t gi = 0;
            for (uint32_t s = 0; s < n_slots; s++) {
                if (da_count[s] == 0) continue;
                dense_counts[gi] = da_count[s];
                for (uint8_t a = 0; a < n_aggs; a++) {
                    size_t si = (size_t)s * n_aggs + a;
                    size_t di = (size_t)gi * n_aggs + a;
                    if (dense_f64)     dense_f64[di]     = da_f64[si];
                    if (dense_i64)     dense_i64[di]     = da_i64[si];
                    if (dense_min_f64) dense_min_f64[di] = da_min_f64[si];
                    if (dense_max_f64) dense_max_f64[di] = da_max_f64[si];
                    if (dense_min_i64) dense_min_i64[di] = da_min_i64[si];
                    if (dense_max_i64) dense_max_i64[di] = da_max_i64[si];
                }
                gi++;
            }

            emit_agg_columns(&result, g, ext, agg_vecs, grp_count, n_keys, n_aggs,
                             dense_f64, dense_i64, dense_min_f64, dense_max_f64,
                             dense_min_i64, dense_max_i64, dense_counts);

            scratch_free(_h_df64); scratch_free(_h_di64);
            scratch_free(_h_dminf); scratch_free(_h_dmaxf);
            scratch_free(_h_dmini); scratch_free(_h_dmaxi);
            scratch_free(_h_dcnt);

            da_accum_free(&accums[0]); scratch_free(accums_hdr);
            return result;
        }
    }

ht_path:;
    /* Right-sized hash table: start small, rehash on load > 0.5 */
    uint32_t ht_cap = 256;
    {
        uint64_t target = (uint64_t)nrows < 65536 ? (uint64_t)nrows : 65536;
        if (target < 256) target = 256;
        while (ht_cap < target) ht_cap *= 2;
    }

    /* Parallel path: radix-partitioned group-by */
    td_pool_t* pool = td_pool_get();
    uint32_t n_total = pool ? td_pool_total_workers(pool) : 1;

    group_ht_t single_ht;
    group_ht_t* final_ht = NULL;

    /* Radix partition state — declared here for cleanup */
    td_t* radix_bufs_hdr = NULL;
    radix_buf_t* radix_bufs = NULL;
    td_t* part_hts_hdr = NULL;
    group_ht_t*  part_hts   = NULL;

    if (pool && nrows >= TD_PARALLEL_THRESHOLD && n_total > 1) {
        /* Allocate per-worker, per-partition buffers */
        size_t n_bufs = (size_t)n_total * RADIX_P;
        radix_bufs = (radix_buf_t*)scratch_calloc(&radix_bufs_hdr,
            n_bufs * sizeof(radix_buf_t));
        if (!radix_bufs) goto sequential_fallback;

        /* Pre-size each buffer with 2x expected to avoid realloc */
        uint32_t buf_init = (uint32_t)((uint64_t)nrows / (RADIX_P * n_total));
        if (buf_init < 64) buf_init = 64;
        buf_init *= 2;  /* 2x headroom */
        for (size_t i = 0; i < n_bufs; i++) {
            radix_bufs[i].entries = (radix_entry_t*)scratch_alloc(
                &radix_bufs[i]._hdr, buf_init * sizeof(radix_entry_t));
            radix_bufs[i].count = 0;
            radix_bufs[i].cap = buf_init;
        }

        /* Phase 1: Parallel partitioning */
        radix_phase1_ctx_t p1ctx = {
            .key_data  = key_data,
            .key_types = key_types,
            .n_keys    = n_keys,
            .n_workers = n_total,
            .bufs      = radix_bufs,
        };
        td_pool_dispatch(pool, radix_phase1_fn, &p1ctx, nrows);

        /* Phase 2: Parallel per-partition aggregation */
        part_hts = (group_ht_t*)scratch_calloc(&part_hts_hdr,
            RADIX_P * sizeof(group_ht_t));
        if (!part_hts) {
            for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
            scratch_free(radix_bufs_hdr);
            radix_bufs = NULL;
            goto sequential_fallback;
        }

        radix_phase2_ctx_t p2ctx = {
            .key_data  = key_data,
            .key_types = key_types,
            .agg_vecs  = agg_vecs,
            .n_keys    = n_keys,
            .n_aggs    = n_aggs,
            .n_workers = n_total,
            .bufs      = radix_bufs,
            .part_hts  = part_hts,
        };
        td_pool_dispatch_n(pool, radix_phase2_fn, &p2ctx, RADIX_P);

        /* Merge partition HTs into a single final HT for result building.
         * Each partition has its own groups — just concatenate them into one. */
        uint32_t total_grps = 0;
        for (uint32_t p = 0; p < RADIX_P; p++)
            total_grps += part_hts[p].grp_count;

        if (total_grps > 0) {
            uint32_t merged_cap = 256;
            while (merged_cap < total_grps) merged_cap *= 2;
            /* We don't need a HT (no lookups), just the flat group/agg arrays */
            group_ht_init(&single_ht, merged_cap, n_aggs);
            uint32_t dest = 0;
            for (uint32_t p = 0; p < RADIX_P; p++) {
                group_ht_t* ph = &part_hts[p];
                for (uint32_t gi = 0; gi < ph->grp_count; gi++) {
                    if (dest >= single_ht.grp_cap)
                        group_ht_grow(&single_ht);
                    single_ht.groups[dest] = ph->groups[gi];
                    size_t src_base = (size_t)gi * n_aggs;
                    size_t dst_base = (size_t)dest * n_aggs;
                    for (uint8_t a = 0; a < n_aggs; a++) {
                        single_ht.all_agg_f64[dst_base + a]     = ph->all_agg_f64[src_base + a];
                        single_ht.all_agg_i64[dst_base + a]     = ph->all_agg_i64[src_base + a];
                        single_ht.all_agg_min_f64[dst_base + a] = ph->all_agg_min_f64[src_base + a];
                        single_ht.all_agg_max_f64[dst_base + a] = ph->all_agg_max_f64[src_base + a];
                        single_ht.all_agg_min_i64[dst_base + a] = ph->all_agg_min_i64[src_base + a];
                        single_ht.all_agg_max_i64[dst_base + a] = ph->all_agg_max_i64[src_base + a];
                    }
                    dest++;
                }
            }
            single_ht.grp_count = dest;
            final_ht = &single_ht;
        } else {
            /* No groups at all — init an empty HT for result building */
            group_ht_init(&single_ht, 256, n_aggs);
            final_ht = &single_ht;
        }
        goto build_result;
    }

sequential_fallback:;
    /* Sequential path */
    group_ht_init(&single_ht, ht_cap, n_aggs);
    group_rows_range(&single_ht, key_data, key_types, n_keys, agg_vecs, n_aggs,
                     ext->agg_ops, 0, nrows);

    final_ht = &single_ht;

build_result:;
    /* Build result DataFrame */
    uint32_t grp_count = final_ht->grp_count;
    int64_t total_cols = n_keys + n_aggs;
    td_t* result = td_table_new(total_cols);
    if (!result || TD_IS_ERR(result)) goto cleanup;

    /* Key columns */
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* src_col = key_vecs[k];
        if (!src_col) continue;
        uint8_t esz = td_elem_size(src_col->type);

        td_t* new_col = td_vec_new(src_col->type, (int64_t)grp_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = (int64_t)grp_count;

        for (uint32_t gi = 0; gi < grp_count; gi++) {
            memcpy((char*)td_data(new_col) + gi * esz,
                   (char*)td_data(src_col) + final_ht->groups[gi].first_row * esz,
                   esz);
        }

        td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
        int64_t name_id = key_ext ? key_ext->sym : k;
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }

    /* Agg columns — extract dense counts then use shared emitter */
    td_t* _h_htcnt = NULL;
    int64_t* ht_counts = (int64_t*)scratch_alloc(&_h_htcnt, grp_count * sizeof(int64_t));
    for (uint32_t gi = 0; gi < grp_count; gi++)
        ht_counts[gi] = final_ht->groups[gi].count;

    emit_agg_columns(&result, g, ext, agg_vecs, grp_count, n_keys, n_aggs,
                     final_ht->all_agg_f64, final_ht->all_agg_i64,
                     final_ht->all_agg_min_f64, final_ht->all_agg_max_f64,
                     final_ht->all_agg_min_i64, final_ht->all_agg_max_i64,
                     ht_counts);
    scratch_free(_h_htcnt);

cleanup:
    /* Free the merged/sequential single_ht (always used as final_ht) */
    if (final_ht == &single_ht) {
        group_ht_free(&single_ht);
    }
    /* Free radix partition state */
    if (radix_bufs) {
        size_t n_bufs = (size_t)n_total * RADIX_P;
        for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
        scratch_free(radix_bufs_hdr);
    }
    if (part_hts) {
        for (uint32_t p = 0; p < RADIX_P; p++) {
            if (part_hts[p].groups) group_ht_free(&part_hts[p]);
        }
        scratch_free(part_hts_hdr);
    }

    return result;
}

/* ============================================================================
 * Join execution (hash join)
 * ============================================================================ */

static td_t* exec_join(td_graph_t* g, td_op_t* op, td_t* left_df, td_t* right_df) {
    if (!left_df || TD_IS_ERR(left_df)) return left_df;
    if (!right_df || TD_IS_ERR(right_df)) return right_df;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t left_rows = td_table_nrows(left_df);
    int64_t right_rows = td_table_nrows(right_df);
    uint8_t n_keys = ext->join.n_join_keys;
    uint8_t join_type = ext->join.join_type;

    td_t* l_key_vecs[n_keys];
    td_t* r_key_vecs[n_keys];
    memset(l_key_vecs, 0, n_keys * sizeof(td_t*));
    memset(r_key_vecs, 0, n_keys * sizeof(td_t*));

    for (uint8_t k = 0; k < n_keys; k++) {
        td_op_ext_t* lk = find_ext(g, ext->join.left_keys[k]->id);
        td_op_ext_t* rk = find_ext(g, ext->join.right_keys[k]->id);
        if (lk && lk->base.opcode == OP_SCAN)
            l_key_vecs[k] = td_table_get_col(left_df, lk->sym);
        if (rk && rk->base.opcode == OP_SCAN)
            r_key_vecs[k] = td_table_get_col(right_df, rk->sym);
        if (rk && rk->base.opcode == OP_CONST && rk->literal)
            r_key_vecs[k] = rk->literal;
    }

    /* Build hash table on right side (sequential) */
    uint32_t ht_cap = 256;
    while (ht_cap < (uint32_t)right_rows * 2) ht_cap *= 2;

    td_t* ht_next_hdr;
    td_t* ht_heads_hdr;
    int64_t* ht_next = (int64_t*)scratch_alloc(&ht_next_hdr, (size_t)right_rows * sizeof(int64_t));
    int64_t* ht_heads = (int64_t*)scratch_alloc(&ht_heads_hdr, ht_cap * sizeof(int64_t));
    if (!ht_next || !ht_heads) {
        scratch_free(ht_next_hdr); scratch_free(ht_heads_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }
    memset(ht_heads, -1, ht_cap * sizeof(int64_t));

    for (int64_t r = 0; r < right_rows; r++) {
        uint64_t h = hash_row_keys(r_key_vecs, n_keys, r);
        uint32_t slot = (uint32_t)(h & (ht_cap - 1));
        ht_next[r] = ht_heads[slot];
        ht_heads[slot] = r;
    }

    /* Probe: collect (left_idx, right_idx) pairs */
    int64_t pair_cap = left_rows;
    int64_t pair_count = 0;
    td_t* l_idx_hdr;
    td_t* r_idx_hdr;
    int64_t* l_idx = (int64_t*)scratch_alloc(&l_idx_hdr, (size_t)pair_cap * sizeof(int64_t));
    int64_t* r_idx = (int64_t*)scratch_alloc(&r_idx_hdr, (size_t)pair_cap * sizeof(int64_t));
    if (!l_idx || !r_idx) {
        scratch_free(ht_next_hdr); scratch_free(ht_heads_hdr);
        scratch_free(l_idx_hdr); scratch_free(r_idx_hdr);
        return TD_ERR_PTR(TD_ERR_OOM);
    }

    for (int64_t l = 0; l < left_rows; l++) {
        uint64_t h = hash_row_keys(l_key_vecs, n_keys, l);
        uint32_t slot = (uint32_t)(h & (ht_cap - 1));
        bool matched = false;

        for (int64_t r = ht_heads[slot]; r >= 0; r = ht_next[r]) {
            bool eq = true;
            for (uint8_t k = 0; k < n_keys && eq; k++) {
                td_t* lc = l_key_vecs[k];
                td_t* rc = r_key_vecs[k];
                if (!lc || !rc) { eq = false; continue; }

                if (lc->type == TD_I64 || lc->type == TD_SYM) {
                    eq = ((int64_t*)td_data(lc))[l] == ((int64_t*)td_data(rc))[r];
                } else if (lc->type == TD_F64) {
                    eq = ((double*)td_data(lc))[l] == ((double*)td_data(rc))[r];
                } else if (lc->type == TD_I32) {
                    eq = ((int32_t*)td_data(lc))[l] == ((int32_t*)td_data(rc))[r];
                } else if (lc->type == TD_ENUM) {
                    eq = ((uint32_t*)td_data(lc))[l] == ((uint32_t*)td_data(rc))[r];
                }
            }

            if (eq) {
                if (pair_count >= pair_cap) {
                    int64_t old_cap = pair_cap;
                    pair_cap *= 2;
                    l_idx = (int64_t*)scratch_realloc(&l_idx_hdr,
                        (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                    r_idx = (int64_t*)scratch_realloc(&r_idx_hdr,
                        (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                }
                l_idx[pair_count] = l;
                r_idx[pair_count] = r;
                pair_count++;
                matched = true;
            }
        }

        if (!matched && join_type == 1) {
            if (pair_count >= pair_cap) {
                int64_t old_cap = pair_cap;
                pair_cap *= 2;
                l_idx = (int64_t*)scratch_realloc(&l_idx_hdr,
                    (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                r_idx = (int64_t*)scratch_realloc(&r_idx_hdr,
                    (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
            }
            l_idx[pair_count] = l;
            r_idx[pair_count] = -1;
            pair_count++;
        }
    }

    /* Build result DataFrame */
    int64_t left_ncols = td_table_ncols(left_df);
    int64_t right_ncols = td_table_ncols(right_df);
    td_t* result = td_table_new(left_ncols + right_ncols);
    if (!result || TD_IS_ERR(result)) goto join_cleanup;

    for (int64_t c = 0; c < left_ncols; c++) {
        td_t* col = td_table_get_col_idx(left_df, c);
        int64_t name_id = td_table_col_name(left_df, c);
        if (!col) continue;

        uint8_t esz = td_elem_size(col->type);
        td_t* new_col = td_vec_new(col->type, pair_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = pair_count;

        char* src = (char*)td_data(col);
        char* dst = (char*)td_data(new_col);
        for (int64_t i = 0; i < pair_count; i++) {
            memcpy(dst + i * esz, src + l_idx[i] * esz, esz);
        }
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }

    for (int64_t c = 0; c < right_ncols; c++) {
        td_t* col = td_table_get_col_idx(right_df, c);
        int64_t name_id = td_table_col_name(right_df, c);
        if (!col) continue;

        bool is_key = false;
        for (uint8_t k = 0; k < n_keys; k++) {
            td_op_ext_t* rk = find_ext(g, ext->join.right_keys[k]->id);
            if (rk && rk->base.opcode == OP_SCAN && rk->sym == name_id) {
                is_key = true;
                break;
            }
        }
        if (is_key) continue;

        uint8_t esz = td_elem_size(col->type);
        td_t* new_col = td_vec_new(col->type, pair_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = pair_count;

        char* src = (char*)td_data(col);
        char* dst = (char*)td_data(new_col);
        for (int64_t i = 0; i < pair_count; i++) {
            if (r_idx[i] >= 0) {
                memcpy(dst + i * esz, src + r_idx[i] * esz, esz);
            } else {
                memset(dst + i * esz, 0, esz);
            }
        }
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }

join_cleanup:
    scratch_free(ht_next_hdr);
    scratch_free(ht_heads_hdr);
    scratch_free(l_idx_hdr);
    scratch_free(r_idx_hdr);

    return result;
}

/* ============================================================================
 * Recursive executor
 * ============================================================================ */

static td_t* exec_node(td_graph_t* g, td_op_t* op);

static td_t* exec_node(td_graph_t* g, td_op_t* op) {
    if (!op) return TD_ERR_PTR(TD_ERR_NYI);

    switch (op->opcode) {
        case OP_SCAN: {
            td_op_ext_t* ext = find_ext(g, op->id);
            if (!ext) return TD_ERR_PTR(TD_ERR_NYI);
            if (!g->df) return TD_ERR_PTR(TD_ERR_SCHEMA);
            td_t* col = td_table_get_col(g->df, ext->sym);
            if (!col) return TD_ERR_PTR(TD_ERR_SCHEMA);
            td_retain(col);
            return col;
        }

        case OP_CONST: {
            td_op_ext_t* ext = find_ext(g, op->id);
            if (!ext || !ext->literal) return TD_ERR_PTR(TD_ERR_NYI);
            td_retain(ext->literal);
            return ext->literal;
        }

        /* Unary element-wise */
        case OP_NEG: case OP_ABS: case OP_NOT: case OP_SQRT:
        case OP_LOG: case OP_EXP: case OP_CEIL: case OP_FLOOR:
        case OP_ISNULL: case OP_CAST: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* result = exec_elementwise_unary(g, op, input);
            td_release(input);
            return result;
        }

        /* Binary element-wise */
        case OP_ADD: case OP_SUB: case OP_MUL: case OP_DIV: case OP_MOD:
        case OP_EQ: case OP_NE: case OP_LT: case OP_LE:
        case OP_GT: case OP_GE: case OP_AND: case OP_OR:
        case OP_MIN2: case OP_MAX2: {
            td_t* lhs = exec_node(g, op->inputs[0]);
            td_t* rhs = exec_node(g, op->inputs[1]);
            if (!lhs || TD_IS_ERR(lhs)) { if (rhs && !TD_IS_ERR(rhs)) td_release(rhs); return lhs; }
            if (!rhs || TD_IS_ERR(rhs)) { td_release(lhs); return rhs; }
            td_t* result = exec_elementwise_binary(g, op, lhs, rhs);
            td_release(lhs);
            td_release(rhs);
            return result;
        }

        /* Reductions */
        case OP_SUM: case OP_PROD: case OP_MIN: case OP_MAX:
        case OP_COUNT: case OP_AVG: case OP_FIRST: case OP_LAST: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* result = exec_reduction(g, op, input);
            td_release(input);
            return result;
        }

        case OP_FILTER: {
            td_t* input = exec_node(g, op->inputs[0]);
            td_t* pred  = exec_node(g, op->inputs[1]);
            if (!input || TD_IS_ERR(input)) { if (pred && !TD_IS_ERR(pred)) td_release(pred); return input; }
            if (!pred || TD_IS_ERR(pred)) { td_release(input); return pred; }
            td_t* result = exec_filter(g, op, input, pred);
            td_release(input);
            td_release(pred);
            return result;
        }

        case OP_SORT: {
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            td_t* df = (input->type == TD_TABLE) ? input : g->df;
            td_t* result = exec_sort(g, op, df);
            if (input != g->df) td_release(input);
            return result;
        }

        case OP_GROUP: {
            td_t* result = exec_group(g, op, g->df);
            return result;
        }

        case OP_JOIN: {
            td_t* left = exec_node(g, op->inputs[0]);
            td_t* right = exec_node(g, op->inputs[1]);
            if (!left || TD_IS_ERR(left)) { if (right && !TD_IS_ERR(right)) td_release(right); return left; }
            if (!right || TD_IS_ERR(right)) { td_release(left); return right; }
            td_t* result = exec_join(g, op, left, right);
            td_release(left);
            td_release(right);
            return result;
        }

        case OP_HEAD: {
            td_op_ext_t* ext = find_ext(g, op->id);
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            int64_t n = ext ? ext->sym : 10;
            if (input->type == TD_TABLE) {
                int64_t ncols = td_table_ncols(input);
                int64_t nrows = td_table_nrows(input);
                if (n > nrows) n = nrows;
                td_t* result = td_table_new(ncols);
                for (int64_t c = 0; c < ncols; c++) {
                    td_t* col = td_table_get_col_idx(input, c);
                    int64_t name_id = td_table_col_name(input, c);
                    td_t* sliced = td_vec_slice(col, 0, n);
                    result = td_table_add_col(result, name_id, sliced);
                    td_release(sliced);
                }
                td_release(input);
                return result;
            }
            if (n > input->len) n = input->len;
            td_t* result = td_vec_slice(input, 0, n);
            td_release(input);
            return result;
        }

        case OP_ALIAS: {
            return exec_node(g, op->inputs[0]);
        }

        case OP_MATERIALIZE: {
            return exec_node(g, op->inputs[0]);
        }

        default:
            return TD_ERR_PTR(TD_ERR_NYI);
    }
}

/* ============================================================================
 * td_execute -- top-level entry point (lazy pool init)
 * ============================================================================ */

td_t* td_execute(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return TD_ERR_PTR(TD_ERR_NYI);

    /* Lazy-init the global thread pool on first call */
    td_pool_get();

    return exec_node(g, root);
}
