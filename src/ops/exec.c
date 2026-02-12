#include "exec.h"
#include "hash.h"
#include "pool.h"
#include <string.h>
#include <math.h>
#include <float.h>
#include <ctype.h>

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

    int32_t* lp_i32 = NULL; uint32_t* lp_u32 = NULL;
    int32_t* rp_i32 = NULL; uint32_t* rp_u32 = NULL;

    if (!l_scalar) {
        char* lbase = (char*)td_data(lhs) + start * td_elem_size(lhs->type);
        if (lhs->type == TD_F64) lp_f64 = (double*)lbase;
        else if (lhs->type == TD_I64 || lhs->type == TD_SYM) lp_i64 = (int64_t*)lbase;
        else if (lhs->type == TD_I32) lp_i32 = (int32_t*)lbase;
        else if (lhs->type == TD_ENUM) lp_u32 = (uint32_t*)lbase;
        else if (lhs->type == TD_BOOL) lp_bool = (uint8_t*)lbase;
    }
    if (!r_scalar) {
        char* rbase = (char*)td_data(rhs) + start * td_elem_size(rhs->type);
        if (rhs->type == TD_F64) rp_f64 = (double*)rbase;
        else if (rhs->type == TD_I64 || rhs->type == TD_SYM) rp_i64 = (int64_t*)rbase;
        else if (rhs->type == TD_I32) rp_i32 = (int32_t*)rbase;
        else if (rhs->type == TD_ENUM) rp_u32 = (uint32_t*)rbase;
        else if (rhs->type == TD_BOOL) rp_bool = (uint8_t*)rbase;
    }

    for (int64_t i = 0; i < n; i++) {
        double lv, rv;
        if (lp_f64)       lv = lp_f64[i];
        else if (lp_i64)  lv = (double)lp_i64[i];
        else if (lp_i32)  lv = (double)lp_i32[i];
        else if (lp_u32)  lv = (double)lp_u32[i];
        else if (lp_bool) lv = (double)lp_bool[i];
        else if (l_scalar && (lhs->type == TD_ATOM_F64 || lhs->type == -TD_F64)) lv = l_f64;
        else              lv = (double)l_i64;

        if (rp_f64)       rv = rp_f64[i];
        else if (rp_i64)  rv = (double)rp_i64[i];
        else if (rp_i32)  rv = (double)rp_i32[i];
        else if (rp_u32)  rv = (double)rp_u32[i];
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

    /* ENUM/SYM vs STR comparison: resolve string constant to intern ID so we
       can compare numerically against ENUM uint32_t intern indices.
       td_sym_find returns -1 if string not in table → no match. */
    bool str_resolved = false;
    int64_t resolved_sym_id = 0;
    if (r_scalar && (rhs->type == TD_ATOM_STR || rhs->type == TD_STR) &&
        (lhs->type == TD_ENUM || lhs->type == TD_SYM)) {
        const char* s = td_str_ptr(rhs);
        size_t slen = td_str_len(rhs);
        resolved_sym_id = td_sym_find(s, slen);
        str_resolved = true;
    } else if (l_scalar && (lhs->type == TD_ATOM_STR || lhs->type == TD_STR) &&
               (rhs->type == TD_ENUM || rhs->type == TD_SYM)) {
        const char* s = td_str_ptr(lhs);
        size_t slen = td_str_len(lhs);
        resolved_sym_id = td_sym_find(s, slen);
        str_resolved = true;
    }

    double l_f64_val = 0, r_f64_val = 0;
    int64_t l_i64_val = 0, r_i64_val = 0;
    if (l_scalar) {
        if (str_resolved && (lhs->type == TD_ATOM_STR || lhs->type == TD_STR))
            l_i64_val = resolved_sym_id;
        else if (lhs->type == TD_ATOM_F64 || lhs->type == -TD_F64) l_f64_val = lhs->f64;
        else l_i64_val = lhs->i64;
    }
    if (r_scalar) {
        if (str_resolved && (rhs->type == TD_ATOM_STR || rhs->type == TD_STR))
            r_i64_val = resolved_sym_id;
        else if (rhs->type == TD_ATOM_F64 || rhs->type == -TD_F64) r_f64_val = rhs->f64;
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
            else if (in_type == TD_ENUM)
                v = (int64_t)((uint32_t*)base)[row];
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
        td_t* accs_hdr;
        reduce_acc_t* accs = (reduce_acc_t*)scratch_calloc(&accs_hdr, nw * sizeof(reduce_acc_t));
        if (!accs) return TD_ERR_PTR(TD_ERR_OOM);
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

        td_t* result;
        switch (op->opcode) {
            case OP_SUM:   result = in_type == TD_F64 ? td_f64(merged.sum_f) : td_i64(merged.sum_i); break;
            case OP_PROD:  result = in_type == TD_F64 ? td_f64(merged.prod_f) : td_i64(merged.prod_i); break;
            case OP_MIN:   result = in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.min_f : 0.0) : td_i64(merged.cnt > 0 ? merged.min_i : 0); break;
            case OP_MAX:   result = in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.max_f : 0.0) : td_i64(merged.cnt > 0 ? merged.max_i : 0); break;
            case OP_COUNT: result = td_i64(merged.cnt); break;
            case OP_AVG:   result = in_type == TD_F64 ? td_f64(merged.cnt > 0 ? merged.sum_f / merged.cnt : 0.0) : td_f64(merged.cnt > 0 ? (double)merged.sum_i / merged.cnt : 0.0); break;
            case OP_FIRST: result = in_type == TD_F64 ? td_f64(merged.first_f) : td_i64(merged.first_i); break;
            case OP_LAST:  result = in_type == TD_F64 ? td_f64(merged.last_f) : td_i64(merged.last_i); break;
            default:       result = TD_ERR_PTR(TD_ERR_NYI); break;
        }
        scratch_free(accs_hdr);
        return result;
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

/* Filter a single vector by boolean predicate. */
static td_t* exec_filter_vec(td_t* input, td_t* pred, int64_t pass_count) {
    uint8_t esz = td_elem_size(input->type);
    td_t* result = td_vec_new(input->type, pass_count);
    if (!result || TD_IS_ERR(result)) return result;
    result->len = pass_count;

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

static td_t* exec_filter(td_graph_t* g, td_op_t* op, td_t* input, td_t* pred) {
    (void)g;
    (void)op;
    if (!input || TD_IS_ERR(input)) return input;
    if (!pred || TD_IS_ERR(pred)) return pred;

    /* Count passing elements */
    int64_t pass_count = 0;
    td_morsel_t mp;
    td_morsel_init(&mp, pred);
    while (td_morsel_next(&mp)) {
        uint8_t* bits = (uint8_t*)mp.morsel_ptr;
        for (int64_t i = 0; i < mp.morsel_len; i++)
            if (bits[i]) pass_count++;
    }

    /* DataFrame filter: filter each column, build new table */
    if (input->type == TD_TABLE) {
        int64_t ncols = td_table_ncols(input);
        td_t* tbl = td_table_new(ncols);
        if (!tbl || TD_IS_ERR(tbl)) return tbl;

        for (int64_t c = 0; c < ncols; c++) {
            td_t* col = td_table_get_col_idx(input, c);
            if (!col || TD_IS_ERR(col)) continue;
            int64_t name_id = td_table_col_name(input, c);
            td_t* filtered = exec_filter_vec(col, pred, pass_count);
            if (!filtered || TD_IS_ERR(filtered)) { td_release(tbl); return filtered; }
            td_table_add_col(tbl, name_id, filtered);
            td_release(filtered);
        }
        return tbl;
    }

    /* Vector filter */
    return exec_filter_vec(input, pred, pass_count);
}

/* ============================================================================
 * Sort execution (simple insertion sort)
 * ============================================================================ */

/* Forward declaration — exec_node is defined later */
static td_t* exec_node(td_graph_t* g, td_op_t* op);

/* --------------------------------------------------------------------------
 * Sort comparator: compare two row indices across all sort keys.
 * Returns negative if a < b, positive if a > b, 0 if equal.
 * -------------------------------------------------------------------------- */
typedef struct {
    td_t**       vecs;
    uint8_t*     desc;
    uint8_t*     nulls_first;
    uint8_t      n_sort;
} sort_cmp_ctx_t;

static int sort_cmp(const sort_cmp_ctx_t* ctx, int64_t a, int64_t b) {
    for (uint8_t k = 0; k < ctx->n_sort; k++) {
        td_t* col = ctx->vecs[k];
        if (!col) continue;
        int cmp = 0;
        int null_cmp = 0;
        int desc = ctx->desc ? ctx->desc[k] : 0;
        int nf = ctx->nulls_first ? ctx->nulls_first[k] : desc;

        if (col->type == TD_F64) {
            double va = ((double*)td_data(col))[a];
            double vb = ((double*)td_data(col))[b];
            int a_null = isnan(va);
            int b_null = isnan(vb);
            if (a_null && b_null) { cmp = 0; null_cmp = 1; }
            else if (a_null) { cmp = nf ? -1 : 1; null_cmp = 1; }
            else if (b_null) { cmp = nf ? 1 : -1; null_cmp = 1; }
            else if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_I64 || col->type == TD_SYM || col->type == TD_TIMESTAMP) {
            int64_t va = ((int64_t*)td_data(col))[a];
            int64_t vb = ((int64_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_I32) {
            int32_t va = ((int32_t*)td_data(col))[a];
            int32_t vb = ((int32_t*)td_data(col))[b];
            if (va < vb) cmp = -1;
            else if (va > vb) cmp = 1;
        } else if (col->type == TD_ENUM) {
            uint32_t va = ((uint32_t*)td_data(col))[a];
            uint32_t vb = ((uint32_t*)td_data(col))[b];
            td_t* sa = td_sym_str((int64_t)va);
            td_t* sb = td_sym_str((int64_t)vb);
            if (sa && sb) cmp = td_str_cmp(sa, sb);
        }

        if (desc && !null_cmp) cmp = -cmp;
        if (cmp != 0) return cmp;
    }
    return 0;
}

/* Insertion sort for small arrays — used as base case for merge sort */
static void sort_insertion(const sort_cmp_ctx_t* ctx, int64_t* arr, int64_t n) {
    for (int64_t i = 1; i < n; i++) {
        int64_t key = arr[i];
        int64_t j = i - 1;
        while (j >= 0 && sort_cmp(ctx, arr[j], key) > 0) {
            arr[j + 1] = arr[j];
            j--;
        }
        arr[j + 1] = key;
    }
}

/* Single-threaded merge sort (recursive, with insertion sort base case) */
static void sort_merge_recursive(const sort_cmp_ctx_t* ctx,
                                  int64_t* arr, int64_t* tmp, int64_t n) {
    if (n <= 64) {
        sort_insertion(ctx, arr, n);
        return;
    }
    int64_t mid = n / 2;
    sort_merge_recursive(ctx, arr, tmp, mid);
    sort_merge_recursive(ctx, arr + mid, tmp + mid, n - mid);

    /* Merge arr[0..mid) and arr[mid..n) into tmp, then copy back */
    int64_t i = 0, j = mid, k = 0;
    while (i < mid && j < n) {
        if (sort_cmp(ctx, arr[i], arr[j]) <= 0)
            tmp[k++] = arr[i++];
        else
            tmp[k++] = arr[j++];
    }
    while (i < mid) tmp[k++] = arr[i++];
    while (j < n) tmp[k++] = arr[j++];
    memcpy(arr, tmp, (size_t)n * sizeof(int64_t));
}

/* Parallel sort phase 1 context */
typedef struct {
    const sort_cmp_ctx_t* cmp_ctx;
    int64_t*  indices;
    int64_t*  tmp;
    int64_t   nrows;
    uint32_t  n_chunks;
} sort_phase1_ctx_t;

static void sort_phase1_fn(void* arg, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    sort_phase1_ctx_t* ctx = (sort_phase1_ctx_t*)arg;
    for (int64_t chunk_idx = start; chunk_idx < end; chunk_idx++) {
        int64_t chunk_size = (ctx->nrows + ctx->n_chunks - 1) / ctx->n_chunks;
        int64_t lo = chunk_idx * chunk_size;
        int64_t hi = lo + chunk_size;
        if (hi > ctx->nrows) hi = ctx->nrows;
        if (lo >= hi) continue;
        sort_merge_recursive(ctx->cmp_ctx, ctx->indices + lo, ctx->tmp + lo, hi - lo);
    }
}

/* Merge two adjacent sorted runs: [lo..mid) and [mid..hi) from src into dst */
static void merge_runs(const sort_cmp_ctx_t* ctx,
                        const int64_t* src, int64_t* dst,
                        int64_t lo, int64_t mid, int64_t hi) {
    int64_t i = lo, j = mid, k = lo;
    while (i < mid && j < hi) {
        if (sort_cmp(ctx, src[i], src[j]) <= 0)
            dst[k++] = src[i++];
        else
            dst[k++] = src[j++];
    }
    while (i < mid) dst[k++] = src[i++];
    while (j < hi) dst[k++] = src[j++];
}

/* Parallel merge pass context */
typedef struct {
    const sort_cmp_ctx_t* cmp_ctx;
    const int64_t*  src;
    int64_t*        dst;
    int64_t         nrows;
    int64_t         run_size;
} sort_merge_ctx_t;

static void sort_merge_fn(void* arg, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    sort_merge_ctx_t* ctx = (sort_merge_ctx_t*)arg;
    for (int64_t pair_idx = start; pair_idx < end; pair_idx++) {
        int64_t lo = pair_idx * 2 * ctx->run_size;
        int64_t mid = lo + ctx->run_size;
        int64_t hi = mid + ctx->run_size;
        if (mid > ctx->nrows) mid = ctx->nrows;
        if (hi > ctx->nrows) hi = ctx->nrows;
        if (lo >= ctx->nrows) continue;
        if (mid >= hi) {
            /* Only one run — copy directly */
            memcpy(ctx->dst + lo, ctx->src + lo, (size_t)(hi - lo) * sizeof(int64_t));
        } else {
            merge_runs(ctx->cmp_ctx, ctx->src, ctx->dst, lo, mid, hi);
        }
    }
}

static td_t* exec_sort(td_graph_t* g, td_op_t* op, td_t* df) {
    if (!df || TD_IS_ERR(df)) return df;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) return TD_ERR_PTR(TD_ERR_NYI);

    int64_t nrows = td_table_nrows(df);
    int64_t ncols = td_table_ncols(df);
    uint8_t n_sort = ext->sort.n_cols;

    /* Allocate index array */
    td_t* indices_hdr;
    int64_t* indices = (int64_t*)scratch_alloc(&indices_hdr, (size_t)nrows * sizeof(int64_t));
    if (!indices) return TD_ERR_PTR(TD_ERR_OOM);
    for (int64_t i = 0; i < nrows; i++) indices[i] = i;

    /* Resolve sort key vectors */
    td_t* sort_vecs[n_sort];
    uint8_t sort_owned[n_sort];
    memset(sort_vecs, 0, n_sort * sizeof(td_t*));
    memset(sort_owned, 0, n_sort);

    for (uint8_t k = 0; k < n_sort; k++) {
        td_op_t* key_op = ext->sort.columns[k];
        td_op_ext_t* key_ext = find_ext(g, key_op->id);
        if (key_ext && key_ext->base.opcode == OP_SCAN) {
            sort_vecs[k] = td_table_get_col(df, key_ext->sym);
        } else {
            td_t* saved = g->df;
            g->df = df;
            sort_vecs[k] = exec_node(g, key_op);
            g->df = saved;
            sort_owned[k] = 1;
        }
    }

    /* Build comparator context */
    sort_cmp_ctx_t cmp_ctx = {
        .vecs = sort_vecs,
        .desc = ext->sort.desc,
        .nulls_first = ext->sort.nulls_first,
        .n_sort = n_sort,
    };

    /* Sort using parallel merge sort for large arrays, insertion sort for small */
    if (nrows <= 64) {
        sort_insertion(&cmp_ctx, indices, nrows);
    } else {
        td_pool_t* pool = td_pool_get();
        uint32_t n_workers = pool ? td_pool_total_workers(pool) : 1;

        /* Allocate temporary buffer for merge operations */
        td_t* tmp_hdr;
        int64_t* tmp = (int64_t*)scratch_alloc(&tmp_hdr, (size_t)nrows * sizeof(int64_t));
        if (!tmp) {
            scratch_free(indices_hdr);
            return TD_ERR_PTR(TD_ERR_OOM);
        }

        /* Phase 1: parallel local sort of chunks */
        uint32_t n_chunks = n_workers;
        if (pool && n_chunks > 1 && nrows > 1024) {
            sort_phase1_ctx_t p1ctx = {
                .cmp_ctx = &cmp_ctx,
                .indices = indices,
                .tmp = tmp,
                .nrows = nrows,
                .n_chunks = n_chunks,
            };
            td_pool_dispatch_n(pool, sort_phase1_fn, &p1ctx, n_chunks);
        } else {
            /* Single-threaded: sort the entire array */
            n_chunks = 1;
            sort_merge_recursive(&cmp_ctx, indices, tmp, nrows);
        }

        /* Phase 2: iterative merge passes (log2(n_chunks) levels) */
        if (n_chunks > 1) {
            int64_t chunk_size = (nrows + n_chunks - 1) / n_chunks;
            int64_t run_size = chunk_size;
            int64_t* src = indices;
            int64_t* dst = tmp;

            while (run_size < nrows) {
                int64_t n_pairs = (nrows + 2 * run_size - 1) / (2 * run_size);
                sort_merge_ctx_t mctx = {
                    .cmp_ctx = &cmp_ctx,
                    .src = src,
                    .dst = dst,
                    .nrows = nrows,
                    .run_size = run_size,
                };
                if (pool && n_pairs > 1) {
                    td_pool_dispatch_n(pool, sort_merge_fn, &mctx, (uint32_t)n_pairs);
                } else {
                    /* Single merge pair */
                    sort_merge_fn(&mctx, 0, 0, n_pairs);
                }
                /* Swap src and dst */
                int64_t* t = src; src = dst; dst = t;
                run_size *= 2;
            }

            /* If final result is in tmp (not indices), copy back */
            if (src != indices) {
                memcpy(indices, src, (size_t)nrows * sizeof(int64_t));
            }
        }

        scratch_free(tmp_hdr);
    }

    /* Materialize sorted result */
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

    /* Free expression-evaluated sort keys */
    for (uint8_t k = 0; k < n_sort; k++) {
        if (sort_owned[k] && sort_vecs[k] && !TD_IS_ERR(sort_vecs[k]))
            td_release(sort_vecs[k]);
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

/* Extract salt from hash (upper 16 bits) for fast mismatch rejection */
#define HT_SALT(h) ((uint8_t)((h) >> 56))

/* Flags controlling which accumulator arrays are allocated */
#define GHT_NEED_SUM 0x01
#define GHT_NEED_MIN 0x02
#define GHT_NEED_MAX 0x04

/* ── Row-layout HT ──────────────────────────────────────────────────────
 * Keys + accumulators stored inline in both radix entries and group rows.
 * After phase1 copies data from original columns, phase2 and phase3 never
 * touch column data again — all access is sequential/local.
 * ────────────────────────────────────────────────────────────────────── */

typedef struct {
    uint16_t entry_stride;    /* bytes per radix entry: 8 + n_keys*8 + n_agg_vals*8 */
    uint16_t row_stride;      /* bytes per group row: 8 + n_keys*8 + accum_bytes */
    uint8_t  n_keys;
    uint8_t  n_aggs;
    uint8_t  n_agg_vals;      /* non-NULL agg columns (excludes COUNT) */
    uint8_t  need_flags;
    uint8_t  agg_is_f64;      /* bitmask: bit a set => agg[a] source is f64 */
    int8_t   agg_val_slot[8]; /* agg_idx -> entry/accum slot (-1 = no value) */
    /* Unified accumulator offsets: each block is n_agg_vals * 8 bytes.
     * Each 8B slot is double or int64_t based on agg_is_f64 bitmask. */
    uint16_t off_sum;         /* 0 => not allocated */
    uint16_t off_min;
    uint16_t off_max;
} ght_layout_t;

static ght_layout_t ght_compute_layout(uint8_t n_keys, uint8_t n_aggs,
                                        td_t** agg_vecs, uint8_t need_flags) {
    ght_layout_t ly;
    memset(&ly, 0, sizeof(ly));
    ly.n_keys = n_keys;
    ly.n_aggs = n_aggs;
    ly.need_flags = need_flags;

    uint8_t nv = 0;
    for (uint8_t a = 0; a < n_aggs && a < 8; a++) {
        if (agg_vecs[a]) {
            ly.agg_val_slot[a] = (int8_t)nv;
            if (agg_vecs[a]->type == TD_F64)
                ly.agg_is_f64 |= (1u << a);
            nv++;
        } else {
            ly.agg_val_slot[a] = -1;
        }
    }
    ly.n_agg_vals = nv;
    ly.entry_stride = (uint16_t)(8 + (uint16_t)n_keys * 8 + (uint16_t)nv * 8);

    uint16_t off = (uint16_t)(8 + (uint16_t)n_keys * 8);
    uint16_t block = (uint16_t)nv * 8;
    if (need_flags & GHT_NEED_SUM) { ly.off_sum = off; off += block; }
    if (need_flags & GHT_NEED_MIN) { ly.off_min = off; off += block; }
    if (need_flags & GHT_NEED_MAX) { ly.off_max = off; off += block; }
    ly.row_stride = off;
    return ly;
}

/* Packed HT slots: [salt:8 | gid:24] in 4 bytes.
 * Max groups per HT = 16M (24 bits) — ample for partitioned probes.
 * 4B slots halve cache footprint vs 8B, fitting HT in L2. */
#define HT_EMPTY    UINT32_MAX
#define HT_PACK(salt, gid)  (((uint32_t)(uint8_t)(salt) << 24) | ((gid) & 0xFFFFFF))
#define HT_GID(s)   ((s) & 0xFFFFFF)
#define HT_SALT_V(s) ((uint8_t)((s) >> 24))

typedef struct {
    uint32_t*    slots;       /* packed [salt:8|gid:24], HT_EMPTY=empty */
    uint32_t     ht_cap;
    char*        rows;        /* flat row store: rows + gid * layout.row_stride */
    uint32_t     grp_count;
    uint32_t     grp_cap;
    ght_layout_t layout;
    td_t*        _h_slots;
    td_t*        _h_rows;
} group_ht_t;

static bool group_ht_init_sized(group_ht_t* ht, uint32_t cap,
                                 const ght_layout_t* ly, uint32_t init_grp_cap) {
    ht->ht_cap = cap;
    ht->layout = *ly;
    ht->slots = (uint32_t*)scratch_alloc(&ht->_h_slots, (size_t)cap * sizeof(uint32_t));
    if (!ht->slots) return false;
    memset(ht->slots, 0xFF, (size_t)cap * sizeof(uint32_t)); /* HT_EMPTY = all-1s */
    ht->grp_cap = init_grp_cap;
    ht->grp_count = 0;
    ht->rows = (char*)scratch_alloc(&ht->_h_rows,
        (size_t)init_grp_cap * ly->row_stride);
    if (!ht->rows) return false;
    return true;
}

static bool group_ht_init(group_ht_t* ht, uint32_t cap, const ght_layout_t* ly) {
    return group_ht_init_sized(ht, cap, ly, 256);
}

static void group_ht_free(group_ht_t* ht) {
    scratch_free(ht->_h_slots);
    scratch_free(ht->_h_rows);
}

static void group_ht_grow(group_ht_t* ht) {
    uint32_t old_cap = ht->grp_cap;
    uint32_t new_cap = old_cap * 2;
    uint16_t rs = ht->layout.row_stride;
    char* new_rows = (char*)scratch_realloc(
        &ht->_h_rows, (size_t)old_cap * rs, (size_t)new_cap * rs);
    if (!new_rows) return;
    ht->rows = new_rows;
    ht->grp_cap = new_cap;
}

/* Hash inline int64_t keys (for rehash — no original column access) */
static inline uint64_t hash_keys_inline(const int64_t* keys, const int8_t* key_types,
                                         uint8_t n_keys) {
    uint64_t h = 0;
    for (uint8_t k = 0; k < n_keys; k++) {
        uint64_t kh;
        if (key_types[k] == TD_F64) {
            double dv;
            memcpy(&dv, &keys[k], 8);
            kh = td_hash_f64(dv);
        } else {
            kh = td_hash_i64(keys[k]);
        }
        h = (k == 0) ? kh : td_hash_combine(h, kh);
    }
    return h;
}

static void group_ht_rehash(group_ht_t* ht, const int8_t* key_types) {
    uint32_t new_cap = ht->ht_cap * 2;
    scratch_free(ht->_h_slots);
    ht->slots = (uint32_t*)scratch_alloc(&ht->_h_slots, (size_t)new_cap * sizeof(uint32_t));
    memset(ht->slots, 0xFF, (size_t)new_cap * sizeof(uint32_t));
    ht->ht_cap = new_cap;
    uint32_t mask = new_cap - 1;
    uint16_t rs = ht->layout.row_stride;
    uint8_t nk = ht->layout.n_keys;
    for (uint32_t gi = 0; gi < ht->grp_count; gi++) {
        const int64_t* row_keys = (const int64_t*)(ht->rows + (size_t)gi * rs + 8);
        uint64_t h = hash_keys_inline(row_keys, key_types, nk);
        uint32_t slot = (uint32_t)(h & mask);
        while (ht->slots[slot] != HT_EMPTY)
            slot = (slot + 1) & mask;
        ht->slots[slot] = HT_PACK(HT_SALT(h), gi);
    }
}

/* Initialize accumulators for a new group from entry's inline agg values.
 * Each unified block has n_agg_vals slots of 8 bytes, typed by agg_is_f64. */
static inline void init_accum_from_entry(char* row, const char* entry,
                                          const ght_layout_t* ly) {
    uint16_t accum_start = (uint16_t)(8 + (uint16_t)ly->n_keys * 8);
    if (ly->row_stride > accum_start)
        memset(row + accum_start, 0, ly->row_stride - accum_start);

    const char* agg_data = entry + 8 + ly->n_keys * 8;
    uint8_t na = ly->n_aggs;
    uint8_t nf = ly->need_flags;

    for (uint8_t a = 0; a < na; a++) {
        int8_t s = ly->agg_val_slot[a];
        if (s < 0) continue;
        /* Copy raw 8 bytes from entry into each enabled accumulator block */
        if (nf & GHT_NEED_SUM) memcpy(row + ly->off_sum + s * 8, agg_data + s * 8, 8);
        if (nf & GHT_NEED_MIN) memcpy(row + ly->off_min + s * 8, agg_data + s * 8, 8);
        if (nf & GHT_NEED_MAX) memcpy(row + ly->off_max + s * 8, agg_data + s * 8, 8);
    }
}

/* Accumulate into existing group from entry's inline agg values */
static inline void accum_from_entry(char* row, const char* entry,
                                     const ght_layout_t* ly) {
    const char* agg_data = entry + 8 + ly->n_keys * 8;
    uint8_t na = ly->n_aggs;
    uint8_t nf = ly->need_flags;

    for (uint8_t a = 0; a < na; a++) {
        int8_t s = ly->agg_val_slot[a];
        if (s < 0) continue;
        char* base = row;
        const char* val = agg_data + s * 8;

        if (ly->agg_is_f64 & (1u << a)) {
            double v;
            memcpy(&v, val, 8);
            if (nf & GHT_NEED_SUM) { double* p = (double*)(base + ly->off_sum) + s; *p += v; }
            if (nf & GHT_NEED_MIN) { double* p = (double*)(base + ly->off_min) + s; if (v < *p) *p = v; }
            if (nf & GHT_NEED_MAX) { double* p = (double*)(base + ly->off_max) + s; if (v > *p) *p = v; }
        } else {
            int64_t v;
            memcpy(&v, val, 8);
            if (nf & GHT_NEED_SUM) { int64_t* p = (int64_t*)(base + ly->off_sum) + s; *p += v; }
            if (nf & GHT_NEED_MIN) { int64_t* p = (int64_t*)(base + ly->off_min) + s; if (v < *p) *p = v; }
            if (nf & GHT_NEED_MAX) { int64_t* p = (int64_t*)(base + ly->off_max) + s; if (v > *p) *p = v; }
        }
    }
}

/* Probe + accumulate a single fat entry into the HT. Returns updated mask. */
static inline uint32_t group_probe_entry(group_ht_t* ht,
    const char* entry, const int8_t* key_types, uint32_t mask) {
    const ght_layout_t* ly = &ht->layout;
    uint64_t hash = *(const uint64_t*)entry;
    const char* ekeys = entry + 8;
    uint8_t salt = HT_SALT(hash);
    uint32_t slot = (uint32_t)(hash & mask);
    uint16_t key_bytes = ly->n_keys * 8;

    for (;;) {
        uint32_t sv = ht->slots[slot];
        if (sv == HT_EMPTY) {
            /* New group */
            if (ht->grp_count >= ht->grp_cap)
                group_ht_grow(ht);
            uint32_t gid = ht->grp_count++;
            char* row = ht->rows + (size_t)gid * ly->row_stride;
            *(int64_t*)row = 1;   /* count = 1 */
            memcpy(row + 8, ekeys, key_bytes);
            init_accum_from_entry(row, entry, ly);
            ht->slots[slot] = HT_PACK(salt, gid);
            if (ht->grp_count * 2 > ht->ht_cap) {
                group_ht_rehash(ht, key_types);
                mask = ht->ht_cap - 1;
            }
            return mask;
        }
        if (HT_SALT_V(sv) == salt) {
            uint32_t gid = HT_GID(sv);
            char* row = ht->rows + (size_t)gid * ly->row_stride;
            if (memcmp(row + 8, ekeys, key_bytes) == 0) {
                (*(int64_t*)row)++;   /* count++ */
                accum_from_entry(row, entry, ly);
                return mask;
            }
        }
        slot = (slot + 1) & mask;
    }
}

/* Process rows [start, end) from original columns into a local hash table.
 * Converts each row to a fat entry on the stack, then probes. */
#define GROUP_PREFETCH_BATCH 16

static void group_rows_range(group_ht_t* ht, void** key_data, int8_t* key_types,
                              td_t** agg_vecs, int64_t start, int64_t end) {
    const ght_layout_t* ly = &ht->layout;
    uint8_t nk = ly->n_keys;
    uint8_t na = ly->n_aggs;
    uint32_t mask = ht->ht_cap - 1;
    /* Stack buffer for one entry (max: 8 + 8*8 + 8*8 = 136 bytes) */
    char ebuf[8 + 8 * 8 + 8 * 8];

    for (int64_t row = start; row < end; row++) {
        uint64_t h = 0;
        int64_t* ek = (int64_t*)(ebuf + 8);
        for (uint8_t k = 0; k < nk; k++) {
            int8_t t = key_types[k];
            int64_t kv;
            if (t == TD_I64 || t == TD_SYM || t == TD_TIMESTAMP)
                kv = ((int64_t*)key_data[k])[row];
            else if (t == TD_F64)
                memcpy(&kv, &((double*)key_data[k])[row], 8);
            else if (t == TD_I32)
                kv = (int64_t)((int32_t*)key_data[k])[row];
            else if (t == TD_ENUM)
                kv = (int64_t)((uint32_t*)key_data[k])[row];
            else
                kv = 0;
            ek[k] = kv;
            uint64_t kh = (t == TD_F64) ? td_hash_f64(((double*)key_data[k])[row])
                                        : td_hash_i64(kv);
            h = (k == 0) ? kh : td_hash_combine(h, kh);
        }
        *(uint64_t*)ebuf = h;

        int64_t* ev = (int64_t*)(ebuf + 8 + nk * 8);
        uint8_t vi = 0;
        for (uint8_t a = 0; a < na; a++) {
            td_t* ac = agg_vecs[a];
            if (!ac) continue;
            if (ac->type == TD_F64)
                memcpy(&ev[vi], &((double*)td_data(ac))[row], 8);
            else if (ac->type == TD_I64 || ac->type == TD_SYM)
                ev[vi] = ((int64_t*)td_data(ac))[row];
            else if (ac->type == TD_I32)
                ev[vi] = (int64_t)((int32_t*)td_data(ac))[row];
            else if (ac->type == TD_ENUM)
                ev[vi] = (int64_t)((uint32_t*)td_data(ac))[row];
            else
                ev[vi] = (int64_t)((uint8_t*)td_data(ac))[row];
            vi++;
        }

        mask = group_probe_entry(ht, ebuf, key_types, mask);
    }
}

/* ============================================================================
 * Radix-partitioned parallel group-by
 *
 * Phase 1 (parallel): Each worker reads keys+agg values from original columns,
 *         packs into fat entries (hash, keys, agg_vals), scatters into
 *         thread-local per-partition buffers.
 * Phase 2 (parallel): Each partition is aggregated independently using
 *         inline data — no original column access needed.
 * Phase 3: Build result columns from inline group rows.
 * ============================================================================ */

#define RADIX_BITS  8
#define RADIX_P     (1u << RADIX_BITS)   /* 256 partitions */
#define RADIX_MASK  (RADIX_P - 1)
#define RADIX_PART(h) (((uint32_t)((h) >> 16)) & RADIX_MASK)

/* Per-worker, per-partition buffer of fat entries */
typedef struct {
    char*    data;           /* flat buffer: data[i * entry_stride] */
    uint32_t count;
    uint32_t cap;
    td_t*    _hdr;
} radix_buf_t;

static inline void radix_buf_push(radix_buf_t* buf, uint16_t entry_stride,
                                   uint64_t hash, const int64_t* keys, uint8_t n_keys,
                                   const int64_t* agg_vals, uint8_t n_agg_vals) {
    if (__builtin_expect(buf->count >= buf->cap, 0)) {
        uint32_t old_cap = buf->cap;
        uint32_t new_cap = old_cap * 2;
        char* new_data = (char*)scratch_realloc(
            &buf->_hdr, (size_t)old_cap * entry_stride,
            (size_t)new_cap * entry_stride);
        if (!new_data) return;
        buf->data = new_data;
        buf->cap = new_cap;
    }
    char* dst = buf->data + (size_t)buf->count * entry_stride;
    *(uint64_t*)dst = hash;
    memcpy(dst + 8, keys, (size_t)n_keys * 8);
    if (n_agg_vals)
        memcpy(dst + 8 + (size_t)n_keys * 8, agg_vals, (size_t)n_agg_vals * 8);
    buf->count++;
}

typedef struct {
    void**       key_data;
    int8_t*      key_types;
    td_t**       agg_vecs;
    uint32_t     n_workers;
    radix_buf_t* bufs;        /* [n_workers * RADIX_P] */
    ght_layout_t layout;
    const uint8_t* mask;
} radix_phase1_ctx_t;

static void radix_phase1_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    radix_phase1_ctx_t* c = (radix_phase1_ctx_t*)ctx;
    const ght_layout_t* ly = &c->layout;
    radix_buf_t* my_bufs = &c->bufs[(size_t)worker_id * RADIX_P];
    uint8_t nk = ly->n_keys;
    uint8_t na = ly->n_aggs;
    uint8_t nv = ly->n_agg_vals;
    uint16_t estride = ly->entry_stride;
    const uint8_t* mask = c->mask;

    int64_t keys[8];
    int64_t agg_vals[8];

    for (int64_t row = start; row < end; row++) {
        if (mask && !mask[row]) continue;
        uint64_t h = 0;
        for (uint8_t k = 0; k < nk; k++) {
            int8_t t = c->key_types[k];
            int64_t kv;
            if (t == TD_I64 || t == TD_SYM || t == TD_TIMESTAMP)
                kv = ((int64_t*)c->key_data[k])[row];
            else if (t == TD_F64)
                memcpy(&kv, &((double*)c->key_data[k])[row], 8);
            else if (t == TD_I32)
                kv = (int64_t)((int32_t*)c->key_data[k])[row];
            else if (t == TD_ENUM)
                kv = (int64_t)((uint32_t*)c->key_data[k])[row];
            else
                kv = 0;
            keys[k] = kv;
            uint64_t kh = (t == TD_F64) ? td_hash_f64(((double*)c->key_data[k])[row])
                                        : td_hash_i64(kv);
            h = (k == 0) ? kh : td_hash_combine(h, kh);
        }

        uint8_t vi = 0;
        for (uint8_t a = 0; a < na; a++) {
            td_t* ac = c->agg_vecs[a];
            if (!ac) continue;
            if (ac->type == TD_F64)
                memcpy(&agg_vals[vi], &((double*)td_data(ac))[row], 8);
            else if (ac->type == TD_I64 || ac->type == TD_SYM)
                agg_vals[vi] = ((int64_t*)td_data(ac))[row];
            else if (ac->type == TD_I32)
                agg_vals[vi] = (int64_t)((int32_t*)td_data(ac))[row];
            else if (ac->type == TD_ENUM)
                agg_vals[vi] = (int64_t)((uint32_t*)td_data(ac))[row];
            else
                agg_vals[vi] = (int64_t)((uint8_t*)td_data(ac))[row];
            vi++;
        }

        uint32_t part = RADIX_PART(h);
        radix_buf_push(&my_bufs[part], estride, h, keys, nk, agg_vals, nv);
    }
}

/* Process pre-partitioned fat entries into an HT with prefetch batching.
 * Two-phase prefetch: (1) prefetch HT slots, (2) prefetch group rows. */
static void group_rows_indirect(group_ht_t* ht, const int8_t* key_types,
                                 const char* entries, uint32_t n_entries,
                                 uint16_t entry_stride) {
    uint32_t mask = ht->ht_cap - 1;
    /* Stride-ahead prefetch: prefetch HT slot for entry i+D while processing i.
     * D=8 covers ~200ns L2/L3 latency at ~25ns per probe iteration. */
    enum { PF_DIST = 8 };
    /* Prime the prefetch pipeline */
    uint32_t pf_end = (n_entries < PF_DIST) ? n_entries : PF_DIST;
    for (uint32_t j = 0; j < pf_end; j++) {
        uint64_t h = *(const uint64_t*)(entries + (size_t)j * entry_stride);
        __builtin_prefetch(&ht->slots[(uint32_t)(h & mask)], 0, 1);
    }
    for (uint32_t i = 0; i < n_entries; i++) {
        /* Prefetch PF_DIST entries ahead */
        if (i + PF_DIST < n_entries) {
            uint64_t h = *(const uint64_t*)(entries + (size_t)(i + PF_DIST) * entry_stride);
            __builtin_prefetch(&ht->slots[(uint32_t)(h & mask)], 0, 1);
        }
        const char* e = entries + (size_t)i * entry_stride;
        mask = group_probe_entry(ht, e, key_types, mask);
    }
}

/* Phase 3: build result columns from inline group rows */
typedef struct {
    int8_t  out_type;
    bool    src_f64;
    uint16_t agg_op;
    void*   dst;
} agg_out_t;

typedef struct {
    group_ht_t*   part_hts;
    uint32_t*     part_offsets;
    char**        key_dsts;
    int8_t*       key_types;
    uint8_t*      key_esizes;
    uint8_t       n_keys;
    agg_out_t*    agg_outs;
    uint8_t       n_aggs;
} radix_phase3_ctx_t;

static void radix_phase3_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    radix_phase3_ctx_t* c = (radix_phase3_ctx_t*)ctx;
    uint8_t nk = c->n_keys;
    uint8_t na = c->n_aggs;

    for (int64_t p = start; p < end; p++) {
        group_ht_t* ph = &c->part_hts[p];
        uint32_t gc = ph->grp_count;
        if (gc == 0) continue;
        uint32_t off = c->part_offsets[p];
        const ght_layout_t* ly = &ph->layout;
        uint16_t rs = ly->row_stride;

        /* Single pass over group rows: read each row once, scatter keys + aggs.
         * Reduces memory traffic from nk+na passes over group data to 1 pass. */
        for (uint32_t gi = 0; gi < gc; gi++) {
            const char* row = ph->rows + (size_t)gi * rs;
            const int64_t* rkeys = (const int64_t*)(row + 8);
            int64_t cnt = *(const int64_t*)row;
            uint32_t di = off + gi;

            /* Scatter keys to result columns */
            for (uint8_t k = 0; k < nk; k++) {
                int64_t kv = rkeys[k];
                int8_t kt = c->key_types[k];
                char* dst = c->key_dsts[k];
                uint8_t esz = c->key_esizes[k];
                size_t doff = (size_t)di * esz;
                if (kt == TD_I64 || kt == TD_SYM || kt == TD_TIMESTAMP)
                    *(int64_t*)(dst + doff) = kv;
                else if (kt == TD_F64)
                    memcpy(dst + doff, &kv, 8);
                else if (kt == TD_ENUM)
                    *(uint32_t*)(dst + doff) = (uint32_t)kv;
                else if (kt == TD_I32)
                    *(int32_t*)(dst + doff) = (int32_t)kv;
            }

            /* Scatter agg results to result columns */
            for (uint8_t a = 0; a < na; a++) {
                agg_out_t* ao = &c->agg_outs[a];
                uint16_t op = ao->agg_op;
                bool sf = ao->src_f64;
                int8_t s = ly->agg_val_slot[a];
                if (ao->out_type == TD_F64) {
                    double v;
                    switch (op) {
                        case OP_SUM:
                            v = sf ? ((const double*)(row + ly->off_sum))[s]
                                   : (double)((const int64_t*)(row + ly->off_sum))[s];
                            break;
                        case OP_AVG:
                            v = sf ? ((const double*)(row + ly->off_sum))[s] / cnt
                                   : (double)((const int64_t*)(row + ly->off_sum))[s] / cnt;
                            break;
                        case OP_MIN:
                            v = sf ? ((const double*)(row + ly->off_min))[s]
                                   : (double)((const int64_t*)(row + ly->off_min))[s];
                            break;
                        case OP_MAX:
                            v = sf ? ((const double*)(row + ly->off_max))[s]
                                   : (double)((const int64_t*)(row + ly->off_max))[s];
                            break;
                        default: v = 0.0; break;
                    }
                    ((double*)ao->dst)[di] = v;
                } else {
                    int64_t v;
                    switch (op) {
                        case OP_SUM:   v = ((const int64_t*)(row + ly->off_sum))[s]; break;
                        case OP_COUNT: v = cnt; break;
                        case OP_MIN:   v = ((const int64_t*)(row + ly->off_min))[s]; break;
                        case OP_MAX:   v = ((const int64_t*)(row + ly->off_max))[s]; break;
                        case OP_FIRST: v = ((const int64_t*)(row + ly->off_sum))[s]; break;
                        default:       v = 0; break;
                    }
                    ((int64_t*)ao->dst)[di] = v;
                }
            }
        }
    }
}

/* Phase 2: aggregate each partition independently using inline data */
typedef struct {
    int8_t*      key_types;
    uint8_t      n_keys;
    uint32_t     n_workers;
    radix_buf_t* bufs;
    group_ht_t*  part_hts;
    ght_layout_t layout;
} radix_phase2_ctx_t;

static void radix_phase2_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    (void)worker_id;
    radix_phase2_ctx_t* c = (radix_phase2_ctx_t*)ctx;
    uint16_t estride = c->layout.entry_stride;

    for (int64_t p = start; p < end; p++) {
        uint32_t total = 0;
        for (uint32_t w = 0; w < c->n_workers; w++)
            total += c->bufs[(size_t)w * RADIX_P + p].count;
        if (total == 0) continue;

        uint32_t part_ht_cap = 256;
        {
            uint64_t target = (uint64_t)total * 2;
            if (target < 256) target = 256;
            while (part_ht_cap < target) part_ht_cap *= 2;
        }
        /* Pre-size group store to avoid grows. Use next_pow2(total) as upper
         * bound on groups. Over-allocation is bounded: worst case total >> groups,
         * but total * row_stride is already committed via HT capacity anyway. */
        uint32_t init_grp = 256;
        while (init_grp < total) init_grp *= 2;
        if (!group_ht_init_sized(&c->part_hts[p], part_ht_cap, &c->layout, init_grp))
            continue;

        for (uint32_t w = 0; w < c->n_workers; w++) {
            radix_buf_t* buf = &c->bufs[(size_t)w * RADIX_P + p];
            if (buf->count == 0) continue;
            group_rows_indirect(&c->part_hts[p], c->key_types,
                                buf->data, buf->count, estride);
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
    const uint8_t* mask;
} minmax_ctx_t;

static void minmax_scan_fn(void* ctx, uint32_t worker_id, int64_t start, int64_t end) {
    minmax_ctx_t* c = (minmax_ctx_t*)ctx;
    uint32_t wid = worker_id % c->n_workers;
    const uint8_t* mask = c->mask;
    int64_t kmin = INT64_MAX, kmax = INT64_MIN;
    int8_t t = c->key_type;
    if (t == TD_I64 || t == TD_SYM) {
        const int64_t* kd = (const int64_t*)c->key_data;
        for (int64_t r = start; r < end; r++) {
            if (mask && !mask[r]) continue;
            if (kd[r] < kmin) kmin = kd[r];
            if (kd[r] > kmax) kmax = kd[r];
        }
    } else if (t == TD_ENUM) {
        const uint32_t* kd = (const uint32_t*)c->key_data;
        for (int64_t r = start; r < end; r++) {
            if (mask && !mask[r]) continue;
            int64_t v = (int64_t)kd[r];
            if (v < kmin) kmin = v;
            if (v > kmax) kmax = v;
        }
    } else { /* TD_I32 */
        const int32_t* kd = (const int32_t*)c->key_data;
        for (int64_t r = start; r < end; r++) {
            if (mask && !mask[r]) continue;
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
                              td_t** agg_vecs, uint32_t grp_count,
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
                    case OP_FIRST: case OP_LAST:
                        v = is_f64 ? sum_f64[idx] : (double)sum_i64[idx]; break;
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
                    case OP_FIRST: case OP_LAST: v = sum_i64[idx]; break;
                    default:       v = 0; break;
                }
                ((int64_t*)td_data(new_col))[gi] = v;
            }
        }
        /* Generate unique column name: base_name + agg suffix (e.g. "v1_sum") */
        td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
        int64_t name_id;
        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            td_t* name_atom = td_sym_str(agg_ext->sym);
            const char* base = name_atom ? td_str_ptr(name_atom) : NULL;
            size_t blen = base ? td_str_len(name_atom) : 0;
            const char* sfx = "";
            size_t slen = 0;
            switch (agg_op) {
                case OP_SUM:   sfx = "_sum";   slen = 4; break;
                case OP_COUNT: sfx = "_count"; slen = 6; break;
                case OP_AVG:   sfx = "_mean";  slen = 5; break;
                case OP_MIN:   sfx = "_min";   slen = 4; break;
                case OP_MAX:   sfx = "_max";   slen = 4; break;
                case OP_FIRST: sfx = "_first"; slen = 6; break;
                case OP_LAST:  sfx = "_last";  slen = 5; break;
            }
            char buf[256];
            if (base && blen + slen < sizeof(buf)) {
                memcpy(buf, base, blen);
                memcpy(buf + blen, sfx, slen);
                name_id = td_sym_intern(buf, blen + slen);
            } else {
                name_id = agg_ext->sym;
            }
        } else {
            /* Expression agg input — synthetic name like "_e0_sum" */
            char nbuf[32];
            int np = 0;
            nbuf[np++] = '_'; nbuf[np++] = 'e';
            nbuf[np++] = (char)('0' + a);
            const char* nsfx = "";
            size_t nslen = 0;
            switch (agg_op) {
                case OP_SUM:   nsfx = "_sum";   nslen = 4; break;
                case OP_COUNT: nsfx = "_count"; nslen = 6; break;
                case OP_AVG:   nsfx = "_mean";  nslen = 5; break;
                case OP_MIN:   nsfx = "_min";   nslen = 4; break;
                case OP_MAX:   nsfx = "_max";   nslen = 4; break;
                case OP_FIRST: nsfx = "_first"; nslen = 6; break;
                case OP_LAST:  nsfx = "_last";  nslen = 5; break;
            }
            memcpy(nbuf + np, nsfx, nslen);
            name_id = td_sym_intern(nbuf, (size_t)np + nslen);
        }
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
    const uint8_t* mask;
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
    da_accum_t* acc = &c->accums[worker_id];
    uint8_t n_aggs = c->n_aggs;
    uint8_t n_keys = c->n_keys;
    const uint8_t* mask = c->mask;

    /* Fast path: single key — avoid composite GID loop overhead */
    if (n_keys == 1) {
        const void* kptr = c->key_ptrs[0];
        int8_t kt = c->key_types[0];
        int64_t kmin = c->key_mins[0];
        for (int64_t r = start; r < end; r++) {
            if (mask && !mask[r]) continue;
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
                } else if (op == OP_FIRST) {
                    if (acc->count[gid] == 1) { acc->f64[idx] = fv; acc->i64[idx] = iv; }
                } else if (op == OP_LAST) {
                    acc->f64[idx] = fv; acc->i64[idx] = iv;
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
        if (mask && !mask[r]) continue;
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
            } else if (op == OP_FIRST) {
                if (acc->count[gid] == 1) { acc->f64[idx] = fv; acc->i64[idx] = iv; }
            } else if (op == OP_LAST) {
                acc->f64[idx] = fv; acc->i64[idx] = iv;
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

    /* Extract filter mask for pushdown (skip filtered rows in scan loops) */
    const uint8_t* mask = NULL;
    if (g->filter_mask && g->filter_mask->type == TD_BOOL
        && g->filter_mask->len == nrows)
        mask = (const uint8_t*)td_data(g->filter_mask);

    if (n_keys > 8 || n_aggs > 8) return TD_ERR_PTR(TD_ERR_NYI);

    /* Resolve key columns (VLA — n_keys ≤ 8) */
    td_t* key_vecs[n_keys];
    memset(key_vecs, 0, n_keys * sizeof(td_t*));

    uint8_t key_owned[n_keys]; /* 1 = we allocated via exec_node, must free */
    memset(key_owned, 0, n_keys * sizeof(uint8_t));
    for (uint8_t k = 0; k < n_keys; k++) {
        td_op_t* key_op = ext->keys[k];
        td_op_ext_t* key_ext = find_ext(g, key_op->id);
        if (key_ext && key_ext->base.opcode == OP_SCAN) {
            key_vecs[k] = td_table_get_col(df, key_ext->sym);
        } else {
            /* Expression key (CASE WHEN etc) — evaluate against current df */
            td_t* saved_df = g->df;
            g->df = df;
            td_t* vec = exec_node(g, key_op);
            g->df = saved_df;
            if (vec && !TD_IS_ERR(vec)) {
                key_vecs[k] = vec;
                key_owned[k] = 1;
            }
        }
    }

    /* Resolve agg input columns (VLA — n_aggs ≤ 8) */
    td_t* agg_vecs[n_aggs];
    uint8_t agg_owned[n_aggs]; /* 1 = we allocated via exec_node, must free */
    memset(agg_vecs, 0, n_aggs * sizeof(td_t*));
    memset(agg_owned, 0, n_aggs * sizeof(uint8_t));

    for (uint8_t a = 0; a < n_aggs; a++) {
        td_op_t* agg_op = ext->agg_ins[a];
        td_op_ext_t* agg_ext = find_ext(g, agg_op->id);
        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            agg_vecs[a] = td_table_get_col(df, agg_ext->sym);
        } else if (agg_ext && agg_ext->base.opcode == OP_CONST && agg_ext->literal) {
            agg_vecs[a] = agg_ext->literal;
        } else {
            /* Expression node (ADD/MUL etc) — evaluate against current df */
            td_t* saved_df = g->df;
            g->df = df;
            td_t* vec = exec_node(g, agg_op);
            g->df = saved_df;
            if (vec && !TD_IS_ERR(vec)) {
                agg_vecs[a] = vec;
                agg_owned[a] = 1;
            }
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
                    .mask           = mask,
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
                if (op == OP_SUM || op == OP_AVG || op == OP_FIRST || op == OP_LAST) need_flags |= DA_NEED_SUM;
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
                if (op == OP_SUM || op == OP_AVG || op == OP_FIRST || op == OP_LAST) need_flags |= DA_NEED_SUM;
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

            /* Check memory budget — need one accumulator set per worker.
             * If total memory exceeds budget, fall to sequential (1 worker)
             * rather than throttling to a subset, which would alias workers
             * to shared accumulators and cause data races. */
            uint32_t arrays_per_agg = 0;
            if (need_flags & DA_NEED_SUM) arrays_per_agg += 2;
            if (need_flags & DA_NEED_MIN) arrays_per_agg += 2;
            if (need_flags & DA_NEED_MAX) arrays_per_agg += 2;
            uint64_t per_worker_bytes = (uint64_t)n_slots * (arrays_per_agg * n_aggs + 1u) * 8u;
            if ((uint64_t)da_n_workers * per_worker_bytes > DA_MEM_BUDGET)
                da_n_workers = 1;

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
                .mask        = mask,
            };

            if (da_n_workers > 1)
                td_pool_dispatch(da_pool, da_accum_fn, &da_ctx, nrows);
            else
                da_accum_fn(&da_ctx, 0, 0, nrows);

            /* Check if any agg is FIRST/LAST (needs per-slot merge) */
            bool has_first_last = false;
            for (uint8_t a = 0; a < n_aggs; a++) {
                uint16_t op = ext->agg_ops[a];
                if (op == OP_FIRST || op == OP_LAST) { has_first_last = true; break; }
            }

            /* Merge per-worker accumulators into accums[0] */
            da_accum_t* merged = &accums[0];
            for (uint32_t w = 1; w < da_n_workers; w++) {
                da_accum_t* wa = &accums[w];
                if (need_flags & DA_NEED_SUM) {
                    if (has_first_last) {
                        /* Per-slot merge: FIRST/LAST need different semantics */
                        for (uint32_t s = 0; s < n_slots; s++) {
                            size_t base = (size_t)s * n_aggs;
                            for (uint8_t a = 0; a < n_aggs; a++) {
                                size_t idx = base + a;
                                uint16_t op = ext->agg_ops[a];
                                if (op == OP_SUM || op == OP_AVG) {
                                    merged->f64[idx] += wa->f64[idx];
                                    merged->i64[idx] += wa->i64[idx];
                                } else if (op == OP_FIRST) {
                                    /* Keep first worker's value (merged already has it if count > 0) */
                                    if (merged->count[s] == 0 && wa->count[s] > 0) {
                                        merged->f64[idx] = wa->f64[idx];
                                        merged->i64[idx] = wa->i64[idx];
                                    }
                                } else if (op == OP_LAST) {
                                    /* Take last worker's value if it saw the group */
                                    if (wa->count[s] > 0) {
                                        merged->f64[idx] = wa->f64[idx];
                                        merged->i64[idx] = wa->i64[idx];
                                    }
                                }
                            }
                        }
                    } else {
                        for (size_t i = 0; i < total; i++) {
                            merged->f64[i] += wa->f64[i];
                            merged->i64[i] += wa->i64[i];
                        }
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
                /* Merge counts AFTER agg merge (FIRST/LAST check merged count) */
                for (uint32_t s = 0; s < n_slots; s++) {
                    merged->count[s] += wa->count[s];
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

            emit_agg_columns(&result, g, ext, agg_vecs, grp_count, n_aggs,
                             dense_f64, dense_i64, dense_min_f64, dense_max_f64,
                             dense_min_i64, dense_max_i64, dense_counts);

            scratch_free(_h_df64); scratch_free(_h_di64);
            scratch_free(_h_dminf); scratch_free(_h_dmaxf);
            scratch_free(_h_dmini); scratch_free(_h_dmaxi);
            scratch_free(_h_dcnt);

            da_accum_free(&accums[0]); scratch_free(accums_hdr);
            for (uint8_t a = 0; a < n_aggs; a++)
                if (agg_owned[a] && agg_vecs[a]) td_release(agg_vecs[a]);
            for (uint8_t k = 0; k < n_keys; k++)
                if (key_owned[k] && key_vecs[k]) td_release(key_vecs[k]);
            return result;
        }
    }

ht_path:;
    /* Compute which accumulator arrays the HT needs based on agg ops.
     * COUNT only reads group row's count field — no accumulator needed. */
    uint8_t ght_need = 0;
    for (uint8_t a = 0; a < n_aggs; a++) {
        uint16_t op = ext->agg_ops[a];
        if (op == OP_SUM || op == OP_AVG || op == OP_FIRST || op == OP_LAST)
            ght_need |= GHT_NEED_SUM;
        if (op == OP_MIN) ght_need |= GHT_NEED_MIN;
        if (op == OP_MAX) ght_need |= GHT_NEED_MAX;
    }

    /* Compute row-layout: keys + agg values inline */
    ght_layout_t ght_layout = ght_compute_layout(n_keys, n_aggs, agg_vecs, ght_need);

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
    td_t* result = NULL;

    td_t* radix_bufs_hdr = NULL;
    radix_buf_t* radix_bufs = NULL;
    td_t* part_hts_hdr = NULL;
    group_ht_t*  part_hts   = NULL;

    if (pool && nrows >= TD_PARALLEL_THRESHOLD && n_total > 1) {
        size_t n_bufs = (size_t)n_total * RADIX_P;
        radix_bufs = (radix_buf_t*)scratch_calloc(&radix_bufs_hdr,
            n_bufs * sizeof(radix_buf_t));
        if (!radix_bufs) goto sequential_fallback;

        /* Pre-size each buffer: 1.5x expected entries per partition per worker */
        uint32_t buf_init = (uint32_t)((uint64_t)nrows / (RADIX_P * n_total));
        if (buf_init < 64) buf_init = 64;
        buf_init = buf_init + buf_init / 2;  /* 1.5x headroom */
        uint16_t estride = ght_layout.entry_stride;
        for (size_t i = 0; i < n_bufs; i++) {
            radix_bufs[i].data = (char*)scratch_alloc(
                &radix_bufs[i]._hdr, (size_t)buf_init * estride);
            radix_bufs[i].count = 0;
            radix_bufs[i].cap = buf_init;
        }

        /* Phase 1: parallel hash + copy keys/agg values into fat entries */
        radix_phase1_ctx_t p1ctx = {
            .key_data  = key_data,
            .key_types = key_types,
            .agg_vecs  = agg_vecs,
            .n_workers = n_total,
            .bufs      = radix_bufs,
            .layout    = ght_layout,
            .mask      = mask,
        };
        td_pool_dispatch(pool, radix_phase1_fn, &p1ctx, nrows);

        /* Phase 2: parallel per-partition aggregation (no column access) */
        part_hts = (group_ht_t*)scratch_calloc(&part_hts_hdr,
            RADIX_P * sizeof(group_ht_t));
        if (!part_hts) {
            for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
            scratch_free(radix_bufs_hdr);
            radix_bufs = NULL;
            goto sequential_fallback;
        }

        radix_phase2_ctx_t p2ctx = {
            .key_types   = key_types,
            .n_keys      = n_keys,
            .n_workers   = n_total,
            .bufs        = radix_bufs,
            .part_hts    = part_hts,
            .layout      = ght_layout,
        };
        td_pool_dispatch_n(pool, radix_phase2_fn, &p2ctx, RADIX_P);

        /* Prefix offsets */
        uint32_t part_offsets[RADIX_P + 1];
        part_offsets[0] = 0;
        for (uint32_t p = 0; p < RADIX_P; p++)
            part_offsets[p + 1] = part_offsets[p] + part_hts[p].grp_count;
        uint32_t total_grps = part_offsets[RADIX_P];

        /* Build result directly from partition HTs */
        int64_t total_cols = n_keys + n_aggs;
        result = td_table_new(total_cols);
        if (!result || TD_IS_ERR(result)) goto cleanup;

        /* Pre-allocate key columns */
        td_t* key_cols[n_keys];
        char* key_dsts[n_keys];
        int8_t key_out_types[n_keys];
        uint8_t key_esizes[n_keys];
        for (uint8_t k = 0; k < n_keys; k++) {
            td_t* src_col = key_vecs[k];
            key_cols[k] = NULL;
            key_dsts[k] = NULL;
            key_out_types[k] = 0;
            key_esizes[k] = 0;
            if (!src_col) continue;
            uint8_t esz = td_elem_size(src_col->type);
            td_t* new_col = td_vec_new(src_col->type, (int64_t)total_grps);
            if (!new_col || TD_IS_ERR(new_col)) continue;
            new_col->len = (int64_t)total_grps;
            key_cols[k] = new_col;
            key_dsts[k] = (char*)td_data(new_col);
            key_out_types[k] = src_col->type;
            key_esizes[k] = esz;
        }

        /* Pre-allocate agg result vectors */
        agg_out_t agg_outs[n_aggs];
        td_t* agg_cols[n_aggs];
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
            td_t* new_col = td_vec_new(out_type, (int64_t)total_grps);
            if (!new_col || TD_IS_ERR(new_col)) { agg_cols[a] = NULL; continue; }
            new_col->len = (int64_t)total_grps;
            agg_cols[a] = new_col;
            agg_outs[a] = (agg_out_t){
                .out_type = out_type, .src_f64 = is_f64,
                .agg_op = agg_op, .dst = td_data(new_col),
            };
        }

        /* Phase 3: parallel key gather + agg result building from inline rows */
        {
            radix_phase3_ctx_t p3ctx = {
                .part_hts     = part_hts,
                .part_offsets = part_offsets,
                .key_dsts     = key_dsts,
                .key_types    = key_out_types,
                .key_esizes   = key_esizes,
                .n_keys       = n_keys,
                .agg_outs     = agg_outs,
                .n_aggs       = n_aggs,
            };
            td_pool_dispatch_n(pool, radix_phase3_fn, &p3ctx, RADIX_P);
        }

        /* Add key columns to result */
        for (uint8_t k = 0; k < n_keys; k++) {
            if (!key_cols[k]) continue;
            td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
            int64_t name_id = key_ext ? key_ext->sym : k;
            result = td_table_add_col(result, name_id, key_cols[k]);
            td_release(key_cols[k]);
        }

        /* Add agg columns to result */
        for (uint8_t a = 0; a < n_aggs; a++) {
            if (!agg_cols[a]) continue;
            uint16_t agg_op = ext->agg_ops[a];
            td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
            int64_t name_id;
            if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
                td_t* name_atom = td_sym_str(agg_ext->sym);
                const char* base = name_atom ? td_str_ptr(name_atom) : NULL;
                size_t blen = base ? td_str_len(name_atom) : 0;
                const char* sfx = "";
                size_t slen = 0;
                switch (agg_op) {
                    case OP_SUM:   sfx = "_sum";   slen = 4; break;
                    case OP_COUNT: sfx = "_count"; slen = 6; break;
                    case OP_AVG:   sfx = "_mean";  slen = 5; break;
                    case OP_MIN:   sfx = "_min";   slen = 4; break;
                    case OP_MAX:   sfx = "_max";   slen = 4; break;
                    case OP_FIRST: sfx = "_first"; slen = 6; break;
                    case OP_LAST:  sfx = "_last";  slen = 5; break;
                }
                char buf[256];
                if (base && blen + slen < sizeof(buf)) {
                    memcpy(buf, base, blen);
                    memcpy(buf + blen, sfx, slen);
                    name_id = td_sym_intern(buf, blen + slen);
                } else {
                    name_id = agg_ext->sym;
                }
            } else {
                name_id = (int64_t)(n_keys + a);
            }
            result = td_table_add_col(result, name_id, agg_cols[a]);
            td_release(agg_cols[a]);
        }

        goto cleanup;
    }

sequential_fallback:;
    /* Sequential path using row-layout HT */
    if (!group_ht_init(&single_ht, ht_cap, &ght_layout))
        return TD_ERR_PTR(TD_ERR_OOM);
    group_rows_range(&single_ht, key_data, key_types, agg_vecs, 0, nrows);

    final_ht = &single_ht;

    /* Build result from sequential HT (inline row layout) */
    {
    uint32_t grp_count = final_ht->grp_count;
    const ght_layout_t* ly = &final_ht->layout;
    int64_t total_cols = n_keys + n_aggs;
    result = td_table_new(total_cols);
    if (!result || TD_IS_ERR(result)) goto cleanup;

    /* Key columns: read from inline group rows, narrow to original type */
    for (uint8_t k = 0; k < n_keys; k++) {
        td_t* src_col = key_vecs[k];
        if (!src_col) continue;
        uint8_t esz = td_elem_size(src_col->type);
        int8_t kt = src_col->type;

        td_t* new_col = td_vec_new(kt, (int64_t)grp_count);
        if (!new_col || TD_IS_ERR(new_col)) continue;
        new_col->len = (int64_t)grp_count;

        for (uint32_t gi = 0; gi < grp_count; gi++) {
            const char* row = final_ht->rows + (size_t)gi * ly->row_stride;
            int64_t kv = ((const int64_t*)(row + 8))[k];
            char* dst = (char*)td_data(new_col) + (size_t)gi * esz;
            if (kt == TD_I64 || kt == TD_SYM || kt == TD_TIMESTAMP)
                *(int64_t*)dst = kv;
            else if (kt == TD_F64)
                memcpy(dst, &kv, 8);
            else if (kt == TD_ENUM)
                *(uint32_t*)dst = (uint32_t)kv;
            else if (kt == TD_I32)
                *(int32_t*)dst = (int32_t)kv;
        }

        td_op_ext_t* key_ext = find_ext(g, ext->keys[k]->id);
        int64_t name_id = key_ext ? key_ext->sym : k;
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }

    /* Agg columns from inline accumulators */
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

        int8_t s = ly->agg_val_slot[a]; /* unified accum slot */
        for (uint32_t gi = 0; gi < grp_count; gi++) {
            const char* row = final_ht->rows + (size_t)gi * ly->row_stride;
            int64_t cnt = *(const int64_t*)row;
            if (out_type == TD_F64) {
                double v;
                switch (agg_op) {
                    case OP_SUM:
                        v = is_f64 ? ((const double*)(row + ly->off_sum))[s]
                                   : (double)((const int64_t*)(row + ly->off_sum))[s];
                        break;
                    case OP_AVG:
                        v = is_f64 ? ((const double*)(row + ly->off_sum))[s] / cnt
                                   : (double)((const int64_t*)(row + ly->off_sum))[s] / cnt;
                        break;
                    case OP_MIN:
                        v = is_f64 ? ((const double*)(row + ly->off_min))[s]
                                   : (double)((const int64_t*)(row + ly->off_min))[s];
                        break;
                    case OP_MAX:
                        v = is_f64 ? ((const double*)(row + ly->off_max))[s]
                                   : (double)((const int64_t*)(row + ly->off_max))[s];
                        break;
                    default: v = 0.0; break;
                }
                ((double*)td_data(new_col))[gi] = v;
            } else {
                int64_t v;
                switch (agg_op) {
                    case OP_SUM:   v = ((const int64_t*)(row + ly->off_sum))[s]; break;
                    case OP_COUNT: v = cnt; break;
                    case OP_MIN:   v = ((const int64_t*)(row + ly->off_min))[s]; break;
                    case OP_MAX:   v = ((const int64_t*)(row + ly->off_max))[s]; break;
                    case OP_FIRST: v = ((const int64_t*)(row + ly->off_sum))[s]; break;
                    default:       v = 0; break;
                }
                ((int64_t*)td_data(new_col))[gi] = v;
            }
        }

        /* Generate unique column name */
        td_op_ext_t* agg_ext = find_ext(g, ext->agg_ins[a]->id);
        int64_t name_id;
        if (agg_ext && agg_ext->base.opcode == OP_SCAN) {
            td_t* name_atom = td_sym_str(agg_ext->sym);
            const char* base = name_atom ? td_str_ptr(name_atom) : NULL;
            size_t blen = base ? td_str_len(name_atom) : 0;
            const char* sfx = "";
            size_t slen = 0;
            switch (agg_op) {
                case OP_SUM:   sfx = "_sum";   slen = 4; break;
                case OP_COUNT: sfx = "_count"; slen = 6; break;
                case OP_AVG:   sfx = "_mean";  slen = 5; break;
                case OP_MIN:   sfx = "_min";   slen = 4; break;
                case OP_MAX:   sfx = "_max";   slen = 4; break;
                case OP_FIRST: sfx = "_first"; slen = 6; break;
                case OP_LAST:  sfx = "_last";  slen = 5; break;
            }
            char buf[256];
            if (base && blen + slen < sizeof(buf)) {
                memcpy(buf, base, blen);
                memcpy(buf + blen, sfx, slen);
                name_id = td_sym_intern(buf, blen + slen);
            } else {
                name_id = agg_ext->sym;
            }
        } else {
            /* Expression agg input — synthetic name like "_e0_sum" */
            char nbuf[32];
            int np = 0;
            nbuf[np++] = '_'; nbuf[np++] = 'e';
            nbuf[np++] = (char)('0' + a);
            const char* nsfx = "";
            size_t nslen = 0;
            switch (agg_op) {
                case OP_SUM:   nsfx = "_sum";   nslen = 4; break;
                case OP_COUNT: nsfx = "_count"; nslen = 6; break;
                case OP_AVG:   nsfx = "_mean";  nslen = 5; break;
                case OP_MIN:   nsfx = "_min";   nslen = 4; break;
                case OP_MAX:   nsfx = "_max";   nslen = 4; break;
                case OP_FIRST: nsfx = "_first"; nslen = 6; break;
                case OP_LAST:  nsfx = "_last";  nslen = 5; break;
            }
            memcpy(nbuf + np, nsfx, nslen);
            name_id = td_sym_intern(nbuf, (size_t)np + nslen);
        }
        result = td_table_add_col(result, name_id, new_col);
        td_release(new_col);
    }
    }

cleanup:
    if (final_ht == &single_ht) {
        group_ht_free(&single_ht);
    }
    if (radix_bufs) {
        size_t n_bufs = (size_t)n_total * RADIX_P;
        for (size_t i = 0; i < n_bufs; i++) scratch_free(radix_bufs[i]._hdr);
        scratch_free(radix_bufs_hdr);
    }
    if (part_hts) {
        for (uint32_t p = 0; p < RADIX_P; p++) {
            if (part_hts[p].rows) group_ht_free(&part_hts[p]);
        }
        scratch_free(part_hts_hdr);
    }
    for (uint8_t a = 0; a < n_aggs; a++)
        if (agg_owned[a] && agg_vecs[a]) td_release(agg_vecs[a]);
    for (uint8_t k = 0; k < n_keys; k++)
        if (key_owned[k] && key_vecs[k]) td_release(key_vecs[k]);

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
    uint64_t target = (uint64_t)right_rows * 2;
    while (ht_cap < target && ht_cap != 0) ht_cap *= 2;
    if (ht_cap == 0) ht_cap = (uint32_t)(target | (target >> 1)); /* saturate */

    td_t* result = NULL;  /* declared before goto targets for Clang -Wsometimes-uninitialized */
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
                    int64_t* new_l = (int64_t*)scratch_realloc(&l_idx_hdr,
                        (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                    int64_t* new_r = (int64_t*)scratch_realloc(&r_idx_hdr,
                        (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                    if (!new_l || !new_r) goto join_cleanup;
                    l_idx = new_l;
                    r_idx = new_r;
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
                int64_t* new_l = (int64_t*)scratch_realloc(&l_idx_hdr,
                    (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                int64_t* new_r = (int64_t*)scratch_realloc(&r_idx_hdr,
                    (size_t)old_cap * sizeof(int64_t), (size_t)pair_cap * sizeof(int64_t));
                if (!new_l || !new_r) goto join_cleanup;
                l_idx = new_l;
                r_idx = new_r;
            }
            l_idx[pair_count] = l;
            r_idx[pair_count] = -1;
            pair_count++;
        }
    }

    /* Build result DataFrame */
    int64_t left_ncols = td_table_ncols(left_df);
    int64_t right_ncols = td_table_ncols(right_df);
    result = td_table_new(left_ncols + right_ncols);
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
 * OP_IF: ternary select  result[i] = cond[i] ? then[i] : else[i]
 * ============================================================================ */

static td_t* exec_if(td_graph_t* g, td_op_t* op) {
    /* cond = inputs[0], then = inputs[1], else_id stored in ext->literal */
    td_t* cond_v = exec_node(g, op->inputs[0]);
    td_t* then_v = exec_node(g, op->inputs[1]);

    td_op_ext_t* ext = find_ext(g, op->id);
    uint32_t else_id = (uint32_t)(uintptr_t)ext->literal;
    td_t* else_v = exec_node(g, &g->nodes[else_id]);

    if (!cond_v || TD_IS_ERR(cond_v)) {
        if (then_v && !TD_IS_ERR(then_v)) td_release(then_v);
        if (else_v && !TD_IS_ERR(else_v)) td_release(else_v);
        return cond_v;
    }
    if (!then_v || TD_IS_ERR(then_v)) {
        td_release(cond_v);
        if (else_v && !TD_IS_ERR(else_v)) td_release(else_v);
        return then_v;
    }
    if (!else_v || TD_IS_ERR(else_v)) {
        td_release(cond_v); td_release(then_v);
        return else_v;
    }

    int64_t len = cond_v->len;
    bool then_scalar = td_is_atom(then_v);
    bool else_scalar = td_is_atom(else_v);
    if (then_scalar && !else_scalar) len = else_v->len;
    if (!then_scalar) len = then_v->len;

    int8_t out_type = op->out_type;
    td_t* result = td_vec_new(out_type, len);
    if (!result || TD_IS_ERR(result)) {
        td_release(cond_v); td_release(then_v); td_release(else_v);
        return result;
    }
    result->len = len;

    uint8_t* cond_p = (uint8_t*)td_data(cond_v);

    if (out_type == TD_F64) {
        double t_scalar = then_scalar ? then_v->f64 : 0;
        double e_scalar = else_scalar ? else_v->f64 : 0;
        double* t_arr = then_scalar ? NULL : (double*)td_data(then_v);
        double* e_arr = else_scalar ? NULL : (double*)td_data(else_v);
        double* dst = (double*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_I64 || out_type == TD_SYM) {
        /* For SYM output with TD_STR scalar constants, intern to get SYM IDs */
        int64_t t_scalar = 0, e_scalar = 0;
        if (then_scalar) {
            if (then_v->type == TD_ATOM_STR) {
                t_scalar = td_sym_intern(td_str_ptr(then_v), td_str_len(then_v));
            } else {
                t_scalar = then_v->i64;
            }
        }
        if (else_scalar) {
            if (else_v->type == TD_ATOM_STR) {
                e_scalar = td_sym_intern(td_str_ptr(else_v), td_str_len(else_v));
            } else {
                e_scalar = else_v->i64;
            }
        }
        int64_t* t_arr = then_scalar ? NULL : (int64_t*)td_data(then_v);
        int64_t* e_arr = else_scalar ? NULL : (int64_t*)td_data(else_v);
        int64_t* dst = (int64_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_I32) {
        int32_t t_scalar = then_scalar ? then_v->i32 : 0;
        int32_t e_scalar = else_scalar ? else_v->i32 : 0;
        int32_t* t_arr = then_scalar ? NULL : (int32_t*)td_data(then_v);
        int32_t* e_arr = else_scalar ? NULL : (int32_t*)td_data(else_v);
        int32_t* dst = (int32_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_ENUM) {
        uint32_t t_scalar = then_scalar ? then_v->u32 : 0;
        uint32_t e_scalar = else_scalar ? else_v->u32 : 0;
        uint32_t* t_arr = then_scalar ? NULL : (uint32_t*)td_data(then_v);
        uint32_t* e_arr = else_scalar ? NULL : (uint32_t*)td_data(else_v);
        uint32_t* dst = (uint32_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    } else if (out_type == TD_BOOL) {
        uint8_t t_scalar = then_scalar ? then_v->b8 : 0;
        uint8_t e_scalar = else_scalar ? else_v->b8 : 0;
        uint8_t* t_arr = then_scalar ? NULL : (uint8_t*)td_data(then_v);
        uint8_t* e_arr = else_scalar ? NULL : (uint8_t*)td_data(else_v);
        uint8_t* dst = (uint8_t*)td_data(result);
        for (int64_t i = 0; i < len; i++)
            dst[i] = cond_p[i] ? (t_arr ? t_arr[i] : t_scalar)
                               : (e_arr ? e_arr[i] : e_scalar);
    }

    td_release(cond_v); td_release(then_v); td_release(else_v);
    return result;
}

/* ============================================================================
 * OP_LIKE: SQL LIKE pattern matching on SYM/ENUM columns
 * ============================================================================ */

/* Simple SQL LIKE matcher: % = any (including empty), _ = single char */
static bool like_match(const char* str, size_t slen, const char* pat, size_t plen) {
    size_t si = 0, pi = 0;
    size_t star_p = (size_t)-1, star_s = 0;
    while (si < slen) {
        if (pi < plen && (pat[pi] == str[si] || pat[pi] == '_')) {
            si++; pi++;
        } else if (pi < plen && pat[pi] == '%') {
            star_p = pi; star_s = si;
            pi++;
        } else if (star_p != (size_t)-1) {
            pi = star_p + 1;
            star_s++;
            si = star_s;
        } else {
            return false;
        }
    }
    while (pi < plen && pat[pi] == '%') pi++;
    return pi == plen;
}

static td_t* exec_like(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* pat_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (pat_v && !TD_IS_ERR(pat_v)) td_release(pat_v); return input; }
    if (!pat_v || TD_IS_ERR(pat_v)) { td_release(input); return pat_v; }

    /* Get pattern string */
    const char* pat_str = td_str_ptr(pat_v);
    size_t pat_len = td_str_len(pat_v);

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_BOOL, len);
    if (!result || TD_IS_ERR(result)) {
        td_release(input); td_release(pat_v);
        return result;
    }
    result->len = len;
    uint8_t* dst = (uint8_t*)td_data(result);

    int8_t in_type = input->type;
    if (in_type == TD_ENUM) {
        uint32_t* data = (uint32_t*)td_data(input);
        for (int64_t i = 0; i < len; i++) {
            td_t* s = td_sym_str((int64_t)data[i]);
            if (!s) { dst[i] = 0; continue; }
            const char* sp = td_str_ptr(s);
            size_t sl = td_str_len(s);
            dst[i] = like_match(sp, sl, pat_str, pat_len) ? 1 : 0;
        }
    } else if (in_type == TD_SYM) {
        int64_t* data = (int64_t*)td_data(input);
        for (int64_t i = 0; i < len; i++) {
            td_t* s = td_sym_str(data[i]);
            if (!s) { dst[i] = 0; continue; }
            const char* sp = td_str_ptr(s);
            size_t sl = td_str_len(s);
            dst[i] = like_match(sp, sl, pat_str, pat_len) ? 1 : 0;
        }
    } else {
        /* Non-string type: all false */
        memset(dst, 0, (size_t)len);
    }

    td_release(input); td_release(pat_v);
    return result;
}

/* ============================================================================
 * String functions: UPPER, LOWER, TRIM, STRLEN, SUBSTR, REPLACE, CONCAT
 * ============================================================================ */

/* Helper: resolve sym/enum element to string */
static inline void sym_elem(const td_t* input, int64_t i,
                            const char** out_str, size_t* out_len) {
    int64_t sym_id;
    if (input->type == TD_ENUM)
        sym_id = (int64_t)((const uint32_t*)td_data((td_t*)input))[i];
    else
        sym_id = ((const int64_t*)td_data((td_t*)input))[i];
    td_t* atom = td_sym_str(sym_id);
    if (!atom) { *out_str = ""; *out_len = 0; return; }
    *out_str = td_str_ptr(atom);
    *out_len = td_str_len(atom);
}

/* UPPER / LOWER / TRIM — unary SYM/ENUM → SYM */
static td_t* exec_string_unary(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_SYM, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;
    int64_t* dst = (int64_t*)td_data(result);

    uint16_t opc = op->opcode;
    for (int64_t i = 0; i < len; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        char buf[1024];
        size_t out_len = sl < sizeof(buf) ? sl : sizeof(buf) - 1;
        if (opc == OP_UPPER) {
            for (size_t j = 0; j < out_len; j++) buf[j] = (char)toupper((unsigned char)sp[j]);
        } else if (opc == OP_LOWER) {
            for (size_t j = 0; j < out_len; j++) buf[j] = (char)tolower((unsigned char)sp[j]);
        } else { /* OP_TRIM */
            size_t start = 0, end = sl;
            while (start < sl && isspace((unsigned char)sp[start])) start++;
            while (end > start && isspace((unsigned char)sp[end - 1])) end--;
            out_len = end - start;
            if (out_len > sizeof(buf) - 1) out_len = sizeof(buf) - 1;
            memcpy(buf, sp + start, out_len);
        }
        buf[out_len] = '\0';
        dst[i] = td_sym_intern(buf, out_len);
    }
    td_release(input);
    return result;
}

/* LENGTH — SYM/ENUM → I64 */
static td_t* exec_strlen(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    int64_t len = input->len;
    td_t* result = td_vec_new(TD_I64, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;
    int64_t* dst = (int64_t*)td_data(result);

    for (int64_t i = 0; i < len; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        dst[i] = (int64_t)sl;
    }
    td_release(input);
    return result;
}

/* SUBSTR(str, start, len) — 1-based start */
static td_t* exec_substr(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* start_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (start_v && !TD_IS_ERR(start_v)) td_release(start_v); return input; }
    if (!start_v || TD_IS_ERR(start_v)) { td_release(input); return start_v; }

    /* Get len arg from ext node's literal field */
    td_op_ext_t* ext = find_ext(g, op->id);
    uint32_t len_id = (uint32_t)(uintptr_t)ext->literal;
    td_t* len_v = exec_node(g, &g->nodes[len_id]);
    if (!len_v || TD_IS_ERR(len_v)) { td_release(input); td_release(start_v); return len_v; }

    int64_t nrows = input->len;
    td_t* result = td_vec_new(TD_SYM, nrows);
    if (!result || TD_IS_ERR(result)) { td_release(input); td_release(start_v); td_release(len_v); return result; }
    result->len = nrows;
    int64_t* dst = (int64_t*)td_data(result);

    /* start_v and len_v may be atom scalars or vectors */
    int64_t s_scalar = 0, l_scalar = 0;
    const int64_t* s_data = NULL;
    const int64_t* l_data = NULL;
    if (start_v->type == TD_ATOM_I64) s_scalar = start_v->i64;
    else if (start_v->type == TD_ATOM_F64) s_scalar = (int64_t)start_v->f64;
    else if (start_v->len == 1) s_scalar = ((int64_t*)td_data(start_v))[0];
    else s_data = (const int64_t*)td_data(start_v);
    if (len_v->type == TD_ATOM_I64) l_scalar = len_v->i64;
    else if (len_v->type == TD_ATOM_F64) l_scalar = (int64_t)len_v->f64;
    else if (len_v->len == 1) l_scalar = ((int64_t*)td_data(len_v))[0];
    else l_data = (const int64_t*)td_data(len_v);

    for (int64_t i = 0; i < nrows; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        int64_t st = (s_data ? s_data[i] : s_scalar) - 1; /* 1-based → 0-based */
        int64_t ln = l_data ? l_data[i] : l_scalar;
        if (st < 0) st = 0;
        if ((size_t)st >= sl) { dst[i] = td_sym_intern("", 0); continue; }
        if (ln < 0 || ln > (int64_t)(sl - (size_t)st)) ln = (int64_t)sl - st;
        dst[i] = td_sym_intern(sp + st, (size_t)ln);
    }
    td_release(input); td_release(start_v); td_release(len_v);
    return result;
}

/* REPLACE(str, from, to) */
static td_t* exec_replace(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    td_t* from_v = exec_node(g, op->inputs[1]);
    if (!input || TD_IS_ERR(input)) { if (from_v && !TD_IS_ERR(from_v)) td_release(from_v); return input; }
    if (!from_v || TD_IS_ERR(from_v)) { td_release(input); return from_v; }

    td_op_ext_t* ext = find_ext(g, op->id);
    uint32_t to_id = (uint32_t)(uintptr_t)ext->literal;
    td_t* to_v = exec_node(g, &g->nodes[to_id]);
    if (!to_v || TD_IS_ERR(to_v)) { td_release(input); td_release(from_v); return to_v; }

    /* from_v and to_v should be string constants (SYM atoms) */
    const char* from_str = td_str_ptr(from_v);
    size_t from_len = td_str_len(from_v);
    const char* to_str = td_str_ptr(to_v);
    size_t to_len = td_str_len(to_v);

    int64_t nrows = input->len;
    td_t* result = td_vec_new(TD_SYM, nrows);
    if (!result || TD_IS_ERR(result)) { td_release(input); td_release(from_v); td_release(to_v); return result; }
    result->len = nrows;
    int64_t* dst = (int64_t*)td_data(result);

    for (int64_t i = 0; i < nrows; i++) {
        const char* sp; size_t sl;
        sym_elem(input, i, &sp, &sl);
        /* Simple find-and-replace-all */
        char buf[4096];
        size_t bi = 0;
        for (size_t j = 0; j < sl; ) {
            if (from_len > 0 && j + from_len <= sl && memcmp(sp + j, from_str, from_len) == 0) {
                if (bi + to_len < sizeof(buf)) { memcpy(buf + bi, to_str, to_len); bi += to_len; }
                j += from_len;
            } else {
                if (bi < sizeof(buf) - 1) buf[bi++] = sp[j];
                j++;
            }
        }
        buf[bi] = '\0';
        dst[i] = td_sym_intern(buf, bi);
    }
    td_release(input); td_release(from_v); td_release(to_v);
    return result;
}

/* CONCAT(a, b, ...) */
static td_t* exec_concat(td_graph_t* g, td_op_t* op) {
    td_op_ext_t* ext = find_ext(g, op->id);
    int n_args = (int)ext->sym;

    /* Evaluate all inputs */
    td_t* args[16];
    int nn = n_args < 16 ? n_args : 16;
    args[0] = exec_node(g, op->inputs[0]);
    args[1] = exec_node(g, op->inputs[1]);
    uint32_t* trail = (uint32_t*)((char*)(ext + 1));
    for (int i = 2; i < nn; i++) {
        args[i] = exec_node(g, &g->nodes[trail[i - 2]]);
    }
    /* Error check */
    for (int i = 0; i < nn; i++) {
        if (!args[i] || TD_IS_ERR(args[i])) {
            td_t* err = args[i];
            for (int j = 0; j < nn; j++) {
                if (j != i && args[j] && !TD_IS_ERR(args[j])) td_release(args[j]);
            }
            return err;
        }
    }

    int64_t nrows = args[0]->len;
    td_t* result = td_vec_new(TD_SYM, nrows);
    if (!result || TD_IS_ERR(result)) {
        for (int i = 0; i < nn; i++) td_release(args[i]);
        return result;
    }
    result->len = nrows;
    int64_t* dst = (int64_t*)td_data(result);

    for (int64_t r = 0; r < nrows; r++) {
        char buf[4096];
        size_t bi = 0;
        for (int a = 0; a < nn; a++) {
            int8_t t = args[a]->type;
            if (t == TD_SYM || t == TD_ENUM) {
                const char* sp; size_t sl;
                sym_elem(args[a], r, &sp, &sl);
                if (bi + sl < sizeof(buf)) { memcpy(buf + bi, sp, sl); bi += sl; }
            } else if (t == TD_STR || t == TD_ATOM_STR) {
                /* String constant (atom or vector) */
                const char* sp = td_str_ptr(args[a]);
                size_t sl = td_str_len(args[a]);
                if (sp && bi + sl < sizeof(buf)) { memcpy(buf + bi, sp, sl); bi += sl; }
            }
        }
        buf[bi] = '\0';
        dst[r] = td_sym_intern(buf, bi);
    }
    for (int i = 0; i < nn; i++) td_release(args[i]);
    return result;
}

/* ============================================================================
 * EXTRACT — date/time component extraction from timestamps
 *
 * Input:  i64 vector of microseconds since 2000-01-01T00:00:00Z
 * Output: i64 vector of extracted field values
 *
 * Uses Howard Hinnant's civil_from_days algorithm (public domain) for
 * Gregorian calendar decomposition.
 * ============================================================================ */

static td_t* exec_extract(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) { td_release(input); return TD_ERR_PTR(TD_ERR_NYI); }

    int64_t field = ext->sym;
    int64_t len = input->len;

    td_t* result = td_vec_new(TD_I64, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;

    int64_t* out = (int64_t*)td_data(result);

    #define USEC_PER_SEC  1000000LL
    #define USEC_PER_MIN  (60LL  * USEC_PER_SEC)
    #define USEC_PER_HOUR (3600LL * USEC_PER_SEC)
    #define USEC_PER_DAY  (86400LL * USEC_PER_SEC)

    td_morsel_t m;
    td_morsel_init(&m, input);
    int64_t off = 0;

    while (td_morsel_next(&m)) {
        int64_t n = m.morsel_len;
        const int64_t* src = (const int64_t*)m.morsel_ptr;

        for (int64_t i = 0; i < n; i++) {
            int64_t us = src[i];  /* microseconds since 2000-01-01 */

            if (field == TD_EXTRACT_EPOCH) {
                out[off + i] = us;
            } else if (field == TD_EXTRACT_HOUR) {
                int64_t day_us = us % USEC_PER_DAY;
                if (day_us < 0) day_us += USEC_PER_DAY;
                out[off + i] = day_us / USEC_PER_HOUR;
            } else if (field == TD_EXTRACT_MINUTE) {
                int64_t day_us = us % USEC_PER_DAY;
                if (day_us < 0) day_us += USEC_PER_DAY;
                out[off + i] = (day_us % USEC_PER_HOUR) / USEC_PER_MIN;
            } else if (field == TD_EXTRACT_SECOND) {
                int64_t day_us = us % USEC_PER_DAY;
                if (day_us < 0) day_us += USEC_PER_DAY;
                out[off + i] = (day_us % USEC_PER_MIN) / USEC_PER_SEC;
            } else {
                /* Calendar fields: YEAR, MONTH, DAY, DOW, DOY */
                /* Floor-divide microseconds to get day count */
                int64_t days_since_2000 = us / USEC_PER_DAY;
                if (us < 0 && us % USEC_PER_DAY != 0) days_since_2000--;

                /* Hinnant civil_from_days: shift to 0000-03-01 era-based epoch */
                int64_t z = days_since_2000 + 10957 + 719468;
                int64_t era = (z >= 0 ? z : z - 146096) / 146097;
                uint64_t doe = (uint64_t)(z - era * 146097);
                uint64_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                int64_t y = (int64_t)yoe + era * 400;
                uint64_t doy_mar = doe - (365*yoe + yoe/4 - yoe/100);
                uint64_t mp = (5*doy_mar + 2) / 153;
                uint64_t d = doy_mar - (153*mp + 2) / 5 + 1;
                uint64_t mo = mp < 10 ? mp + 3 : mp - 9;
                y += (mo <= 2);

                if (field == TD_EXTRACT_YEAR) {
                    out[off + i] = y;
                } else if (field == TD_EXTRACT_MONTH) {
                    out[off + i] = (int64_t)mo;
                } else if (field == TD_EXTRACT_DAY) {
                    out[off + i] = (int64_t)d;
                } else if (field == TD_EXTRACT_DOW) {
                    /* ISO day of week: Mon=1 .. Sun=7
                     * 2000-01-01 was Saturday (ISO 6).
                     * Formula: ((days%7)+7+5)%7 + 1 */
                    out[off + i] = ((days_since_2000 % 7) + 7 + 5) % 7 + 1;
                } else if (field == TD_EXTRACT_DOY) {
                    /* Day of year [1..366], January-based */
                    static const int dbm[13] = {
                        0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
                    };
                    int leap = (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0));
                    int64_t doy_jan = dbm[mo] + (int64_t)d;
                    if (mo > 2 && leap) doy_jan++;
                    out[off + i] = doy_jan;
                } else {
                    out[off + i] = 0;
                }
            }
        }
        off += n;
    }

    #undef USEC_PER_SEC
    #undef USEC_PER_MIN
    #undef USEC_PER_HOUR
    #undef USEC_PER_DAY

    td_release(input);
    return result;
}

/* ============================================================================
 * DATE_TRUNC — truncate timestamp to specified precision
 *
 * Returns microseconds since 2000-01-01 truncated to the given field.
 * Sub-day: modular arithmetic. Month/year: calendar decompose + recompose.
 * ============================================================================ */

/* Convert (year, month, day) to days since 2000-01-01 using the inverse of
 * Hinnant's civil_from_days. */
static int64_t days_from_civil(int64_t y, int64_t m, int64_t d) {
    y -= (m <= 2);
    int64_t era = (y >= 0 ? y : y - 399) / 400;
    uint64_t yoe = (uint64_t)(y - era * 400);
    uint64_t doy = (153 * (m > 2 ? (uint64_t)m - 3 : (uint64_t)m + 9) + 2) / 5 + (uint64_t)d - 1;
    uint64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int64_t)doe - 719468 - 10957;
}

static td_t* exec_date_trunc(td_graph_t* g, td_op_t* op) {
    td_t* input = exec_node(g, op->inputs[0]);
    if (!input || TD_IS_ERR(input)) return input;

    td_op_ext_t* ext = find_ext(g, op->id);
    if (!ext) { td_release(input); return TD_ERR_PTR(TD_ERR_NYI); }

    int64_t field = ext->sym;
    int64_t len = input->len;

    td_t* result = td_vec_new(TD_I64, len);
    if (!result || TD_IS_ERR(result)) { td_release(input); return result; }
    result->len = len;

    int64_t* out = (int64_t*)td_data(result);

    #define DT_USEC_PER_SEC  1000000LL
    #define DT_USEC_PER_MIN  (60LL  * DT_USEC_PER_SEC)
    #define DT_USEC_PER_HOUR (3600LL * DT_USEC_PER_SEC)
    #define DT_USEC_PER_DAY  (86400LL * DT_USEC_PER_SEC)

    td_morsel_t m;
    td_morsel_init(&m, input);
    int64_t off = 0;

    while (td_morsel_next(&m)) {
        int64_t n = m.morsel_len;
        const int64_t* src = (const int64_t*)m.morsel_ptr;

        for (int64_t i = 0; i < n; i++) {
            int64_t us = src[i];

            switch (field) {
                case TD_EXTRACT_SECOND: {
                    /* Truncate to second boundary */
                    int64_t r = us % DT_USEC_PER_SEC;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_SEC : 0);
                    break;
                }
                case TD_EXTRACT_MINUTE: {
                    int64_t r = us % DT_USEC_PER_MIN;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_MIN : 0);
                    break;
                }
                case TD_EXTRACT_HOUR: {
                    int64_t r = us % DT_USEC_PER_HOUR;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_HOUR : 0);
                    break;
                }
                case TD_EXTRACT_DAY: {
                    int64_t r = us % DT_USEC_PER_DAY;
                    out[off + i] = us - r - (r < 0 ? DT_USEC_PER_DAY : 0);
                    break;
                }
                case TD_EXTRACT_MONTH: {
                    /* Decompose to y/m/d, set d=1, recompose */
                    int64_t days2k = us / DT_USEC_PER_DAY;
                    if (us < 0 && us % DT_USEC_PER_DAY != 0) days2k--;
                    int64_t z = days2k + 10957 + 719468;
                    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
                    uint64_t doe = (uint64_t)(z - era * 146097);
                    uint64_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                    int64_t y = (int64_t)yoe + era * 400;
                    uint64_t doy_mar = doe - (365*yoe + yoe/4 - yoe/100);
                    uint64_t mp = (5*doy_mar + 2) / 153;
                    uint64_t mo = mp < 10 ? mp + 3 : mp - 9;
                    y += (mo <= 2);
                    out[off + i] = days_from_civil(y, (int64_t)mo, 1) * DT_USEC_PER_DAY;
                    break;
                }
                case TD_EXTRACT_YEAR: {
                    /* Decompose to y/m/d, set m=1 d=1, recompose */
                    int64_t days2k = us / DT_USEC_PER_DAY;
                    if (us < 0 && us % DT_USEC_PER_DAY != 0) days2k--;
                    int64_t z = days2k + 10957 + 719468;
                    int64_t era = (z >= 0 ? z : z - 146096) / 146097;
                    uint64_t doe = (uint64_t)(z - era * 146097);
                    uint64_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                    int64_t y = (int64_t)yoe + era * 400;
                    uint64_t doy_mar = doe - (365*yoe + yoe/4 - yoe/100);
                    uint64_t mp = (5*doy_mar + 2) / 153;
                    uint64_t mo = mp < 10 ? mp + 3 : mp - 9;
                    y += (mo <= 2);
                    out[off + i] = days_from_civil(y, 1, 1) * DT_USEC_PER_DAY;
                    break;
                }
                default:
                    out[off + i] = us;
                    break;
            }
        }
        off += n;
    }

    #undef DT_USEC_PER_SEC
    #undef DT_USEC_PER_MIN
    #undef DT_USEC_PER_HOUR
    #undef DT_USEC_PER_DAY

    td_release(input);
    return result;
}

/* ============================================================================
 * Recursive executor
 * ============================================================================ */

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
                    /* Materialized copy (td_vec_slice creates zero-copy views
                       that the morsel iterator can't handle) */
                    uint8_t esz = td_elem_size(col->type);
                    td_t* head_vec = td_vec_new(col->type, n);
                    if (head_vec && !TD_IS_ERR(head_vec)) {
                        head_vec->len = n;
                        memcpy(td_data(head_vec), td_data(col), (size_t)n * esz);
                    }
                    result = td_table_add_col(result, name_id, head_vec);
                    td_release(head_vec);
                }
                td_release(input);
                return result;
            }
            if (n > input->len) n = input->len;
            /* Materialized copy for vector head */
            uint8_t esz = td_elem_size(input->type);
            td_t* result = td_vec_new(input->type, n);
            if (result && !TD_IS_ERR(result)) {
                result->len = n;
                memcpy(td_data(result), td_data(input), (size_t)n * esz);
            }
            td_release(input);
            return result;
        }

        case OP_TAIL: {
            td_op_ext_t* ext = find_ext(g, op->id);
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            int64_t n = ext ? ext->sym : 10;
            if (input->type == TD_TABLE) {
                int64_t ncols = td_table_ncols(input);
                int64_t nrows = td_table_nrows(input);
                if (n > nrows) n = nrows;
                int64_t skip = nrows - n;
                td_t* result = td_table_new(ncols);
                for (int64_t c = 0; c < ncols; c++) {
                    td_t* col = td_table_get_col_idx(input, c);
                    int64_t name_id = td_table_col_name(input, c);
                    uint8_t esz = td_elem_size(col->type);
                    td_t* tail_vec = td_vec_new(col->type, n);
                    if (tail_vec && !TD_IS_ERR(tail_vec)) {
                        tail_vec->len = n;
                        memcpy(td_data(tail_vec),
                               (char*)td_data(col) + (size_t)skip * esz,
                               (size_t)n * esz);
                    }
                    result = td_table_add_col(result, name_id, tail_vec);
                    td_release(tail_vec);
                }
                td_release(input);
                return result;
            }
            if (n > input->len) n = input->len;
            int64_t skip = input->len - n;
            uint8_t esz = td_elem_size(input->type);
            td_t* result = td_vec_new(input->type, n);
            if (result && !TD_IS_ERR(result)) {
                result->len = n;
                memcpy(td_data(result),
                       (char*)td_data(input) + (size_t)skip * esz,
                       (size_t)n * esz);
            }
            td_release(input);
            return result;
        }

        case OP_IF: {
            return exec_if(g, op);
        }

        case OP_LIKE: {
            return exec_like(g, op);
        }

        case OP_UPPER: case OP_LOWER: case OP_TRIM: {
            return exec_string_unary(g, op);
        }
        case OP_STRLEN: {
            return exec_strlen(g, op);
        }
        case OP_SUBSTR: {
            return exec_substr(g, op);
        }
        case OP_REPLACE: {
            return exec_replace(g, op);
        }
        case OP_CONCAT: {
            return exec_concat(g, op);
        }

        case OP_EXTRACT: {
            return exec_extract(g, op);
        }

        case OP_DATE_TRUNC: {
            return exec_date_trunc(g, op);
        }

        case OP_ALIAS: {
            return exec_node(g, op->inputs[0]);
        }

        case OP_MATERIALIZE: {
            return exec_node(g, op->inputs[0]);
        }

        case OP_SELECT: {
            /* Column projection: select/compute columns from input table */
            td_t* input = exec_node(g, op->inputs[0]);
            if (!input || TD_IS_ERR(input)) return input;
            if (input->type != TD_TABLE) {
                td_release(input);
                return TD_ERR_PTR(TD_ERR_NYI);
            }
            td_op_ext_t* ext = find_ext(g, op->id);
            if (!ext) { td_release(input); return TD_ERR_PTR(TD_ERR_NYI); }
            uint8_t n_cols = ext->sort.n_cols;
            td_op_t** columns = ext->sort.columns;
            td_t* result = td_table_new(n_cols);

            /* Set g->df so SCAN nodes inside expressions resolve correctly */
            td_t* saved_df = g->df;
            g->df = input;

            for (uint8_t c = 0; c < n_cols; c++) {
                if (columns[c]->opcode == OP_SCAN) {
                    /* Direct column reference — copy from input table */
                    td_op_ext_t* col_ext = find_ext(g, columns[c]->id);
                    if (!col_ext) continue;
                    int64_t name_id = col_ext->sym;
                    td_t* src_col = td_table_get_col(input, name_id);
                    if (src_col) {
                        td_retain(src_col);
                        result = td_table_add_col(result, name_id, src_col);
                        td_release(src_col);
                    }
                } else {
                    /* Expression column — evaluate against input table */
                    td_t* vec = exec_node(g, columns[c]);
                    if (vec && !TD_IS_ERR(vec)) {
                        /* Synthetic name: _expr_0, _expr_1, ... */
                        char name_buf[16];
                        int n = 0;
                        name_buf[n++] = '_'; name_buf[n++] = 'e';
                        name_buf[n++] = '0' + c;
                        int64_t name_id = td_sym_intern(name_buf, (size_t)n);
                        result = td_table_add_col(result, name_id, vec);
                        td_release(vec);
                    }
                }
            }

            g->df = saved_df;
            td_release(input);
            return result;
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
