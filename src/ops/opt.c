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

#include "opt.h"
#include "mem/sys.h"
#include <math.h>
#include <string.h>

/* --------------------------------------------------------------------------
 * Optimizer passes (v1): Type Inference + Constant Folding + Fusion + DCE
 *
 * Per the spec's staged rollout:
 *   v1: Type Inference + Constant Folding + Fusion + DCE
 *   v2: Predicate/Projection Pushdown + CSE (future)
 *   v3: Op Reordering + Join Optimization (future)
 * -------------------------------------------------------------------------- */

/* --------------------------------------------------------------------------
 * Pass 1: Type inference (bottom-up)
 *
 * Most type inference is done during graph construction (graph.c).
 * This pass validates and propagates any missing types.
 * -------------------------------------------------------------------------- */

static int8_t promote_type(int8_t a, int8_t b) {
    if (a == TD_F64 || b == TD_F64) return TD_F64;
    /* Treat SYM/ENUM/TIMESTAMP/DATE/TIME as integer-class types */
    if (a == TD_I64 || b == TD_I64 || a == TD_SYM || b == TD_SYM ||
        a == TD_TIMESTAMP || b == TD_TIMESTAMP) return TD_I64;
    if (a == TD_I32 || b == TD_I32 || a == TD_ENUM || b == TD_ENUM ||
        a == TD_DATE || b == TD_DATE || a == TD_TIME || b == TD_TIME) return TD_I32;
    if (a == TD_I16 || b == TD_I16) return TD_I16;
    if (a == TD_U8 || b == TD_U8) return TD_U8;
    return TD_BOOL;
}

static void infer_type_for_node(td_op_t* node) {
    if (node->out_type == 0 && node->opcode != OP_SCAN && node->opcode != OP_CONST) {
        if (node->arity >= 2 && node->inputs[0] && node->inputs[1]) {
            node->out_type = promote_type(node->inputs[0]->out_type,
                                           node->inputs[1]->out_type);
        } else if (node->arity >= 1 && node->inputs[0]) {
            node->out_type = node->inputs[0]->out_type;
        }
    }
}

static void pass_type_inference(td_graph_t* g, td_op_t* root) {
    if (!root || root->flags & OP_FLAG_DEAD) return;

    /* Iterative post-order: collect nodes into an order array, then
       process in reverse (children before parents). */
    uint32_t nc = g->node_count;
    uint32_t stack_local[256], order_local[256];
    bool visited_stack[256];
    uint32_t *stack = nc <= 256 ? stack_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    uint32_t *order = nc <= 256 ? order_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    bool* visited;
    if (nc <= 256) {
        visited = visited_stack;
    } else {
        visited = (bool*)td_sys_alloc(nc * sizeof(bool));
    }
    if (!stack || !order || !visited) {
        if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
        return;
    }
    memset(visited, 0, nc * sizeof(bool));

    int sp = 0, oc = 0;
    stack[sp++] = root->id;
    while (sp > 0 && oc < (int)nc) {
        uint32_t nid = stack[--sp];
        td_op_t* n = &g->nodes[nid];
        if (!n || n->flags & OP_FLAG_DEAD) continue;
        if (visited[nid]) continue;
        visited[nid] = true;
        order[oc++] = nid;
        /* NOTE: ext node children (GROUP/SORT/JOIN/WINDOW keys, aggs) have types
           set during graph construction — no inference traversal needed beyond
           inputs[0..1]. */
        for (int i = 0; i < 2 && i < n->arity; i++) {
            if (n->inputs[i] && sp < (int)nc)
                stack[sp++] = n->inputs[i]->id;
        }
    }
    /* Process in reverse order (children before parents) */
    for (int i = oc - 1; i >= 0; i--)
        infer_type_for_node(&g->nodes[order[i]]);

    if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
}

/* --------------------------------------------------------------------------
 * Pass 2: Constant folding
 *
 * If all inputs to an element-wise op are OP_CONST, evaluate immediately
 * and replace the node with a new OP_CONST.
 * -------------------------------------------------------------------------- */

static bool is_const(td_op_t* n) {
    return n && n->opcode == OP_CONST;
}

/* O(n) scan — acceptable for typical graph sizes (tens to hundreds of nodes). */
static td_op_ext_t* find_ext(td_graph_t* g, uint32_t node_id) {
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i] && g->ext_nodes[i]->base.id == node_id)
            return g->ext_nodes[i];
    }
    return NULL;
}

static bool track_ext_node(td_graph_t* g, td_op_ext_t* ext) {
    if (g->ext_count >= g->ext_cap) {
        uint32_t new_cap = g->ext_cap == 0 ? 16 : g->ext_cap * 2;
        td_op_ext_t** new_exts =
            (td_op_ext_t**)td_sys_realloc(g->ext_nodes, new_cap * sizeof(td_op_ext_t*));
        if (!new_exts) return false;
        g->ext_nodes = new_exts;
        g->ext_cap = new_cap;
    }
    g->ext_nodes[g->ext_count++] = ext;
    return true;
}

static td_op_ext_t* ensure_ext_node(td_graph_t* g, uint32_t node_id) {
    td_op_ext_t* ext = find_ext(g, node_id);
    if (ext) return ext;

    ext = (td_op_ext_t*)td_sys_alloc(sizeof(td_op_ext_t));
    if (!ext) return NULL;
    ext->base.id = node_id;
    if (!track_ext_node(g, ext)) {
        td_sys_free(ext);
        return NULL;
    }
    return ext;
}

static bool atom_to_numeric(td_t* v, double* out_f, int64_t* out_i, bool* is_f64) {
    if (!v || !td_is_atom(v)) return false;
    switch (v->type) {
        case TD_ATOM_F64:
            *out_f = v->f64;
            *out_i = (int64_t)v->f64;
            *is_f64 = true;
            return true;
        case TD_ATOM_I64:
        case TD_ATOM_SYM:
        case TD_ATOM_DATE:
        case TD_ATOM_TIME:
        case TD_ATOM_TIMESTAMP:
            *out_i = v->i64;
            *out_f = (double)v->i64;
            *is_f64 = false;
            return true;
        case TD_ATOM_I32:
            *out_i = (int64_t)v->i32;
            *out_f = (double)v->i32;
            *is_f64 = false;
            return true;
        case TD_ATOM_I16:
            *out_i = (int64_t)v->i16;
            *out_f = (double)v->i16;
            *is_f64 = false;
            return true;
        case TD_ATOM_U8:
        case TD_ATOM_BOOL:
            *out_i = (int64_t)v->u8;
            *out_f = (double)v->u8;
            *is_f64 = false;
            return true;
        case TD_ATOM_ENUM:
            *out_i = (int64_t)v->u32;
            *out_f = (double)v->u32;
            *is_f64 = false;
            return true;
        default:
            return false;
    }
}

static bool replace_with_const(td_graph_t* g, td_op_t* node, td_t* literal) {
    td_op_ext_t* ext = ensure_ext_node(g, node->id);
    if (!ext) return false;

    ext->base = *node;
    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.inputs[0] = NULL;
    ext->base.inputs[1] = NULL;
    ext->base.flags &= (uint8_t)~OP_FLAG_FUSED;
    ext->base.out_type = literal->type < 0 ? (int8_t)-literal->type : literal->type;
    ext->literal = literal;

    *node = ext->base;
    g->nodes[node->id] = ext->base;
    return true;
}

static bool fold_binary_const(td_graph_t* g, td_op_t* node) {
    td_op_t* lhs = node->inputs[0];
    td_op_t* rhs = node->inputs[1];
    if (!is_const(lhs) || !is_const(rhs)) return false;

    td_op_ext_t* le = find_ext(g, lhs->id);
    td_op_ext_t* re = find_ext(g, rhs->id);
    if (!le || !re || !le->literal || !re->literal) return false;
    if (!td_is_atom(le->literal) || !td_is_atom(re->literal)) return false;

    double lf = 0.0, rf = 0.0;
    int64_t li = 0, ri = 0;
    bool l_is_f64 = false, r_is_f64 = false;
    if (!atom_to_numeric(le->literal, &lf, &li, &l_is_f64)) return false;
    if (!atom_to_numeric(re->literal, &rf, &ri, &r_is_f64)) return false;

    td_t* folded = NULL;
    switch (node->out_type) {
        case TD_F64: {
            double lv = l_is_f64 ? lf : (double)li;
            double rv = r_is_f64 ? rf : (double)ri;
            double r = 0.0;
            switch (node->opcode) {
                case OP_ADD: r = lv + rv; break;
                case OP_SUB: r = lv - rv; break;
                case OP_MUL: r = lv * rv; break;
                case OP_DIV: r = rv != 0.0 ? lv / rv : 0.0; break;
                case OP_MOD: r = rv != 0.0 ? fmod(lv, rv) : 0.0; break;
                case OP_MIN2: r = lv < rv ? lv : rv; break;
                case OP_MAX2: r = lv > rv ? lv : rv; break;
                default: return false;
            }
            folded = td_f64(r);
            break;
        }
        case TD_I64: {
            int64_t lv = l_is_f64 ? (int64_t)lf : li;
            int64_t rv = r_is_f64 ? (int64_t)rf : ri;
            int64_t r = 0;
            switch (node->opcode) {
                case OP_ADD: r = (int64_t)((uint64_t)lv + (uint64_t)rv); break;
                case OP_SUB: r = (int64_t)((uint64_t)lv - (uint64_t)rv); break;
                case OP_MUL: r = (int64_t)((uint64_t)lv * (uint64_t)rv); break;
                case OP_DIV:
                    if (rv == 0) r = 0;
                    else if (lv == INT64_MIN && rv == -1) r = INT64_MIN;
                    else r = lv / rv;
                    break;
                case OP_MOD:
                    if (rv == 0) r = 0;
                    else if (lv == INT64_MIN && rv == -1) r = 0;
                    else r = lv % rv;
                    break;
                case OP_MIN2: r = lv < rv ? lv : rv; break;
                case OP_MAX2: r = lv > rv ? lv : rv; break;
                default: return false;
            }
            folded = td_i64(r);
            break;
        }
        case TD_BOOL: {
            double lv = l_is_f64 ? lf : (double)li;
            double rv = r_is_f64 ? rf : (double)ri;
            bool r = false;
            switch (node->opcode) {
                case OP_EQ:  r = lv == rv; break;
                case OP_NE:  r = lv != rv; break;
                case OP_LT:  r = lv < rv; break;
                case OP_LE:  r = lv <= rv; break;
                case OP_GT:  r = lv > rv; break;
                case OP_GE:  r = lv >= rv; break;
                case OP_AND: r = (lv != 0.0) && (rv != 0.0); break;
                case OP_OR:  r = (lv != 0.0) || (rv != 0.0); break;
                default: return false;
            }
            folded = td_bool(r);
            break;
        }
        default:
            return false;
    }

    if (!folded || TD_IS_ERR(folded)) return false;
    if (!replace_with_const(g, node, folded)) {
        td_release(folded);
        return false;
    }
    return true;
}

static bool atom_to_bool(td_t* v, bool* out) {
    double vf = 0.0;
    int64_t vi = 0;
    bool is_f64 = false;
    if (!atom_to_numeric(v, &vf, &vi, &is_f64)) return false;
    if (is_f64) {
        *out = (uint8_t)vf != 0;
    } else {
        *out = vi != 0;
    }
    return true;
}

static bool fold_filter_const_predicate(td_graph_t* g, td_op_t* node) {
    if (node->opcode != OP_FILTER || node->arity != 2) return false;
    td_op_t* pred = node->inputs[1];
    if (!is_const(pred)) return false;

    td_op_ext_t* pred_ext = find_ext(g, pred->id);
    if (!pred_ext || !pred_ext->literal || !td_is_atom(pred_ext->literal)) return false;

    bool keep_rows = false;
    if (!atom_to_bool(pred_ext->literal, &keep_rows)) return false;

    if (keep_rows) {
        node->opcode = OP_MATERIALIZE;
        node->arity = 1;
        node->inputs[1] = NULL;
        node->flags &= (uint8_t)~OP_FLAG_FUSED;
        g->nodes[node->id] = *node;
        return true;
    }

    td_op_ext_t* ext = ensure_ext_node(g, node->id);
    if (!ext) return false;
    ext->base = *node;
    ext->base.opcode = OP_HEAD;
    ext->base.arity = 1;
    ext->base.inputs[1] = NULL;
    ext->base.est_rows = 0;
    ext->base.flags &= (uint8_t)~OP_FLAG_FUSED;
    ext->sym = 0;

    *node = ext->base;
    g->nodes[node->id] = ext->base;
    return true;
}

static void fold_node(td_graph_t* g, td_op_t* node) {
    /* Only fold element-wise binary ops with two const inputs */
    if (node->arity == 2 && node->opcode >= OP_ADD && node->opcode <= OP_MAX2) {
        (void)fold_binary_const(g, node);
    }
    /* FILTER with constant predicate can be reduced to pass-through/empty. */
    (void)fold_filter_const_predicate(g, node);
}

static void pass_constant_fold(td_graph_t* g, td_op_t* root) {
    if (!root || root->flags & OP_FLAG_DEAD) return;

    /* Iterative post-order: collect nodes, then process in reverse
       (children before parents). */
    uint32_t nc = g->node_count;
    uint32_t stack_local[256], order_local[256];
    bool visited_stack[256];
    uint32_t *stack = nc <= 256 ? stack_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    uint32_t *order = nc <= 256 ? order_local : (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
    bool* visited;
    if (nc <= 256) {
        visited = visited_stack;
    } else {
        visited = (bool*)td_sys_alloc(nc * sizeof(bool));
    }
    if (!stack || !order || !visited) {
        if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
        return;
    }
    memset(visited, 0, nc * sizeof(bool));

    int sp = 0, oc = 0;
    stack[sp++] = root->id;
    while (sp > 0 && oc < (int)nc) {
        uint32_t nid = stack[--sp];
        td_op_t* n = &g->nodes[nid];
        if (!n || n->flags & OP_FLAG_DEAD) continue;
        if (visited[nid]) continue;
        visited[nid] = true;
        order[oc++] = nid;
        for (int i = 0; i < 2 && i < n->arity; i++) {
            if (n->inputs[i] && sp < (int)nc)
                stack[sp++] = n->inputs[i]->id;
        }
    }
    /* Process in reverse order (children before parents) */
    for (int i = oc - 1; i >= 0; i--)
        fold_node(g, &g->nodes[order[i]]);

    if (nc > 256) { td_sys_free(stack); td_sys_free(order); td_sys_free(visited); }
}

/* --------------------------------------------------------------------------
 * Pass 3: Dead code elimination
 *
 * Mark nodes unreachable from root as DEAD.
 * -------------------------------------------------------------------------- */

static void mark_live(td_graph_t* g, td_op_t* root, bool* live) {
    if (!root) return;

    uint32_t nc = g->node_count;
    /* Worst case: each node can contribute up to ~N children (CONCAT trailing),
       but nc*2 is a safe upper bound for the stack. */
    uint32_t stack_cap = nc * 2;
    uint32_t stack_local[256];
    uint32_t *stack = stack_cap <= 256 ? stack_local : (uint32_t*)td_sys_alloc(stack_cap * sizeof(uint32_t));
    if (!stack) return;
    int sp = 0;
    stack[sp++] = root->id;
    while (sp > 0) {
        uint32_t nid = stack[--sp];
        if (live[nid]) continue;
        live[nid] = true;
        td_op_t* n = &g->nodes[nid];
        for (int i = 0; i < 2; i++) {
            if (n->inputs[i] && sp < (int)stack_cap)
                stack[sp++] = n->inputs[i]->id;
        }
        /* H4: 3-input ops (OP_IF, OP_SUBSTR, OP_REPLACE) store the third
           operand node ID as (uintptr_t)ext->literal. */
        if (n->opcode == OP_IF || n->opcode == OP_SUBSTR || n->opcode == OP_REPLACE) {
            td_op_ext_t* ext = find_ext(g, nid);
            if (ext) {
                uint32_t third_id = (uint32_t)(uintptr_t)ext->literal;
                if (third_id < nc && sp < (int)stack_cap)
                    stack[sp++] = third_id;
            }
        }
        /* H5: OP_CONCAT stores extra arg IDs (beyond inputs[0..1]) as
           uint32_t values in trailing bytes after the ext node.
           ext->sym holds the total arg count. */
        if (n->opcode == OP_CONCAT) {
            td_op_ext_t* ext = find_ext(g, nid);
            if (ext) {
                int n_args = (int)ext->sym;
                uint32_t* trail = (uint32_t*)((char*)(ext + 1));
                for (int i = 2; i < n_args; i++) {
                    uint32_t arg_id = trail[i - 2];
                    if (arg_id < nc && sp < (int)stack_cap)
                        stack[sp++] = arg_id;
                }
            }
        }
    }
    if (stack_cap > 256) td_sys_free(stack);
}

static void pass_dce(td_graph_t* g, td_op_t* root) {
    uint32_t nc = g->node_count;
    bool* live;
    bool live_stack[256];
    if (nc <= 256) {
        live = live_stack;
    } else {
        live = (bool*)td_sys_alloc(nc * sizeof(bool));
        if (!live) return;
    }
    memset(live, 0, nc * sizeof(bool));

    mark_live(g, root, live);

    for (uint32_t i = 0; i < nc; i++) {
        if (!live[i]) {
            g->nodes[i].flags |= OP_FLAG_DEAD;
        }
    }
    if (nc > 256) td_sys_free(live);
}

/* --------------------------------------------------------------------------
 * td_optimize — run all passes in order, return (possibly updated) root
 * -------------------------------------------------------------------------- */

td_op_t* td_optimize(td_graph_t* g, td_op_t* root) {
    if (!g || !root) return root;

    /* Pass 1: Type inference */
    pass_type_inference(g, root);

    /* Pass 2: Constant folding */
    pass_constant_fold(g, root);

    /* Pass 3: Fusion */
    td_fuse_pass(g, root);

    /* Pass 4: DCE */
    pass_dce(g, root);

    /* Return root — may have been replaced during folding.
       Use g->nodes[root_id] pattern for safety. */
    return &g->nodes[root->id];
}
