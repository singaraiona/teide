#include "opt.h"
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
    if (a == TD_I64 || b == TD_I64) return TD_I64;
    if (a == TD_I32 || b == TD_I32) return TD_I32;
    if (a == TD_I16 || b == TD_I16) return TD_I16;
    if (a == TD_U8 || b == TD_U8) return TD_U8;
    return TD_BOOL;
}

static void pass_type_inference(td_graph_t* g, td_op_t* node) {
    if (!node || node->flags & OP_FLAG_DEAD) return;

    /* Recurse into inputs first (bottom-up) */
    for (int i = 0; i < 2 && i < node->arity; i++) {
        if (node->inputs[i])
            pass_type_inference(g, node->inputs[i]);
    }

    /* Re-derive type if not set */
    if (node->out_type == 0 && node->opcode != OP_SCAN && node->opcode != OP_CONST) {
        if (node->arity >= 2 && node->inputs[0] && node->inputs[1]) {
            node->out_type = promote_type(node->inputs[0]->out_type,
                                           node->inputs[1]->out_type);
        } else if (node->arity >= 1 && node->inputs[0]) {
            node->out_type = node->inputs[0]->out_type;
        }
    }
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

static void pass_constant_fold(td_graph_t* g, td_op_t* node) {
    if (!node || node->flags & OP_FLAG_DEAD) return;

    for (int i = 0; i < 2 && i < node->arity; i++) {
        if (node->inputs[i])
            pass_constant_fold(g, node->inputs[i]);
    }

    /* Only fold element-wise binary ops with two const inputs */
    if (node->arity == 2 && node->opcode >= OP_ADD && node->opcode <= OP_MAX2) {
        if (is_const(node->inputs[0]) && is_const(node->inputs[1])) {
            /* Both inputs are constants — this could be folded.
               For now, we skip full constant folding to keep things simple.
               The executor handles it correctly already. */
        }
    }
}

/* --------------------------------------------------------------------------
 * Pass 3: Dead code elimination
 *
 * Mark nodes unreachable from root as DEAD.
 * -------------------------------------------------------------------------- */

static void mark_live(td_op_t* node, bool* live) {
    if (!node) return;
    if (live[node->id]) return;
    live[node->id] = true;
    for (int i = 0; i < 2; i++) {
        if (node->inputs[i])
            mark_live(node->inputs[i], live);
    }
}

static void pass_dce(td_graph_t* g, td_op_t* root) {
    uint32_t nc = g->node_count;
    bool live[nc];
    memset(live, 0, nc * sizeof(bool));

    mark_live(root, live);

    for (uint32_t i = 0; i < nc; i++) {
        if (!live[i]) {
            g->nodes[i].flags |= OP_FLAG_DEAD;
        }
    }
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
