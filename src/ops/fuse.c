#include "fuse.h"
#include <string.h>
#include <stdlib.h>

/* --------------------------------------------------------------------------
 * Fusion pass: merge element-wise chains into single fused nodes
 *
 * Detection: find maximal chains of element-wise ops where each intermediate
 * has exactly one consumer. Mark chains with OP_FLAG_FUSED.
 *
 * For now this is a lightweight implementation that marks fuseable chains
 * but relies on the executor's existing per-op evaluation. A full bytecode
 * interpreter over register slots would be added in a production version.
 * -------------------------------------------------------------------------- */

static bool is_elementwise(uint16_t opcode) {
    return (opcode >= OP_NEG && opcode <= OP_CAST) ||
           (opcode >= OP_ADD && opcode <= OP_MAX2);
}

/* Count references to each node */
static void count_refs(td_graph_t* g, td_op_t* node, uint32_t* ref_counts) {
    if (!node) return;
    ref_counts[node->id]++;
    if (ref_counts[node->id] > 1) return;  /* already counted children */
    for (int i = 0; i < node->arity && i < 2; i++) {
        if (node->inputs[i])
            count_refs(g, node->inputs[i], ref_counts);
    }
}

void td_fuse_pass(td_graph_t* g, td_op_t* root) {
    if (!g || !root || g->node_count == 0) return;

    uint32_t* ref_counts = (uint32_t*)calloc(g->node_count, sizeof(uint32_t));
    if (!ref_counts) return;

    count_refs(g, root, ref_counts);

    /* Mark fuseable chains: element-wise nodes whose inputs have exactly
       one consumer (this node) and are also element-wise */
    for (uint32_t i = 0; i < g->node_count; i++) {
        td_op_t* n = &g->nodes[i];
        if (!is_elementwise(n->opcode)) continue;
        if (n->flags & OP_FLAG_DEAD) continue;

        /* Check if all inputs are single-consumer element-wise */
        bool can_fuse = false;
        for (int j = 0; j < n->arity && j < 2; j++) {
            td_op_t* inp = n->inputs[j];
            if (inp && is_elementwise(inp->opcode) && ref_counts[inp->id] == 1) {
                can_fuse = true;
            }
        }
        if (can_fuse) {
            n->flags |= OP_FLAG_FUSED;
        }
    }

    free(ref_counts);
}
