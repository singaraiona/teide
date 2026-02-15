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

#include "fuse.h"
#include "mem/sys.h"
#include <string.h>
#include <teide/td.h>

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

/* O(n) scan — acceptable for typical graph sizes (tens to hundreds of nodes). */
static td_op_ext_t* find_ext(td_graph_t* g, uint32_t node_id) {
    for (uint32_t i = 0; i < g->ext_count; i++) {
        if (g->ext_nodes[i] && g->ext_nodes[i]->base.id == node_id)
            return g->ext_nodes[i];
    }
    return NULL;
}

/* Count references to each node (iterative) */
static void count_refs(td_graph_t* g, td_op_t* root, uint32_t* ref_counts) {
    if (!root) return;

    uint32_t nc = g->node_count;
    uint32_t stack_cap = nc * 2;
    uint32_t stack_local[256];
    uint32_t *stack = stack_cap <= 256 ? stack_local : (uint32_t*)td_sys_alloc(stack_cap * sizeof(uint32_t));
    if (!stack) return;
    int sp = 0;
    stack[sp++] = root->id;
    while (sp > 0) {
        uint32_t nid = stack[--sp];
        td_op_t* n = &g->nodes[nid];
        ref_counts[nid]++;
        if (ref_counts[nid] > 1) continue;  /* already counted children */
        for (int i = 0; i < n->arity && i < 2; i++) {
            if (n->inputs[i] && sp < (int)stack_cap)
                stack[sp++] = n->inputs[i]->id;
        }
        /* M11: 3-input ops (OP_IF, OP_SUBSTR, OP_REPLACE) store the third
           operand node ID as (uintptr_t)ext->literal. */
        if (n->opcode == OP_IF || n->opcode == OP_SUBSTR || n->opcode == OP_REPLACE) {
            td_op_ext_t* ext = find_ext(g, nid);
            if (ext) {
                uint32_t third_id = (uint32_t)(uintptr_t)ext->literal;
                if (third_id < nc && sp < (int)stack_cap)
                    stack[sp++] = third_id;
            }
        }
        /* M11: OP_CONCAT stores extra arg IDs (beyond inputs[0..1]) as
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

void td_fuse_pass(td_graph_t* g, td_op_t* root) {
    if (!g || !root || g->node_count == 0) return;

    uint32_t nc = g->node_count;
    uint32_t* ref_counts;
    uint32_t ref_counts_stack[256];
    if (nc <= 256) {
        ref_counts = ref_counts_stack;
    } else {
        ref_counts = (uint32_t*)td_sys_alloc(nc * sizeof(uint32_t));
        if (!ref_counts) return;
    }
    memset(ref_counts, 0, nc * sizeof(uint32_t));

    count_refs(g, root, ref_counts);

    /* Mark fuseable chains: element-wise nodes whose inputs have exactly
       one consumer (this node) and are also element-wise */
    for (uint32_t i = 0; i < nc; i++) {
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
    if (nc > 256) td_sys_free(ref_counts);
}
