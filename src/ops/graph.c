#include "graph.h"
#include <string.h>
#include <stdlib.h>

/* --------------------------------------------------------------------------
 * Graph allocation helpers
 * -------------------------------------------------------------------------- */

#define GRAPH_INIT_CAP 64

static td_op_t* graph_alloc_node(td_graph_t* g) {
    if (g->node_count >= g->node_cap) {
        uint32_t new_cap = g->node_cap * 2;
        td_op_t* new_nodes = (td_op_t*)realloc(g->nodes,
                                                 new_cap * sizeof(td_op_t));
        if (!new_nodes) return NULL;
        g->nodes = new_nodes;
        g->node_cap = new_cap;
    }
    td_op_t* n = &g->nodes[g->node_count];
    memset(n, 0, sizeof(td_op_t));
    n->id = g->node_count;
    g->node_count++;
    return n;
}

static td_op_ext_t* graph_alloc_ext_node(td_graph_t* g) {
    /* Extended nodes are 64 bytes; we store them separately via malloc */
    td_op_ext_t* ext = (td_op_ext_t*)calloc(1, sizeof(td_op_ext_t));
    if (!ext) return NULL;

    /* Also add a placeholder in the nodes array for ID tracking */
    if (g->node_count >= g->node_cap) {
        uint32_t new_cap = g->node_cap * 2;
        td_op_t* new_nodes = (td_op_t*)realloc(g->nodes,
                                                 new_cap * sizeof(td_op_t));
        if (!new_nodes) { free(ext); return NULL; }
        g->nodes = new_nodes;
        g->node_cap = new_cap;
    }
    ext->base.id = g->node_count;
    g->nodes[g->node_count] = ext->base;
    g->node_count++;

    /* Track ext node for cleanup */
    if (g->ext_count >= g->ext_cap) {
        uint32_t new_cap = g->ext_cap == 0 ? 16 : g->ext_cap * 2;
        td_op_ext_t** new_exts = (td_op_ext_t**)realloc(g->ext_nodes,
                                                          new_cap * sizeof(td_op_ext_t*));
        if (!new_exts) { free(ext); return NULL; }
        g->ext_nodes = new_exts;
        g->ext_cap = new_cap;
    }
    g->ext_nodes[g->ext_count++] = ext;

    return ext;
}

/* --------------------------------------------------------------------------
 * td_graph_new / td_graph_free
 * -------------------------------------------------------------------------- */

td_graph_t* td_graph_new(td_t* df) {
    td_graph_t* g = (td_graph_t*)calloc(1, sizeof(td_graph_t));
    if (!g) return NULL;

    g->nodes = (td_op_t*)calloc(GRAPH_INIT_CAP, sizeof(td_op_t));
    if (!g->nodes) { free(g); return NULL; }
    g->node_cap = GRAPH_INIT_CAP;
    g->node_count = 0;
    g->df = df;
    if (df) td_retain(df);

    g->ext_nodes = NULL;
    g->ext_count = 0;
    g->ext_cap = 0;

    return g;
}

void td_graph_free(td_graph_t* g) {
    if (!g) return;

    /* Free extended nodes */
    for (uint32_t i = 0; i < g->ext_count; i++) {
        free(g->ext_nodes[i]);
    }
    free(g->ext_nodes);

    free(g->nodes);
    if (g->df) td_release(g->df);
    free(g);
}

/* --------------------------------------------------------------------------
 * Source ops
 * -------------------------------------------------------------------------- */

td_op_t* td_scan(td_graph_t* g, const char* col_name) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_SCAN;
    ext->base.arity = 0;

    /* Intern the column name to get symbol ID */
    int64_t sym_id = td_sym_intern(col_name, strlen(col_name));
    ext->sym = sym_id;

    /* Infer output type from the bound table */
    if (g->df) {
        td_t* col = td_df_get_col(g->df, sym_id);
        if (col) {
            ext->base.out_type = col->type;
            ext->base.est_rows = (uint32_t)col->len;
        }
    }

    /* Update the nodes array with the filled base */
    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_f64(td_graph_t* g, double val) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_F64;
    ext->literal = td_f64(val);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_i64(td_graph_t* g, int64_t val) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_I64;
    ext->literal = td_i64(val);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_bool(td_graph_t* g, bool val) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_BOOL;
    ext->literal = td_bool(val);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_str(td_graph_t* g, const char* s) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_STR;
    ext->literal = td_str(s, strlen(s));

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_vec(td_graph_t* g, td_t* vec) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = vec->type;
    ext->base.est_rows = (uint32_t)vec->len;
    ext->literal = vec;
    td_retain(vec);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_const_df(td_graph_t* g, td_t* df) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONST;
    ext->base.arity = 0;
    ext->base.out_type = TD_TABLE;
    ext->literal = df;
    td_retain(df);

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

/* --------------------------------------------------------------------------
 * Helper: create unary/binary node
 * -------------------------------------------------------------------------- */

static td_op_t* make_unary(td_graph_t* g, uint16_t opcode, td_op_t* a, int8_t out_type) {
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;

    n->opcode = opcode;
    n->arity = 1;
    n->inputs[0] = a;
    n->out_type = out_type;
    n->est_rows = a->est_rows;
    return n;
}

static td_op_t* make_binary(td_graph_t* g, uint16_t opcode, td_op_t* a, td_op_t* b, int8_t out_type) {
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;

    n->opcode = opcode;
    n->arity = 2;
    n->inputs[0] = a;
    n->inputs[1] = b;
    n->out_type = out_type;
    n->est_rows = a->est_rows > b->est_rows ? a->est_rows : b->est_rows;
    return n;
}

/* Type promotion: BOOL < U8 < I16 < I32 < I64 < F64 */
static int8_t promote(int8_t a, int8_t b) {
    if (a == TD_F64 || b == TD_F64) return TD_F64;
    if (a == TD_I64 || b == TD_I64) return TD_I64;
    if (a == TD_I32 || b == TD_I32) return TD_I32;
    if (a == TD_I16 || b == TD_I16) return TD_I16;
    if (a == TD_U8 || b == TD_U8) return TD_U8;
    return TD_BOOL;
}

/* --------------------------------------------------------------------------
 * Unary element-wise ops
 * -------------------------------------------------------------------------- */

td_op_t* td_neg(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_NEG, a, a->out_type); }
td_op_t* td_abs(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_ABS, a, a->out_type); }
td_op_t* td_not(td_graph_t* g, td_op_t* a)     { return make_unary(g, OP_NOT, a, TD_BOOL); }
td_op_t* td_sqrt_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_SQRT, a, TD_F64); }
td_op_t* td_log_op(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_LOG, a, TD_F64); }
td_op_t* td_exp_op(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_EXP, a, TD_F64); }
td_op_t* td_ceil_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_CEIL, a, a->out_type); }
td_op_t* td_floor_op(td_graph_t* g, td_op_t* a){ return make_unary(g, OP_FLOOR, a, a->out_type); }
td_op_t* td_isnull(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_ISNULL, a, TD_BOOL); }

td_op_t* td_cast(td_graph_t* g, td_op_t* a, int8_t target_type) {
    return make_unary(g, OP_CAST, a, target_type);
}

/* --------------------------------------------------------------------------
 * Binary element-wise ops
 * -------------------------------------------------------------------------- */

td_op_t* td_add(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_ADD, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_sub(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_SUB, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_mul(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_MUL, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_div(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_DIV, a, b, TD_F64); }
td_op_t* td_mod(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_MOD, a, b, promote(a->out_type, b->out_type)); }

td_op_t* td_eq(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_EQ, a, b, TD_BOOL); }
td_op_t* td_ne(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_NE, a, b, TD_BOOL); }
td_op_t* td_lt(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_LT, a, b, TD_BOOL); }
td_op_t* td_le(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_LE, a, b, TD_BOOL); }
td_op_t* td_gt(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_GT, a, b, TD_BOOL); }
td_op_t* td_ge(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_GE, a, b, TD_BOOL); }
td_op_t* td_and(td_graph_t* g, td_op_t* a, td_op_t* b){ return make_binary(g, OP_AND, a, b, TD_BOOL); }
td_op_t* td_or(td_graph_t* g, td_op_t* a, td_op_t* b) { return make_binary(g, OP_OR, a, b, TD_BOOL); }
td_op_t* td_min2(td_graph_t* g, td_op_t* a, td_op_t* b){ return make_binary(g, OP_MIN2, a, b, promote(a->out_type, b->out_type)); }
td_op_t* td_max2(td_graph_t* g, td_op_t* a, td_op_t* b){ return make_binary(g, OP_MAX2, a, b, promote(a->out_type, b->out_type)); }

/* --------------------------------------------------------------------------
 * Reduction ops
 * -------------------------------------------------------------------------- */

td_op_t* td_sum(td_graph_t* g, td_op_t* a)    { return make_unary(g, OP_SUM, a, a->out_type == TD_F64 ? TD_F64 : TD_I64); }
td_op_t* td_prod(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_PROD, a, a->out_type == TD_F64 ? TD_F64 : TD_I64); }
td_op_t* td_min_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_MIN, a, a->out_type); }
td_op_t* td_max_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_MAX, a, a->out_type); }
td_op_t* td_count(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_COUNT, a, TD_I64); }
td_op_t* td_avg(td_graph_t* g, td_op_t* a)    { return make_unary(g, OP_AVG, a, TD_F64); }
td_op_t* td_first(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_FIRST, a, a->out_type); }
td_op_t* td_last(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_LAST, a, a->out_type); }

/* --------------------------------------------------------------------------
 * Structural ops
 * -------------------------------------------------------------------------- */

td_op_t* td_filter(td_graph_t* g, td_op_t* input, td_op_t* predicate) {
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;

    n->opcode = OP_FILTER;
    n->arity = 2;
    n->inputs[0] = input;
    n->inputs[1] = predicate;
    n->out_type = input->out_type;
    n->est_rows = input->est_rows / 2;  /* estimate: 50% selectivity */
    return n;
}

td_op_t* td_sort_op(td_graph_t* g, td_op_t* df_node,
                     td_op_t** keys, uint8_t* descs, uint8_t n_cols) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_SORT;
    ext->base.arity = 1;
    ext->base.inputs[0] = df_node;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = df_node->est_rows;

    ext->sort.columns = keys;
    ext->sort.desc = descs;
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_group(td_graph_t* g, td_op_t** keys, uint8_t n_keys,
                   uint16_t* agg_ops, td_op_t** agg_ins, uint8_t n_aggs) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_GROUP;
    ext->base.arity = 0;
    ext->base.out_type = TD_TABLE;
    if (n_keys > 0 && keys[0])
        ext->base.est_rows = keys[0]->est_rows / 10;  /* rough estimate */
    ext->base.inputs[0] = n_keys > 0 ? keys[0] : NULL;

    ext->keys = keys;
    ext->n_keys = n_keys;
    ext->n_aggs = n_aggs;
    ext->agg_ops = agg_ops;
    ext->agg_ins = agg_ins;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_join(td_graph_t* g,
                  td_op_t* left_df, td_op_t** left_keys,
                  td_op_t* right_df, td_op_t** right_keys,
                  uint8_t n_keys, uint8_t join_type) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_JOIN;
    ext->base.arity = 2;
    ext->base.inputs[0] = left_df;
    ext->base.inputs[1] = right_df;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_df->est_rows;

    ext->join.left_keys = left_keys;
    ext->join.right_keys = right_keys;
    ext->join.n_join_keys = n_keys;
    ext->join.join_type = join_type;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_window_join(td_graph_t* g,
                         td_op_t* left_df, td_op_t* right_df,
                         td_op_t* time_key, td_op_t* sym_key,
                         int64_t window_lo, int64_t window_hi,
                         uint16_t* agg_ops, td_op_t** agg_ins,
                         uint8_t n_aggs) {
    (void)time_key; (void)sym_key;
    (void)window_lo; (void)window_hi;
    (void)agg_ops;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_WINDOW_JOIN;
    ext->base.arity = 2;
    ext->base.inputs[0] = left_df;
    ext->base.inputs[1] = right_df;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_df->est_rows;

    /* Store window join params in the join union */
    ext->join.n_join_keys = n_aggs;
    ext->join.join_type = 0;
    ext->join.left_keys = agg_ins;
    ext->join.right_keys = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_project(td_graph_t* g, td_op_t* input,
                     td_op_t** cols, uint8_t n_cols) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_PROJECT;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = input->est_rows;

    ext->sort.columns = cols;
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_select(td_graph_t* g, td_op_t* input,
                    td_op_t** cols, uint8_t n_cols) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_SELECT;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = input->est_rows;

    ext->sort.columns = cols;
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_head(td_graph_t* g, td_op_t* input, int64_t n) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_HEAD;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = input->out_type;
    ext->base.est_rows = (uint32_t)n;
    ext->sym = n;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_tail(td_graph_t* g, td_op_t* input, int64_t n) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_TAIL;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = input->out_type;
    ext->base.est_rows = (uint32_t)n;
    ext->sym = n;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_alias(td_graph_t* g, td_op_t* input, const char* name) {
    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    ext->base.opcode = OP_ALIAS;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = input->out_type;
    ext->base.est_rows = input->est_rows;
    ext->sym = td_sym_intern(name, strlen(name));

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_materialize(td_graph_t* g, td_op_t* input) {
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;

    n->opcode = OP_MATERIALIZE;
    n->arity = 1;
    n->inputs[0] = input;
    n->out_type = input->out_type;
    n->est_rows = input->est_rows;
    return n;
}
