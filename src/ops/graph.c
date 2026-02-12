#include "graph.h"
#include <string.h>
#include <stdlib.h>

/* --------------------------------------------------------------------------
 * Graph allocation helpers
 * -------------------------------------------------------------------------- */

#define GRAPH_INIT_CAP 4096

/* After realloc moves g->nodes, fix up all stored input pointers */
static void graph_fixup_ptrs(td_graph_t* g, td_op_t* old_nodes) {
    ptrdiff_t delta = (char*)g->nodes - (char*)old_nodes;
    if (delta == 0) return;
    for (uint32_t i = 0; i < g->node_count; i++) {
        if (g->nodes[i].inputs[0])
            g->nodes[i].inputs[0] = (td_op_t*)((char*)g->nodes[i].inputs[0] + delta);
        if (g->nodes[i].inputs[1])
            g->nodes[i].inputs[1] = (td_op_t*)((char*)g->nodes[i].inputs[1] + delta);
    }
}

static td_op_t* graph_alloc_node(td_graph_t* g) {
    if (g->node_count >= g->node_cap) {
        td_op_t* old_nodes = g->nodes;
        uint32_t new_cap = g->node_cap * 2;
        td_op_t* new_nodes = (td_op_t*)realloc(g->nodes,
                                                 new_cap * sizeof(td_op_t));
        if (!new_nodes) return NULL;
        g->nodes = new_nodes;
        g->node_cap = new_cap;
        graph_fixup_ptrs(g, old_nodes);
    }
    td_op_t* n = &g->nodes[g->node_count];
    memset(n, 0, sizeof(td_op_t));
    n->id = g->node_count;
    g->node_count++;
    return n;
}

static td_op_ext_t* graph_alloc_ext_node_ex(td_graph_t* g, size_t extra) {
    /* Extended nodes are 64 bytes; extra bytes appended for inline arrays */
    td_op_ext_t* ext = (td_op_ext_t*)calloc(1, sizeof(td_op_ext_t) + extra);
    if (!ext) return NULL;

    /* Also add a placeholder in the nodes array for ID tracking */
    if (g->node_count >= g->node_cap) {
        td_op_t* old_nodes = g->nodes;
        uint32_t new_cap = g->node_cap * 2;
        td_op_t* new_nodes = (td_op_t*)realloc(g->nodes,
                                                 new_cap * sizeof(td_op_t));
        if (!new_nodes) { free(ext); return NULL; }
        g->nodes = new_nodes;
        g->node_cap = new_cap;
        graph_fixup_ptrs(g, old_nodes);
    }
    ext->base.id = g->node_count;
    g->nodes[g->node_count] = ext->base;
    g->node_count++;

    /* Track ext node for cleanup */
    if (g->ext_count >= g->ext_cap) {
        uint32_t new_cap = g->ext_cap == 0 ? 16 : g->ext_cap * 2;
        td_op_ext_t** new_exts = (td_op_ext_t**)realloc(g->ext_nodes,
                                                          new_cap * sizeof(td_op_ext_t*));
        if (!new_exts) { g->node_count--; free(ext); return NULL; }
        g->ext_nodes = new_exts;
        g->ext_cap = new_cap;
    }
    g->ext_nodes[g->ext_count++] = ext;

    return ext;
}

static td_op_ext_t* graph_alloc_ext_node(td_graph_t* g) {
    return graph_alloc_ext_node_ex(g, 0);
}

/* Pointer to trailing bytes after the ext node */
#define EXT_TRAIL(ext) ((char*)((ext) + 1))

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
    g->filter_mask = NULL;

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
    if (g->filter_mask) td_release(g->filter_mask);
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
        td_t* col = td_table_get_col(g->df, sym_id);
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
    /* Save ID before alloc — realloc may invalidate the pointer */
    uint32_t a_id = a->id;
    uint32_t est = a->est_rows;
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;
    a = &g->nodes[a_id];  /* re-resolve after potential realloc */

    n->opcode = opcode;
    n->arity = 1;
    n->inputs[0] = a;
    n->out_type = out_type;
    n->est_rows = est;
    return n;
}

static td_op_t* make_binary(td_graph_t* g, uint16_t opcode, td_op_t* a, td_op_t* b, int8_t out_type) {
    /* Save IDs before alloc — realloc may invalidate the pointers */
    uint32_t a_id = a->id;
    uint32_t b_id = b->id;
    uint32_t est = a->est_rows > b->est_rows ? a->est_rows : b->est_rows;
    td_op_t* n = graph_alloc_node(g);
    if (!n) return NULL;
    a = &g->nodes[a_id];  /* re-resolve after potential realloc */
    b = &g->nodes[b_id];

    n->opcode = opcode;
    n->arity = 2;
    n->inputs[0] = a;
    n->inputs[1] = b;
    n->out_type = out_type;
    n->est_rows = est;
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

td_op_t* td_if(td_graph_t* g, td_op_t* cond, td_op_t* then_val, td_op_t* else_val) {
    /* 3-input node: cond, then, else — needs ext node */
    uint32_t cond_id = cond->id;
    uint32_t then_id = then_val->id;
    uint32_t else_id = else_val->id;
    int8_t out_type = promote(then_val->out_type, else_val->out_type);
    /* For string types, propagate SYM/ENUM/STR → SYM */
    if (then_val->out_type == TD_SYM || then_val->out_type == TD_ENUM)
        out_type = then_val->out_type;
    else if (else_val->out_type == TD_SYM || else_val->out_type == TD_ENUM)
        out_type = else_val->out_type;
    else if (then_val->out_type == TD_STR || else_val->out_type == TD_STR)
        out_type = TD_SYM;  /* string constants → intern as SYM */
    uint32_t est = cond->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;

    /* Re-resolve after potential realloc */
    cond = &g->nodes[cond_id];
    then_val = &g->nodes[then_id];
    else_val = &g->nodes[else_id];

    ext->base.opcode = OP_IF;
    ext->base.arity = 2;  /* inputs[0]=cond, inputs[1]=then; else via ext */
    ext->base.inputs[0] = cond;
    ext->base.inputs[1] = then_val;
    ext->base.out_type = out_type;
    ext->base.est_rows = est;
    /* Store else_val pointer via the literal field */
    ext->literal = (td_t*)(uintptr_t)else_id;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_like(td_graph_t* g, td_op_t* input, td_op_t* pattern) {
    return make_binary(g, OP_LIKE, input, pattern, TD_BOOL);
}

/* String ops */
td_op_t* td_upper(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_UPPER, a, TD_SYM); }
td_op_t* td_lower(td_graph_t* g, td_op_t* a)   { return make_unary(g, OP_LOWER, a, TD_SYM); }
td_op_t* td_strlen(td_graph_t* g, td_op_t* a)  { return make_unary(g, OP_STRLEN, a, TD_I64); }
td_op_t* td_trim_op(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_TRIM, a, TD_SYM); }

td_op_t* td_substr(td_graph_t* g, td_op_t* str, td_op_t* start, td_op_t* len) {
    /* 3-input: str=inputs[0], start=inputs[1], len stored via literal field */
    uint32_t s_id = str->id;
    uint32_t st_id = start->id;
    uint32_t l_id = len->id;
    uint32_t est = str->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    str   = &g->nodes[s_id];
    start = &g->nodes[st_id];

    ext->base.opcode = OP_SUBSTR;
    ext->base.arity = 2;
    ext->base.inputs[0] = str;
    ext->base.inputs[1] = start;
    ext->base.out_type = TD_SYM;
    ext->base.est_rows = est;
    ext->literal = (td_t*)(uintptr_t)l_id;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_replace(td_graph_t* g, td_op_t* str, td_op_t* from, td_op_t* to) {
    /* 3-input: str=inputs[0], from=inputs[1], to stored via literal field */
    uint32_t s_id = str->id;
    uint32_t f_id = from->id;
    uint32_t t_id = to->id;
    uint32_t est = str->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    str  = &g->nodes[s_id];
    from = &g->nodes[f_id];

    ext->base.opcode = OP_REPLACE;
    ext->base.arity = 2;
    ext->base.inputs[0] = str;
    ext->base.inputs[1] = from;
    ext->base.out_type = TD_SYM;
    ext->base.est_rows = est;
    ext->literal = (td_t*)(uintptr_t)t_id;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_concat(td_graph_t* g, td_op_t** args, int n) {
    /* Variadic: first 2 in inputs[], rest in trailing IDs */
    if (n < 2) return NULL;
    size_t extra = (n > 2) ? (size_t)(n - 2) * sizeof(uint32_t) : 0;

    /* Save IDs before alloc */
    uint32_t ids[16]; /* reasonable max */
    int nn = n < 16 ? n : 16;
    for (int i = 0; i < nn; i++) ids[i] = args[i]->id;
    uint32_t est = args[0]->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, extra);
    if (!ext) return NULL;

    ext->base.opcode = OP_CONCAT;
    ext->base.arity = 2;
    ext->base.inputs[0] = &g->nodes[ids[0]];
    ext->base.inputs[1] = &g->nodes[ids[1]];
    ext->base.out_type = TD_SYM;
    ext->base.est_rows = est;
    ext->sym = n; /* total arg count stored in sym field */

    /* Extra args in trailing bytes */
    uint32_t* trail = (uint32_t*)EXT_TRAIL(ext);
    for (int i = 2; i < nn; i++) trail[i - 2] = ids[i];

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

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
td_op_t* td_count_distinct(td_graph_t* g, td_op_t* a) { return make_unary(g, OP_COUNT_DISTINCT, a, TD_I64); }

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
                     td_op_t** keys, uint8_t* descs, uint8_t* nulls_first,
                     uint8_t n_cols) {
    size_t keys_sz = (size_t)n_cols * sizeof(td_op_t*);
    size_t descs_sz = (size_t)n_cols;
    size_t nf_sz = (size_t)n_cols;
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, keys_sz + descs_sz + nf_sz);
    if (!ext) return NULL;

    ext->base.opcode = OP_SORT;
    ext->base.arity = 1;
    ext->base.inputs[0] = df_node;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = df_node->est_rows;

    /* Arrays embedded in trailing space — freed with ext node */
    char* trail = EXT_TRAIL(ext);
    ext->sort.columns = (td_op_t**)trail;
    memcpy(ext->sort.columns, keys, keys_sz);
    ext->sort.desc = (uint8_t*)(trail + keys_sz);
    memcpy(ext->sort.desc, descs, descs_sz);
    ext->sort.nulls_first = (uint8_t*)(trail + keys_sz + descs_sz);
    if (nulls_first) {
        memcpy(ext->sort.nulls_first, nulls_first, nf_sz);
    } else {
        /* Default: NULLS LAST for ASC, NULLS FIRST for DESC (PostgreSQL convention) */
        for (uint8_t i = 0; i < n_cols; i++)
            ext->sort.nulls_first[i] = descs[i] ? 1 : 0;
    }
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_group(td_graph_t* g, td_op_t** keys, uint8_t n_keys,
                   uint16_t* agg_ops, td_op_t** agg_ins, uint8_t n_aggs) {
    size_t keys_sz = (size_t)n_keys * sizeof(td_op_t*);
    size_t ops_sz  = (size_t)n_aggs * sizeof(uint16_t);
    size_t ins_sz  = (size_t)n_aggs * sizeof(td_op_t*);
    /* Align ops after keys (pointer-sized), ins after ops (needs ptr alignment) */
    size_t ops_off = keys_sz;
    size_t ins_off = ops_off + ops_sz;
    /* Round ins_off up to pointer alignment */
    ins_off = (ins_off + sizeof(td_op_t*) - 1) & ~(sizeof(td_op_t*) - 1);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, ins_off + ins_sz);
    if (!ext) return NULL;

    ext->base.opcode = OP_GROUP;
    ext->base.arity = 0;
    ext->base.out_type = TD_TABLE;
    if (n_keys > 0 && keys[0])
        ext->base.est_rows = keys[0]->est_rows / 10;  /* rough estimate */
    ext->base.inputs[0] = n_keys > 0 ? keys[0] : NULL;

    /* Arrays embedded in trailing space — freed with ext node */
    char* trail = EXT_TRAIL(ext);
    ext->keys = (td_op_t**)trail;
    memcpy(ext->keys, keys, keys_sz);
    ext->agg_ops = (uint16_t*)(trail + ops_off);
    memcpy(ext->agg_ops, agg_ops, ops_sz);
    ext->agg_ins = (td_op_t**)(trail + ins_off);
    memcpy(ext->agg_ins, agg_ins, ins_sz);
    ext->n_keys = n_keys;
    ext->n_aggs = n_aggs;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_join(td_graph_t* g,
                  td_op_t* left_df, td_op_t** left_keys,
                  td_op_t* right_df, td_op_t** right_keys,
                  uint8_t n_keys, uint8_t join_type) {
    size_t keys_sz = (size_t)n_keys * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, keys_sz * 2);
    if (!ext) return NULL;

    ext->base.opcode = OP_JOIN;
    ext->base.arity = 2;
    ext->base.inputs[0] = left_df;
    ext->base.inputs[1] = right_df;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_df->est_rows;

    /* Arrays embedded in trailing space — freed with ext node */
    char* trail = EXT_TRAIL(ext);
    ext->join.left_keys = (td_op_t**)trail;
    memcpy(ext->join.left_keys, left_keys, (size_t)n_keys * sizeof(td_op_t*));
    ext->join.right_keys = (td_op_t**)(trail + (size_t)n_keys * sizeof(td_op_t*));
    memcpy(ext->join.right_keys, right_keys, (size_t)n_keys * sizeof(td_op_t*));
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

    size_t ins_sz = (size_t)n_aggs * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, ins_sz);
    if (!ext) return NULL;

    ext->base.opcode = OP_WINDOW_JOIN;
    ext->base.arity = 2;
    ext->base.inputs[0] = left_df;
    ext->base.inputs[1] = right_df;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = left_df->est_rows;

    /* Array embedded in trailing space — freed with ext node */
    ext->join.n_join_keys = n_aggs;
    ext->join.join_type = 0;
    ext->join.left_keys = (td_op_t**)EXT_TRAIL(ext);
    memcpy(ext->join.left_keys, agg_ins, ins_sz);
    ext->join.right_keys = NULL;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_project(td_graph_t* g, td_op_t* input,
                     td_op_t** cols, uint8_t n_cols) {
    size_t cols_sz = (size_t)n_cols * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, cols_sz);
    if (!ext) return NULL;

    ext->base.opcode = OP_PROJECT;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = input->est_rows;

    /* Array embedded in trailing space — freed with ext node */
    ext->sort.columns = (td_op_t**)EXT_TRAIL(ext);
    memcpy(ext->sort.columns, cols, cols_sz);
    ext->sort.n_cols = n_cols;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_select(td_graph_t* g, td_op_t* input,
                    td_op_t** cols, uint8_t n_cols) {
    size_t cols_sz = (size_t)n_cols * sizeof(td_op_t*);
    td_op_ext_t* ext = graph_alloc_ext_node_ex(g, cols_sz);
    if (!ext) return NULL;

    ext->base.opcode = OP_SELECT;
    ext->base.arity = 1;
    ext->base.inputs[0] = input;
    ext->base.out_type = TD_TABLE;
    ext->base.est_rows = input->est_rows;

    /* Array embedded in trailing space — freed with ext node */
    ext->sort.columns = (td_op_t**)EXT_TRAIL(ext);
    memcpy(ext->sort.columns, cols, cols_sz);
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

td_op_t* td_extract(td_graph_t* g, td_op_t* col, int64_t field) {
    uint32_t col_id = col->id;
    uint32_t est = col->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    col = &g->nodes[col_id];  /* re-resolve after potential realloc */

    ext->base.opcode = OP_EXTRACT;
    ext->base.arity = 1;
    ext->base.inputs[0] = col;
    ext->base.out_type = TD_I64;
    ext->base.est_rows = est;
    ext->sym = field;

    g->nodes[ext->base.id] = ext->base;
    return &g->nodes[ext->base.id];
}

td_op_t* td_date_trunc(td_graph_t* g, td_op_t* col, int64_t field) {
    uint32_t col_id = col->id;
    uint32_t est = col->est_rows;

    td_op_ext_t* ext = graph_alloc_ext_node(g);
    if (!ext) return NULL;
    col = &g->nodes[col_id];  /* re-resolve after potential realloc */

    ext->base.opcode = OP_DATE_TRUNC;
    ext->base.arity = 1;
    ext->base.inputs[0] = col;
    ext->base.out_type = TD_I64;  /* returns timestamp (microseconds) */
    ext->base.est_rows = est;
    ext->sym = field;

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
