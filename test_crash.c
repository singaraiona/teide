#include <stdio.h>
#include <string.h>
#include "include/teide/td.h"

int main(void) {
    td_arena_init();
    td_sym_init();
    td_pool_init(0);

    /* Test sym load */
    td_err_t serr = td_sym_load("/tmp/db/sym");
    fprintf(stderr, "td_sym_load: %d\n", (int)serr);

    /* Test read_splayed directly */
    td_t* sp = td_read_splayed("/tmp/db/2024.01.01/quotes", "/tmp/db/sym");
    fprintf(stderr, "td_read_splayed: %p err=%d\n",
            (void*)sp, sp ? TD_IS_ERR(sp) : -1);
    if (sp && !TD_IS_ERR(sp)) {
        fprintf(stderr, "  ncols=%lld nrows=%lld\n",
                (long long)td_table_ncols(sp), (long long)td_table_nrows(sp));
    }

    /* Now try read_parted */
    td_t* tbl = td_read_parted("/tmp/db", "quotes");
    fprintf(stderr, "td_read_parted: %p err=%d\n",
            (void*)tbl, tbl ? TD_IS_ERR(tbl) : -1);
    if (tbl && !TD_IS_ERR(tbl)) {
        fprintf(stderr, "  ncols=%lld nrows=%lld\n",
                (long long)td_table_ncols(tbl), (long long)td_table_nrows(tbl));

        /* Build GROUP BY date, SUM(v1) */
        td_graph_t* g = td_graph_new(tbl);
        td_op_t* key = td_scan(g, "date");
        td_op_t* agg_in = td_scan(g, "v1");
        uint16_t agg_op = OP_SUM;
        td_op_t* grp = td_group(g, &key, 1, &agg_op, &agg_in, 1);

        fprintf(stderr, "Executing GROUP BY...\n");
        td_t* result = td_execute(g, grp);
        fprintf(stderr, "result: %p err=%d\n",
                (void*)result, result ? TD_IS_ERR(result) : -1);
        if (result && !TD_IS_ERR(result)) {
            fprintf(stderr, "  ncols=%lld nrows=%lld\n",
                    (long long)td_table_ncols(result),
                    (long long)td_table_nrows(result));
            td_release(result);
            fprintf(stderr, "Released result OK\n");
        }
        td_graph_free(g);
        td_release(tbl);
    }

    fprintf(stderr, "Done\n");
    return 0;
}
