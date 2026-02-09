#include "cow.h"
#include "arena.h"
#include <stdatomic.h>

/* Defined in arena.c (non-static). 0 = sequential, 1 = parallel. */
extern _Atomic(uint32_t) td_parallel_flag;

/* --------------------------------------------------------------------------
 * td_retain
 * -------------------------------------------------------------------------- */

void td_retain(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;
    atomic_fetch_add_explicit(&v->rc, 1, memory_order_relaxed);
}

/* --------------------------------------------------------------------------
 * td_release
 * -------------------------------------------------------------------------- */

void td_release(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;
    uint32_t prev;
    if (TD_LIKELY(!td_parallel_flag)) {
        prev = atomic_fetch_sub_explicit(&v->rc, 1, memory_order_relaxed);
    } else {
        prev = atomic_fetch_sub_explicit(&v->rc, 1, memory_order_acq_rel);
    }
    if (prev == 1) td_free(v);
}

/* --------------------------------------------------------------------------
 * td_cow
 * -------------------------------------------------------------------------- */

td_t* td_cow(td_t* v) {
    if (!v || TD_IS_ERR(v)) return v;
    uint32_t rc = atomic_load_explicit(&v->rc, memory_order_acquire);
    if (rc == 1) return v;  /* sole owner -- mutate in place */
    td_t* copy = td_alloc_copy(v);
    if (!copy || TD_IS_ERR(copy)) return copy;
    atomic_store_explicit(&copy->rc, 1, memory_order_relaxed);
    td_release(v);
    return copy;
}
