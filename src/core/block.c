#include "block.h"
#include <stdlib.h>

/* Weak stub for td_alloc — replaced by buddy allocator in Phase 1.
 * Uses aligned_alloc to satisfy the 32-byte alignment invariant. */
__attribute__((weak))
td_t* td_alloc(size_t size) {
    if (size < 32) size = 32;
    /* Round up to multiple of 32 for alignment */
    size = (size + 31) & ~(size_t)31;
    void* p = aligned_alloc(32, size);
    if (!p) return NULL;
    memset(p, 0, size);
    return (td_t*)p;
}

size_t td_block_size(td_t* v) {
    if (td_is_atom(v)) return 32;
    /* Vectors: header (32 bytes) + len * elem_size */
    int8_t t = td_type(v);
    if (t <= 0 || t >= TD_TYPE_COUNT) return 32;
    return 32 + (size_t)td_len(v) * td_elem_size(t);
}

td_t* td_block_copy(td_t* src) {
    size_t sz = td_block_size(src);
    td_t* dst = td_alloc(sz);
    if (!dst) return (td_t*)0;
    memcpy(dst, src, sz);
    atomic_store_explicit(&dst->rc, 1, memory_order_relaxed);
    return dst;
}
