#include "morsel.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * td_morsel_init
 *
 * Initialize a morsel iterator over the given vector. Sets up offset,
 * length, and element size. Issues a sequential madvise hint for mmap'd
 * vectors to optimize readahead.
 * -------------------------------------------------------------------------- */

void td_morsel_init(td_morsel_t* m, td_t* vec) {
    m->vec = vec;
    m->offset = 0;
    m->len = td_len(vec);
    m->elem_size = td_elem_size(vec->type);
    m->morsel_len = 0;
    m->morsel_ptr = NULL;
    m->null_bits = NULL;

    /* One-time hint for mmap'd vectors */
    if (vec->mmod == 1) {
        td_vm_advise_seq(td_data(vec), (size_t)m->len * m->elem_size);
    }
}

/* --------------------------------------------------------------------------
 * td_morsel_next
 *
 * Advance to the next morsel. Returns true if a morsel is available, false
 * when the vector is exhausted. Sets morsel_ptr to the data for the current
 * chunk, morsel_len to the number of elements, and null_bits to the null
 * bitmap (or NULL if no nulls).
 * -------------------------------------------------------------------------- */

bool td_morsel_next(td_morsel_t* m) {
    m->offset += m->morsel_len;
    if (m->offset >= m->len) return false;

    int64_t remaining = m->len - m->offset;
    m->morsel_len = remaining < TD_MORSEL_ELEMS ? remaining : TD_MORSEL_ELEMS;
    m->morsel_ptr = (uint8_t*)td_data(m->vec) + (size_t)m->offset * m->elem_size;

    /* Null bitmap: only if HAS_NULLS */
    m->null_bits = NULL;
    if (m->vec->attrs & TD_ATTR_HAS_NULLS) {
        if (m->vec->attrs & TD_ATTR_NULLMAP_EXT) {
            /* External bitmap: point to correct byte offset */
            td_t* ext = m->vec->ext_nullmap;
            m->null_bits = (uint8_t*)td_data(ext) + (m->offset / 8);
        } else if (m->offset < 128) {
            /* Inline bitmap */
            m->null_bits = m->vec->nullmap + (m->offset / 8);
        }
    }

    return true;
}

/* --------------------------------------------------------------------------
 * td_morsel_init_range
 *
 * Initialize a morsel iterator over a sub-range [start, end) of the vector.
 * Used by parallel dispatch so each worker iterates a disjoint portion.
 * -------------------------------------------------------------------------- */

void td_morsel_init_range(td_morsel_t* m, td_t* vec, int64_t start, int64_t end) {
    m->vec = vec;
    m->offset = start;
    m->len = end;
    m->elem_size = td_elem_size(vec->type);
    m->morsel_len = 0;
    m->morsel_ptr = NULL;
    m->null_bits = NULL;
}
