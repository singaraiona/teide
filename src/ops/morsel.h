#ifndef TD_MORSEL_H
#define TD_MORSEL_H

/*
 * morsel.h -- Morsel iterator infrastructure.
 *
 * A morsel is a chunk of up to TD_MORSEL_ELEMS (1024) elements from a vector.
 * The iterator advances through the vector one morsel at a time, providing
 * direct data pointers and null bitmap pointers for each chunk.
 */

#include <teide/td.h>

/* Initialize a morsel iterator over a sub-range [start, end) of vec.
 * Used by parallel dispatch to partition work across workers. */
void td_morsel_init_range(td_morsel_t* m, td_t* vec, int64_t start, int64_t end);

#endif /* TD_MORSEL_H */
