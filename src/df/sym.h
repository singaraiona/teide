#ifndef TD_SYM_H
#define TD_SYM_H

/*
 * sym.h -- Global symbol intern table.
 *
 * Sequential mode: simple hash map + array. FNV-1a 32-bit hashing,
 * open addressing with linear probing. Stores (hash32 << 32) | (id + 1)
 * so that 0 means empty bucket.
 */

#include <teide/td.h>

#endif /* TD_SYM_H */
