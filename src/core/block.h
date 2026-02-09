#ifndef TD_BLOCK_H
#define TD_BLOCK_H

/*
 * block.h — Internal block header utilities.
 *
 * Provides td_block_size() and td_block_copy(). The core td_t struct and
 * accessor macros (td_type, td_is_atom, td_is_vec, td_len, td_data,
 * td_elem_size) are defined in <teide/td.h>.
 */
#include <teide/td.h>
#include <string.h>

/* Compute total block size in bytes (header + data) */
size_t td_block_size(td_t* v);

/* Allocate a new block and copy header + data from src.
 * Requires td_alloc (declared in td.h, provided by the buddy allocator). */
td_t* td_block_copy(td_t* src);

#endif /* TD_BLOCK_H */
