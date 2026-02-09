#ifndef TD_VEC_H
#define TD_VEC_H

/*
 * vec.h -- Vector operations.
 *
 * Vectors are td_t blocks with positive type tags. Data follows the 32-byte
 * header. Supports append, get, set, slice (zero-copy), concat, and nullable
 * bitmap (inline for <=128 elements, external for >128).
 */

#include <teide/td.h>

#endif /* TD_VEC_H */
