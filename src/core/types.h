#ifndef TD_TYPES_H
#define TD_TYPES_H

/*
 * types.h — Internal types header.
 *
 * The canonical type definitions (td_t, type constants, attribute flags)
 * live in <teide/td.h> (the public header).
 * Internal .c files can include either td.h directly or types.h.
 */
#include <teide/td.h>

/* --------------------------------------------------------------------------
 * Type classification helpers (operate on positive type tags)
 * -------------------------------------------------------------------------- */
/* Numeric: BOOL, U8, CHAR, I16, I32, I64, F64 */
#define TD_IS_NUMERIC(t) ((t) >= TD_BOOL && (t) <= TD_F64)

/* Integer: BOOL, U8, CHAR, I16, I32, I64 */
#define TD_IS_INTEGER(t) ((t) >= TD_BOOL && (t) <= TD_I64)

/* Float: F64 only */
#define TD_IS_FLOAT(t)   ((t) == TD_F64)

#endif /* TD_TYPES_H */
