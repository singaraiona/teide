#ifndef TD_TABLE_H
#define TD_TABLE_H

/*
 * table.h -- Table operations.
 *
 * A table has type = TD_TABLE (13), len = current column count.
 * Data region: first sizeof(td_t*) bytes = pointer to schema (I64 vector
 * of column name symbol IDs), then ncols * sizeof(td_t*) = column vector
 * pointers.
 */

#include <teide/td.h>

#endif /* TD_TABLE_H */
