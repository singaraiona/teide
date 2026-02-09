#ifndef TD_DF_H
#define TD_DF_H

/*
 * df.h -- Table (DataFrame) operations.
 *
 * A table has type = TD_TABLE (13), len = current column count.
 * Data region: first sizeof(td_t*) bytes = pointer to schema (I64 vector
 * of column name symbol IDs), then ncols * sizeof(td_t*) = column vector
 * pointers.
 */

#include <teide/td.h>

#endif /* TD_DF_H */
