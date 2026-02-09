#ifndef TD_ERR_H
#define TD_ERR_H

/*
 * err.h — Internal error header.
 *
 * The canonical td_err_t definition lives in <teide/td.h> (the public header).
 * This file just includes it and adds the td_err_str() declaration.
 * Internal .c files can include either td.h directly or err.h.
 */
#include <teide/td.h>

#endif /* TD_ERR_H */
