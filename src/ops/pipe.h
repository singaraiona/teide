#ifndef TD_PIPE_H
#define TD_PIPE_H

/*
 * pipe.h -- Pipeline infrastructure.
 *
 * A pipe connects operation nodes in the executor pipeline. Each pipe
 * holds a morsel iterator state, optional materialized intermediate,
 * and upstream input pipe references.
 */

#include <teide/td.h>

/* Allocate and initialize a new pipe (all fields zeroed, spill_fd = -1). */
td_pipe_t* td_pipe_new(void);

/* Free a pipe. Closes spill_fd if open. Does NOT free upstream pipes. */
void td_pipe_free(td_pipe_t* pipe);

#endif /* TD_PIPE_H */
