#include "pipe.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* --------------------------------------------------------------------------
 * td_pipe_new
 *
 * Allocate a new pipe structure with all fields zeroed and spill_fd = -1.
 * -------------------------------------------------------------------------- */

td_pipe_t* td_pipe_new(void) {
    td_pipe_t* p = (td_pipe_t*)malloc(sizeof(td_pipe_t));
    if (!p) return NULL;

    memset(p, 0, sizeof(td_pipe_t));
    p->spill_fd = -1;

    return p;
}

/* --------------------------------------------------------------------------
 * td_pipe_free
 *
 * Free a pipe. Closes the spill file descriptor if it was opened.
 * Does NOT recursively free upstream input pipes.
 * -------------------------------------------------------------------------- */

void td_pipe_free(td_pipe_t* pipe) {
    if (!pipe) return;

    if (pipe->spill_fd >= 0) {
        close(pipe->spill_fd);
    }

    free(pipe);
}
