#ifndef TD_ARENA_H
#define TD_ARENA_H

/*
 * arena.h -- Thread-local arena management.
 *
 * Each thread owns a chain of arenas. Each arena is a contiguous mmap'd
 * region managed by a buddy allocator, plus slab caches for small blocks
 * and an MPSC return queue for cross-thread free.
 */

#include "buddy.h"
#include <teide/td.h>
#include <stdatomic.h>

/* --------------------------------------------------------------------------
 * Slab cache: LIFO stack of free blocks per small order (5-9)
 * -------------------------------------------------------------------------- */
typedef struct {
    int64_t count;
    td_t*   stack[TD_SLAB_CACHE_SIZE];
} td_slab_cache_t;

/* --------------------------------------------------------------------------
 * Direct mmap tracker (blocks > 1 GiB, bypassing buddy)
 * -------------------------------------------------------------------------- */
typedef struct td_direct_block {
    void*                    ptr;
    size_t                   mapped_size;
    struct td_direct_block*  next;
} td_direct_block_t;

/* --------------------------------------------------------------------------
 * Arena struct
 * -------------------------------------------------------------------------- */
struct td_arena {
    td_buddy_t          buddy;
    td_slab_cache_t     slabs[TD_SLAB_ORDERS]; /* orders 5..9 */
    _Atomic(td_t*)      return_queue;           /* MPSC lock-free stack head */
    size_t              region_size;
    struct td_arena*    next;                   /* linked list of arenas per thread */
};

/* --------------------------------------------------------------------------
 * Thread-local state (extern -- defined in arena.c)
 * -------------------------------------------------------------------------- */
extern TD_TLS td_arena_t*        td_tl_arena;
extern TD_TLS td_direct_block_t* td_tl_direct_blocks;
extern TD_TLS td_mem_stats_t     td_tl_stats;

/* --------------------------------------------------------------------------
 * Internal helpers (used by arena.c, buddy tests, etc.)
 * -------------------------------------------------------------------------- */

/* Allocate a new arena of the given size, initialize buddy, link into
 * the thread-local arena chain. Returns the new arena, or NULL on failure. */
td_arena_t* td_arena_create(size_t size);

/* Find the arena that owns a given pointer (by checking containment). */
td_arena_t* td_arena_find(td_t* ptr);

/* Find the arena that owns a given pointer across ALL threads (global registry).
 * Used by td_free() for cross-thread free when td_arena_find() returns NULL. */
td_arena_t* td_arena_find_global(td_t* ptr);

/* Drain the return queue for a single arena, freeing blocks back to buddy. */
void td_arena_drain_return_queue(td_arena_t* a);

#endif /* TD_ARENA_H */
