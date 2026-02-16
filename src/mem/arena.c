/*
 *   Copyright (c) 2024-2026 Anton Kundenko <singaraiona@gmail.com>
 *   All rights reserved.

 *   Permission is hereby granted, free of charge, to any person obtaining a copy
 *   of this software and associated documentation files (the "Software"), to deal
 *   in the Software without restriction, including without limitation the rights
 *   to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *   copies of the Software, and to permit persons to whom the Software is
 *   furnished to do so, subject to the following conditions:

 *   The above copyright notice and this permission notice shall be included in all
 *   copies or substantial portions of the Software.

 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *   IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *   FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *   AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *   LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *   OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *   SOFTWARE.
 */

#include "arena.h"
#include "sys.h"
#include "core/platform.h"
#include <string.h>
#include <sched.h>
#ifndef NDEBUG
#include <stdio.h>
#endif

/* Ensure the return queue next-pointer overlay (bytes 0-7) does not clobber
   the order field used by td_arena_drain_return_queue(). */
_Static_assert(offsetof(td_t, order) >= 8,
               "order must not overlap return queue next ptr");

/* --------------------------------------------------------------------------
 * Constants
 * -------------------------------------------------------------------------- */
#define TD_ARENA_DEFAULT_SIZE  (64ULL * 1024 * 1024)   /* 64 MiB */
#define TD_ARENA_MAX_SIZE      (1ULL << TD_ORDER_MAX)   /* 1 GiB */

/* M2: Compile-time check that TD_ORDER_MAX and TD_ARENA_MAX_SIZE are consistent.
 * If TD_ORDER_MAX is changed, TD_ARENA_MAX_SIZE must be updated to match. */
_Static_assert((1ULL << TD_ORDER_MAX) <= TD_ARENA_MAX_SIZE,
               "TD_ORDER_MAX must fit within TD_ARENA_MAX_SIZE");

/* --------------------------------------------------------------------------
 * Thread-local state
 * -------------------------------------------------------------------------- */
TD_TLS td_arena_t*        td_tl_arena         = NULL;
TD_TLS td_direct_block_t* td_tl_direct_blocks = NULL;
TD_TLS td_mem_stats_t     td_tl_stats;

/* --------------------------------------------------------------------------
 * Global arena registry — enables cross-thread free via return queue.
 *
 * Every arena created by any thread is registered here. td_arena_find_global()
 * scans this array to find the owning arena for a block when td_arena_find()
 * (thread-local) returns NULL. Reads are lock-free; writes use a CAS spinlock.
 * -------------------------------------------------------------------------- */
/* L1: Hard cap on registered arenas. If exceeded, new arenas are not registered
 * and cross-thread frees to those arenas will silently leak. This limit is
 * sufficient for current workloads (nproc threads * ~2-3 arenas each). If
 * more arenas are needed, increase this constant. */
#define TD_ARENA_REGISTRY_CAP 1024

static td_arena_t*       g_arena_registry[TD_ARENA_REGISTRY_CAP];
static _Atomic(uint32_t) g_arena_registry_len  = 0;
static _Atomic(uint32_t) g_arena_registry_lock = 0;

static void arena_registry_lock(void) {
    unsigned spin_count = 0;
    while (atomic_exchange_explicit(&g_arena_registry_lock, 1,
                                    memory_order_acquire) != 0) {
#if defined(__x86_64__) || defined(__i386__)
        __builtin_ia32_pause();
#elif defined(__aarch64__)
        __asm__ volatile("yield" ::: "memory");
#endif
        /* conc-L1: Yield to OS scheduler after sustained contention to avoid
         * wasting CPU cycles when another thread holds the lock. */
        if (++spin_count % 1024 == 0) sched_yield();
    }
}

static void arena_registry_unlock(void) {
    atomic_store_explicit(&g_arena_registry_lock, 0, memory_order_release);
}

/* --------------------------------------------------------------------------
 * Arena creation
 * -------------------------------------------------------------------------- */

td_arena_t* td_arena_create(size_t size) {
    uint8_t* region = (uint8_t*)td_vm_alloc(size);
    if (!region) return NULL;

    size_t bm_sz = td_buddy_bitmap_size(size);

    size_t meta_size = sizeof(td_arena_t) + 2 * bm_sz;
    meta_size = (meta_size + 4095) & ~(size_t)4095;
    uint8_t* meta = (uint8_t*)td_vm_alloc(meta_size);
    if (!meta) {
        td_vm_free(region, size);
        return NULL;
    }
    memset(meta, 0, meta_size);

    td_arena_t* a = (td_arena_t*)meta;
    uint8_t* split_bits = meta + sizeof(td_arena_t);
    uint8_t* buddy_bits = split_bits + bm_sz;

    a->region_size = size;
    a->return_queue = (td_t*)NULL;
    a->next = NULL;

    td_buddy_init(&a->buddy, region, size, split_bits, bm_sz, buddy_bits, bm_sz);

    a->next = td_tl_arena;
    td_tl_arena = a;

    /* Register in global array for cross-thread free lookup */
    arena_registry_lock();
    /* L4: Relaxed ordering OK here — the lock's acquire/release provides the
     * necessary ordering guarantees for all accesses within the critical section. */
    uint32_t idx = atomic_load_explicit(&g_arena_registry_len, memory_order_relaxed);
    if (idx < TD_ARENA_REGISTRY_CAP) {
        g_arena_registry[idx] = a;
        atomic_store_explicit(&g_arena_registry_len, idx + 1, memory_order_relaxed);
    } else {
        /* Registry full — cross-thread frees to this arena will leak.
           Increase TD_ARENA_REGISTRY_CAP if this fires frequently. */
    }
    arena_registry_unlock();

    return a;
}

/* --------------------------------------------------------------------------
 * Arena lookup by pointer containment
 * -------------------------------------------------------------------------- */

td_arena_t* td_arena_find(td_t* ptr) {
    uint8_t* p = (uint8_t*)ptr;
    for (td_arena_t* a = td_tl_arena; a; a = a->next) {
        if (p >= a->buddy.base && p < a->buddy.base + a->region_size)
            return a;
    }
    return NULL;
}

/* --------------------------------------------------------------------------
 * Global arena lookup by pointer containment
 *
 * M4: This is an O(N) linear scan over all registered arenas. Acceptable
 * for current workloads — max TD_ARENA_REGISTRY_CAP (1024) arenas, and
 * cross-thread free is infrequent relative to same-thread free.
 * -------------------------------------------------------------------------- */

td_arena_t* td_arena_find_global(td_t* ptr) {
    uint8_t* p = (uint8_t*)ptr;
    uint32_t len = atomic_load_explicit(&g_arena_registry_len, memory_order_acquire);
    for (uint32_t i = 0; i < len; i++) {
        td_arena_t* a = g_arena_registry[i];
        if (!a) continue;
        if (p >= a->buddy.base && p < a->buddy.base + a->region_size)
            return a;
    }
    return NULL;
}

/* --------------------------------------------------------------------------
 * Return queue drain (MPSC Treiber stack)
 * -------------------------------------------------------------------------- */

void td_arena_drain_return_queue(td_arena_t* a) {
    td_t* head = atomic_exchange_explicit(&a->return_queue, NULL, memory_order_acquire);
    while (head) {
        td_t* next = *(td_t**)head;
        uint8_t order = head->order;
        td_buddy_free(&a->buddy, head, order);
        td_tl_stats.free_count++;
        /* M1: bytes_allocated is decremented on the owning (draining) thread,
         * but was incremented on the allocating thread. Stats are advisory
         * only and not used for correctness — this mismatch is acceptable. */
        td_tl_stats.bytes_allocated -= (size_t)1 << order;
        head = next;
    }
}

/* --------------------------------------------------------------------------
 * Public API: td_arena_init / td_arena_destroy_all
 * -------------------------------------------------------------------------- */

void td_arena_init(void) {
    if (td_tl_arena) return;
    memset(&td_tl_stats, 0, sizeof(td_tl_stats));
    td_arena_create(TD_ARENA_DEFAULT_SIZE);
}

void td_arena_destroy_all(void) {
    td_direct_block_t* db = td_tl_direct_blocks;
    while (db) {
        td_direct_block_t* next = db->next;
        if (db->ptr) td_vm_free(db->ptr, db->mapped_size);
        db = next;
    }
    td_tl_direct_blocks = NULL;

    /* Unregister from global array FIRST so no new cross-thread frees
       target these arenas after this point. (H1/M7: fixes TOCTOU race
       where another thread could push to a return queue between drain
       and munmap.) */
    arena_registry_lock();
    for (td_arena_t* a = td_tl_arena; a; a = a->next) {
        uint32_t len = atomic_load_explicit(&g_arena_registry_len, memory_order_relaxed);
        for (uint32_t i = 0; i < len; i++) {
            if (g_arena_registry[i] == a) {
                /* L1: compact registry — swap last entry into hole */
                if (i < len - 1) {
                    g_arena_registry[i] = g_arena_registry[len - 1];
                    g_arena_registry[len - 1] = NULL;
                } else {
                    g_arena_registry[i] = NULL;
                }
                atomic_store_explicit(&g_arena_registry_len, len - 1,
                                      memory_order_relaxed);
                break;
            }
        }
    }
    arena_registry_unlock();

    /* Ensure all pending cross-thread pushes have completed before draining.
     * NOTE (H1): Between the unlock above and this fence, another thread that
     * obtained the arena pointer from td_arena_find_global() BEFORE unregistration
     * could still be mid-CAS push to the return queue. The seq_cst fence ensures
     * visibility, and we perform two drain passes to catch any in-flight pushes
     * that land between the first drain and the munmap below.
     *
     * IMPORTANT: Callers must ensure all worker threads have joined (e.g. via
     * td_pool_free()) before calling td_arena_destroy_all(). The double-drain
     * only handles the narrow TOCTOU window; it cannot protect against threads
     * that continue pushing indefinitely after unregistration. */
    atomic_thread_fence(memory_order_seq_cst);

    /* First drain pass: reclaim cross-thread frees that arrived before unregister */
    for (td_arena_t* a = td_tl_arena; a; a = a->next)
        td_arena_drain_return_queue(a);

    /* Second drain pass (H1): catch any pushes that landed between the first
     * drain and now, due to in-flight CAS operations that started before
     * unregistration completed. */
    atomic_thread_fence(memory_order_seq_cst);
    for (td_arena_t* a = td_tl_arena; a; a = a->next)
        td_arena_drain_return_queue(a);

    td_arena_t* a = td_tl_arena;
    while (a) {
        td_arena_t* next = a->next;
        td_vm_free(a->buddy.base, a->region_size);
        size_t meta_size = sizeof(td_arena_t) + a->buddy.split_bits_sz + a->buddy.buddy_bits_sz;
        meta_size = (meta_size + 4095) & ~(size_t)4095;
        td_vm_free(a, meta_size);
        a = next;
    }
    td_tl_arena = NULL;
    memset(&td_tl_stats, 0, sizeof(td_tl_stats));
}

/* --------------------------------------------------------------------------
 * td_alloc
 * -------------------------------------------------------------------------- */

td_t* td_alloc(size_t data_size) {
    if (TD_UNLIKELY(!td_tl_arena)) {
        td_arena_init();
        if (!td_tl_arena) return NULL;
    }

    /* Only drain first arena's return queue on allocation hot path.
       Secondary arenas are drained during td_parallel_end() or
       td_arena_destroy_all(). */
    if (atomic_load_explicit(&td_tl_arena->return_queue,
                             memory_order_relaxed) != NULL)
        td_arena_drain_return_queue(td_tl_arena);

    uint8_t order = td_order_for_size(data_size);

    /* Direct mmap for blocks > ORDER_MAX */
    if (order > TD_ORDER_MAX) {
        if (data_size > SIZE_MAX - 32) return NULL;
        size_t total = data_size + 32;
        total = (total + 4095) & ~(size_t)4095;
        void* ptr = td_vm_alloc(total);
        if (!ptr) return NULL;
        memset(ptr, 0, 32);
        td_t* v = (td_t*)ptr;
        v->mmod = 2;
        v->order = 0;
        atomic_store_explicit(&v->rc, 1, memory_order_relaxed);

        /* Need order 6 (64B) to hold 32B header + 24B td_direct_block_t data */
        td_t* tracker_block = td_buddy_alloc(&td_tl_arena->buddy, TD_ORDER_MIN + 1);
        if (!tracker_block) {
            td_vm_free(ptr, total);
            return NULL;
        }
        memset(tracker_block, 0, 32);
        tracker_block->mmod = 0;
        tracker_block->order = TD_ORDER_MIN + 1;
        atomic_store_explicit(&tracker_block->rc, 1, memory_order_relaxed);

        td_direct_block_t* db = (td_direct_block_t*)td_data(tracker_block);
        db->ptr = ptr;
        db->mapped_size = total;
        db->next = td_tl_direct_blocks;
        td_tl_direct_blocks = db;

        td_tl_stats.alloc_count++;
        td_tl_stats.bytes_allocated += total;
        if (td_tl_stats.bytes_allocated > td_tl_stats.peak_bytes)
            td_tl_stats.peak_bytes = td_tl_stats.bytes_allocated;
        td_tl_stats.direct_count++;
        td_tl_stats.direct_bytes += total;
        return v;
    }

    /* Buddy allocator path -- try each arena */
    td_t* v = NULL;
    for (td_arena_t* a = td_tl_arena; a; a = a->next) {
        v = td_buddy_alloc(&a->buddy, order);
        if (v) break;
    }

    /* If no arena has space, grow */
    if (!v) {
        size_t new_size = td_tl_arena->region_size * 2;
        if (new_size > TD_ARENA_MAX_SIZE) new_size = TD_ARENA_MAX_SIZE;
        if (new_size < ((size_t)1 << order)) new_size = (size_t)1 << order;
        size_t s = 1;
        while (s < new_size) s <<= 1;
        new_size = s;
        if (new_size > TD_ARENA_MAX_SIZE) new_size = TD_ARENA_MAX_SIZE;

        td_arena_t* new_arena = td_arena_create(new_size);
        if (!new_arena) return NULL;
        v = td_buddy_alloc(&new_arena->buddy, order);
        if (!v) return NULL;
    }

    v->mmod = 0;
    v->order = order;
    atomic_store_explicit(&v->rc, 1, memory_order_relaxed);

    td_tl_stats.alloc_count++;
    td_tl_stats.bytes_allocated += (size_t)1 << order;
    if (td_tl_stats.bytes_allocated > td_tl_stats.peak_bytes)
        td_tl_stats.peak_bytes = td_tl_stats.bytes_allocated;

    return v;
}

/* --------------------------------------------------------------------------
 * Owned-reference helpers
 * -------------------------------------------------------------------------- */

/* L6: SSO (Small String Optimization) detection relies on the obj pointer
 * being NULL for empty strings, which holds because td_alloc() zero-inits
 * the 32-byte header via memset. The slen/obj fields share a union in td_t. */
static bool td_atom_str_is_sso(const td_t* s) {
    if (s->slen >= 1 && s->slen <= 7) return true;
    if (s->slen == 0 && s->obj == NULL) return true;
    return false;
}

static bool td_atom_owns_obj(const td_t* v) {
    if (v->type == TD_ATOM_GUID) return v->obj != NULL;
    if (v->type == TD_ATOM_STR) return !td_atom_str_is_sso(v);
    return false;
}

/* NOTE: recursive — depth bounded by data structure nesting. Dataframe
   workloads don't create deeply nested structures. */
static void td_release_owned_refs(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    if (td_is_atom(v)) {
        if (td_atom_owns_obj(v) && v->obj && !TD_IS_ERR(v->obj))
            td_release(v->obj);
        return;
    }

    if (v->attrs & TD_ATTR_SLICE) {
        if (v->slice_parent && !TD_IS_ERR(v->slice_parent))
            td_release(v->slice_parent);
        return;
    }

    if ((v->attrs & TD_ATTR_NULLMAP_EXT) &&
        v->ext_nullmap && !TD_IS_ERR(v->ext_nullmap))
        td_release(v->ext_nullmap);

    /* Parted column: release all segment vectors */
    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++) {
            if (segs[i] && !TD_IS_ERR(segs[i]))
                td_release(segs[i]);
        }
        return;
    }

    /* MAPCOMMON: release key_values and row_counts vectors */
    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        if (ptrs[0] && !TD_IS_ERR(ptrs[0])) td_release(ptrs[0]);
        if (ptrs[1] && !TD_IS_ERR(ptrs[1])) td_release(ptrs[1]);
        return;
    }

    if (v->type == TD_TABLE) {
        /* M5: Guard against corrupted negative len to prevent underflow loop */
        if (v->len < 0) return;
        td_t** slots = (td_t**)td_data(v);
        td_t* schema = slots[0];
        if (schema && !TD_IS_ERR(schema)) td_release(schema);

        td_t** cols = slots + 1;
        for (int64_t i = 0; i < v->len; i++) {
            td_t* col = cols[i];
            if (col && !TD_IS_ERR(col)) td_release(col);
        }
        return;
    }

    if (v->type == TD_LIST || v->type == TD_STR) {
        td_t** ptrs = (td_t**)td_data(v);
        for (int64_t i = 0; i < v->len; i++) {
            td_t* child = ptrs[i];
            if (child && !TD_IS_ERR(child)) td_release(child);
        }
    }
}

static void td_retain_owned_refs(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    if (td_is_atom(v)) {
        if (td_atom_owns_obj(v) && v->obj && !TD_IS_ERR(v->obj))
            td_retain(v->obj);
        return;
    }

    if (v->attrs & TD_ATTR_SLICE) {
        if (v->slice_parent && !TD_IS_ERR(v->slice_parent))
            td_retain(v->slice_parent);
        return;
    }

    if ((v->attrs & TD_ATTR_NULLMAP_EXT) &&
        v->ext_nullmap && !TD_IS_ERR(v->ext_nullmap))
        td_retain(v->ext_nullmap);

    /* Parted column: retain all segment vectors */
    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++) {
            if (segs[i] && !TD_IS_ERR(segs[i]))
                td_retain(segs[i]);
        }
        return;
    }

    /* MAPCOMMON: retain key_values and row_counts vectors */
    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        if (ptrs[0] && !TD_IS_ERR(ptrs[0])) td_retain(ptrs[0]);
        if (ptrs[1] && !TD_IS_ERR(ptrs[1])) td_retain(ptrs[1]);
        return;
    }

    if (v->type == TD_TABLE) {
        td_t** slots = (td_t**)td_data(v);
        td_t* schema = slots[0];
        if (schema && !TD_IS_ERR(schema)) td_retain(schema);

        td_t** cols = slots + 1;
        for (int64_t i = 0; i < v->len; i++) {
            td_t* col = cols[i];
            if (col && !TD_IS_ERR(col)) td_retain(col);
        }
        return;
    }

    if (v->type == TD_LIST || v->type == TD_STR) {
        td_t** ptrs = (td_t**)td_data(v);
        for (int64_t i = 0; i < v->len; i++) {
            td_t* child = ptrs[i];
            if (child && !TD_IS_ERR(child)) td_retain(child);
        }
    }
}

/* Detach owned refs before freeing a moved-from object (realloc move path). */
static void td_detach_owned_refs(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    if (td_is_atom(v)) {
        if (td_atom_owns_obj(v)) v->obj = NULL;
        return;
    }

    if (v->attrs & TD_ATTR_SLICE) {
        v->slice_parent = NULL;
        v->slice_offset = 0;
        v->attrs &= (uint8_t)~TD_ATTR_SLICE;
        return;
    }

    if (v->attrs & TD_ATTR_NULLMAP_EXT) {
        v->ext_nullmap = NULL;
        v->attrs &= (uint8_t)~TD_ATTR_NULLMAP_EXT;
    }

    /* Parted column: null out segment pointers to detach ownership */
    if (TD_IS_PARTED(v->type)) {
        int64_t n_segs = v->len;
        td_t** segs = (td_t**)td_data(v);
        for (int64_t i = 0; i < n_segs; i++)
            segs[i] = NULL;
        return;
    }

    if (v->type == TD_MAPCOMMON) {
        td_t** ptrs = (td_t**)td_data(v);
        ptrs[0] = NULL;
        ptrs[1] = NULL;
        return;
    }

    if (v->type == TD_TABLE) {
        td_t** slots = (td_t**)td_data(v);
        slots[0] = NULL;
        v->len = 0;
        return;
    }

    if (v->type == TD_LIST || v->type == TD_STR) {
        v->len = 0;
    }
}

/* --------------------------------------------------------------------------
 * td_free
 * -------------------------------------------------------------------------- */

void td_free(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

    td_release_owned_refs(v);

    /* NOTE: direct-mmap blocks (mmod==2) must be freed from the allocating
       thread. Cross-thread free of direct blocks silently leaks. */
    if (v->mmod == 2) {
        td_direct_block_t** pp = &td_tl_direct_blocks;
        while (*pp) {
            if ((*pp)->ptr == (void*)v) {
                td_direct_block_t* db = *pp;
                *pp = db->next;
                size_t sz = db->mapped_size;
                td_vm_free(v, sz);

                td_t* tracker_block = (td_t*)((uint8_t*)db - 32);
                td_arena_t* ta = td_arena_find(tracker_block);
                if (ta) td_buddy_free(&ta->buddy, tracker_block, TD_ORDER_MIN + 1);

                td_tl_stats.free_count++;
                td_tl_stats.bytes_allocated -= sz;
                td_tl_stats.direct_count--;
                td_tl_stats.direct_bytes -= sz;
                return;
            }
            pp = &(*pp)->next;
        }
        /* H2: Direct-mmap block not found in this thread's list. This means
         * td_free() was called from a non-owning thread for a direct-mmap
         * block (mmod==2). The block silently leaks because direct blocks
         * are tracked per-thread and cannot be freed cross-thread. */
#ifndef NDEBUG
        fprintf(stderr, "td_free: direct-mmap block %p (mmod==2) not found in "
                "calling thread's direct_blocks list — cross-thread free leaks\n",
                (void*)v);
#endif
        return;
    }

    /* mmod==1 is only used for simple vectors (td_col_mmap). Tables and
       lists should never have mmod==1; bail out to avoid incorrect size.
       M6: The unmap size is computed from len * esz + 32, which assumes this
       matches the original mmap size (page-rounded). On Linux, munmap of
       pages that were never mapped is a no-op, so slight overestimation is
       harmless. Underestimation would leak pages but is prevented because
       the original mapping also rounds to page boundaries. */
    if (v->mmod == 1) {
        if (v->type == TD_TABLE || v->type == TD_LIST) return;
        if (v->type > 0 && v->type < TD_TYPE_COUNT) {
            uint8_t esz = td_elem_size(v->type);
            size_t data_size = 32 + (size_t)v->len * esz;
            size_t mapped_size = (data_size + 4095) & ~(size_t)4095; /* round up to page */
            td_vm_unmap_file(v, mapped_size);
        } else {
            /* Invalid type — still unmap to avoid leak (header-only page) */
            size_t mapped_size = 4096; /* 32-byte header rounded up to page */
            td_vm_unmap_file(v, mapped_size);
        }
        td_tl_stats.free_count++;
        return;
    }

    uint8_t order = v->order;
    size_t block_size = (size_t)1 << order;

    td_arena_t* a = td_arena_find(v);
    if (!a) {
        /* Cross-thread free: find owning arena via global registry
         * and push to its MPSC return queue (Treiber stack). */
        a = td_arena_find_global(v);
        if (!a) return;
        td_t* old_head;
        do {
            old_head = atomic_load_explicit(&a->return_queue,
                                            memory_order_relaxed);
            /* INVARIANT: next-pointer overlay clobbers bytes 0-7 of the block header
               (nullmap[0..7]), but order (byte 17) and other header fields remain intact.
               The drain path reads head->order after this overlay — safe as long as
               td_t layout keeps order at offset >= 8. */
            *(td_t**)v = old_head;  /* reuse bytes 0-7 as next pointer */
        } while (!atomic_compare_exchange_weak_explicit(
                    &a->return_queue, &old_head, v,
                    memory_order_release, memory_order_relaxed));
        return;
    }

    td_buddy_free(&a->buddy, v, order);
    td_tl_stats.free_count++;
    /* Stats are per-thread; cross-thread freed blocks are decremented on
       the owning thread via return queue drain. */
    td_tl_stats.bytes_allocated -= block_size;
}

/* --------------------------------------------------------------------------
 * td_alloc_copy
 * -------------------------------------------------------------------------- */

td_t* td_alloc_copy(td_t* v) {
    if (!v || TD_IS_ERR(v)) return NULL;
    size_t data_size;
    if (td_is_atom(v)) {
        data_size = 0;
    } else if (v->type == TD_TABLE) {
        if (v->len < 0) return TD_ERR_PTR(TD_ERR_OOM);
        data_size = (size_t)(td_len(v) + 1) * sizeof(td_t*);
    } else if (TD_IS_PARTED(v->type) || v->type == TD_MAPCOMMON) {
        int64_t n_ptrs = v->len;
        if (v->type == TD_MAPCOMMON) n_ptrs = 2;
        if (n_ptrs < 0) return TD_ERR_PTR(TD_ERR_OOM);
        data_size = (size_t)n_ptrs * sizeof(td_t*);
    } else {
        int8_t t = td_type(v);
        if (t <= 0 || t >= TD_TYPE_COUNT)
            data_size = 0;
        else {
            uint8_t esz = td_elem_size(t);
            /* H3: Overflow check — if v->len is corrupted, the product could
             * wrap or produce an arbitrarily large copy size, leading to a
             * read overrun in the memcpy below. */
            if (v->len < 0 || (esz > 0 && (uint64_t)v->len > SIZE_MAX / esz))
                return TD_ERR_PTR(TD_ERR_OOM);
            data_size = (size_t)td_len(v) * esz;
        }
    }
    td_t* copy = td_alloc(data_size);
    if (!copy) return NULL;

    /* Save allocator metadata before memcpy overwrites the header */
    uint8_t new_order = copy->order;
    uint8_t new_mmod  = copy->mmod;
    memcpy(copy, v, 32 + data_size);
    copy->mmod  = new_mmod;
    copy->order = new_order;
    atomic_store_explicit(&copy->rc, 1, memory_order_relaxed);
    td_retain_owned_refs(copy);
    return copy;
}

/* --------------------------------------------------------------------------
 * td_scratch_alloc / td_scratch_realloc
 * -------------------------------------------------------------------------- */

td_t* td_scratch_alloc(size_t data_size) {
    return td_alloc(data_size);
}

td_t* td_scratch_realloc(td_t* v, size_t new_data_size) {
    td_t* new_v = td_alloc(new_data_size);
    if (!new_v) return NULL;
    if (v && !TD_IS_ERR(v)) {
        size_t old_data;
        if (td_is_atom(v))
            old_data = 0;
        else if (v->type == TD_LIST) {
            if (v->len < 0) { old_data = 0; }
            else old_data = (size_t)td_len(v) * sizeof(td_t*);
        } else if (v->type == TD_TABLE) {
            if (v->len < 0) { old_data = 0; }
            else old_data = (size_t)(td_len(v) + 1) * sizeof(td_t*);
        } else if (TD_IS_PARTED(v->type) || v->type == TD_MAPCOMMON) {
            int64_t n_ptrs = v->len;
            if (v->type == TD_MAPCOMMON) n_ptrs = 2;
            if (n_ptrs < 0) n_ptrs = 0;
            old_data = (size_t)n_ptrs * sizeof(td_t*);
        } else {
            int8_t t = td_type(v);
            old_data = (t > 0 && t < TD_TYPE_COUNT && v->len >= 0) ?
                       (size_t)td_len(v) * td_elem_size(t) : 0;
        }
        /* Clamp old_data to actual allocation size to prevent read overrun */
        if (v->mmod == 0 && v->order >= TD_ORDER_MIN && v->order <= TD_ORDER_MAX) {
            size_t alloc_data = ((size_t)1 << v->order) - 32;
            if (old_data > alloc_data) old_data = alloc_data;
        }
        size_t copy_data = old_data < new_data_size ? old_data : new_data_size;
        /* Save allocator metadata before memcpy overwrites the header */
        uint8_t new_mmod = new_v->mmod;
        uint8_t new_order = new_v->order;
        memcpy(new_v, v, 32 + copy_data);
        new_v->mmod = new_mmod;
        new_v->order = new_order;
        atomic_store_explicit(&new_v->rc, 1, memory_order_relaxed);
        td_detach_owned_refs(v);
        td_free(v);
    }
    return new_v;
}

/* --------------------------------------------------------------------------
 * td_mem_stats
 * -------------------------------------------------------------------------- */

void td_mem_stats(td_mem_stats_t* out) {
    *out = td_tl_stats;
    int64_t sc = 0, sp = 0;
    td_sys_get_stat(&sc, &sp);
    out->sys_current = (size_t)sc;
    out->sys_peak    = (size_t)sp;
}

/* --------------------------------------------------------------------------
 * Parallel begin/end stubs
 * -------------------------------------------------------------------------- */

_Atomic(uint32_t) td_parallel_flag = 0;

void td_parallel_begin(void) { atomic_store(&td_parallel_flag, 1); }
void td_parallel_end(void) {
    atomic_store(&td_parallel_flag, 0);
    for (td_arena_t* a = td_tl_arena; a; a = a->next)
        td_arena_drain_return_queue(a);
}
