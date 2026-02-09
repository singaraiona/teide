#include "arena.h"
#include "core/platform.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Constants
 * -------------------------------------------------------------------------- */
#define TD_ARENA_DEFAULT_SIZE  (64ULL * 1024 * 1024)   /* 64 MiB */
#define TD_ARENA_MAX_SIZE      (1ULL << TD_ORDER_MAX)   /* 1 GiB */

/* --------------------------------------------------------------------------
 * Thread-local state
 * -------------------------------------------------------------------------- */
TD_TLS td_arena_t*        td_tl_arena         = NULL;
TD_TLS td_direct_block_t* td_tl_direct_blocks = NULL;
TD_TLS td_mem_stats_t     td_tl_stats;

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
 * Return queue drain (MPSC Treiber stack)
 * -------------------------------------------------------------------------- */

void td_arena_drain_return_queue(td_arena_t* a) {
    td_t* head = atomic_exchange_explicit(&a->return_queue, NULL, memory_order_acquire);
    while (head) {
        td_t* next = *(td_t**)head;
        uint8_t order = head->order;
        td_buddy_free(&a->buddy, head, order);
        td_tl_stats.free_count++;
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

    uint8_t order = td_order_for_size(data_size);

    /* Direct mmap for blocks > ORDER_MAX */
    if (order > TD_ORDER_MAX) {
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
 * td_free
 * -------------------------------------------------------------------------- */

void td_free(td_t* v) {
    if (!v || TD_IS_ERR(v)) return;

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
                if (ta) td_buddy_free(&ta->buddy, tracker_block, TD_ORDER_MIN);

                td_tl_stats.free_count++;
                td_tl_stats.bytes_allocated -= sz;
                td_tl_stats.direct_count--;
                td_tl_stats.direct_bytes -= sz;
                return;
            }
            pp = &(*pp)->next;
        }
        return;
    }

    if (v->mmod == 1) return;

    uint8_t order = v->order;
    size_t block_size = (size_t)1 << order;

    td_arena_t* a = td_arena_find(v);
    if (!a) return;

    td_buddy_free(&a->buddy, v, order);
    td_tl_stats.free_count++;
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
    } else {
        int8_t t = td_type(v);
        if (t <= 0 || t >= TD_TYPE_COUNT)
            data_size = 0;
        else
            data_size = (size_t)td_len(v) * td_elem_size(t);
    }
    td_t* copy = td_alloc(data_size);
    if (!copy) return NULL;

    memcpy(copy, v, 32 + data_size);
    copy->mmod = 0;
    copy->order = td_order_for_size(data_size);
    if (copy->order < TD_ORDER_MIN) copy->order = TD_ORDER_MIN;
    atomic_store_explicit(&copy->rc, 1, memory_order_relaxed);
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
        else if (v->type == TD_LIST || v->type == TD_TABLE)
            old_data = (size_t)td_len(v) * sizeof(td_t*);
        else {
            int8_t t = td_type(v);
            old_data = (t > 0 && t < TD_TYPE_COUNT) ?
                       (size_t)td_len(v) * td_elem_size(t) : 0;
        }
        size_t copy_data = old_data < new_data_size ? old_data : new_data_size;
        memcpy(new_v, v, 32 + copy_data);
        new_v->mmod = 0;
        new_v->order = td_order_for_size(new_data_size);
        if (new_v->order < TD_ORDER_MIN) new_v->order = TD_ORDER_MIN;
        atomic_store_explicit(&new_v->rc, 1, memory_order_relaxed);
        td_free(v);
    }
    return new_v;
}

/* --------------------------------------------------------------------------
 * td_mem_stats
 * -------------------------------------------------------------------------- */

void td_mem_stats(td_mem_stats_t* out) {
    *out = td_tl_stats;
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
