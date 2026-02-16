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

#include "buddy.h"
#include <string.h>
#include <assert.h>

/* --------------------------------------------------------------------------
 * Helpers
 * -------------------------------------------------------------------------- */

/* Ceiling log2 for values > 0. Returns k such that 2^k >= n. */
static uint8_t ceil_log2(size_t n) {
    if (n <= 1) return 0;
    /* clzll gives leading zeros for 64-bit; we want the bit position */
    uint8_t k = (uint8_t)(64 - __builtin_clzll(n - 1));
    return k;
}

uint8_t td_order_for_size(size_t data_size) {
    if (data_size > SIZE_MAX - 32) return TD_ORDER_SENTINEL;
    size_t total = data_size + 32;  /* header is 32 bytes */
    uint8_t k = ceil_log2(total);
    if (k < TD_ORDER_MIN) k = TD_ORDER_MIN;
    return k;
}

/* --------------------------------------------------------------------------
 * Free-list operations (intrusive doubly-linked list with sentinel)
 * -------------------------------------------------------------------------- */

static void free_list_init(td_free_list_t* fl) {
    fl->head.prev = &fl->head;
    fl->head.next = &fl->head;
}

static bool free_list_empty(td_free_list_t* fl) {
    return fl->head.next == &fl->head;
}

/* Push block to front of free list. block_ptr is the td_t* base of the free block.
 * The free-list node is overlaid at the start of the block (offset 0), reusing
 * the header space which is unused while the block is free. This avoids the
 * problem of node storage at offset 32 being overwritten by adjacent block
 * headers at order 5 (32-byte blocks). */
static void free_list_push(td_free_list_t* fl, uint8_t* block_ptr) {
    td_free_node_t* node = (td_free_node_t*)block_ptr;
    node->next = fl->head.next;
    node->prev = &fl->head;
    fl->head.next->prev = node;
    fl->head.next = node;
}

/* Pop and return the first block from the free list. Returns block base pointer.
 * Caller must ensure the list is non-empty. */
static uint8_t* free_list_pop(td_free_list_t* fl) {
    td_free_node_t* node = fl->head.next;
    node->next->prev = &fl->head;
    fl->head.next = node->next;
    return (uint8_t*)node;
}

/* Remove a specific block from its free list. */
static void free_list_remove(uint8_t* block_ptr) {
    td_free_node_t* node = (td_free_node_t*)block_ptr;
    node->prev->next = node->next;
    node->next->prev = node->prev;
}

/* --------------------------------------------------------------------------
 * Bitmap operations
 *
 * We need one bit per pair of buddies at each order. For a region of size S,
 * at order k there are S / 2^(k+1) pairs. We index as:
 *   global_bit_index = pair_offset_for_order_k + (block_offset / 2^(k+1))
 * where pair_offset_for_order_k = sum of pairs for all orders below k.
 *
 * Simpler approach: flat index based on block offset within the region,
 * one bit per minimally-sized block, separate array per order. But that
 * wastes space. Instead we use a single packed array per bitmap type.
 *
 * For simplicity and correctness, we index by:
 *   bit_index(order, offset) = (offset >> (order+1)) + level_base[order]
 * where level_base[order] = sum_{o=ORDER_MIN}^{order-1} (region_size >> (o+1))
 *
 * We pre-compute level bases during init.
 * -------------------------------------------------------------------------- */

/* Get the bit index for a block at the given byte offset and order.
 * offset is relative to arena base. */
static size_t bit_index(td_buddy_t* b, uint8_t order, size_t offset) {
    /* At this order, the pair index is offset / (2 * block_size) = offset >> (order+1) */
    size_t pair_idx = offset >> (order + 1);
    /* Level base: sum of pair counts for all orders below this one */
    size_t base = 0;
    for (uint8_t o = TD_ORDER_MIN; o < order; o++) {
        base += b->size >> (o + 1);
    }
    return base + pair_idx;
}

static void bitmap_set(uint8_t* bm, size_t idx) {
    bm[idx >> 3] |= (uint8_t)(1 << (idx & 7));
}

static void bitmap_clear(uint8_t* bm, size_t idx) {
    bm[idx >> 3] &= (uint8_t)~(1 << (idx & 7));
}

static bool bitmap_toggle(uint8_t* bm, size_t idx) {
    bm[idx >> 3] ^= (uint8_t)(1 << (idx & 7));
    return (bm[idx >> 3] >> (idx & 7)) & 1;
}

/* --------------------------------------------------------------------------
 * Bitmap size computation
 * -------------------------------------------------------------------------- */

size_t td_buddy_bitmap_size(size_t region_size) {
    /* Total bits = sum over all orders of (region_size >> (order+1)) pairs */
    size_t total_bits = 0;
    for (uint8_t o = TD_ORDER_MIN; o <= TD_ORDER_MAX; o++) {
        size_t pairs = region_size >> (o + 1);
        if (pairs == 0) break;
        total_bits += pairs;
    }
    return (total_bits + 7) / 8;
}

/* --------------------------------------------------------------------------
 * Initialization
 * -------------------------------------------------------------------------- */

void td_buddy_init(td_buddy_t* b, uint8_t* base, size_t size,
                   uint8_t* split_bits, size_t split_sz,
                   uint8_t* buddy_bits, size_t buddy_sz) {
    assert(size > 0 && (size & (size - 1)) == 0 && "size must be a power of 2");
    b->base = base;
    b->size = size;
    b->top_order = ceil_log2(size);
    b->split_bits = split_bits;
    b->buddy_bits = buddy_bits;
    b->split_bits_sz = split_sz;
    b->buddy_bits_sz = buddy_sz;

    memset(split_bits, 0, split_sz);
    memset(buddy_bits, 0, buddy_sz);

    for (int i = 0; i < TD_ORDER_COUNT; i++) {
        free_list_init(&b->free_lists[i]);
    }

    /* Put the entire region as one free block at top_order.
     * If top_order > ORDER_MAX, we split down to ORDER_MAX first. */
    if (b->top_order <= TD_ORDER_MAX) {
        free_list_push(&b->free_lists[b->top_order - TD_ORDER_MIN], base);
    } else {
        /* Region larger than ORDER_MAX: split into ORDER_MAX chunks */
        size_t chunk = (size_t)1 << TD_ORDER_MAX;
        for (size_t off = 0; off < size; off += chunk) {
            free_list_push(&b->free_lists[TD_ORDER_MAX - TD_ORDER_MIN], base + off);
        }
    }
}

/* --------------------------------------------------------------------------
 * Allocation
 * -------------------------------------------------------------------------- */

td_t* td_buddy_alloc(td_buddy_t* b, uint8_t order) {
    if (order < TD_ORDER_MIN) order = TD_ORDER_MIN;
    if (order > TD_ORDER_MAX) return NULL;

    /* Find a free block at order >= requested */
    int idx = order - TD_ORDER_MIN;
    int found = -1;
    for (int i = idx; i < TD_ORDER_COUNT; i++) {
        if (!free_list_empty(&b->free_lists[i])) {
            found = i;
            break;
        }
    }
    if (found < 0) return NULL;

    /* Pop from the found order */
    uint8_t* block = free_list_pop(&b->free_lists[found]);
    uint8_t found_order = (uint8_t)(found + TD_ORDER_MIN);

    /* Toggle buddy bit at found_order: block transitions free → in-use.
     * Skip at top_order where there is no buddy pair. */
    if (found_order < b->top_order) {
        size_t offset = (size_t)(block - b->base);
        size_t bi = bit_index(b, found_order, offset);
        bitmap_toggle(b->buddy_bits, bi);
    }

    /* Split down to the requested order */
    while (found_order > order) {
        found_order--;
        size_t half = (size_t)1 << found_order;
        uint8_t* buddy = block + half;

        /* Mark parent as split */
        size_t offset = (size_t)(block - b->base);
        size_t parent_bi = bit_index(b, found_order + 1, offset);
        bitmap_set(b->split_bits, parent_bi);

        /* Put the upper buddy on the free list */
        free_list_push(&b->free_lists[found_order - TD_ORDER_MIN], buddy);

        /* Toggle buddy bit at child level: upper half is free, lower half
         * is in-use (being split further or allocated). */
        size_t bi = bit_index(b, found_order, offset);
        bitmap_toggle(b->buddy_bits, bi);
    }

    /* Zero the header */
    memset(block, 0, 32);

    return (td_t*)block;
}

/* --------------------------------------------------------------------------
 * Deallocation
 * -------------------------------------------------------------------------- */

void td_buddy_free(td_buddy_t* b, td_t* block, uint8_t order) {
    /* M3: Validate order to prevent out-of-bounds bitmap access */
    if (order < TD_ORDER_MIN || order > TD_ORDER_MAX) return;

    uint8_t* ptr = (uint8_t*)block;
    size_t offset = (size_t)(ptr - b->base);

    while (order < b->top_order) {
        /* Toggle the buddy bit for this order */
        size_t bi = bit_index(b, order, offset);
        bool result = bitmap_toggle(b->buddy_bits, bi);

        if (result) {
            /* Buddy is NOT free (result=1 means we just set it).
             * Put this block on the free list. */
            free_list_push(&b->free_lists[order - TD_ORDER_MIN], ptr);
            return;
        }

        /* Buddy IS free (result=0 means buddy was also marked).
         * Remove buddy from free list and coalesce. */
        size_t buddy_offset = offset ^ ((size_t)1 << order);
        uint8_t* buddy_ptr = b->base + buddy_offset;
        free_list_remove(buddy_ptr);

        /* Clear the split bit for the parent level */
        size_t parent_offset = offset & ~((size_t)1 << order);
        size_t parent_bi = bit_index(b, order + 1, parent_offset);
        bitmap_clear(b->split_bits, parent_bi);

        /* Move to the lower-addressed block and try to coalesce at the next order */
        if (buddy_offset < offset) {
            ptr = buddy_ptr;
            offset = buddy_offset;
        }
        order++;
    }

    /* Reached top order -- put the fully coalesced block on the free list */
    if (order <= TD_ORDER_MAX) {
        free_list_push(&b->free_lists[order - TD_ORDER_MIN], ptr);
    }
}
