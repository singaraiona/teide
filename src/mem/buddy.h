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

#ifndef TD_BUDDY_H
#define TD_BUDDY_H

/*
 * buddy.h -- Buddy allocator core.
 *
 * Manages split/coalesce of power-of-2 blocks within a contiguous arena
 * region. Free lists indexed by order (ORDER_MIN..ORDER_MAX). Bitmaps
 * for O(1) buddy-free detection and split tracking.
 */

#include <teide/td.h>
#include <stdint.h>
#include <stdbool.h>

/* --------------------------------------------------------------------------
 * Free-list node (intrusive, overlaid at offset 0 of a free block's header)
 * -------------------------------------------------------------------------- */
typedef struct td_free_node {
    struct td_free_node* prev;
    struct td_free_node* next;
} td_free_node_t;

/* --------------------------------------------------------------------------
 * Free-list head (doubly-linked circular sentinel)
 * -------------------------------------------------------------------------- */
typedef struct {
    td_free_node_t head;  /* sentinel node; head.next = first free block */
} td_free_list_t;

/* Number of orders: ORDER_MIN (5) through ORDER_MAX (30) */
#define TD_ORDER_COUNT (TD_ORDER_MAX - TD_ORDER_MIN + 1)

/* --------------------------------------------------------------------------
 * Buddy allocator state for one arena region
 * -------------------------------------------------------------------------- */
typedef struct {
    uint8_t*       base;          /* start of the mmap'd region */
    size_t         size;          /* total size of region (power of 2) */
    uint8_t        top_order;     /* log2(size): the order of the whole region */
    td_free_list_t free_lists[TD_ORDER_COUNT];
    uint8_t*       split_bits;    /* bitmap: has block been split? */
    uint8_t*       buddy_bits;    /* bitmap: XOR of buddy free status */
    size_t         split_bits_sz; /* bytes allocated for split_bits */
    size_t         buddy_bits_sz; /* bytes allocated for buddy_bits */
} td_buddy_t;

/* --------------------------------------------------------------------------
 * API
 * -------------------------------------------------------------------------- */

/* Compute minimum buddy order for a given data_size (excluding header).
 * Returns order k such that 2^k >= data_size + 32, clamped to [ORDER_MIN, ORDER_MAX+1].
 * If result > ORDER_MAX, caller should use direct mmap. */
uint8_t td_order_for_size(size_t data_size);

/* Initialize buddy state over [base, base+size). size must be a power of 2.
 * split_bits and buddy_bits must point to zeroed memory of adequate size.
 * The entire region is placed on the free list as a single block of top_order. */
void td_buddy_init(td_buddy_t* b, uint8_t* base, size_t size,
                   uint8_t* split_bits, size_t split_sz,
                   uint8_t* buddy_bits, size_t buddy_sz);

/* Allocate a block of the given order from the buddy.
 * Returns pointer to the td_t header, or NULL if the arena is exhausted. */
td_t* td_buddy_alloc(td_buddy_t* b, uint8_t order);

/* Free a block back to the buddy, coalescing with its buddy if possible. */
void td_buddy_free(td_buddy_t* b, td_t* block, uint8_t order);

/* Compute required bitmap size (in bytes) for a region of the given size. */
size_t td_buddy_bitmap_size(size_t region_size);

#endif /* TD_BUDDY_H */
