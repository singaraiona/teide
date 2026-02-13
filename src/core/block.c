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

#include "block.h"

/* Weak stub for td_alloc — replaced by buddy allocator at link time.
 * Uses td_vm_alloc (mmap) — page-aligned and zero-filled. */
__attribute__((weak))
td_t* td_alloc(size_t size) {
    if (size < 32) size = 32;
    size = (size + 4095) & ~(size_t)4095;
    void* p = td_vm_alloc(size);
    return (td_t*)p;
}

size_t td_block_size(td_t* v) {
    if (td_is_atom(v)) return 32;
    /* Vectors: header (32 bytes) + len * elem_size */
    int8_t t = td_type(v);
    if (t <= 0 || t >= TD_TYPE_COUNT) return 32;
    return 32 + (size_t)td_len(v) * td_elem_size(t);
}

td_t* td_block_copy(td_t* src) {
    size_t sz = td_block_size(src);
    td_t* dst = td_alloc(sz);
    if (!dst) return (td_t*)0;
    memcpy(dst, src, sz);
    atomic_store_explicit(&dst->rc, 1, memory_order_relaxed);
    return dst;
}
