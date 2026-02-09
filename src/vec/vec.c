#include "vec.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * Capacity helpers
 *
 * A vector's capacity is determined by its buddy order:
 *   capacity = (2^order - 32) / elem_size
 * When len reaches capacity, realloc to next power-of-2 data size.
 * -------------------------------------------------------------------------- */

static int64_t vec_capacity(td_t* vec) {
    size_t block_size = (size_t)1 << vec->order;
    size_t data_space = block_size - 32;
    uint8_t esz = td_elem_size(vec->type);
    if (esz == 0) return 0;
    return (int64_t)(data_space / esz);
}

/* --------------------------------------------------------------------------
 * td_vec_new
 * -------------------------------------------------------------------------- */

td_t* td_vec_new(int8_t type, int64_t capacity) {
    if (type <= 0 || type >= TD_TYPE_COUNT)
        return TD_ERR_PTR(TD_ERR_TYPE);

    uint8_t esz = td_elem_size(type);
    size_t data_size = (size_t)capacity * esz;

    td_t* v = td_alloc(data_size);
    if (!v || TD_IS_ERR(v)) return v;

    v->type = type;
    v->len = 0;
    v->attrs = 0;
    memset(v->nullmap, 0, 16);

    return v;
}

/* --------------------------------------------------------------------------
 * td_vec_append
 * -------------------------------------------------------------------------- */

td_t* td_vec_append(td_t* vec, const void* elem) {
    if (!vec || TD_IS_ERR(vec)) return vec;
    if (vec->type <= 0 || vec->type >= TD_TYPE_COUNT)
        return TD_ERR_PTR(TD_ERR_TYPE);

    /* COW: if shared, copy first */
    vec = td_cow(vec);
    if (!vec || TD_IS_ERR(vec)) return vec;

    uint8_t esz = td_elem_size(vec->type);
    int64_t cap = vec_capacity(vec);

    /* Grow if needed */
    if (vec->len >= cap) {
        size_t new_data_size = (size_t)(vec->len + 1) * esz;
        /* Round up to next power of 2 block */
        if (new_data_size < 32) new_data_size = 32;
        else {
            size_t s = 32;
            while (s < new_data_size) s *= 2;
            new_data_size = s;
        }
        td_t* new_vec = td_scratch_realloc(vec, new_data_size);
        if (!new_vec || TD_IS_ERR(new_vec)) return new_vec;
        vec = new_vec;
    }

    /* Append element */
    char* dst = (char*)td_data(vec) + vec->len * esz;
    memcpy(dst, elem, esz);
    vec->len++;

    return vec;
}

/* --------------------------------------------------------------------------
 * td_vec_set
 * -------------------------------------------------------------------------- */

td_t* td_vec_set(td_t* vec, int64_t idx, const void* elem) {
    if (!vec || TD_IS_ERR(vec)) return vec;
    if (idx < 0 || idx >= vec->len)
        return TD_ERR_PTR(TD_ERR_RANGE);

    /* COW: if shared, copy first */
    vec = td_cow(vec);
    if (!vec || TD_IS_ERR(vec)) return vec;

    uint8_t esz = td_elem_size(vec->type);
    char* dst = (char*)td_data(vec) + idx * esz;
    memcpy(dst, elem, esz);

    return vec;
}

/* --------------------------------------------------------------------------
 * td_vec_get
 * -------------------------------------------------------------------------- */

void* td_vec_get(td_t* vec, int64_t idx) {
    if (!vec || TD_IS_ERR(vec)) return NULL;

    /* Slice path: redirect to parent */
    if (vec->attrs & TD_ATTR_SLICE) {
        td_t* parent = vec->slice_parent;
        int64_t offset = vec->slice_offset;
        if (idx < 0 || idx >= vec->len) return NULL;
        uint8_t esz = td_elem_size(parent->type);
        return (char*)td_data(parent) + (offset + idx) * esz;
    }

    if (idx < 0 || idx >= vec->len) return NULL;
    uint8_t esz = td_elem_size(vec->type);
    return (char*)td_data(vec) + idx * esz;
}

/* --------------------------------------------------------------------------
 * td_vec_slice  (zero-copy view)
 * -------------------------------------------------------------------------- */

td_t* td_vec_slice(td_t* vec, int64_t offset, int64_t len) {
    if (!vec || TD_IS_ERR(vec)) return vec;
    if (offset < 0 || len < 0 || offset > vec->len || len > vec->len - offset)
        return TD_ERR_PTR(TD_ERR_RANGE);

    /* If input is already a slice, resolve to ultimate parent */
    td_t* parent = vec;
    int64_t parent_offset = offset;
    if (vec->attrs & TD_ATTR_SLICE) {
        parent = vec->slice_parent;
        parent_offset = vec->slice_offset + offset;
    }

    /* Allocate a header-only block for the slice view */
    td_t* s = td_alloc(0);
    if (!s || TD_IS_ERR(s)) return s;

    s->type = parent->type;
    s->attrs = TD_ATTR_SLICE;
    s->len = len;
    s->slice_parent = parent;
    s->slice_offset = parent_offset;

    /* Retain the parent so it stays alive */
    td_retain(parent);

    return s;
}

/* --------------------------------------------------------------------------
 * td_vec_concat
 * -------------------------------------------------------------------------- */

td_t* td_vec_concat(td_t* a, td_t* b) {
    if (!a || TD_IS_ERR(a)) return a;
    if (!b || TD_IS_ERR(b)) return b;
    if (a->type != b->type)
        return TD_ERR_PTR(TD_ERR_TYPE);

    uint8_t esz = td_elem_size(a->type);
    int64_t total_len = a->len + b->len;
    if (total_len < a->len) return TD_ERR_PTR(TD_ERR_OOM); /* overflow */
    size_t data_size = (size_t)total_len * esz;

    td_t* result = td_alloc(data_size);
    if (!result || TD_IS_ERR(result)) return result;

    result->type = a->type;
    result->len = total_len;
    result->attrs = 0;
    memset(result->nullmap, 0, 16);

    /* Copy data from a */
    void* a_data = (a->attrs & TD_ATTR_SLICE) ?
        ((char*)td_data(a->slice_parent) + a->slice_offset * esz) :
        td_data(a);
    memcpy(td_data(result), a_data, (size_t)a->len * esz);

    /* Copy data from b */
    void* b_data = (b->attrs & TD_ATTR_SLICE) ?
        ((char*)td_data(b->slice_parent) + b->slice_offset * esz) :
        td_data(b);
    memcpy((char*)td_data(result) + (size_t)a->len * esz, b_data,
           (size_t)b->len * esz);

    return result;
}

/* --------------------------------------------------------------------------
 * td_vec_from_raw
 * -------------------------------------------------------------------------- */

td_t* td_vec_from_raw(int8_t type, const void* data, int64_t count) {
    if (type <= 0 || type >= TD_TYPE_COUNT)
        return TD_ERR_PTR(TD_ERR_TYPE);

    uint8_t esz = td_elem_size(type);
    size_t data_size = (size_t)count * esz;

    td_t* v = td_alloc(data_size);
    if (!v || TD_IS_ERR(v)) return v;

    v->type = type;
    v->len = count;
    v->attrs = 0;
    memset(v->nullmap, 0, 16);

    memcpy(td_data(v), data, data_size);

    return v;
}

/* --------------------------------------------------------------------------
 * Null bitmap operations
 *
 * Inline: for vectors with <=128 elements, bits stored in nullmap[16] (128 bits).
 * External: for >128 elements, allocate a U8 vector bitmap via ext_nullmap.
 * -------------------------------------------------------------------------- */

void td_vec_set_null(td_t* vec, int64_t idx, bool is_null) {
    if (!vec || TD_IS_ERR(vec)) return;
    if (idx < 0 || idx >= vec->len) return;

    /* Mark HAS_NULLS if setting a null */
    if (is_null) vec->attrs |= TD_ATTR_HAS_NULLS;

    if (!(vec->attrs & TD_ATTR_NULLMAP_EXT)) {
        /* Inline nullmap path (<=128 elements) */
        if (idx < 128) {
            int byte_idx = (int)(idx / 8);
            int bit_idx = (int)(idx % 8);
            if (is_null)
                vec->nullmap[byte_idx] |= (uint8_t)(1u << bit_idx);
            else
                vec->nullmap[byte_idx] &= (uint8_t)~(1u << bit_idx);
            return;
        }
        /* Need to promote to external nullmap */
        int64_t bitmap_len = (vec->len + 7) / 8;
        td_t* ext = td_vec_new(TD_U8, bitmap_len);
        if (!ext || TD_IS_ERR(ext)) return;
        ext->len = bitmap_len;
        /* Copy existing inline bits */
        memcpy(td_data(ext), vec->nullmap, 16);
        /* Zero remaining bytes */
        if (bitmap_len > 16)
            memset((char*)td_data(ext) + 16, 0, (size_t)(bitmap_len - 16));
        vec->attrs |= TD_ATTR_NULLMAP_EXT;
        vec->ext_nullmap = ext;
    }

    /* External nullmap path */
    td_t* ext = vec->ext_nullmap;
    /* Grow external bitmap if needed */
    int64_t needed_bytes = (idx / 8) + 1;
    if (needed_bytes > ext->len) {
        int64_t new_len = (vec->len + 7) / 8;
        if (new_len < needed_bytes) new_len = needed_bytes;
        size_t new_data_size = (size_t)new_len;
        int64_t old_len = ext->len;
        td_t* new_ext = td_scratch_realloc(ext, new_data_size);
        if (!new_ext || TD_IS_ERR(new_ext)) return;
        /* Zero new bytes */
        if (new_len > old_len)
            memset((char*)td_data(new_ext) + old_len, 0,
                   (size_t)(new_len - old_len));
        new_ext->len = new_len;
        vec->ext_nullmap = new_ext;
        ext = new_ext;
    }

    uint8_t* bits = (uint8_t*)td_data(ext);
    int byte_idx = (int)(idx / 8);
    int bit_idx = (int)(idx % 8);
    if (is_null)
        bits[byte_idx] |= (uint8_t)(1u << bit_idx);
    else
        bits[byte_idx] &= (uint8_t)~(1u << bit_idx);
}

bool td_vec_is_null(td_t* vec, int64_t idx) {
    if (!vec || TD_IS_ERR(vec)) return false;
    if (idx < 0 || idx >= vec->len) return false;
    if (!(vec->attrs & TD_ATTR_HAS_NULLS)) return false;

    if (vec->attrs & TD_ATTR_NULLMAP_EXT) {
        td_t* ext = vec->ext_nullmap;
        int64_t byte_idx = idx / 8;
        if (byte_idx >= ext->len) return false;
        uint8_t* bits = (uint8_t*)td_data(ext);
        return (bits[byte_idx] >> (idx % 8)) & 1;
    }

    /* Inline nullmap */
    if (idx >= 128) return false;
    int byte_idx = (int)(idx / 8);
    int bit_idx = (int)(idx % 8);
    return (vec->nullmap[byte_idx] >> bit_idx) & 1;
}
