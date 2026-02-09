#include "sym.h"
#include <string.h>
#include <stdlib.h>

/* --------------------------------------------------------------------------
 * FNV-1a 32-bit hash
 * -------------------------------------------------------------------------- */

static uint32_t fnv1a(const char* data, size_t len) {
    uint32_t h = 0x811c9dc5u;
    for (size_t i = 0; i < len; i++) {
        h ^= (uint8_t)data[i];
        h *= 0x01000193u;
    }
    return h;
}

/* --------------------------------------------------------------------------
 * Symbol table structure (static global, sequential mode only).
 * NOT thread-safe: all interning must happen before td_parallel_begin().
 * -------------------------------------------------------------------------- */

#define SYM_INIT_CAP     256
#define SYM_LOAD_FACTOR  0.7

typedef struct {
    /* Hash table: each bucket stores (hash32 << 32) | (id + 1), 0 = empty */
    uint64_t*  buckets;
    uint32_t   bucket_cap;   /* always power of 2 */

    /* String array: strings[id] = td_t* string atom */
    td_t**     strings;
    uint32_t   str_count;
    uint32_t   str_cap;
} sym_table_t;

static sym_table_t g_sym;
static bool        g_sym_inited = false;

/* --------------------------------------------------------------------------
 * td_sym_init
 * -------------------------------------------------------------------------- */

void td_sym_init(void) {
    if (g_sym_inited) return;

    g_sym.bucket_cap = SYM_INIT_CAP;
    g_sym.buckets = (uint64_t*)calloc(g_sym.bucket_cap, sizeof(uint64_t));
    if (!g_sym.buckets) return;

    g_sym.str_cap = SYM_INIT_CAP;
    g_sym.str_count = 0;
    g_sym.strings = (td_t**)calloc(g_sym.str_cap, sizeof(td_t*));
    if (!g_sym.strings) { free(g_sym.buckets); g_sym.buckets = NULL; return; }

    g_sym_inited = true;
}

/* --------------------------------------------------------------------------
 * td_sym_destroy
 * -------------------------------------------------------------------------- */

void td_sym_destroy(void) {
    if (!g_sym_inited) return;

    /* Release all interned string atoms */
    for (uint32_t i = 0; i < g_sym.str_count; i++) {
        if (g_sym.strings[i]) {
            td_release(g_sym.strings[i]);
        }
    }

    free(g_sym.strings);
    free(g_sym.buckets);

    memset(&g_sym, 0, sizeof(g_sym));
    g_sym_inited = false;
}

/* --------------------------------------------------------------------------
 * Hash table helpers
 * -------------------------------------------------------------------------- */

static void ht_insert(uint64_t* buckets, uint32_t cap, uint32_t hash, uint32_t id) {
    uint32_t mask = cap - 1;
    uint32_t slot = hash & mask;
    uint64_t entry = ((uint64_t)hash << 32) | ((uint64_t)(id + 1));

    for (;;) {
        if (buckets[slot] == 0) {
            buckets[slot] = entry;
            return;
        }
        slot = (slot + 1) & mask;
    }
}

static void ht_grow(void) {
    uint32_t new_cap = g_sym.bucket_cap * 2;
    uint64_t* new_buckets = (uint64_t*)calloc(new_cap, sizeof(uint64_t));
    if (!new_buckets) return; /* stay at current capacity */

    /* Re-insert all existing entries */
    for (uint32_t i = 0; i < g_sym.bucket_cap; i++) {
        uint64_t e = g_sym.buckets[i];
        if (e == 0) continue;
        uint32_t h = (uint32_t)(e >> 32);
        uint32_t id = (uint32_t)(e & 0xFFFFFFFF) - 1;
        ht_insert(new_buckets, new_cap, h, id);
    }

    free(g_sym.buckets);
    g_sym.buckets = new_buckets;
    g_sym.bucket_cap = new_cap;
}

/* --------------------------------------------------------------------------
 * td_sym_intern
 * -------------------------------------------------------------------------- */

int64_t td_sym_intern(const char* str, size_t len) {
    if (!g_sym_inited) return -1;
    uint32_t hash = fnv1a(str, len);
    uint32_t mask = g_sym.bucket_cap - 1;
    uint32_t slot = hash & mask;

    /* Probe for existing entry */
    for (;;) {
        uint64_t e = g_sym.buckets[slot];
        if (e == 0) break;  /* empty — not found */

        uint32_t e_hash = (uint32_t)(e >> 32);
        if (e_hash == hash) {
            uint32_t e_id = (uint32_t)(e & 0xFFFFFFFF) - 1;
            td_t* existing = g_sym.strings[e_id];
            if (td_str_len(existing) == len &&
                memcmp(td_str_ptr(existing), str, len) == 0) {
                return (int64_t)e_id;
            }
        }
        slot = (slot + 1) & mask;
    }

    /* Refuse insert if table is critically full (ht_grow may have failed) */
    if (g_sym.str_count >= (uint32_t)(g_sym.bucket_cap * 0.95))
        return -1;

    /* Not found — create new entry */
    uint32_t new_id = g_sym.str_count;

    /* Grow strings array if needed */
    if (new_id >= g_sym.str_cap) {
        uint32_t new_str_cap = g_sym.str_cap * 2;
        td_t** new_strings = (td_t**)realloc(g_sym.strings,
                                               new_str_cap * sizeof(td_t*));
        if (!new_strings) return -1;
        g_sym.strings = new_strings;
        g_sym.str_cap = new_str_cap;
    }

    /* Create string atom and retain it */
    td_t* s = td_str(str, len);
    if (!s || TD_IS_ERR(s)) return -1;
    td_retain(s);  /* sym table owns a ref */
    g_sym.strings[new_id] = s;
    g_sym.str_count++;

    /* Insert into hash table */
    ht_insert(g_sym.buckets, g_sym.bucket_cap, hash, new_id);

    /* Check load factor and grow if needed */
    if ((double)g_sym.str_count / (double)g_sym.bucket_cap > SYM_LOAD_FACTOR) {
        ht_grow();
    }

    return (int64_t)new_id;
}

/* --------------------------------------------------------------------------
 * td_sym_find
 * -------------------------------------------------------------------------- */

int64_t td_sym_find(const char* str, size_t len) {
    if (!g_sym_inited) return -1;

    uint32_t hash = fnv1a(str, len);
    uint32_t mask = g_sym.bucket_cap - 1;
    uint32_t slot = hash & mask;

    for (;;) {
        uint64_t e = g_sym.buckets[slot];
        if (e == 0) return -1;  /* empty — not found */

        uint32_t e_hash = (uint32_t)(e >> 32);
        if (e_hash == hash) {
            uint32_t e_id = (uint32_t)(e & 0xFFFFFFFF) - 1;
            td_t* existing = g_sym.strings[e_id];
            if (td_str_len(existing) == len &&
                memcmp(td_str_ptr(existing), str, len) == 0) {
                return (int64_t)e_id;
            }
        }
        slot = (slot + 1) & mask;
    }
}

/* --------------------------------------------------------------------------
 * td_sym_str
 * -------------------------------------------------------------------------- */

td_t* td_sym_str(int64_t id) {
    if (!g_sym_inited) return NULL;
    if (id < 0 || (uint32_t)id >= g_sym.str_count) return NULL;
    return g_sym.strings[id];
}

/* --------------------------------------------------------------------------
 * td_sym_count
 * -------------------------------------------------------------------------- */

uint32_t td_sym_count(void) {
    if (!g_sym_inited) return 0;
    return g_sym.str_count;
}
