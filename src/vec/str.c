#include "str.h"
#include <string.h>

/* --------------------------------------------------------------------------
 * SSO vs long-string detection
 *
 * The slen/sdata and obj fields share the same 8-byte union in td_t.
 * SSO: slen is 0..7, sdata contains the string bytes.
 * Long: obj is a non-NULL pointer to a CHAR vector.
 *
 * Distinction:
 *   - slen 1..7 → always SSO (a 32B-aligned pointer's low byte is a
 *     multiple of 32, never 1..7)
 *   - slen 0 with obj == NULL → empty SSO (all 8 union bytes are zero)
 *   - slen 0 with obj != NULL → long string (pointer's low byte is 0)
 *   - slen > 7 → long string (pointer's low byte is 32, 64, ... or higher)
 * -------------------------------------------------------------------------- */

static bool is_sso(td_t* s) {
    if (s->slen >= 1 && s->slen <= 7) return true;
    if (s->slen == 0 && s->obj == NULL) return true;
    return false;
}

/* --------------------------------------------------------------------------
 * td_str_ptr
 * -------------------------------------------------------------------------- */

const char* td_str_ptr(td_t* s) {
    if (!s || TD_IS_ERR(s)) return NULL;
    if (is_sso(s)) return (const char*)s->sdata;
    return (const char*)td_data(s->obj);
}

/* --------------------------------------------------------------------------
 * td_str_len
 * -------------------------------------------------------------------------- */

size_t td_str_len(td_t* s) {
    if (!s || TD_IS_ERR(s)) return 0;
    if (is_sso(s)) return (size_t)s->slen;
    return (size_t)s->obj->len;
}

/* --------------------------------------------------------------------------
 * td_str_cmp -- Compare two string atoms.
 *
 * Compare by memcmp of the min length, then by length difference.
 * -------------------------------------------------------------------------- */

int td_str_cmp(td_t* a, td_t* b) {
    if (!a || TD_IS_ERR(a) || !b || TD_IS_ERR(b)) return 0;

    const char* ap = td_str_ptr(a);
    const char* bp = td_str_ptr(b);
    size_t alen = td_str_len(a);
    size_t blen = td_str_len(b);

    size_t minlen = alen < blen ? alen : blen;
    int cmp = 0;
    if (minlen > 0) cmp = memcmp(ap, bp, minlen);
    if (cmp != 0) return cmp;

    if (alen < blen) return -1;
    if (alen > blen) return 1;
    return 0;
}
