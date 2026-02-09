#ifndef TD_COW_H
#define TD_COW_H

/*
 * cow.h -- COW (Copy-on-Write) ref counting.
 *
 * td_retain: increment reference count
 * td_release: decrement reference count, free when it reaches zero
 * td_cow: copy-on-write — return same pointer if sole owner, else copy
 */

#include <teide/td.h>

#endif /* TD_COW_H */
