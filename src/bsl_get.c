/*
 * bsl_get.c
 *
 * This file contains the reference implementation of the two-phase
 * optimistic HOH validation protocol used throughout all traversal paths.
 * Other traversal files (bsl_insert.c, bsl_delete, bsl_limit_scan.c) use the same
 * protocol with a short reference comment in place of the full explanation.
 */

#include "bskiplist.h"
#include "node.h"
#include "epoch.h"
#include <assert.h>

static inline int
_bsl_get_value(bsl_t *list, bsl_key_t key, bsl_val_t *out_val)
{
    if (key <= BSL_KEY_MIN || key >= BSL_KEY_MAX)
        return 0;

top_retry:;
    node_header_t *curr   = (node_header_t *)list->headers[MAX_LEVEL - 1];
    hocc64_t       curr_v = NODE_LOAD_VERSION(curr);

    for (int i = MAX_LEVEL - 1; i >= 1; i--)
    {
        /* Horizontal traversal */
        while (curr && LOAD_RELAXED(curr->next_header) <= key)
        {
            curr = LOAD_RELAXED(curr->next);
        }
        
        if (!curr) goto top_retry;
        curr_v = NODE_LOAD_VERSION(curr);

        int        num  = LOAD_RELAXED(curr->num_elts);
        bsl_key_t *keys = NODE_KEYS(curr);
        int        rank = find_rank(keys, num, key);

        void          **children = INTERNAL_CHILDREN(curr);
        node_header_t  *child    = (node_header_t *)LOAD_RELAXED(children[rank]);
        if (!child) goto top_retry;

        /*
        * Two-phase validation for optimistic HOH traversal.
        *
        * Phase 1: validate curr before dereferencing child. A pointer
        * read from curr's contents may be stale if curr was concurrently
        * modified; this confirms the pointer is legitimate before any
        * dereference occurs.
        *
        * Phase 2: validate curr again after reading child's version.
        * This ensures child_v was observed within curr's stable window,
        * preserving the HOH invariant that parent and child states are
        * consistent.
        *
        */
        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v)) /* Phase 1 */
            goto top_retry;

        if (i != 1)
        {
            hocc64_t child_v = NODE_LOAD_VERSION(child);

            if (!NODE_VALIDATE(curr, curr_v)) /* Phase 2 */
                goto top_retry;

            curr = child;
            curr_v = child_v;
        }
        else /* Acquire read lock on leaf to ensure linearizability. */
        {
            NODE_READ_LOCK(child);
            if (!NODE_VALIDATE(curr, curr_v)) /* Phase 2 */
            {
                NODE_READ_UNLOCK(child);
                goto top_retry;
            }
            curr = child;
        }
    }

    assert(curr->level == 0);

    node_header_t *prev = curr;
    while (curr->next_header <= key)
    {
        NODE_READ_LOCK(curr->next);
        prev = curr;
        curr = curr->next;
        NODE_READ_UNLOCK(prev);
    }

    int        num  = curr->num_elts;
    bsl_key_t *keys = NODE_KEYS(curr);
    int        rank = find_rank(keys, num, key);

    int ret = (key == keys[rank]);

    if (ret)
        *out_val = LEAF_VALUES(curr)[rank];

    NODE_READ_UNLOCK(curr);
    return ret;
}

int bsl_get(bsl_t *list, bsl_key_t key, bsl_val_t *out_val)
{
    epoch_enter();
    int ret = _bsl_get_value(list, key, out_val);
    epoch_exit();
    return ret;
}