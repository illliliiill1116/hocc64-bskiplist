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

    for (int i = MAX_LEVEL - 1; i >= 0; i--)
    {
        /* Horizontal traversal */
        while (curr_v && LOAD_RELAXED(curr->next_header) <= key)
        {
            node_header_t *next = LOAD_RELAXED(curr->next);
            if (!next) goto top_retry;

            /*
             * next is atomically read and always points to an existing node,
             * so NODE_LOAD_VERSION before validate is safe here. This differs
             * from the two-phase protocol required for children pointers.
             */
            hocc64_t next_v = NODE_LOAD_VERSION(next);
            if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr   = next;
            curr_v = next_v;
        }

        if (curr_v & HOCC_WRITER_BIT) goto top_retry;

        int        num  = LOAD_RELAXED(curr->num_elts);
        bsl_key_t *keys = NODE_KEYS(curr);
        int        rank = find_rank(keys, num, key);

        if (i == 0) /* Leaf level */
        {   
            if (keys[rank] == key)
            {
                bsl_val_t v = LEAF_VALUES(curr)[rank];

                 __atomic_thread_fence(__ATOMIC_ACQUIRE); /* for arm arch, no effect on x86 */
                if (!NODE_VALIDATE(curr, curr_v))
                    goto top_retry;

                if (out_val) *out_val = v;
                
                return 1;
            } 
            
            /* Prevent keys[rank] load from being reordered after NODE_VALIDATE */
            __atomic_thread_fence(__ATOMIC_ACQUIRE);  /* for arm arch, no effect on x86 */
            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;
            
            return 0;
        }
        else  /* Internal level: drop down */
        {
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
             */
            {
                if (!NODE_VALIDATE(curr, curr_v)) /* Phase 1 */
                    goto top_retry;
                hocc64_t child_v = NODE_LOAD_VERSION(child);
                if (!NODE_VALIDATE(curr, curr_v)) /* Phase 2 */
                    goto top_retry;
                curr   = child;
                curr_v = child_v;
            }
        }
    }
    return 0;
}

int bsl_get(bsl_t *list, bsl_key_t key, bsl_val_t *out_val)
{
    epoch_enter();
    int ret = _bsl_get_value(list, key, out_val);
    epoch_exit();
    return ret;
}