/*
 * bsl_scan.c
 *
 * This implementation is inspired by the algorithm described in:
 *   Yicong Luo et al., "<Bridging Cache-Friendliness and Concurrency: A Locality-Optimized In-Memory B-Skiplist>", ICPP 2025
 *   arXiv: https://arxiv.org/abs/2507.21492
 *
 * Original C++ reference implementation (Apache 2.0):
 *   https://github.com/Ratbuyer/bskip_artifact
 *
 * This is an independent C reimplementation. The read-only traversal phase
 * (levels above level_to_promote) uses optimistic version validation instead
 * of reader-writer locks, falling back to pessimistic write locking during
 * the modification phase.
 */

#include "bskiplist.h"
#include "node.h"
#include "epoch.h"
#include <assert.h>
#include <string.h>

static inline void 
_bsl_scan_n(bsl_t *list, bsl_key_t start, size_t length, range_cb cb, void *arg)
{
    if (unlikely(length <= 0)) return;

    size_t remaining = length;
    bsl_key_t current_start = start;

top_retry:;

    node_header_t *curr = list->headers[MAX_LEVEL - 1];
    hocc64_t curr_v = NODE_LOAD_VERSION(curr);

    for (int level = MAX_LEVEL - 1; level > 0; level--)
    {
        while (LOAD_RELAXED(curr->next_header) <= current_start)
        {
            node_header_t *next = LOAD_RELAXED(curr->next);
            if (!next) break;

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = next;
            curr_v = next_v;
        }

        int rank = find_rank(NODE_KEYS(curr), LOAD_RELAXED(curr->num_elts), current_start);
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (!child) goto top_retry; 

        /* two-phase HOH validation, see bsl_get.c */
        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
            goto top_retry;
        
        if (level != 1)
        {
            hocc64_t child_v = NODE_LOAD_VERSION(child);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = child;
            curr_v = child_v;
        } 
        else
        {
            NODE_READ_LOCK(child);
            if (!NODE_VALIDATE(curr, curr_v))
            {
                NODE_READ_UNLOCK(child);
                goto top_retry;
            }
            curr = child;
        }
    }

    node_header_t *prev = curr;
    while (curr->next_header <= current_start)
    {
        NODE_READ_LOCK(curr->next);
        prev = curr;
        curr = curr->next;
        NODE_READ_UNLOCK(prev);
    }

    leaf_node_t *leaf = (leaf_node_t *)curr;

    int num_elts = LOAD_RELAXED(leaf->header.num_elts);
    int rank = find_rank(leaf->keys, num_elts, current_start);

    if (LOAD_RELAXED(leaf->keys[rank]) != current_start || (current_start == BSL_KEY_MIN && leaf == list->headers[0]))
    {
        rank++;
        if (rank == num_elts)
        {
            leaf_node_t *next = (leaf_node_t *)LOAD_RELAXED(leaf->header.next);
            if (!next)
            {
                NODE_READ_UNLOCK(&leaf->header);
                return;
            }

            if (!NODE_READ_TRYLOCK(&next->header))
            {
                NODE_READ_UNLOCK(&leaf->header);
                goto top_retry;
            }

            NODE_READ_UNLOCK(&leaf->header);

            rank = 0;
            leaf = next;
        }
    }

    while (remaining)
    {
        int num = LOAD_RELAXED(leaf->header.num_elts);

        size_t batch_size = num - rank;
        if (remaining < batch_size)
            batch_size = remaining;

        if (batch_size > 0)
        {
            for (size_t i = 0; i < batch_size; ++i)
            {
                int ofs = rank + i;
                cb(leaf->keys[ofs], leaf->values[ofs], arg);
            }

            remaining -= batch_size;
            current_start = LOAD_RELAXED(leaf->keys[rank + batch_size - 1]) + 1;
        }


        leaf_node_t *next = (leaf_node_t *)LOAD_ACQUIRE(leaf->header.next);
        if (!next || remaining == 0)
        {
            NODE_READ_UNLOCK(&leaf->header);
            break;
        }

        if (unlikely(!NODE_READ_TRYLOCK(&next->header)))
        {
            NODE_READ_UNLOCK(&leaf->header);
            goto top_retry;
        }

        NODE_READ_UNLOCK(&leaf->header);
        rank = 0;
        leaf = next;
    }
}

void bsl_scan_n(bsl_t *list, bsl_key_t start, size_t length, range_cb cb, void *arg)
{
    epoch_enter();
    _bsl_scan_n(list, start, length, cb, arg);
    epoch_exit();
}

static inline void 
_bsl_scan_n_batch(bsl_t *list, bsl_key_t start, size_t length, range_batch_cb cb, void *arg)
{
    if (unlikely(length <= 0)) return;

    size_t remaining = length;
    bsl_key_t current_start = start;

top_retry:;

    node_header_t *curr = list->headers[MAX_LEVEL - 1];
    hocc64_t curr_v = NODE_LOAD_VERSION(curr);

    for (int level = MAX_LEVEL - 1; level > 0; level--)
    {
        while (LOAD_RELAXED(curr->next_header) <= current_start)
        {
            node_header_t *next = LOAD_RELAXED(curr->next);
            if (!next) break;

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = next;
            curr_v = next_v;
        }

        int rank = find_rank(NODE_KEYS(curr), LOAD_RELAXED(curr->num_elts), current_start);
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (!child) goto top_retry; 

        /* two-phase HOH validation, see bsl_get.c */
        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
            goto top_retry;
        
        if (level != 1)
        {
            hocc64_t child_v = NODE_LOAD_VERSION(child);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = child;
            curr_v = child_v;
        } 
        else
        {
            NODE_READ_LOCK(child);
            if (!NODE_VALIDATE(curr, curr_v))
            {
                NODE_READ_UNLOCK(child);
                goto top_retry;
            }
            curr = child;
        }
    }
    
    node_header_t *prev = curr;
    while (curr->next_header <= current_start)
    {
        NODE_READ_LOCK(curr->next);
        prev = curr;
        curr = curr->next;
        NODE_READ_UNLOCK(prev);
    }

    leaf_node_t *leaf = (leaf_node_t *)curr;

    int num_elts = LOAD_RELAXED(leaf->header.num_elts);
    int rank = find_rank(leaf->keys, num_elts, current_start);

    if (LOAD_RELAXED(leaf->keys[rank]) != current_start || (current_start == BSL_KEY_MIN && leaf == list->headers[0]))
    {
        rank++;
        if (rank == num_elts)
        {
            leaf_node_t *next = (leaf_node_t *)LOAD_RELAXED(leaf->header.next);
            if (!next)
            {
                NODE_READ_UNLOCK(&leaf->header);
                return;
            }

            if (!NODE_READ_TRYLOCK(&next->header))
            {
                NODE_READ_UNLOCK(&leaf->header);
                goto top_retry;
            }

            NODE_READ_UNLOCK(&leaf->header);

            rank = 0;
            leaf = next;
        }
    }

    while (remaining)
    {
        int num = LOAD_RELAXED(leaf->header.num_elts);

        size_t batch_size = num - rank;
        if (remaining < batch_size)
            batch_size = remaining;

        if (batch_size > 0)
        {
            bsl_range_t range = {
                .keys = &leaf->keys[rank],
                .vals = &leaf->values[rank],
                .count = batch_size
            };
            
            cb(range, arg);
            remaining -= batch_size;
            
            current_start = LOAD_RELAXED(leaf->keys[rank + batch_size - 1]) + 1;
        }

        leaf_node_t *next = (leaf_node_t *)LOAD_ACQUIRE(leaf->header.next);
        if (!next || remaining == 0)
        {
            NODE_READ_UNLOCK(&leaf->header);
            break;
        }

        if (unlikely(!NODE_READ_TRYLOCK(&next->header)))
        {
            NODE_READ_UNLOCK(&leaf->header);
            goto top_retry;
        }

        NODE_READ_UNLOCK(&leaf->header);
        rank = 0;
        leaf = next;
    }
}

static inline void 
_bsl_scan_n_batch_optimistic(bsl_t *list, bsl_key_t start, size_t length, range_batch_cb cb, void *arg)
{
    if (unlikely(length <= 0)) return;

    size_t remaining = length;
    bsl_key_t current_start = start;

top_retry:;

    node_header_t *curr = list->headers[MAX_LEVEL - 1];
    hocc64_t curr_v = NODE_LOAD_VERSION(curr);

    for (int level = MAX_LEVEL - 1; level > 0; level--)
    {
        while (LOAD_RELAXED(curr->next_header) <= current_start)
        {
            node_header_t *next = LOAD_RELAXED(curr->next);
            if (!next) break;

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = next;
            curr_v = next_v;
        }

        int rank = find_rank(NODE_KEYS(curr), LOAD_RELAXED(curr->num_elts), current_start);
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (!child) goto top_retry; 

        /* two-phase HOH validation, see bsl_get.c */
        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
            goto top_retry;
        
        hocc64_t child_v = NODE_LOAD_VERSION(child);

        if (!NODE_VALIDATE(curr, curr_v))
            goto top_retry;

        curr = child;
        curr_v = child_v;
    }
    
    while (LOAD_RELAXED(curr->next_header) <= current_start)
    {
        node_header_t *next = LOAD_RELAXED(curr->next);
        if (!next) break;

        hocc64_t next_v = NODE_LOAD_VERSION(next);

        if (!NODE_VALIDATE(curr, curr_v))
            goto top_retry;

        curr = next;
        curr_v = next_v;
    }

    leaf_node_t *leaf = (leaf_node_t *)curr;

    int num_elts = LOAD_RELAXED(leaf->header.num_elts);
    int rank = find_rank(leaf->keys, num_elts, current_start);

    if (LOAD_RELAXED(leaf->keys[rank]) != current_start || (current_start == BSL_KEY_MIN && leaf == list->headers[0]))
    {
        rank++;
        if (rank == num_elts)
        {
            leaf_node_t *next = (leaf_node_t *)LOAD_RELAXED(leaf->header.next);
            if (leaf->header.next_header == BSL_KEY_MAX)
            {
                return;
            }
            
            if (!next) goto top_retry;

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            rank = 0;
            leaf = next;
            curr_v = next_v;
        }
    }

    while (remaining)
    {
        int num = LOAD_RELAXED(leaf->header.num_elts);

        size_t batch_size = num - rank;
        if (remaining < batch_size)
            batch_size = remaining;

        if (batch_size > 0)
        {
            bsl_range_t range = {
                .keys = &leaf->keys[rank],
                .vals = &leaf->values[rank],
                .count = batch_size
            };
            
            cb(range, arg);
            remaining -= batch_size;
            
            current_start = LOAD_RELAXED(leaf->keys[rank + batch_size - 1]) + 1;
        }

        leaf_node_t *next = (leaf_node_t *)LOAD_ACQUIRE(leaf->header.next);
        if (leaf->header.next_header == BSL_KEY_MAX || remaining == 0)
        {
            break;
        }
        if (!next) goto top_retry;

        hocc64_t next_v = NODE_LOAD_VERSION(leaf);

        if (!NODE_VALIDATE(curr, curr_v))
            goto top_retry;

        rank = 0;
        leaf = next;
        curr_v = next_v;
    }
}

void bsl_scan_n_batch(bsl_t *list, bsl_key_t start, size_t length, range_batch_cb cb, void *arg)
{
    epoch_enter();
    _bsl_scan_n_batch_optimistic(list, start, length, cb, arg);
    epoch_exit();
}