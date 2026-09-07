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
#include "stats.h"
#include <assert.h>
#include <string.h>

typedef struct {
    bsl_key_t *keys;
    bsl_val_t *vals;
} scan_out_t;

static inline size_t
_bsl_scan_n_batch_optimistic(bsl_t *list, bsl_key_t start, size_t length, void *arg,
                             const scan_out_t *out)
{
    if (unlikely(length <= 0)) return 0;

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
            if (!next)
            {
                RECORD_RETRY();
                goto top_retry;
            }

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(curr, curr_v))
            {
                RECORD_RETRY();
                goto top_retry;
            }

            curr = next;
            curr_v = next_v;
        }

        int rank = find_rank(NODE_KEYS(curr), LOAD_RELAXED(curr->num_elts), current_start);
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (!child)
        {
            RECORD_RETRY();
            goto top_retry; 
        }

        /* two-phase HOH validation, see bsl_get.c */
        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
        {
            RECORD_RETRY();
            goto top_retry; 
        }
        
        hocc64_t child_v = NODE_LOAD_VERSION(child);

        if (!NODE_VALIDATE(curr, curr_v))
        {
            RECORD_RETRY();
            goto top_retry;
        }

        curr = child;
        curr_v = child_v;
    }
    
    while (LOAD_RELAXED(curr->next_header) <= current_start)
    {
        node_header_t *next = LOAD_RELAXED(curr->next);
        if (!next)
        {
            RECORD_RETRY();
            goto top_retry;
        }

        hocc64_t next_v = NODE_LOAD_VERSION(next);

        if (!NODE_VALIDATE(curr, curr_v))
        {
            RECORD_RETRY();
            goto top_retry;
        }

        curr = next;
        curr_v = next_v;
    }

    leaf_node_t *leaf = (leaf_node_t *)curr;

    if (curr_v & HOCC_WRITER_BIT)
    {
        RECORD_RETRY("Leaf: writer active on entry");
        goto top_retry;
    }

    int num_elts = LOAD_RELAXED(leaf->header.num_elts);
    int rank = find_rank(leaf->keys, num_elts, current_start);

    if (LOAD_RELAXED(leaf->keys[rank]) != current_start || (current_start == BSL_KEY_MIN && leaf == list->headers[0]))
    {
        rank++;
        if (rank == num_elts)
        {
            bsl_key_t next_header = LOAD_RELAXED(leaf->header.next_header);
            leaf_node_t *next = (leaf_node_t *)LOAD_ACQUIRE(leaf->header.next);

            if (!NODE_VALIDATE(leaf, curr_v))
            {
                RECORD_RETRY();
                goto top_retry;
            }

            if (next_header == BSL_KEY_MAX)
                return 0;

            if (!next)
            {
                RECORD_RETRY();
                goto top_retry;
            }

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(leaf, curr_v))
            {
                RECORD_RETRY();
                goto top_retry;
            }

            rank = 0;
            leaf = next;
            curr_v = next_v;
        }
    }

    while (remaining)
    {
        if (curr_v & HOCC_WRITER_BIT)
        {
            RECORD_RETRY("Leaf: writer active");
            goto top_retry;
        }

        int num = LOAD_RELAXED(leaf->header.num_elts);

        if (rank > num)
        {
            RECORD_RETRY("Leaf: stale rank");
            goto top_retry;
        }

        size_t batch_size = (size_t)(num - rank);
        if (remaining < batch_size)
            batch_size = remaining;

        if (batch_size > 0)
        {
            bsl_range_t range = {
                .keys = &leaf->keys[rank],
                .vals = &leaf->values[rank],
                .count = batch_size
            };
            
            uint64_t local_sum = 0;
            size_t offset = length - remaining;

            if (out)
            {
                memcpy(out->keys + offset, range.keys,
                       batch_size * sizeof(bsl_key_t));
                memcpy(out->vals + offset, range.vals,
                       batch_size * sizeof(bsl_val_t));
            }
            else
            {
                for (size_t i = 0; i < range.count; i++)
                {
                    local_sum += range.keys[i];
                    local_sum += range.vals[i];
                }
            }

            bsl_key_t last_key = range.keys[batch_size - 1];

            if (!NODE_VALIDATE(leaf, curr_v))
            {
                RECORD_RETRY("Leaf: diff version after summing");
                goto top_retry;
            }

            if (!out)
                *(uint64_t *)arg += local_sum;

            remaining -= batch_size;
            current_start = last_key + 1;
        }

        if (remaining == 0)
        {
            break;
        }

        bsl_key_t next_header = LOAD_RELAXED(leaf->header.next_header);
        leaf_node_t *next = (leaf_node_t *)LOAD_ACQUIRE(leaf->header.next);

        if (!NODE_VALIDATE(leaf, curr_v))
        {
            RECORD_RETRY("Leaf: Forward");
            goto top_retry;
        }

        if (next_header == BSL_KEY_MAX)
        {
            break;
        }

        if (!next)
        {
            RECORD_RETRY();
            goto top_retry;
        }

        hocc64_t next_v = NODE_LOAD_VERSION(next);

        if (!NODE_VALIDATE(leaf, curr_v))
        {
            RECORD_RETRY("Leaf: Forward");
            goto top_retry;
        }

        rank = 0;
        leaf = next;
        curr_v = next_v;
    }

    return length - remaining;
}

void bsl_scan_n_batch(bsl_t *list, bsl_key_t start, size_t length, void *arg)
{
    epoch_enter();
    _bsl_scan_n_batch_optimistic(list, start, length, arg, NULL);
    epoch_exit();
}

size_t bsl_scan_n(bsl_t *list, bsl_key_t start, size_t length,
                  bsl_key_t *out_keys, bsl_val_t *out_vals, size_t capacity)
{
    if (unlikely(!out_keys || !out_vals)) return 0;
    if (length > capacity) length = capacity;

    scan_out_t out = { out_keys, out_vals };

    epoch_enter();
    size_t n = _bsl_scan_n_batch_optimistic(list, start, length, NULL, &out);
    epoch_exit();
    return n;
}