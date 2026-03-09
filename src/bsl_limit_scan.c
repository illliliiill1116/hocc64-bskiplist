#include "bskiplist.h"
#include "node.h"
#include "epoch.h"
#include <assert.h>
#include <string.h>

static inline void 
_bsl_limit_scan(bsl_t *list, bsl_key_t start, size_t limit, range_cb cb, void *arg)
{
    if (limit <= 0) return;

    size_t found_count = 0;
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
                break;

            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (!NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = next;
            curr_v = next_v;
        }

        int rank = find_rank(NODE_KEYS(curr), LOAD_RELAXED(curr->num_elts), current_start);
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (!child) goto top_retry; 

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

    leaf_node_t *leaf = (leaf_node_t *)curr;

    while (leaf && found_count < limit)
    {
        int num = LOAD_RELAXED(leaf->header.num_elts);

        int i = 0;
        if (num > 0 && leaf->keys[0] < current_start)
        {
            i = find_rank(leaf->keys, num, current_start);
        }

        int remaining_needed = limit - found_count;
        int remaining_in_node = num - i;
        int batch_size = (remaining_in_node < remaining_needed) ? remaining_in_node : remaining_needed;
        int end_idx = i + batch_size;

        for (; i < end_idx; i++)
        {
            cb(leaf->keys[i], leaf->values[i], arg);
        }

        found_count += batch_size;
        if (batch_size > 0)
        {
            current_start = leaf->keys[end_idx - 1] + 1;
        }

        if (found_count >= limit)
        {
            NODE_READ_LOCK(&leaf->header);
            return;
        }

        leaf_node_t *next = (leaf_node_t *)LOAD_RELAXED(leaf->header.next);
        
        if (next == NULL)
        {
            NODE_READ_UNLOCK(&leaf->header);
            break;
        }

        if (!NODE_READ_TRYLOCK(&next->header))
        {
            NODE_READ_UNLOCK(&leaf->header);
            goto top_retry;
        }

        NODE_READ_UNLOCK(&leaf->header);
        
        leaf = next;
    }
}

void bsl_limit_scan(bsl_t *list, bsl_key_t start, size_t limit, range_cb cb, void *arg)
{
    epoch_enter();
    _bsl_limit_scan(list, start, limit, cb, arg);
    epoch_exit();
}

static inline void 
_bsl_limit_scan_batch(bsl_t *list, bsl_key_t start, size_t limit, range_batch_cb cb, void *arg)
{
    if (unlikely(limit <= 0)) return;

    size_t found_count = 0;
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

    leaf_node_t *leaf = (leaf_node_t *)curr;

    while (leaf && found_count < limit)
    {
        int num = LOAD_RELAXED(leaf->header.num_elts);
        
        int i = (found_count == 0) ? find_rank(leaf->keys, num, current_start) : 0;

        int remaining_in_node = num - i;
        int remaining_needed = (int)(limit - found_count);
        int batch_size = (remaining_in_node < remaining_needed) ? remaining_in_node : remaining_needed;

        if (batch_size > 0)
        {
            bsl_range_t range = {
                .keys = &leaf->keys[i],
                .vals = &leaf->values[i],
                .count = (size_t)batch_size
            };
            
            cb(range, arg);
            found_count += batch_size;
            
            current_start = LOAD_RELAXED(leaf->keys[i + batch_size - 1]) + 1;
        }


        leaf_node_t *next = (leaf_node_t *)LOAD_RELAXED(leaf->header.next);
        if (!next || found_count >= limit)
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
        leaf = next;
    }
}

void bsl_limit_scan_batch(bsl_t *list, bsl_key_t start, size_t limit, range_batch_cb cb, void *arg)
{
    epoch_enter();
    _bsl_limit_scan_batch(list, start, limit, cb, arg);
    epoch_exit();
}