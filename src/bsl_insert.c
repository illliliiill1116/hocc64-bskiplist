/*
 * bsl_insert.c
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
#include "bsl_level.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>

static inline int 
split_keys(void *src_node, void *dst_node, uint32_t start_rank, int dst_offset) 
{
    node_header_t *src_h = (node_header_t *)src_node;
    node_header_t *dst_h = (node_header_t *)dst_node;
    
    uint32_t move_cnt = src_h->num_elts - start_rank;
    assert(start_rank <= src_h->num_elts);

    bsl_key_t *src_keys = NODE_KEYS(src_node);
    bsl_key_t *dst_keys = NODE_KEYS(dst_node);

    memmove(dst_keys + dst_offset, src_keys + start_rank, move_cnt * sizeof(bsl_key_t));

    STORE_RELAXED(src_h->num_elts, (uint32_t)start_rank);
    __atomic_add_fetch(&dst_h->num_elts, (uint32_t)move_cnt, __ATOMIC_RELAXED);
    return move_cnt;
}

static inline void
insert_internal_slot(void *node, int rank, bsl_key_t key, void *child)
{
    node_header_t *h = (node_header_t *)node;
    bsl_key_t *keys = NODE_KEYS(node);
    void **children = INTERNAL_CHILDREN(node);

    memmove(keys + rank + 1, keys + rank, (h->num_elts - rank) * sizeof(bsl_key_t));
    memmove(children + rank + 1, children + rank, (h->num_elts - rank) * sizeof(void *));

    STORE_RELAXED(keys[rank], key);
    STORE_RELAXED(children[rank], child);

    __atomic_add_fetch(&h->num_elts, 1, __ATOMIC_RELAXED);
}

static inline void
insert_leaf_slot(void *node, int rank, bsl_key_t key, bsl_val_t value)
{
    node_header_t *h = (node_header_t *)node;
    bsl_key_t *keys = NODE_KEYS(node);
    bsl_val_t *values = LEAF_VALUES(node);

    memmove(keys + rank + 1, keys + rank, (h->num_elts - rank) * sizeof(bsl_key_t));
    memmove(values + rank + 1, values + rank, (h->num_elts - rank) * sizeof(bsl_val_t));

    STORE_RELAXED(keys[rank], key);
    STORE_RELAXED(values[rank], value);

    __atomic_add_fetch(&h->num_elts, 1, __ATOMIC_RELAXED);
}

static inline int 
_bsl_insert(bsl_t *list, bsl_key_t key, bsl_val_t value)
{
    if (key <= BSL_KEY_MIN || key >= BSL_KEY_MAX)
        return 0;

    int level_to_promote = bsl_level_for_key(key);

    void* new_nodes[MAX_LEVEL] = {0}; 
    for (int i = 0; i < level_to_promote; i++)
    {
        new_nodes[i] = bsl_node_alloc();
        NODE_WRITE_LOCK(new_nodes[i]);
    }

top_retry:;

    node_header_t *curr = list->headers[MAX_LEVEL - 1];
    hocc64_t curr_v = NODE_LOAD_VERSION(curr);

    int level;
    for (level = MAX_LEVEL - 1; level > level_to_promote; level--)
    {
        while (LOAD_RELAXED(curr->next_header) <= key)
        {
            node_header_t *next = LOAD_RELAXED(curr->next);
            if (next == NULL)
                goto top_retry; 
            hocc64_t next_v = NODE_LOAD_VERSION(next);

            if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = next;
            curr_v = next_v;
        }

        bsl_key_t *keys = NODE_KEYS(curr);
        int rank = find_rank_optimistic(keys, LOAD_RELAXED(curr->num_elts), key);
        
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (child == NULL) goto top_retry;
        
        /* two-phase HOH validation, see bsl_get.c */
        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
            goto top_retry;
        
        hocc64_t child_v = NODE_LOAD_VERSION(child); 

        if (!NODE_VALIDATE(curr, curr_v))
            goto top_retry;

        curr = child;
        curr_v = child_v;
    }
    while (LOAD_RELAXED(curr->next_header) <= key)
    {
        node_header_t *next = LOAD_RELAXED(curr->next);

        if (next == NULL)
            goto top_retry;

        hocc64_t next_v = NODE_LOAD_VERSION(next);

        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
            goto top_retry;

        curr = next;
        curr_v = next_v;
    }
    
    assert(level == level_to_promote);

    NODE_WRITE_LOCK(curr);
    if (!NODE_VALIDATE(curr, curr_v + HOCC_WRITER_BIT))
    {
        NODE_WRITE_UNLOCK(curr);
        goto top_retry;
    }

    bsl_key_t *keys = NODE_KEYS(curr);
    uint32_t rank = find_rank_locked(keys, curr->num_elts, key);
    int found_key = (keys[rank] == key);

    if (found_key)
    {
        if (curr->level == 0)
        {
            assert(level_to_promote == 0);
            STORE_RELAXED(LEAF_VALUES(curr)[rank], value);
            NODE_WRITE_UNLOCK(curr);
            return 1;
        }
        else
        {
            node_header_t *child = (node_header_t *)INTERNAL_CHILDREN(curr)[rank];
            hocc64_t child_v = NODE_LOAD_VERSION(child);
            
            NODE_WRITE_UNLOCK(curr);
            
            while (LOAD_RELAXED(child->level) > 0)
            {
                node_header_t *next_child = (node_header_t *)INTERNAL_CHILDREN(child)[0];
                if (next_child == NULL)
                    goto top_retry;

                /* two-phase HOH validation, see bsl_get.c */
                if (child_v & HOCC_WRITER_BIT || !NODE_VALIDATE(child, child_v))
                    goto top_retry;

                hocc64_t next_v = NODE_LOAD_VERSION(next_child);

                if (!NODE_VALIDATE(child, child_v))
                    goto top_retry;

                child = next_child;
                child_v = next_v;
            }

            NODE_WRITE_LOCK(child);
            if (!NODE_VALIDATE(child, child_v + HOCC_WRITER_BIT))
            {
                NODE_WRITE_UNLOCK(child);
                goto top_retry;
            }

            STORE_RELAXED(LEAF_VALUES(child)[0], value);

            NODE_WRITE_UNLOCK(child);

            for (int i = 0; i < level_to_promote; i++)
            {
                if (new_nodes[i]) bsl_node_destroy(new_nodes[i]);
            }
            return 1;
        } 
    }

    node_header_t *parent_to_unlock = NULL;
    int num_split = 0;

    uint32_t max_key = (level == 0) ? B_LEAF : B_INTERNAL;
    if (curr->num_elts + 1 > max_key)
    {
        node_header_t *new_node = bsl_node_alloc();
        NODE_WRITE_LOCK(new_node);

        new_node->next = curr->next;
        new_node->next_header = curr->next_header;
        new_node->level = level;

        int half_keys = curr->num_elts / 2;
        
        int elts_moved = split_keys(curr, new_node, half_keys, 0);

        STORE_RELAXED(curr->next_header, NODE_KEYS(new_node)[0]);
        STORE_RELAXED(curr->next, new_node);

        if (level > 0)
        {
            memmove(
                INTERNAL_CHILDREN(new_node),
                INTERNAL_CHILDREN(curr) + half_keys,
                elts_moved * sizeof(void *)
            ); 
        }
        else
        {
            memmove(
                LEAF_VALUES(new_node),
                LEAF_VALUES(curr) + half_keys,
                elts_moved * sizeof(bsl_val_t)
            );
        }

        if (rank + 1 <= curr->num_elts)
        {
            NODE_WRITE_UNLOCK(new_node);
            assert(num_split == 0);
            if (level > 0)
            {
                insert_internal_slot(curr, rank + 1, key, new_nodes[num_split]);
                parent_to_unlock = curr;
                curr = INTERNAL_CHILDREN(curr)[rank];
            }
            else
            {
                insert_leaf_slot(curr, rank + 1, key, value);
                NODE_WRITE_UNLOCK(curr);
                return 1;
            }
        }
        else
        {
            assert(new_node->num_elts > 0);
            int rank_to_insert = rank - curr->num_elts;

            NODE_WRITE_UNLOCK(curr);

            if (level > 0)
            {
                insert_internal_slot(new_node, rank_to_insert + 1, key, new_nodes[num_split]);
                parent_to_unlock = new_node;
                curr = (node_header_t *)INTERNAL_CHILDREN(new_node)[rank_to_insert];
            } else {
                insert_leaf_slot(new_node, rank_to_insert + 1, key, value);
                NODE_WRITE_UNLOCK(new_node);
                return 1;
            }
        }
    }
    else
    {
        assert(level_to_promote == level);
        if (level > 0)
        {
            insert_internal_slot(curr, rank + 1, key, new_nodes[num_split]);
            parent_to_unlock = curr;
            curr = (node_header_t *)INTERNAL_CHILDREN(curr)[rank];
        } else {
            insert_leaf_slot(curr, rank + 1, key, value);
            NODE_WRITE_UNLOCK(curr);
            return 1;
        }
    }
    

    for (; level-- > 0;)
    {
        NODE_WRITE_LOCK(curr);

        if (parent_to_unlock)
        {
            NODE_WRITE_UNLOCK(parent_to_unlock);
            parent_to_unlock = NULL;
        }

        node_header_t *prev = curr;

        while (curr->next_header <= key)
        {
            NODE_WRITE_LOCK(curr->next);
            prev = curr;
            curr = curr->next;
            NODE_WRITE_UNLOCK(prev);
        }

        bsl_key_t *keys = NODE_KEYS(curr);
        int rank = find_rank_locked(keys, curr->num_elts, key);

        assert(level_to_promote > level);
        node_header_t *new_node = (node_header_t *)new_nodes[num_split];
        num_split++;

        STORE_RELAXED(new_node->next_header, curr->next_header);
        STORE_RELAXED(new_node->next, curr->next);
        STORE_RELAXED(new_node->level, level);

        if (level > 0)
        {
            insert_internal_slot(new_node, 0, key, new_nodes[num_split]);
        } 
        else
        {
            insert_leaf_slot(new_node, 0, key, value);
        }

        uint32_t elts_moved = split_keys(curr, new_node, rank + 1, 1);

        if (level > 0)
        {
            memmove(
                INTERNAL_CHILDREN(new_node) + 1,
                INTERNAL_CHILDREN(curr) + rank + 1,
                elts_moved * sizeof(void *)
            ); 
        }
        else
        {
            memmove(
                LEAF_VALUES(new_node) + 1,
                LEAF_VALUES(curr) + rank + 1,
                elts_moved * sizeof(bsl_val_t)
            );
        }
        
        STORE_RELAXED(curr->next_header, NODE_KEYS(new_node)[0]);
        STORE_RELAXED(curr->next, new_node);


        /*
        bsl_key_t next_header = new_node->next_header;
        if (level == 0 && bsl_random_level(next_header) == 0 && next_header != BSL_KEY_MAX)
        {
            node_header_t *next_node = (node_header_t *)new_node->next;
            NODE_WRITE_LOCK(next_node);

            int total_elts = new_node->num_elts + next_node->num_elts;

            if (total_elts <= (B_LEAF * 7 / 10)) 
            {
                memcpy(NODE_KEYS(new_node) + new_node->num_elts, NODE_KEYS(next_node), next_node->num_elts * sizeof(bsl_key_t));
                memcpy(LEAF_VALUES(new_node) + new_node->num_elts, LEAF_VALUES(next_node), next_node->num_elts * sizeof(bsl_val_t));

                new_node->num_elts = total_elts;
                STORE_RELAXED(new_node->next, next_node->next);
                STORE_RELAXED(new_node->next_header, next_node->next_header);

                ebr_retire(next_node);
            }
                
            NODE_WRITE_UNLOCK(next_node);
        }
        */
    

        NODE_WRITE_UNLOCK(new_node);

        if (level > 0)
        {
            parent_to_unlock = curr;
            curr = (node_header_t *)INTERNAL_CHILDREN(curr)[rank];
        }
        else
        {
            NODE_WRITE_UNLOCK(curr);
        }
    }

    assert(num_split == level_to_promote);
    return 1;
}

int bsl_insert(bsl_t *list, bsl_key_t key, bsl_val_t value)
{
    epoch_enter();
    int ret = _bsl_insert(list, key, value);
    epoch_exit();
    return ret;
}