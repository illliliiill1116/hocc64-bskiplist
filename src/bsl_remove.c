#include "bskiplist.h"
#include "node.h"
#include "epoch.h"
#include "random_level.h"
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <unistd.h>

static inline void delete_internal_slot(void *node, int rank)
{
    node_header_t *h = (node_header_t *)node;
    bsl_key_t *keys = NODE_KEYS(node);
    void **children = INTERNAL_CHILDREN(node);

    int num_to_move = h->num_elts - rank - 1;
    if (num_to_move > 0)
    {
        memmove(keys + rank, keys + rank + 1, num_to_move * sizeof(bsl_key_t));
        memmove(children + rank, children + rank + 1, num_to_move * sizeof(void *));
    }

    __atomic_sub_fetch(&h->num_elts, 1, __ATOMIC_RELAXED);
}

static inline void delete_leaf_slot(void *node, int rank)
{
    node_header_t *h = (node_header_t *)node;
    bsl_key_t *keys = NODE_KEYS(node);
    bsl_val_t *values = LEAF_VALUES(node);

    int num_to_move = h->num_elts - rank - 1;

    if (num_to_move > 0)
    {
        memmove(keys + rank, keys + rank + 1, num_to_move * sizeof(bsl_key_t));
        memmove(values + rank, values + rank + 1, num_to_move * sizeof(bsl_val_t));
    }

    __atomic_sub_fetch(&h->num_elts, 1, __ATOMIC_RELAXED);
}


static inline int _bsl_remove(bsl_t *list, bsl_key_t key)
{
    if (key <= BSL_KEY_MIN || key >= BSL_KEY_MAX)
        return -1;

    int level_to_promote = bsl_random_level(key);

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
        int rank = find_rank(keys, LOAD_RELAXED(curr->num_elts), key);
        
        node_header_t *child = LOAD_RELAXED(INTERNAL_CHILDREN(curr)[rank]);
        if (child == NULL) goto top_retry;

        hocc64_t child_v = NODE_LOAD_VERSION(child);

        if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
            goto top_retry;
        
        curr = child;
        curr_v = child_v;
    }

    assert(level == level_to_promote);

    NODE_WRITE_LOCK(curr);
    if (!NODE_VALIDATE(curr, curr_v + HOCC_WRITER_BIT))
    {
        NODE_WRITE_UNLOCK(curr);
        goto top_retry;
    }

    bsl_key_t *keys = NODE_KEYS(curr);
    int rank = find_rank(keys, curr->num_elts, key);
    int found_key = (keys[rank] == key);

    node_header_t *prev = NULL;

    while (!found_key && curr->next_header <= key)
    {
        NODE_WRITE_LOCK(curr->next);
        prev = curr;
        curr = curr->next;

        keys = NODE_KEYS(curr);
        rank = find_rank(keys, curr->num_elts, key);
        found_key = (keys[rank] == key);

        if (!found_key) 
            NODE_WRITE_UNLOCK(prev);
    }

    if (!found_key)
    {
        NODE_WRITE_UNLOCK(curr);
        return -1;
    }

    assert(NODE_KEYS(curr)[rank] == key);

    node_header_t *current;
    node_header_t *left_node;

    if (rank != 0)
    {
        if (curr->level == 0)
        {
            delete_leaf_slot(curr, rank);
            if (prev) 
                NODE_WRITE_UNLOCK(prev);
            NODE_WRITE_UNLOCK(curr);
            return 1;
        }
        else
        {
            node_header_t **children = (node_header_t **)INTERNAL_CHILDREN(curr);
            current = children[rank];
            left_node = children[rank - 1];

            delete_internal_slot(curr, rank);

            NODE_WRITE_LOCK(left_node);
            NODE_WRITE_LOCK_EVICTING(current);

            if (prev) 
                NODE_WRITE_UNLOCK(prev);
            NODE_WRITE_UNLOCK(curr);
        }
    }
    else
    {
        assert(prev);
        left_node = prev;
        current = curr;
    }

    assert(left_node && current);

    prev = NULL;
    for (level = current->level; level > 0; level--)
    {
        while (left_node->next != current)
        {
            NODE_WRITE_LOCK(left_node->next);
            prev = left_node;
            left_node = left_node->next;
            NODE_WRITE_UNLOCK(prev);
        }

        assert(NODE_KEYS(current)[0] == key);
        assert(left_node->next_header == key);

        node_header_t *old_curr = current;
        node_header_t *old_left = left_node;

        current = INTERNAL_CHILDREN(current)[0];
        left_node = INTERNAL_CHILDREN(left_node)[left_node->num_elts - 1];

        if (old_curr->num_elts == 1)
        {
            STORE_RELAXED(old_left->next_header, old_curr->next_header);
            STORE_RELAXED(old_left->next, old_curr->next);
            STORE_RELAXED(old_curr->num_elts, 0);

            NODE_WRITE_UNLOCK(old_curr);
            bsl_free_node(old_curr);
        }
        else
        {
            delete_internal_slot(old_curr, 0);
            STORE_RELAXED(old_left->next_header, NODE_KEYS(old_curr)[0]);
            NODE_WRITE_UNLOCK(old_curr);
        }

        NODE_WRITE_LOCK(left_node);
        NODE_WRITE_LOCK_EVICTING(current);
        NODE_WRITE_UNLOCK(old_left);
    }

    assert(current->level == 0 && left_node->level == 0);

    prev = NULL;
    while (left_node->next != current)
    {
        NODE_WRITE_LOCK(left_node->next);
        prev = left_node;
        left_node = left_node->next;
        NODE_WRITE_UNLOCK(prev);
    }

    if (current->num_elts == 1)
    {
        STORE_RELAXED(left_node->next_header, current->next_header);
        STORE_RELAXED(left_node->next, current->next);
        STORE_RELAXED(current->num_elts, 0);
        NODE_WRITE_UNLOCK(current);
        bsl_free_node(current);
    }
    else
    {
        delete_leaf_slot(current, 0);
        STORE_RELAXED(left_node->next_header, NODE_KEYS(current)[0]);
        NODE_WRITE_UNLOCK(current);
    }

    NODE_WRITE_UNLOCK(left_node);

    return 1;
}

int bsl_remove(bsl_t *list, bsl_key_t key)
{
    epoch_enter();
    int ret = _bsl_remove(list, key);
    epoch_exit();
    return ret;
}