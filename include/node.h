#ifndef BSL_NODE_H
#define BSL_NODE_H

#include <stdint.h>
#include <stddef.h>
#include "params.h"
#include "util.h"
#include "hocc64.h"

typedef struct {
    hocc64_t ctrl;
    void *next;
    bsl_key_t next_header;
    uint32_t num_elts;
    uint32_t level;
} /* __attribute__((aligned(CACHE_LINE_SIZE))) */ node_header_t;
//STATIC_ASSERT(sizeof(node_header_t) == CACHE_LINE_SIZE);

#define NODE_HEADER_SIZE (sizeof(node_header_t))
#define B_INTERNAL ((NODE_SIZE - NODE_HEADER_SIZE) / (sizeof(bsl_key_t) + sizeof(void*)))
#define B_LEAF ((NODE_SIZE - NODE_HEADER_SIZE) / (sizeof(bsl_key_t) + sizeof(bsl_val_t)))

typedef struct internal_node {
    node_header_t header;
    bsl_key_t keys[B_INTERNAL];
    void *children[B_INTERNAL];
} internal_node_t;

typedef struct leaf_node {
    node_header_t header;
    bsl_key_t keys[B_LEAF];
    bsl_val_t values[B_LEAF];
} leaf_node_t;

#define LEAF_VALUES_OFFSET       (offsetof(leaf_node_t, values))
#define INTERNAL_CHILDREN_OFFSET (offsetof(internal_node_t, children))

#define NODE_KEYS(n)            ((bsl_key_t *)((node_header_t *)(n) + 1))
#define INTERNAL_CHILDREN(n)    (((internal_node_t *)(n))->children)
#define LEAF_VALUES(n)          (((leaf_node_t *)(n))->values)

/* --- HOCC-64 Synchronization Wrappers for Node Header --- */

#define NODE_LOAD_VERSION(n)        hocc_load(&((node_header_t *)(n))->ctrl)
#define NODE_VALIDATE(n, v)         hocc_validate(&((node_header_t *)(n))->ctrl, (v))
#define NODE_READ_LOCK(n)           hocc_read_lock(&((node_header_t *)(n))->ctrl)
#define NODE_READ_TRYLOCK(n)        hocc_read_trylock(&((node_header_t *)(n))->ctrl)
#define NODE_READ_UNLOCK(n)         hocc_read_unlock(&((node_header_t *)(n))->ctrl)
#define NODE_WRITE_LOCK(n)          hocc_write_lock(&((node_header_t *)(n))->ctrl)
#define NODE_WRITE_LOCK_EVICTING(n) hocc_write_lock_evicting(&((node_header_t *)(n))->ctrl)
#define NODE_WRITE_UNLOCK(n)        hocc_write_unlock(&((node_header_t *)(n))->ctrl)


static inline int find_rank(bsl_key_t *keys, int num_elts, bsl_key_t key)
{
    if (num_elts < 0) return 0;

    bsl_key_t *base = keys;
    int n = num_elts;

    while (n > 1)
    {
        int half = n >> 1;
        base = (LOAD_RELAXED(base[half]) <= key) ? &base[half] : base;
        n -= half;
    }

    int rank = (int)(base - keys);
    if (LOAD_RELAXED(keys[rank]) > key) return (rank > 0) ? rank - 1 : 0;
    return rank;
}

#endif