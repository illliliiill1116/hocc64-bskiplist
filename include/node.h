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
/* STATIC_ASSERT(sizeof(node_header_t) == CACHE_LINE_SIZE); */

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

#define NODE_GC_NEXT_OFFSET (NODE_SIZE - sizeof(void*))
STATIC_ASSERT(NODE_GC_NEXT_OFFSET >= LEAF_VALUES_OFFSET);
STATIC_ASSERT(NODE_GC_NEXT_OFFSET >= INTERNAL_CHILDREN_OFFSET);

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


/*
 * find_rank_locked - linear scan under lock protection
 *
 * Caller must hold a read or write lock on the node, which establishes
 * a happens-before with the last writer. Plain loads are safe and allow
 * the compiler to vectorize the inner loop.
 */
static inline uint32_t
find_rank_locked(bsl_key_t *keys, uint32_t num_elts, bsl_key_t key)
{
    uint32_t i = 0;
    while (i < num_elts && keys[i] <= key)
        i++;
    return i > 0 ? i - 1 : 0;
}


/*
 * find_rank_optimistic - binary search without lock protection
 *
 * Used in optimistic traversal where no lock is held. LOAD_RELAXED
 * prevents the compiler from caching stale values across iterations.
 * The caller must validate the parent node after this call to confirm
 * the result was observed in a consistent state.
 */
static inline uint32_t
find_rank_optimistic(bsl_key_t *keys, uint32_t num_elts, bsl_key_t key)
{
    bsl_key_t *base = keys;
    uint32_t n = num_elts;
    while (n > 1)
    {
        uint32_t half = n >> 1;
        base = (LOAD_RELAXED(base[half]) <= key) ? &base[half] : base;
        n -= half;
    }

    uint32_t rank = (uint32_t)(base - keys);
    if (LOAD_RELAXED(keys[rank]) > key) return (rank > 0) ? rank - 1 : 0;
    return rank;
}

void *bsl_node_alloc(void);
void bsl_node_destroy(void* ptr);

#endif