#ifndef EPOCH_H
#define EPOCH_H

#include <stdint.h>
#include <stdlib.h>
#include <node.h>

#define EPOCH_COUNT     3 
#define GC_THRESHOLD    1024

/**
 * GC next pointer.
 * RATIONALE: To avoid TSan false positives, place 'next' at the absolute end.
 * This prevents HOCC Readers (find_key) from overlapping with the intrusive 
 * link while scanning the 'keys' region.
 */
#define NODE_GC_NEXT_OFFSET (NODE_SIZE - sizeof(void*))
STATIC_ASSERT(NODE_GC_NEXT_OFFSET >= LEAF_VALUES_OFFSET);
STATIC_ASSERT(NODE_GC_NEXT_OFFSET >= INTERNAL_CHILDREN_OFFSET);

typedef struct {
    /* Read-mostly by others */
        int local_epoch;
        int in_critical;
    char __padding[CACHE_LINE_SIZE - sizeof(int) * 2];

    void* pending_gc[EPOCH_COUNT];
    uint64_t op_count;
} __attribute__((aligned(CACHE_LINE_SIZE))) thread_context_t;

extern __thread thread_context_t* my_ctx;

void epoch_enter(void);
void epoch_exit(void);

void* bsl_alloc_node(void);
void bsl_free_node(void* ptr);
void try_gc(void);

#endif