#include "node.h"
#include <stdlib.h>
#include <string.h>

void *bsl_node_alloc(void)
{
    void* ptr;
    if (posix_memalign(&ptr, CACHE_LINE_SIZE, NODE_SIZE) != 0)
        return NULL;
    memset(ptr, 0, NODE_SIZE);
    return ptr;
}


void bsl_node_destroy(void* ptr)
{
    if (!ptr) return;
    free(ptr);
}