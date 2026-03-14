#include "bskiplist.h"
#include "epoch.h"
#include "node.h"
#include <stdlib.h>

bsl_t* bsl_new()
{
    bsl_t *list = (bsl_t*)malloc(sizeof(bsl_t));

    if (!list) return NULL;

    leaf_node_t *leaf_sentinel = (leaf_node_t*)bsl_alloc_node();

    if (!leaf_sentinel)
    {
        free(list); 
        return NULL;
    }
    
    leaf_sentinel->header.level = 0;
    leaf_sentinel->header.num_elts = 1; 
    leaf_sentinel->header.ctrl = HOCC_INIT;
    leaf_sentinel->header.next = NULL;
    leaf_sentinel->header.next_header = BSL_KEY_MAX;
    leaf_sentinel->keys[0] = BSL_KEY_MIN;
    list->headers[0] = leaf_sentinel;

    for (int i = 1; i < MAX_LEVEL; i++)
    {
        internal_node_t *internal_sentinel = (internal_node_t*)bsl_alloc_node();
        if (!internal_sentinel) 
        {
            for (int j = 0; j < i; j++) 
                free(list->headers[j]);
            free(list);
            return NULL; 
        }
        internal_sentinel->header.level = i;
        internal_sentinel->header.num_elts = 1;
        internal_sentinel->header.ctrl = HOCC_INIT;
        internal_sentinel->header.next = NULL;
        internal_sentinel->header.next_header = BSL_KEY_MAX;
        internal_sentinel->keys[0] = BSL_KEY_MIN;
        internal_sentinel->children[0] = list->headers[i-1];
        list->headers[i] = internal_sentinel;
    }

    return list;
}

void bsl_destroy(bsl_t *list)
{
    if (!list) return;

    epoch_enter();

    for (int l = 0; l < MAX_LEVEL; l++)
    {
        node_header_t *curr = (node_header_t *)list->headers[l];
        while (curr) 
        {
            node_header_t *next = (node_header_t *)curr->next;
            free(curr);
            curr = next;
        }
    }

    free(list);

    epoch_exit();
}