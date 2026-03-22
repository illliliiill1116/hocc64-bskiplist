#include "bskiplist.h"
#include "epoch.h"
#include "node.h"
#include <stdlib.h>

bsl_t* bsl_new(void)
{
    int i, j;

    bsl_t *list = (bsl_t*)malloc(sizeof(bsl_t));
    if (!list) return NULL;

    /* level 0：leaf sentinel */
    leaf_node_t *leaf_sentinel = (leaf_node_t*)bsl_node_alloc();
    if (!leaf_sentinel)
    {
        free(list);
        return NULL;
    }

    leaf_sentinel->header.ctrl        = HOCC_INIT;
    leaf_sentinel->header.level       = 0;
    leaf_sentinel->header.num_elts    = 1;
    leaf_sentinel->header.next        = NULL;
    leaf_sentinel->header.next_header = BSL_KEY_MAX;
    leaf_sentinel->keys[0]            = BSL_KEY_MIN;
    list->headers[0] = (node_header_t*)leaf_sentinel;

    for (i = 1; i < MAX_LEVEL; i++)
    {
        internal_node_t *s = (internal_node_t*)bsl_node_alloc();
        if (!s)
        {
            for (j = 0; j < i; j++)
                bsl_node_destroy(list->headers[j]);
            free(list);
            return NULL;
        }

        s->header.ctrl        = HOCC_INIT;
        s->header.level       = i;
        s->header.num_elts    = 1;
        s->header.next        = NULL;
        s->header.next_header = BSL_KEY_MAX;
        s->keys[0]            = BSL_KEY_MIN;
        s->children[0]        = list->headers[i - 1];
        list->headers[i]      = (node_header_t*)s;
    }

    return list;
}


void bsl_destroy(bsl_t *list)
{
    if (!list) return;

    epoch_enter();
    epoch_exit();

    ebr_sync();

    for (int l = 0; l < MAX_LEVEL; l++)
    {
        node_header_t *curr = list->headers[l];
        while (curr)
        {
            node_header_t *next = (node_header_t*)curr->next;
            bsl_node_destroy(curr);
            curr = next;
        }
    }

    free(list);
}