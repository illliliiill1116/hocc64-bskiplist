#include "bskiplist.h"
#include "node.h"
#include "epoch.h"
#include <assert.h>

static inline int _bsl_get_value(bsl_t *list, bsl_key_t key, bsl_val_t *out_val)
{
    if (key <= BSL_KEY_MIN || key >= BSL_KEY_MAX) return -1;

top_retry:;

    node_header_t *curr = (node_header_t *)list->headers[MAX_LEVEL - 1];
    hocc64_t curr_v = NODE_LOAD_VERSION(curr);

    for (int i = MAX_LEVEL - 1; i >= 0; i--)
    {
        /* Horizontal traversal*/
        while (LOAD_RELAXED(curr->next_header) <= key)
        {
            node_header_t *next = LOAD_RELAXED(curr->next);
            if (!next) break;

            hocc64_t next_v = NODE_LOAD_VERSION(next);
            if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = next;
            curr_v = next_v;
        }

        int num = LOAD_RELAXED(curr->num_elts);
        bsl_key_t *keys = NODE_KEYS(curr);
        int rank = find_rank(keys, num, key);

        if (i == 0) /* Leaf level */
        {
            if (num > 0 && LOAD_RELAXED(keys[rank]) == key)
            {
                bsl_val_t v = LOAD_RELAXED(LEAF_VALUES(curr)[rank]);

                if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
                    goto top_retry;

                if (out_val) *out_val = v;
                return 0;
            }
            return -1;
        } 
        else /* Internal level: drop down */
        {
            void **children = INTERNAL_CHILDREN(curr);
            node_header_t *child = (node_header_t *)LOAD_RELAXED(children[rank]);
            if (!child) goto top_retry;
          
            hocc64_t child_v = NODE_LOAD_VERSION(child);
            if (curr_v & HOCC_WRITER_BIT || !NODE_VALIDATE(curr, curr_v))
                goto top_retry;

            curr = child;
            curr_v = child_v;
        }
    }
    return -1;
}

int bsl_get(bsl_t *list, bsl_key_t key, bsl_val_t *out_val)
{
    epoch_enter();
    int ret = _bsl_get_value(list, key, out_val);
    epoch_exit();
    return ret;
}