#include "bsl_inspect.h"
#include "node.h"
#include <stdio.h>
#include <stdint.h>

static node_header_t *
leaf_head(bsl_t *list)
{
    return (node_header_t *)list->headers[0];
}

static node_header_t *
level_head(bsl_t *list, int level)
{
    return (node_header_t *)list->headers[level];
}

static void
collect_stats(bsl_t *list, bsl_inspect_result_t *r)
{
    node_header_t *curr = leaf_head(list);
    while (curr)
    {
        uint32_t       num  = curr->num_elts;
        node_header_t *next = (node_header_t *)curr->next;

        r->total_keys += num;
        r->total_leaves++;

        int idx = (int)((float)num / (float)B_LEAF * 10.0f);
        if (idx > 10) idx = 10;
        r->fill_buckets[idx]++;

        curr = next;
    }

    for (int lv = 1; lv < MAX_LEVEL; lv++)
    {
        node_header_t *curr = level_head(list, lv);
        while (curr)
        {
            node_header_t *next = (node_header_t *)curr->next;
            r->total_internals++;
            curr = next;
        }
    }

    if (r->total_leaves > 0)
    {
        r->avg_leaf_fill = (double)r->total_keys /
                           ((double)r->total_leaves * (double)B_LEAF);
    }
}

int
bsl_inspect_order(bsl_t *list, bsl_val_t *checksum_out)
{
    if (!list || !list->headers[0])
        return 1;

    int            ok       = 1;
    int            is_first = 1;
    bsl_key_t      prev     = 0;
    bsl_val_t      sum      = 0;
    node_header_t *curr     = leaf_head(list);

    while (curr)
    {
        uint32_t       num  = curr->num_elts;
        bsl_key_t     *keys = NODE_KEYS(curr);
        bsl_val_t     *vals = LEAF_VALUES(curr);
        node_header_t *next = (node_header_t *)curr->next;

        for (uint32_t i = 0; i < num; i++)
        {
            if (is_first)
            {
                is_first = 0;
            }
            else if (keys[i] <= prev)
            {
                printf("  order FAIL: leaf %p keys[%u]=%lu <= prev=%lu\n",
                       (void *)curr, i, (unsigned long)keys[i], (unsigned long)prev);
                ok = 0;
            }
            prev = keys[i];
            sum += vals[i];
        }

        curr = next;
    }

    if (checksum_out) *checksum_out = sum;
    return ok;
}

/*
 * children[i]->header == keys[i]  for i in [0, num)  (n keys, n children)
 */
int
bsl_inspect_index(bsl_t *list)
{
    if (!list)
        return 1;

    int ok = 1;

    for (int lv = MAX_LEVEL - 1; lv > 0; lv--)
    {
        node_header_t *curr = level_head(list, lv);
        if (!curr) continue;

        while (curr)
        {
            uint32_t    num      = curr->num_elts;
            bsl_key_t  *keys     = NODE_KEYS(curr);
            void      **children = INTERNAL_CHILDREN(curr);

            for (uint32_t i = 0; i < num; i++)
            {
                node_header_t *child = (node_header_t *)children[i];
                if (!child)
                {
                    printf("  index FAIL: level %d node %p child[%u] is NULL\n",
                           lv, (void *)curr, i);
                    ok = 0;
                    continue;
                }

                bsl_key_t child_hdr = NODE_KEYS(child)[0];

                if (child_hdr != keys[i])
                {
                    printf("  index FAIL: level %d node %p "
                           "child[%u] header=%lu != keys[%u]=%lu\n",
                           lv, (void *)curr, i,
                           (unsigned long)child_hdr, i, (unsigned long)keys[i]);
                    ok = 0;
                }
            }

            node_header_t *next = (node_header_t *)curr->next;
            curr = next;
        }
    }

    return ok;
}

int
bsl_inspect_levels(bsl_t *list)
{
    if (!list)
        return 1;

    int ok = 1;

    for (int lv = 0; lv < MAX_LEVEL; lv++)
    {
        node_header_t *curr = level_head(list, lv);
        while (curr)
        {
            uint32_t       stored_lv = curr->level;
            node_header_t *next      = (node_header_t *)curr->next;

            if ((int)stored_lv != lv)
            {
                printf("  levels FAIL: node %p at level %d but node->level=%u\n",
                       (void *)curr, lv, stored_lv);
                ok = 0;
            }

            curr = next;
        }
    }

    return ok;
}

int
bsl_inspect_next_headers(bsl_t *list)
{
    if (!list)
        return 1;

    int ok = 1;

    for (int lv = 0; lv < MAX_LEVEL; lv++)
    {
        node_header_t *curr = level_head(list, lv);
        while (curr)
        {
            bsl_key_t      cached_nh = curr->next_header;
            node_header_t *next      = (node_header_t *)curr->next;

            if (next)
            {
                bsl_key_t actual_hdr = NODE_KEYS(next)[0];

                if (cached_nh != actual_hdr)
                {
                    printf("  next_header FAIL: level %d node %p "
                           "cached=%lu actual=%lu\n",
                           lv, (void *)curr,
                           (unsigned long)cached_nh, (unsigned long)actual_hdr);
                    ok = 0;
                }
            }

            curr = next;
        }
    }

    return ok;
}

int
bsl_inspect_all(bsl_t *list, bsl_inspect_result_t *out)
{
    bsl_inspect_result_t local = {0};

    bsl_val_t checksum = 0;
    local.order_ok       = bsl_inspect_order(list, &checksum);
    local.index_ok       = bsl_inspect_index(list);
    local.level_ok       = bsl_inspect_levels(list);
    local.next_header_ok = bsl_inspect_next_headers(list);

    if (list && list->headers[0])
        collect_stats(list, &local);

    int all_ok = local.order_ok && local.index_ok &&
                 local.level_ok && local.next_header_ok;

    printf("  Checksum     : %lu\n", (unsigned long)checksum);
    printf("  Order        : %s\n", local.order_ok       ? "PASS" : "FAIL");
    printf("  Index        : %s\n", local.index_ok       ? "PASS" : "FAIL");
    printf("  Levels       : %s\n", local.level_ok       ? "PASS" : "FAIL");
    printf("  Next-headers : %s\n", local.next_header_ok ? "PASS" : "FAIL");
    printf("  Leaf nodes   : %llu  Internal: %llu  Keys: %llu  Avg fill: %.1f%%\n",
           local.total_leaves, local.total_internals,
           local.total_keys, local.avg_leaf_fill * 100.0);
    printf("\n  --- Leaf Fill Distribution ---\n");
    
    for (int i = 0; i <= 10; i++)
    {
        double pct = 0;
        if (local.total_leaves > 0)
            pct = (double)local.fill_buckets[i] / local.total_leaves * 100.0;

        printf("  %3d%% - %3d%% : [%6d] ", i * 10, (i == 10) ? 100 : (i * 10 + 9), local.fill_buckets[i]);

        int bar_width = (int)(pct * 0.5); 
        for (int j = 0; j < bar_width; j++)
            printf("█");
            
        if (local.fill_buckets[i] > 0 && bar_width == 0)
            printf("▏");

        printf(" %.1f%%\n", pct);
    }
    printf("  --------------------------------------------------\n");
    printf("\n");

    if (out) *out = local;
    return all_ok;
}