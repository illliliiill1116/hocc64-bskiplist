#ifndef BSL_INSPECT_H
#define BSL_INSPECT_H

#include "bskiplist.h"
#include <stdint.h>

typedef struct {
    int      order_ok;
    int      index_ok;
    int      level_ok;
    int      next_header_ok;
    uint64_t total_leaves;
    uint64_t total_internals;
    uint64_t total_keys;
    double   avg_leaf_fill;
    int      fill_buckets[11];   /* 0–9%, 10–19%, …, 100% */
} bsl_inspect_result_t;

int bsl_inspect_order(bsl_t *list);
int bsl_inspect_index(bsl_t *list);
int bsl_inspect_levels(bsl_t *list);
int bsl_inspect_next_headers(bsl_t *list);
int bsl_inspect_all(bsl_t *list, bsl_inspect_result_t *out);

#endif /* BSL_INSPECT_H */