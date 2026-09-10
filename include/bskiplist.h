#ifndef BSKIPLIST_H
#define BSKIPLIST_H

#include <stdint.h>
#include <stddef.h>
#include <params.h>

typedef struct bsl_st {
    void *headers[MAX_LEVEL];
} bsl_t;

bsl_t* bsl_new();
void bsl_destroy(bsl_t *list);

int bsl_insert(bsl_t *list, bsl_key_t key, bsl_val_t value);
int bsl_delete(bsl_t *list, bsl_key_t key);
int bsl_get(bsl_t *list, bsl_key_t key, bsl_val_t *out_val);

size_t bsl_scan_n(bsl_t *list, bsl_key_t start, size_t length,
                  bsl_key_t *out_keys, bsl_val_t *out_vals, size_t capacity);

/*
 * one batch of scan results: `count` entries whose first element sits
 * `offset` positions from the start of the requested range.
 */
typedef struct {
    const bsl_key_t *keys;
    const bsl_val_t *vals;
    size_t count;
    size_t offset;
} bsl_range_t;

typedef void (*range_batch_cb)(bsl_range_t range, void *arg);

size_t bsl_scan_n_batch(bsl_t *list, bsl_key_t start, size_t length,
                        range_batch_cb cb, void *arg);

#endif /* BSKIPLIST_H */