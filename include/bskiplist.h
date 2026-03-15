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

typedef void (*range_cb)(bsl_key_t key, bsl_val_t val, void *arg);
void bsl_limit_scan(bsl_t *list, bsl_key_t start, size_t limit, range_cb cb, void *arg);

typedef struct {
    const bsl_key_t *keys;
    const bsl_val_t *vals;
    size_t count;
} bsl_range_t;

typedef void (*range_batch_cb)(bsl_range_t range, void *arg);

void bsl_limit_scan_batch(bsl_t *list, bsl_key_t start, size_t limit, 
                        range_batch_cb cb, void *arg);

#endif /* BSKIPLIST_H */