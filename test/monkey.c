#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <time.h>
#include <assert.h>
#include <stdint.h>
#include "bskiplist.h"

#define RATIO_INSERT 20
#define RATIO_DELETE 20
#define RATIO_GET    30
#define RATIO_SCAN   30

#define NUM_THREADS    8
#define OPS_PER_THREAD 5000000
#define KEY_RANGE      1000000
#define PRELOAD_COUNT  (KEY_RANGE / 2)
#define SCAN_LIMIT     100

typedef struct {
    bsl_t *list;
    bsl_key_t *key_buffer;
    int *op_buffer; 
    int thread_id;
} thread_context_t;

void scan_chunk_cb(bsl_range_t range, void *arg) {
    uint64_t *sum = (uint64_t *)arg;
    
    for (size_t i = 0; i < range.count; i++) {
        *sum += range.vals[i];
    }
}

void* mixer_worker(void* arg) {
    thread_context_t *ctx = (thread_context_t *)arg;
    bsl_t *list = ctx->list;
    bsl_key_t *kb = ctx->key_buffer;
    int *ob = ctx->op_buffer;

    for (int i = 0; i < OPS_PER_THREAD; i++) {
        bsl_key_t key = kb[i];
        int prob = ob[i];

        if (prob < RATIO_INSERT) {
            bsl_insert(list, key, key + 100);
        } 
        else if (prob < (RATIO_INSERT + RATIO_DELETE)) {
            bsl_remove(list, key);
        } 
        else if (prob < (RATIO_INSERT + RATIO_DELETE + RATIO_GET)) {
            bsl_val_t val;
            bsl_get(list, key, &val);
        } 
        else {
            bsl_val_t sum = 0;
            bsl_limit_scan_batch(list, key, SCAN_LIMIT, scan_chunk_cb, &sum);
        }
    }
    return NULL;
}

double get_time_diff(struct timespec start, struct timespec end) {
    return (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
}

int main() {
    bsl_t *list = bsl_new();
    if (!list) return 1;

    printf("Preloading %d elements...\n", PRELOAD_COUNT);
    for (int i = 0; i < PRELOAD_COUNT; i++) {
        bsl_key_t key = (bsl_key_t)(((uint64_t)i * 1103515245 + 12345) % KEY_RANGE);
        bsl_insert(list, key, key + 100);
    }

    printf("Pre-generating %zu operations into buffers...\n", (size_t)NUM_THREADS * OPS_PER_THREAD);
    bsl_key_t **all_keys = malloc(sizeof(bsl_key_t*) * NUM_THREADS);
    int **all_ops = malloc(sizeof(int*) * NUM_THREADS);
    
    for (int t = 0; t < NUM_THREADS; t++) {
        all_keys[t] = malloc(sizeof(bsl_key_t) * OPS_PER_THREAD);
        all_ops[t] = malloc(sizeof(int) * OPS_PER_THREAD);
        unsigned int seed = (unsigned int)(t ^ time(NULL));
        
        for (int i = 0; i < OPS_PER_THREAD; i++) {
            all_keys[t][i] = rand_r(&seed) % KEY_RANGE;
            all_ops[t][i] = rand_r(&seed) % 100;
        }
    }

    pthread_t threads[NUM_THREADS];
    thread_context_t contexts[NUM_THREADS];
    
    printf("Starting Stress Test (R%d/Scan%d/I%d/D%d)...\n", 
           RATIO_GET, RATIO_SCAN, RATIO_INSERT, RATIO_DELETE);
    
    struct timespec start_ts, end_ts;
    clock_gettime(CLOCK_MONOTONIC, &start_ts);

    for (int i = 0; i < NUM_THREADS; i++) {
        contexts[i].list = list;
        contexts[i].key_buffer = all_keys[i];
        contexts[i].op_buffer = all_ops[i];
        contexts[i].thread_id = i;
        pthread_create(&threads[i], NULL, mixer_worker, &contexts[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    clock_gettime(CLOCK_MONOTONIC, &end_ts);

    double seconds = get_time_diff(start_ts, end_ts);
    size_t total_ops = (size_t)NUM_THREADS * OPS_PER_THREAD;
    
    printf("\nTest Results:\n");
    printf("Total Time:    %.4f seconds\n", seconds);
    printf("Throughput:    \033[1;32m%.2f ops/sec\033[0m\n", (double)total_ops / seconds);

    for (int i = 0; i < NUM_THREADS; i++) {
        free(all_keys[i]);
        free(all_ops[i]);
    }
    free(all_keys); free(all_ops);
    bsl_destroy(list);

    return 0;
}