#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>

#include "bskiplist.h"
#include "node.h"

#define LOAD_SIZE 1000000
#define RUN_SIZE 1000000

#ifdef __APPLE__

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int count;
    int trip_count;
} pthread_barrier_t;

static int pthread_barrier_init(pthread_barrier_t *b, void *attr, int count) {
    pthread_mutex_init(&b->mutex, NULL);
    pthread_cond_init(&b->cond, NULL);
    b->count = 0;
    b->trip_count = count;
    return 0;
}

static int pthread_barrier_wait(pthread_barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->count++;
    if (b->count >= b->trip_count) {
        b->count = 0;
        pthread_cond_broadcast(&b->cond);
        pthread_mutex_unlock(&b->mutex);
        return 1;
    } else {
        pthread_cond_wait(&b->cond, &b->mutex);
        pthread_mutex_unlock(&b->mutex);
        return 0;
    }
}

static int pthread_barrier_destroy(pthread_barrier_t *b) {
    pthread_mutex_destroy(&b->mutex);
    pthread_cond_destroy(&b->cond);
    return 0;
}
#endif

typedef enum { OP_INSERT = 0, OP_READ, OP_SCAN, OP_UNKNOWN } op_t;

typedef struct {
    uint64_t *keys;
    uint64_t *ranges_end;
    op_t *ops;
    size_t size;
} workload_t;

typedef struct {
    bsl_t *list;
    workload_t *wl;
    int start;
    int end;
    size_t executed_ops;
    uint64_t *latencies;
    pthread_barrier_t *barrier; 
} thread_args_t;

static inline uint64_t get_ticks(void) {
#if defined(__aarch64__)
    uint64_t val;
    asm volatile("mrs %0, cntvct_el0" : "=r" (val));
    return val;
#elif defined(__x86_64__)
    unsigned int lo, hi;
    asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)hi << 32) | lo;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
#endif
}

double get_tick_per_ns() {
    uint64_t t1 = get_ticks();
    struct timespec sleep_ts = {0, 100000000}; // 0.1s
    nanosleep(&sleep_ts, NULL);
    uint64_t t2 = get_ticks();
    return (double)(t2 - t1) / 100000000.0;
}

int compare_uint64(const void *a, const void *b) {
    uint64_t arg1 = *(const uint64_t *)a;
    uint64_t arg2 = *(const uint64_t *)b;
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}

void scan_chunk_cb(bsl_range_t range, void *arg) {
    uint64_t *sum = (uint64_t *)arg;
    
    for (size_t i = 0; i < range.count; i++) {
        *sum += range.vals[i];
    }
}

void *worker(void *arg) {
    thread_args_t *a = (thread_args_t *)arg;
    if (!a || !a->list || !a->wl) return NULL;
    
    a->executed_ops = 0;
    pthread_barrier_wait(a->barrier);

    for (int i = a->start; i < a->end; i++) {
        uint64_t ts_start = get_ticks();

        switch (a->wl->ops[i]) {
            case OP_INSERT:
                bsl_insert(a->list, (bsl_key_t)a->wl->keys[i], (bsl_val_t)a->wl->keys[i]);
                break;
            case OP_READ: {
                bsl_val_t out;
                bsl_get_value(a->list, (bsl_key_t)a->wl->keys[i], &out);
                break;
            }
            case OP_SCAN: {
                uint64_t dummy_sum = 0;
                bsl_limit_scan_chunked(a->list, a->wl->keys[i], (size_t)a->wl->ranges_end[i], scan_chunk_cb, &dummy_sum);
                break;
            }
            default: continue;
        }

        uint64_t ts_end = get_ticks();
        a->latencies[a->executed_ops++] = (ts_end - ts_start);
    }
    return NULL;
}

void bsl_verify_and_stat(bsl_t *list) {
    printf("\n[Starting Structure Verification...]\n");
    leaf_node_t *curr = (leaf_node_t *)list->headers[0];
    uint64_t last_key = 0;
    long total_elements = 0;
    bool is_first = true;

    while (curr) {
        for (int i = 0; i < curr->header.num_elts; i++) {
            uint64_t k = (uint64_t)curr->keys[i];
            if (!is_first && k < last_key) {
                printf("ERROR: Sorting violation! Key %llu found after %llu\n", k, last_key);
            }
            is_first = false;
            last_key = k;
            total_elements++;
        }
        curr = (leaf_node_t *)curr->header.next;
    }
    printf("Verification complete. Total elements: %ld\n", total_elements);
}

int main(int argc, char **argv) {
    if (argc != 4) {
        printf("Usage: ./ycsb <path> <workload_name> <threads>\n");
        return 1;
    }

    double tick_per_ns = get_tick_per_ns();
    int num_threads = atoi(argv[3]);
    bsl_t *list = bsl_new();

    workload_t load_wl = { .keys = calloc(LOAD_SIZE, sizeof(uint64_t)), .ops = calloc(LOAD_SIZE, sizeof(op_t)), .size = 0 };
    workload_t run_wl = { .keys = calloc(RUN_SIZE, sizeof(uint64_t)), .ranges_end = calloc(RUN_SIZE, sizeof(uint64_t)), .ops = calloc(RUN_SIZE, sizeof(op_t)), .size = 0 };

    char path[256];
    snprintf(path, 256, "%s/load%s_unif_int.dat", argv[1], argv[2]);
    FILE *f = fopen(path, "r");
    if(!f) { perror("fopen load"); return 1; }
    char op_str[16]; uint64_t k;
    while (load_wl.size < LOAD_SIZE && fscanf(f, "%s %llu", op_str, &k) == 2) {
        load_wl.keys[load_wl.size++] = k;
    }
    fclose(f);

    snprintf(path, 256, "%s/txns%s_unif_int.dat", argv[1], argv[2]);
    f = fopen(path, "r");
    if(!f) { perror("fopen txns"); return 1; }
    while (run_wl.size < RUN_SIZE && fscanf(f, "%s %llu", op_str, &k) == 2) {
        run_wl.keys[run_wl.size] = k;
        if (strcmp(op_str, "INSERT") == 0) run_wl.ops[run_wl.size] = OP_INSERT;
        else if (strcmp(op_str, "READ") == 0) run_wl.ops[run_wl.size] = OP_READ;
        else if (strcmp(op_str, "SCAN") == 0) {
            run_wl.ops[run_wl.size] = OP_SCAN;
            fscanf(f, "%llu", &run_wl.ranges_end[run_wl.size]);
        }
        run_wl.size++;
    }
    fclose(f);

    pthread_t *ts = malloc(num_threads * sizeof(pthread_t));
    thread_args_t *as = malloc(num_threads * sizeof(thread_args_t));
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, num_threads);

    printf("Loading keys...\n");
    for (int i = 0; i < num_threads; i++) {
        size_t n = load_wl.size / num_threads;
        as[i] = (thread_args_t){list, &load_wl, i*n, (i==num_threads-1)?load_wl.size:(i+1)*n, 0, malloc(n * 2 * sizeof(uint64_t)), &barrier};
        pthread_create(&ts[i], NULL, worker, &as[i]);
    }
    for (int i = 0; i < num_threads; i++) { pthread_join(ts[i], NULL); free(as[i].latencies); }

    printf("Running Transactions (%zu ops) with %d threads...\n", run_wl.size, num_threads);
    struct timespec start_wall, end_wall;
    clock_gettime(CLOCK_MONOTONIC, &start_wall);

    for (int i = 0; i < num_threads; i++) {
        size_t n = run_wl.size / num_threads;
        as[i] = (thread_args_t){list, &run_wl, i*n, (i==num_threads-1)?run_wl.size:(i+1)*n, 0, malloc((n + 100) * sizeof(uint64_t)), &barrier};
        pthread_create(&ts[i], NULL, worker, &as[i]);
    }
    for (int i = 0; i < num_threads; i++) pthread_join(ts[i], NULL);
    clock_gettime(CLOCK_MONOTONIC, &end_wall);

    uint64_t *all_lats = malloc(run_wl.size * sizeof(uint64_t));
    uint64_t total_executed = 0;
    uint64_t sum_latency_ticks = 0;

    for (int i = 0; i < num_threads; i++) {
        for(size_t j=0; j < as[i].executed_ops; j++) {
            uint64_t ns_val = (uint64_t)((double)as[i].latencies[j] / tick_per_ns);
            all_lats[total_executed++] = ns_val;
            sum_latency_ticks += ns_val;
        }
        free(as[i].latencies);
    }

    qsort(all_lats, total_executed, sizeof(uint64_t), compare_uint64);

    double wall_time_sec = (end_wall.tv_sec - start_wall.tv_sec) + (end_wall.tv_nsec - start_wall.tv_nsec) / 1e9;
    
    printf("\n--- Benchmark Result (Accuracy: ~%.2f ns) ---", 1.0/tick_per_ns);
    printf("\nThroughput:      %.2f ops/sec", (double)total_executed / wall_time_sec);
    printf("\nAverage Latency: %.2f ns", (double)sum_latency_ticks / total_executed);
    printf("\nP50 Latency:     %llu ns", all_lats[(size_t)(total_executed * 0.50)]);
    printf("\nP90 Latency:     %llu ns", all_lats[(size_t)(total_executed * 0.90)]);
    printf("\nP99 Latency:     %llu ns", all_lats[(size_t)(total_executed * 0.99)]);
    printf("\nP99.9 Latency:   %llu ns", all_lats[(size_t)(total_executed * 0.999)]);

    bsl_verify_and_stat(list);

    pthread_barrier_destroy(&barrier);
    free(ts); free(as); free(all_lats);
    free(load_wl.keys); free(load_wl.ops);
    free(run_wl.keys); free(run_wl.ops); free(run_wl.ranges_end);
    return 0;
}