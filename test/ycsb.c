#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>
#include <math.h>

#include "bskiplist.h"
#include "node.h"

/* ---------------------------------------------------------
   LATENCY TEST CONFIGURATION
   --------------------------------------------------------- */
#define MEASURE_LATENCY 1  /* Set to 1 to enable latency measurement */
#define LATENCY_BATCH   100 /* Measure avg latency every 100 ops to reduce syscall overhead */

typedef enum 
{ 
    OP_INSERT = 0, 
    OP_UPDATE, 
    OP_READ, 
    OP_SCAN, 
    OP_SCAN_END 
} op_t;

typedef struct 
{
    uint64_t *keys;
    uint64_t *ranges_val;
    int *ops;
    size_t size;
} workload_t;

typedef struct 
{
    bsl_t *list;
    uint64_t *keys;
    int *ops;
    uint64_t *ranges_val;
    int start;
    int end;
    /* Latency stats per thread */
    double *latency_samples;
    int sample_count;
} thread_args_t;

/* Get current time in nanoseconds */
static inline uint64_t get_nsecs() 
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Get current time in microseconds for throughput calculation */
static inline uint64_t get_usecs() 
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

int compare_double(const void *a, const void *b) 
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

double find_median(double *arr, int n) 
{
    if (n <= 0) return 0;
    qsort(arr, n, sizeof(double), compare_double);
    return arr[n / 2];
}

void scan_cb(bsl_range_t range, void *arg) 
{
    uint64_t *sum = (uint64_t *)arg;
    for (size_t i = 0; i < range.count; i++) 
    {
        *sum += __atomic_load_n(&range.vals[i], __ATOMIC_RELAXED);
    }
}

void *worker_func(void *arg) 
{
    thread_args_t *a = (thread_args_t *)arg;
    
#if MEASURE_LATENCY
    int total_ops = a->end - a->start;
    int max_samples = (total_ops / LATENCY_BATCH) + 1;
    a->latency_samples = malloc(sizeof(double) * max_samples);
    a->sample_count = 0;
    uint64_t batch_start = get_nsecs();
#endif

    for (int i = a->start; i < a->end; i++) 
    {
        switch (a->ops[i]) 
        {
            case OP_INSERT:
                bsl_insert(a->list, (bsl_key_t)a->keys[i], (bsl_val_t)a->keys[i]);
                break;
            case OP_READ: 
            {
                bsl_val_t out;
                bsl_get(a->list, (bsl_key_t)a->keys[i], &out);
                break;
            }
            case OP_SCAN: 
            {
                uint64_t sum = 0;
                bsl_limit_scan_batch(a->list, a->keys[i], (size_t)a->ranges_val[i], scan_cb, &sum);
                break;
            }
            default: abort();
        }

#if MEASURE_LATENCY
        /* Batch finished, calculate average latency for this window */
        if ((i - a->start + 1) % LATENCY_BATCH == 0) 
        {
            uint64_t batch_end = get_nsecs();
            double avg_lat = (double)(batch_end - batch_start) / LATENCY_BATCH;
            a->latency_samples[a->sample_count++] = avg_lat;
            batch_start = get_nsecs(); 
        }
#endif
    }
    return NULL;
}

/* Helper function: Aggregates and prints latency statistics */
void print_latency_report(thread_args_t *args, int num_threads) 
{
#if MEASURE_LATENCY
    int total_samples = 0;
    for (int i = 0; i < num_threads; i++) 
    {
        total_samples += args[i].sample_count;
    }
    
    if (total_samples == 0) return;

    double *all_samples = malloc(sizeof(double) * total_samples);
    int curr = 0;
    for (int i = 0; i < num_threads; i++) 
    {
        memcpy(all_samples + curr, args[i].latency_samples, sizeof(double) * args[i].sample_count);
        curr += args[i].sample_count;
        free(args[i].latency_samples); 
    }

    qsort(all_samples, total_samples, sizeof(double), compare_double);
    
    printf("\n--- Latency Report (unit: ns) ---\n");
    printf("Metrics calculated via Batch Size: %d\n", LATENCY_BATCH);
    printf("P50  : %.2f ns\n", all_samples[total_samples / 2]);
    printf("P90  : %.2f ns\n", all_samples[(int)(total_samples * 0.90)]);
    printf("P99  : %.2f ns\n", all_samples[(int)(total_samples * 0.99)]);
    printf("P99.9: %.2f ns\n", all_samples[(int)(total_samples * 0.999)]);
    printf("Max  : %.2f ns\n", all_samples[total_samples - 1]);
    printf("---------------------------------\n");

    free(all_samples);
#endif
}

void parallel_worker(int num_threads, int start, int end, bsl_t *list, workload_t *wl) 
{
    pthread_t *threads = malloc(num_threads * sizeof(pthread_t));
    thread_args_t *args = malloc(num_threads * sizeof(thread_args_t));
    int total_range = end - start;
    int per_thread = total_range / num_threads;

    for (int i = 0; i < num_threads; i++) 
    {
        args[i].list = list;
        args[i].keys = wl->keys;
        args[i].ops = wl->ops;
        args[i].ranges_val = wl->ranges_val;
        args[i].start = start + (i * per_thread);
        args[i].end = (i == num_threads - 1) ? end : (args[i].start + per_thread);
        args[i].latency_samples = NULL;
        args[i].sample_count = 0;
        pthread_create(&threads[i], NULL, worker_func, &args[i]);
    }

    for (int i = 0; i < num_threads; i++) 
    {
        pthread_join(threads[i], NULL);
    }

    print_latency_report(args, num_threads);

    free(threads);
    free(args);
}


void bsl_verify_and_stat(bsl_t *list)
{
    printf("\n[Starting Structure Verification...]\n");
    leaf_node_t *curr = (leaf_node_t *)list->headers[0];
    uint64_t last_key = 0;
    long total_elements = 0;
    bool is_first = true;

    while (curr)
    {
        for (int i = 0; i < curr->header.num_elts; i++)
        {
            uint64_t k = (uint64_t)curr->keys[i];
            if (!is_first && k < last_key)
                printf("ERROR: Sorting violation! Key %llu found after %llu\n", k, last_key);

            is_first = false;
            last_key = k;
            total_elements++;
        }
        curr = (leaf_node_t *)curr->header.next;
    }
    printf("Verification complete. Total elements: %ld\n", total_elements);
}

int main(int argc, char **argv)
{
    if (argc != 5) {
        printf("Usage: ./ycsb <load_file> <run_file> <threads> <iterations>\n");
        return 1;
    }

    const char *load_file = argv[1];
    const char *run_file = argv[2];
    int num_threads = atoi(argv[3]);
    int iterations = atoi(argv[4]);

    workload_t load_wl = { malloc(iterations * sizeof(uint64_t)), malloc(iterations * sizeof(uint64_t)), malloc(iterations * sizeof(int)), 0 };
    workload_t run_wl = { malloc(iterations * sizeof(uint64_t)), malloc(iterations * sizeof(uint64_t)), malloc(iterations * sizeof(int)), 0 };

    printf("loading with file: %s\n", load_file);
    FILE *f_load = fopen(load_file, "r");
    char op_str[16]; uint64_t k;
    while (load_wl.size < iterations && fscanf(f_load, "%s %llu", op_str, &k) == 2) {
        load_wl.keys[load_wl.size] = k;
        load_wl.ops[load_wl.size] = OP_INSERT;
        load_wl.size++;
    }
    fclose(f_load);
    fprintf(stderr, "Loaded %zu keys\n", load_wl.size);

    printf("running with file: %s\n", run_file);
    FILE *f_run = fopen(run_file, "r");
    while (run_wl.size < iterations && fscanf(f_run, "%s %llu", op_str, &k) == 2) {
        run_wl.keys[run_wl.size] = k;
        if (strcmp(op_str, "INSERT") == 0) run_wl.ops[run_wl.size] = OP_INSERT;
        else if (strcmp(op_str, "READ") == 0) run_wl.ops[run_wl.size] = OP_READ;
        else if (strcmp(op_str, "SCAN") == 0) {
            run_wl.ops[run_wl.size] = OP_SCAN;
            fscanf(f_run, "%llu", &run_wl.ranges_val[run_wl.size]);
        } else if (strcmp(op_str, "SCANEND") == 0) {
            run_wl.ops[run_wl.size] = OP_SCAN_END;
            fscanf(f_run, "%llu", &run_wl.ranges_val[run_wl.size]);
        }
        run_wl.size++;
    }
    fclose(f_run);
    fprintf(stderr, "Loaded %zu more keys\n", run_wl.size);

    sleep(3);
    fprintf(stderr, "Slept\nbskiplist\n");

    double load_tpts[5], run_tpts[5];
    int tpt_idx = 0;

    for (int k_loop = 0; k_loop < 6; k_loop++) {
        bsl_t *list = bsl_new();

        uint64_t start = get_usecs();
        parallel_worker(num_threads, 0, load_wl.size, list, &load_wl);
        uint64_t duration = get_usecs() - start;
        double load_tpt = (double)load_wl.size / duration;
        
        if (k_loop != 0) load_tpts[tpt_idx] = load_tpt;
        printf("\tLoad took %lu us, throughput = %f ops/us\n", duration, load_tpt);

        start = get_usecs();
        parallel_worker(num_threads, 0, run_wl.size, list, &run_wl);
        duration = get_usecs() - start;
        double run_tpt = (double)run_wl.size / duration;

        if (k_loop != 0) {
            run_tpts[tpt_idx] = run_tpt;
            tpt_idx++;
        }
        printf("\tRun, throughput: %f ,ops/us\n", run_tpt);

        bsl_destroy(list);
    }

    printf("\tMedian Load throughput: %f ,ops/us\n", find_median(load_tpts, 5));
    printf("\tMedian Run throughput: %f ,ops/us\n", find_median(run_tpts, 5));
    printf("\n\n");

    free(load_wl.keys); free(load_wl.ops); free(load_wl.ranges_val);
    free(run_wl.keys); free(run_wl.ops); free(run_wl.ranges_val);

    return 0;
}