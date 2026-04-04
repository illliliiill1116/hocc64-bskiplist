#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <stdbool.h>
#include <unistd.h>
#include <math.h>

#include "bskiplist.h"
#include "bsl_inspect.h"
#include "node.h"


#define LATENCY_BATCH 10

/* First iteration is a warm-up and is excluded from results. */
#define WARMUP_ITERS  1
#define MEASURE_ITERS 5
#define TOTAL_ITERS   (WARMUP_ITERS + MEASURE_ITERS)

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
    int      *ops;
    size_t    size;
} workload_t;

typedef struct
{
    bsl_t              *list;
    uint64_t           *keys;
    int                *ops;
    uint64_t           *ranges_val;
    int                 start;
    int                 end;
    bool                collect_stats;
    double             *latency_samples;
    int                 sample_count;

} thread_args_t;

void load_workload(const char *filename, workload_t *wl, int max_iters)
{
    int fd = open(filename, O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    
    char *data = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    char *ptr = data;
    char *end = data + st.st_size;

    while (wl->size < (size_t)max_iters && ptr < end)
    {
        while (ptr < end && (*ptr == ' ' || *ptr == '\n' || *ptr == '\r')) ptr++;
        if (ptr >= end) break;

        if (*ptr == 'I') { // INSERT
            wl->ops[wl->size] = OP_INSERT;
            ptr += 6; // skip "INSERT"
        } else if (*ptr == 'R') { // READ
            wl->ops[wl->size] = OP_READ;
            ptr += 4; // skip "READ"
        } else if (*ptr == 'S') { // SCAN
            wl->ops[wl->size] = OP_SCAN;
            ptr += 4; // skip "SCAN"
        }

        char *next_ptr;
        wl->keys[wl->size] = strtoull(ptr, &next_ptr, 10);
        ptr = next_ptr;

        if (wl->ops[wl->size] == OP_SCAN)
        {
            wl->ranges_val[wl->size] = strtoull(ptr, &next_ptr, 10);
            ptr = next_ptr;
        }

        wl->size++;
    }

    munmap(data, st.st_size);
    close(fd);
}

static inline uint64_t get_nsecs(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static inline uint64_t get_usecs(void)
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000 + tv.tv_usec;
}

static int compare_double(const void *a, const void *b)
{
    double da = *(const double *)a;
    double db = *(const double *)b;
    return (da > db) - (da < db);
}

static double find_median(double *arr, int n)
{
    if (n <= 0) return 0;
    qsort(arr, n, sizeof(double), compare_double);
    return arr[n / 2];
}

static void scan_cb(bsl_range_t range, void *arg)
{
    uint64_t *sum_ptr = (uint64_t *)arg;

    uint64_t local_sum = 0;
    for (size_t i = 0; i < range.count; i++)
        local_sum += range.vals[i];
    *sum_ptr += local_sum;
}

static void *worker_func(void *arg)
{
    thread_args_t *a = (thread_args_t *)arg;

#if MEASURE_LATENCY
    uint64_t batch_start = 0;
    if (a->collect_stats)
    {
        int total_ops  = a->end - a->start;
        int max_samples = (total_ops / LATENCY_BATCH) + 1;
        a->latency_samples = malloc(sizeof(double) * max_samples);
        a->sample_count    = 0;
        batch_start        = get_nsecs();
    }
#endif

    for (int i = a->start; i < a->end; i++)
    {
        switch (a->ops[i])
        {
            case OP_INSERT:
                bsl_insert(a->list, (bsl_key_t)a->keys[i],
                           (bsl_val_t)a->keys[i]);
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
                bsl_scan_n_batch(a->list, a->keys[i],
                                     (size_t)a->ranges_val[i],
                                     scan_cb, &sum);
                break;
            }
            default: abort();
        }

#if MEASURE_LATENCY
        if (a->collect_stats && (i - a->start + 1) % LATENCY_BATCH == 0)
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

static void aggregate_latency(thread_args_t *args, int num_threads,
                               double **global_store, int *global_count)
{
#if MEASURE_LATENCY
    int i;
    for (i = 0; i < num_threads; i++)
    {
        if (!args[i].latency_samples) continue;
        size_t new_size = sizeof(double) * (*global_count + args[i].sample_count);
        *global_store = realloc(*global_store, new_size);
        memcpy(*global_store + *global_count,
               args[i].latency_samples,
               sizeof(double) * args[i].sample_count);
        *global_count += args[i].sample_count;
        free(args[i].latency_samples);
        args[i].latency_samples = NULL;
    }
#else
    (void)args; (void)num_threads; (void)global_store; (void)global_count;
#endif
}

static void print_final_latency(double *samples, int count, const char *label)
{
#if MEASURE_LATENCY
    if (count == 0 || !samples) return;
    qsort(samples, count, sizeof(double), compare_double);
    printf("--- %s Latency Report (unit: ns) ---\n", label);
    printf("P50  : %.2f ns\n", samples[count / 2]);
    printf("P90  : %.2f ns\n", samples[(int)(count * 0.90)]);
    printf("P99  : %.2f ns\n", samples[(int)(count * 0.99)]);
    printf("P99.9: %.2f ns\n", samples[(int)(count * 0.999)]);
    printf("Max  : %.2f ns\n", samples[count - 1]);
    printf("--------------------------------------------\n");
#else
    (void)samples; (void)count; (void)label;
#endif
}

static void parallel_worker(int num_threads, int start, int end,
                             bsl_t *list, workload_t *wl,
                             bool collect_stats,
                             double **global_lat, int *global_cnt)
{
    pthread_t         *threads = malloc(num_threads * sizeof(pthread_t));
    thread_args_t     *args    = malloc(num_threads * sizeof(thread_args_t));
    int total_range = end - start;
    int per_thread  = total_range / num_threads;
    int i;

    for (i = 0; i < num_threads; i++)
    {
        args[i].list            = list;
        args[i].keys            = wl->keys;
        args[i].ops             = wl->ops;
        args[i].ranges_val      = wl->ranges_val;
        args[i].start           = start + (i * per_thread);
        args[i].end             = (i == num_threads - 1)
                                  ? end : (args[i].start + per_thread);
        args[i].collect_stats   = collect_stats;
        args[i].latency_samples = NULL;
        args[i].sample_count    = 0;
        pthread_create(&threads[i], NULL, worker_func, &args[i]);
    }

    for (i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    if (collect_stats)
        aggregate_latency(args, num_threads, global_lat, global_cnt);

    free(threads);
    free(args);
}

int main(int argc, char **argv)
{
    if (argc != 5)
    {
        printf("Usage: ./ycsb <load_file> <run_file> <threads> <iterations>\n");
        return 1;
    }

    const char *load_file  = argv[1];
    const char *run_file   = argv[2];
    int         num_threads = atoi(argv[3]);
    int         iterations  = atoi(argv[4]);

    workload_t load_wl =
    {
        malloc(iterations * sizeof(uint64_t)),
        malloc(iterations * sizeof(uint64_t)),
        malloc(iterations * sizeof(int)),
        0
    };
    workload_t run_wl =
    {
        malloc(iterations * sizeof(uint64_t)),
        malloc(iterations * sizeof(uint64_t)),
        malloc(iterations * sizeof(int)),
        0
    };

    printf("Loading %s ...\n", load_file);
    load_workload(load_file, &load_wl, iterations);
    printf("Loaded %zu keys from %s\n", load_wl.size, load_file);

    printf("Loading %s ...\n", run_file);
    load_workload(run_file, &run_wl, iterations);
    printf("Loaded %zu ops from %s\n", run_wl.size, run_file);

    sleep(3);

    double  load_tpts[MEASURE_ITERS];
    double  run_tpts[MEASURE_ITERS];
    double *load_lat_all = NULL;
    double *run_lat_all  = NULL;
    int     load_lat_cnt = 0;
    int     run_lat_cnt  = 0;
    int     tpt_idx      = 0;

    for (int iter = 0; iter < TOTAL_ITERS; iter++)
    {
        bool collect = (iter >= WARMUP_ITERS);

        bsl_t *list = bsl_new();

        /* Load phase */
        uint64_t start = get_usecs();
        parallel_worker(num_threads, 0, load_wl.size, list, &load_wl,
                        collect, &load_lat_all, &load_lat_cnt);
        uint64_t duration = get_usecs() - start;
        double load_tpt = (double)load_wl.size / duration;

        if (collect)
            load_tpts[tpt_idx] = load_tpt;

        printf("\tIteration %d Load: %f ops/us%s\n",
               iter, load_tpt, collect ? "" : "  [warm-up]");

        /* Run phase */
        start = get_usecs();
        parallel_worker(num_threads, 0, run_wl.size, list, &run_wl,
                        collect, &run_lat_all, &run_lat_cnt);
        duration = get_usecs() - start;
        double run_tpt = (double)run_wl.size / duration;

        if (collect)
        {
            run_tpts[tpt_idx] = run_tpt;
            tpt_idx++;
        }

        printf("\tIteration %d Run:  %f ops/us%s\n",
               iter, run_tpt, collect ? "" : "  [warm-up]");

#ifdef CHECK_STRUCTURE
        bsl_inspect_all(list, NULL);
#endif

        bsl_destroy(list);
    }

    printf("\n================ FINAL RESULTS ================\n");
    printf("Median Load throughput: %f ops/us\n",
           find_median(load_tpts, MEASURE_ITERS));
    print_final_latency(load_lat_all, load_lat_cnt, "LOAD");

    printf("\nMedian Run throughput : %f ops/us\n",
           find_median(run_tpts, MEASURE_ITERS));
    print_final_latency(run_lat_all, run_lat_cnt, "RUN");
    printf("===============================================\n");

    free(load_lat_all);
    free(run_lat_all);
    free(load_wl.keys); free(load_wl.ops); free(load_wl.ranges_val);
    free(run_wl.keys);  free(run_wl.ops);  free(run_wl.ranges_val);

    return 0;
}