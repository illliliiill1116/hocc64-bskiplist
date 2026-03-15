/**
 * @file bsl_consistency_test.c
 * @brief High-stress consistency test including INSERT, DELETE, GET, and SCAN.
 * * This version integrates Delete operations which test the robustness of 
 * the SkipList's memory reclamation and concurrent structural changes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "bskiplist.h"

/* Test Configuration */
#define DEFAULT_NUM_THREADS 12
#define OPS_PER_THREAD      1000000
#define KEY_RANGE           10000 
#define MAGIC_MASK          0x5555555555555555ULL

/**
 * @brief Generates a deterministic value for a given key.
 */
static inline uint64_t expected_val(uint64_t key)
{
    return key ^ MAGIC_MASK;
}

typedef struct 
{
    bsl_t* list;          /* Pointer to the SkipList instance */
    int      tid;           /* Thread ID */
    uint64_t get_errors;    /* Count of point lookup mismatches */
    uint64_t scan_errors;   /* Count of scan result mismatches */
    uint64_t del_count;     /* Total successful deletes */
    uint64_t ins_count;     /* Total successful inserts */
} thread_ctx_t;

/**
 * @brief Callback for bsl_limit_scan_batch to verify range consistency.
 */
static void scan_verify_callback(bsl_range_t range, void* arg)
{
    uint64_t* err_count = (uint64_t*)arg;
    for (size_t i = 0; i < range.count; i++)
    {
        uint64_t key = (uint64_t)range.keys[i];
        uint64_t val = (uint64_t)range.vals[i];
        uint64_t exp = expected_val(key);
        
        /* In a system with deletes, we only verify if the value matches the key logic.
           If the key exists in the scan, its value MUST be consistent. */
        if (val != exp)
        {
            (*err_count)++;
            fprintf(stderr, "[SCAN ERROR] Key: %lu | Got: 0x%lx | Exp: 0x%lx\n", 
                    (unsigned long)key, (unsigned long)val, (unsigned long)exp);
        }
    }
}

/**
 * @brief Worker thread function performing mixed read/write/delete workloads.
 */
void* stress_worker(void* arg)
{
    thread_ctx_t* ctx = (thread_ctx_t*)arg;
    unsigned int seed = (unsigned int)ctx->tid + (unsigned int)time(NULL);

    for (int i = 0; i < OPS_PER_THREAD; i++)
    {
        uint64_t key = (uint64_t)(rand_r(&seed) % KEY_RANGE) + 1;
        int op_type  = rand_r(&seed) % 100;

        if (op_type < 25) 
        { 
            /* 25% Insert or Update */
            bsl_insert(ctx->list, (bsl_key_t)key, (bsl_val_t)expected_val(key));
            ctx->ins_count++;
        } 
        else if (op_type < 35)
        {
            /* 10% Delete */
            bsl_delete(ctx->list, (bsl_key_t)key);
            ctx->del_count++;
        }
        else if (op_type < 90) 
        { 
            /* 55% Point Lookup (Get) */
            bsl_val_t val = 0;
            if (bsl_get(ctx->list, (bsl_key_t)key, &val) == 1)
            {
                uint64_t exp = expected_val(key);
                if (val != exp)
                {
                    ctx->get_errors++;
                    fprintf(stderr, "[GET ERROR] Thread %2d | Key: %5lu | Got: 0x%016lx | Exp: 0x%016lx\n", 
                            ctx->tid, (unsigned long)key, (unsigned long)val, (unsigned long)exp);
                }
            }
        } 
        else 
        { 
            /* 10% Batch Scan */
            uint64_t local_errors = 0;
            size_t scan_limit = (size_t)(rand_r(&seed) % 100) + 1;
            bsl_limit_scan_batch(ctx->list, (bsl_key_t)key, scan_limit, scan_verify_callback, &local_errors);
            ctx->scan_errors += local_errors;
        }

    }
    return NULL;
}

int main(int argc, char** argv)
{
    int num_threads = (argc > 1) ? atoi(argv[1]) : DEFAULT_NUM_THREADS;
    
    printf("====================================================\n");
    printf("B-SkipList Consistency Benchmark\n");
    printf("====================================================\n");
    printf("Threads     : %d\n", num_threads);
    printf("Ops/Thread  : %d\n", OPS_PER_THREAD);
    printf("Key Range   : 1 - %d\n", KEY_RANGE);
    printf("Operations  : 25%% Ins, 10%% Del, 55%% Get, 10%% Scan\n");
    printf("----------------------------------------------------\n");

    bsl_t* list = bsl_new();
    pthread_t* threads = malloc(num_threads * sizeof(pthread_t));
    thread_ctx_t* contexts = malloc(num_threads * sizeof(thread_ctx_t));

    /* Launching workers */
    for (int i = 0; i < num_threads; i++)
    {
        contexts[i].list = list;
        contexts[i].tid = i;
        contexts[i].get_errors = 0;
        contexts[i].scan_errors = 0;
        contexts[i].ins_count = 0;
        contexts[i].del_count = 0;
        
        if (pthread_create(&threads[i], NULL, stress_worker, &contexts[i]) != 0)
        {
            perror("Failed to create thread");
            return EXIT_FAILURE;
        }
    }

    /* Collecting results */
    uint64_t total_get_err  = 0;
    uint64_t total_scan_err = 0;
    uint64_t total_ins      = 0;
    uint64_t total_del      = 0;

    for (int i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
        total_get_err  += contexts[i].get_errors;
        total_scan_err += contexts[i].scan_errors;
        total_ins      += contexts[i].ins_count;
        total_del      += contexts[i].del_count;
    }

    /* Final Report */
    printf("\n--- Workload Statistics ---\n");
    printf("%-25s : %lu\n", "Total Inserts/Updates", total_ins);
    printf("%-25s : %lu\n", "Total Deletes", total_del);
    printf("\n--- Categorized Error Report ---\n");
    printf("%-25s : %lu\n", "Point Get Mismatches", total_get_err);
    printf("%-25s : %lu\n", "Batch Scan Mismatches", total_scan_err);
    printf("%-25s : %lu\n", "Total Errors", total_get_err + total_scan_err);
    printf("----------------------------------------------------\n");

    if (total_get_err + total_scan_err == 0)
    {
        printf("RESULT: PASSED\n");
    }
    else
    {
        printf("RESULT: FAILED (Check concurrency barriers and EBR)\n");
    }

    /* Cleanup */
    free(threads);
    free(contexts);
    bsl_destroy(list);

    return (total_get_err + total_scan_err > 0) ? EXIT_FAILURE : EXIT_SUCCESS;
}