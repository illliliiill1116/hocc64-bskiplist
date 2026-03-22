#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>
#include <string.h>
#include <time.h>

#include "bskiplist.h"
#include "bsl_inspect.h"

#define DEFAULT_NUM_THREADS 8
#define MAGIC_MASK          0x5555555555555555ULL

static inline bsl_val_t
expected_val(bsl_key_t key)
{
    return (bsl_val_t)((uint64_t)key ^ MAGIC_MASK);
}

static int
run_inspect(const char *label, bsl_t *list)
{
    printf("  [inspect] %s\n", label);
    bsl_inspect_result_t r = {0};
    int ok = bsl_inspect_all(list, &r);
    if (!ok)
        printf("  [inspect] FAILED — %s\n", label);
    return ok;
}

/*
 * verify_all_keys: scan every key in [1, max_key] and check its value.
 * Keys that were deleted are skipped (get returns 0).
 * Returns the number of value mismatches found.
 */
static int
verify_all_keys(bsl_t *list, bsl_key_t max_key)
{
    int errors = 0;
    for (bsl_key_t k = 1; k <= max_key; k++)
    {
        bsl_val_t val = 0;
        if (bsl_get(list, k, &val) == 1 && val != expected_val(k))
        {
            printf("  value FAIL: key=%lu got=0x%lx exp=0x%lx\n",
                   (unsigned long)k,
                   (unsigned long)val,
                   (unsigned long)expected_val(k));
            errors++;
            if (errors >= 10)
            {
                printf("  (further mismatches suppressed)\n");
                break;
            }
        }
    }
    return errors;
}

/* ------------------------------------------------------------------ */
/* Test 1: Contention hammer                                            */
/* All threads hammer a tiny key range, forcing concurrent splits on   */
/* the same nodes.                                                      */
/* ------------------------------------------------------------------ */

#define T1_KEY_RANGE  1000
#define T1_OPS        2000000

typedef struct { bsl_t *list; int tid; } basic_ctx_t;

static void *
t1_worker(void *arg)
{
    basic_ctx_t *ctx  = (basic_ctx_t *)arg;
    unsigned int seed = (unsigned int)ctx->tid ^ 0xdeadbeef;

    for (int i = 0; i < T1_OPS; i++)
    {
        bsl_key_t key = (bsl_key_t)(rand_r(&seed) % T1_KEY_RANGE) + 1;
        int op = rand_r(&seed) % 3;

        if (op == 0)
            bsl_insert(ctx->list, key, expected_val(key));
        else if (op == 1)
            bsl_delete(ctx->list, key);
        else
        {
            bsl_val_t val;
            bsl_get(ctx->list, key, &val);
        }
    }
    return NULL;
}

static int
test_contention_hammer(int num_threads)
{
    printf("\n[Test 1] Contention hammer  (range=%d, threads=%d)\n",
           T1_KEY_RANGE, num_threads);

    bsl_t       *list    = bsl_new();
    basic_ctx_t *ctx     = malloc((size_t)num_threads * sizeof(basic_ctx_t));
    pthread_t   *threads = malloc((size_t)num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        ctx[i] = (basic_ctx_t){ .list = list, .tid = i };
        pthread_create(&threads[i], NULL, t1_worker, &ctx[i]);
    }
    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    int ok = run_inspect("post-stress", list);
    int errs = verify_all_keys(list, (bsl_key_t)T1_KEY_RANGE);
    if (errs > 0)
    {
        printf("  value FAIL: %d mismatches\n", errs);
        ok = 0;
    }

    free(ctx);
    free(threads);
    bsl_destroy(list);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Test 2: Delete heavy                                                 */
/* Fill the list, then all threads only delete, then verify structure. */
/* ------------------------------------------------------------------ */

#define T2_FILL_KEYS  500000
#define T2_KEY_RANGE  500000
#define T2_OPS        2000000

static void *
t2_worker(void *arg)
{
    basic_ctx_t *ctx  = (basic_ctx_t *)arg;
    unsigned int seed = (unsigned int)ctx->tid ^ 0xcafebabe;

    for (int i = 0; i < T2_OPS; i++)
    {
        bsl_key_t key = (bsl_key_t)(rand_r(&seed) % T2_KEY_RANGE) + 1;
        bsl_delete(ctx->list, key);
    }
    return NULL;
}

static int
test_delete_heavy(int num_threads)
{
    printf("\n[Test 2] Delete heavy  (fill=%d, threads=%d)\n",
           T2_FILL_KEYS, num_threads);

    bsl_t *list = bsl_new();

    for (int i = 1; i <= T2_FILL_KEYS; i++)
        bsl_insert(list, (bsl_key_t)i, expected_val((bsl_key_t)i));

    basic_ctx_t *ctx     = malloc((size_t)num_threads * sizeof(basic_ctx_t));
    pthread_t   *threads = malloc((size_t)num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        ctx[i] = (basic_ctx_t){ .list = list, .tid = i };
        pthread_create(&threads[i], NULL, t2_worker, &ctx[i]);
    }
    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    int ok   = run_inspect("post-stress", list);
    int errs = verify_all_keys(list, (bsl_key_t)T2_FILL_KEYS);
    if (errs > 0)
    {
        printf("  value FAIL: %d mismatches\n", errs);
        ok = 0;
    }

    free(ctx);
    free(threads);
    bsl_destroy(list);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Test 3: Writer / Reader split                                        */
/* First half of threads only write, second half only read.            */
/* ------------------------------------------------------------------ */

#define T3_KEY_RANGE  1000000
#define T3_OPS        2000000

typedef struct
{
    bsl_t *list;
    int    tid;
    int    is_writer;
    int    get_errors;
} t3_ctx_t;

static void *
t3_worker(void *arg)
{
    t3_ctx_t    *ctx  = (t3_ctx_t *)arg;
    unsigned int seed = (unsigned int)ctx->tid ^ 0xfeedface;

    for (int i = 0; i < T3_OPS; i++)
    {
        bsl_key_t key = (bsl_key_t)(rand_r(&seed) % T3_KEY_RANGE) + 1;

        if (ctx->is_writer)
        {
            if (rand_r(&seed) % 4 == 0)
                bsl_delete(ctx->list, key);
            else
                bsl_insert(ctx->list, key, expected_val(key));
        }
        else
        {
            bsl_val_t val = 0;
            if (bsl_get(ctx->list, key, &val) == 1)
            {
                if (val != expected_val(key))
                    ctx->get_errors++;
            }
        }
    }
    return NULL;
}

static int
test_writer_reader_split(int num_threads)
{
    printf("\n[Test 3] Writer/Reader split  (threads=%d each half)\n",
           num_threads / 2);

    bsl_t     *list    = bsl_new();
    t3_ctx_t  *ctx     = malloc((size_t)num_threads * sizeof(t3_ctx_t));
    pthread_t *threads = malloc((size_t)num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        ctx[i] = (t3_ctx_t){
            .list       = list,
            .tid        = i,
            .is_writer  = (i < num_threads / 2),
            .get_errors = 0,
        };
        pthread_create(&threads[i], NULL, t3_worker, &ctx[i]);
    }

    int total_get_errors = 0;
    for (int i = 0; i < num_threads; i++)
    {
        pthread_join(threads[i], NULL);
        total_get_errors += ctx[i].get_errors;
    }

    int ok = run_inspect("post-stress", list);
    if (total_get_errors > 0)
    {
        printf("  value FAIL: %d get mismatches\n", total_get_errors);
        ok = 0;
    }

    free(ctx);
    free(threads);
    bsl_destroy(list);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Test 4: Sequential insert                                            */
/* Each thread owns a non-overlapping key range and inserts in order.  */
/* Forces splits to propagate along the rightmost path.                */
/* ------------------------------------------------------------------ */

#define T4_KEYS_PER_THREAD  50000

typedef struct
{
    bsl_t    *list;
    bsl_key_t start;
    bsl_key_t end;
} t4_ctx_t;

static void *
t4_worker(void *arg)
{
    t4_ctx_t *ctx = (t4_ctx_t *)arg;
    for (bsl_key_t k = ctx->start; k <= ctx->end; k++)
        bsl_insert(ctx->list, k, expected_val(k));
    return NULL;
}

static int
test_sequential_insert(int num_threads)
{
    printf("\n[Test 4] Sequential insert  (%d keys/thread, threads=%d)\n",
           T4_KEYS_PER_THREAD, num_threads);

    bsl_t     *list    = bsl_new();
    t4_ctx_t  *ctx     = malloc((size_t)num_threads * sizeof(t4_ctx_t));
    pthread_t *threads = malloc((size_t)num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        ctx[i] = (t4_ctx_t){
            .list  = list,
            .start = (bsl_key_t)(i * T4_KEYS_PER_THREAD + 1),
            .end   = (bsl_key_t)((i + 1) * T4_KEYS_PER_THREAD),
        };
        pthread_create(&threads[i], NULL, t4_worker, &ctx[i]);
    }
    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    int ok   = run_inspect("post-stress", list);
    int errs = verify_all_keys(list, (bsl_key_t)(num_threads * T4_KEYS_PER_THREAD));
    if (errs > 0)
    {
        printf("  value FAIL: %d mismatches\n", errs);
        ok = 0;
    }

    free(ctx);
    free(threads);
    bsl_destroy(list);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Test 5: Thundering herd                                              */
/* All threads insert/delete the exact same key simultaneously.        */
/* Only one insert wins per round; tests the found_key path heavily.   */
/* ------------------------------------------------------------------ */

#define T5_HOT_KEYS  8
#define T5_ROUNDS    1000000

typedef struct { bsl_t *list; int tid; } t5_ctx_t;

static void *
t5_worker(void *arg)
{
    t5_ctx_t    *ctx  = (t5_ctx_t *)arg;
    unsigned int seed = (unsigned int)ctx->tid ^ 0xbaadf00d;

    for (int i = 0; i < T5_ROUNDS; i++)
    {
        bsl_key_t key = (bsl_key_t)(rand_r(&seed) % T5_HOT_KEYS) + 1;

        if (rand_r(&seed) % 2)
            bsl_insert(ctx->list, key, expected_val(key));
        else
            bsl_delete(ctx->list, key);
    }
    return NULL;
}

static int
test_thundering_herd(int num_threads)
{
    printf("\n[Test 5] Thundering herd  (%d hot keys, threads=%d)\n",
           T5_HOT_KEYS, num_threads);

    bsl_t     *list    = bsl_new();
    t5_ctx_t  *ctx     = malloc((size_t)num_threads * sizeof(t5_ctx_t));
    pthread_t *threads = malloc((size_t)num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        ctx[i] = (t5_ctx_t){ .list = list, .tid = i };
        pthread_create(&threads[i], NULL, t5_worker, &ctx[i]);
    }
    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    int ok   = run_inspect("post-stress", list);
    int errs = verify_all_keys(list, (bsl_key_t)T5_HOT_KEYS);
    if (errs > 0)
    {
        printf("  value FAIL: %d mismatches\n", errs);
        ok = 0;
    }

    free(ctx);
    free(threads);
    bsl_destroy(list);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Test 6: Fill / Drain oscillation                                     */
/* Odd threads insert, even threads delete, same key range.            */
/* Structure oscillates between full and empty; tests both paths       */
/* under sustained concurrent pressure.                                 */
/* ------------------------------------------------------------------ */

#define T6_KEY_RANGE  500000
#define T6_OPS        2000000

typedef struct
{
    bsl_t *list;
    int    tid;
    int    is_inserter;
} t6_ctx_t;

static void *
t6_worker(void *arg)
{
    t6_ctx_t    *ctx  = (t6_ctx_t *)arg;
    unsigned int seed = (unsigned int)ctx->tid ^ 0x12345678;

    for (int i = 0; i < T6_OPS; i++)
    {
        bsl_key_t key = (bsl_key_t)(rand_r(&seed) % T6_KEY_RANGE) + 1;

        if (ctx->is_inserter)
            bsl_insert(ctx->list, key, expected_val(key));
        else
            bsl_delete(ctx->list, key);
    }
    return NULL;
}

static int
test_fill_drain(int num_threads)
{
    printf("\n[Test 6] Fill/Drain oscillation  (range=%d, threads=%d)\n",
           T6_KEY_RANGE, num_threads);

    bsl_t     *list    = bsl_new();
    t6_ctx_t  *ctx     = malloc((size_t)num_threads * sizeof(t6_ctx_t));
    pthread_t *threads = malloc((size_t)num_threads * sizeof(pthread_t));

    for (int i = 0; i < num_threads; i++)
    {
        ctx[i] = (t6_ctx_t){
            .list        = list,
            .tid         = i,
            .is_inserter = (i % 2 == 0),
        };
        pthread_create(&threads[i], NULL, t6_worker, &ctx[i]);
    }
    for (int i = 0; i < num_threads; i++)
        pthread_join(threads[i], NULL);

    int ok   = run_inspect("post-stress", list);
    int errs = verify_all_keys(list, (bsl_key_t)T6_KEY_RANGE);
    if (errs > 0)
    {
        printf("  value FAIL: %d mismatches\n", errs);
        ok = 0;
    }

    free(ctx);
    free(threads);
    bsl_destroy(list);
    return ok;
}

/* ------------------------------------------------------------------ */
/* Main                                                                 */
/* ------------------------------------------------------------------ */

int
main(int argc, char **argv)
{
    int num_threads = (argc > 1) ? atoi(argv[1]) : DEFAULT_NUM_THREADS;

    printf("====================================================\n");
    printf("B-SkipList Correctness Tests\n");
    printf("Threads : %d\n", num_threads);
    printf("====================================================\n");

    int results[6];
    results[0] = test_contention_hammer(num_threads);
    results[1] = test_delete_heavy(num_threads);
    results[2] = test_writer_reader_split(num_threads);
    results[3] = test_sequential_insert(num_threads);
    results[4] = test_thundering_herd(num_threads);
    results[5] = test_fill_drain(num_threads);

    printf("\n====================================================\n");
    printf("Results\n");
    printf("----------------------------------------------------\n");
    printf("  Test 1  Contention hammer     : %s\n", results[0] ? "PASS" : "FAIL");
    printf("  Test 2  Delete heavy          : %s\n", results[1] ? "PASS" : "FAIL");
    printf("  Test 3  Writer/Reader split   : %s\n", results[2] ? "PASS" : "FAIL");
    printf("  Test 4  Sequential insert     : %s\n", results[3] ? "PASS" : "FAIL");
    printf("  Test 5  Thundering herd       : %s\n", results[4] ? "PASS" : "FAIL");
    printf("  Test 6  Fill/Drain oscillation: %s\n", results[5] ? "PASS" : "FAIL");
    printf("====================================================\n");

    int all_ok = results[0] && results[1] && results[2] &&
                 results[3] && results[4] && results[5];

    return all_ok ? EXIT_SUCCESS : EXIT_FAILURE;
}