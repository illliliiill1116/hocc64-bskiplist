#include "bskiplist.h"
#include "node.h"
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ------------------------------------------------------------------ */
/*  Minimal test harness                                               */
/* ------------------------------------------------------------------ */

static int g_tests_run = 0;
static int g_tests_passed = 0;
static int g_tests_failed = 0;

#define RUN_TEST(fn)                                                           \
  do {                                                                         \
    g_tests_run++;                                                             \
    printf("  [%2d] %-45s ", g_tests_run, #fn);                                \
    fflush(stdout);                                                            \
    int _rc = fn();                                                            \
    if (_rc == 0) {                                                            \
      g_tests_passed++;                                                        \
      printf("\033[1;32mPASS\033[0m\n");                                       \
    } else {                                                                   \
      g_tests_failed++;                                                        \
      printf("\033[1;31mFAIL\033[0m\n");                                       \
    }                                                                          \
  } while (0)

#define EXPECT(cond)                                                           \
  do {                                                                         \
    if (!(cond)) {                                                             \
      fprintf(stderr, "\n    EXPECT FAILED: %s  [%s:%d]\n", #cond, __FILE__,   \
              __LINE__);                                                       \
      return 1;                                                                \
    }                                                                          \
  } while (0)

/* Thread-safe EXPECT — sets shared error flag instead of returning */
#define THREAD_EXPECT(cond, err_flag)                                          \
  do {                                                                         \
    if (!(cond)) {                                                             \
      fprintf(stderr, "\n    THREAD_EXPECT FAILED: %s  [%s:%d]\n", #cond,      \
              __FILE__, __LINE__);                                             \
      atomic_store(err_flag, 1);                                               \
    }                                                                          \
  } while (0)

/* ------------------------------------------------------------------ */
/*  Constants                                                          */
/* ------------------------------------------------------------------ */

#define NUM_THREADS 8

/* ------------------------------------------------------------------ */
/*  Portable Barrier (macOS lacks pthread_barrier_t)                    */
/* ------------------------------------------------------------------ */

typedef struct {
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int count;
  int total;
  int generation;
} portable_barrier_t;

static portable_barrier_t g_barrier;

static void barrier_init(void) {
  pthread_mutex_init(&g_barrier.mutex, NULL);
  pthread_cond_init(&g_barrier.cond, NULL);
  g_barrier.count = 0;
  g_barrier.total = NUM_THREADS;
  g_barrier.generation = 0;
}

static void barrier_wait(void) {
  pthread_mutex_lock(&g_barrier.mutex);
  int gen = g_barrier.generation;
  g_barrier.count++;
  if (g_barrier.count == g_barrier.total) {
    g_barrier.count = 0;
    g_barrier.generation++;
    pthread_cond_broadcast(&g_barrier.cond);
  } else {
    while (gen == g_barrier.generation)
      pthread_cond_wait(&g_barrier.cond, &g_barrier.mutex);
  }
  pthread_mutex_unlock(&g_barrier.mutex);
}

static void barrier_destroy(void) {
  pthread_mutex_destroy(&g_barrier.mutex);
  pthread_cond_destroy(&g_barrier.cond);
}

/* Verify leaf-level structural integrity.
   Returns: 0 = ok, 1 = error. Prints issues to stderr.
   Writes total element count to *out_count if non-NULL. */
static int verify_structure(bsl_t *list, long *out_count) {
  leaf_node_t *curr = (leaf_node_t *)list->headers[0];
  bsl_key_t prev_key = 0;
  int is_first = 1;
  long total = 0;
  int errors = 0;

  while (curr) {
    if (curr->header.level != 0) {
      fprintf(stderr, "  STRUCT ERR: non-leaf at level 0\n");
      errors++;
    }

    for (uint32_t i = 0; i < curr->header.num_elts; i++) {
      if (!is_first && curr->keys[i] <= prev_key) {
        fprintf(stderr, "  STRUCT ERR: sorting violation %llu <= %llu\n",
                (unsigned long long)curr->keys[i],
                (unsigned long long)prev_key);
        errors++;
      }
      is_first = 0;
      prev_key = curr->keys[i];
      total++;
    }

    leaf_node_t *next = (leaf_node_t *)curr->header.next;
    if (next) {
      if (curr->header.next_header != next->keys[0]) {
        fprintf(stderr,
                "  STRUCT ERR: next_header %llu != next->keys[0] %llu\n",
                (unsigned long long)curr->header.next_header,
                (unsigned long long)next->keys[0]);
        errors++;
      }
    } else {
      if (curr->header.next_header != BSL_KEY_MAX) {
        fprintf(stderr, "  STRUCT ERR: last node next_header != BSL_KEY_MAX\n");
        errors++;
      }
    }
    curr = next;
  }

  if (out_count)
    *out_count = total;
  return errors ? 1 : 0;
}

/* ================================================================== */
/*  Test 1: Concurrent Disjoint Inserts                                */
/* ================================================================== */

typedef struct {
  bsl_t *list;
  bsl_key_t start;
  bsl_key_t end;
  atomic_int *err;
} range_args_t;

static void *disjoint_insert_worker(void *arg) {
  range_args_t *a = (range_args_t *)arg;
  barrier_wait();

  for (bsl_key_t k = a->start; k < a->end; k++) {
    int rc = bsl_insert(a->list, k, k + 1000);
    THREAD_EXPECT(rc == 1, a->err);
  }
  return NULL;
}

static int test_concurrent_disjoint_insert(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  const int KEYS_PER_THREAD = 10000;
  atomic_int err = 0;

  barrier_init();
  pthread_t threads[NUM_THREADS];
  range_args_t args[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    args[i].list = list;
    args[i].start = (bsl_key_t)(i * KEYS_PER_THREAD + 1);
    args[i].end = (bsl_key_t)((i + 1) * KEYS_PER_THREAD + 1);
    args[i].err = &err;
    pthread_create(&threads[i], NULL, disjoint_insert_worker, &args[i]);
  }

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  barrier_destroy();

  EXPECT(atomic_load(&err) == 0);

  /* Verify all keys present */
  for (int i = 0; i < NUM_THREADS; i++) {
    for (bsl_key_t k = args[i].start; k < args[i].end; k++) {
      bsl_val_t v;
      EXPECT(bsl_get(list, k, &v));
      EXPECT(v == k + 1000);
    }
  }

  /* Structural check */
  long count = 0;
  EXPECT(verify_structure(list, &count) == 0);
  EXPECT(count == (long)NUM_THREADS * KEYS_PER_THREAD + 1); /* +1 sentinel */

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Test 2: Concurrent Insert + Remove                                 */
/* ================================================================== */

typedef struct {
  bsl_t *list;
  int thread_id;
  atomic_int *err;
} thread_ctx_t;

#define IR_KEY_RANGE 20000

static void *inserter_worker(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  /* Insert odd keys in the range */
  for (bsl_key_t k = 1; k <= IR_KEY_RANGE; k += 2)
    bsl_insert(ctx->list, k, k);

  return NULL;
}

static void *remover_worker(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  /* Remove even keys (some may not exist) */
  for (bsl_key_t k = 2; k <= IR_KEY_RANGE; k += 2)
    bsl_delete(ctx->list, k);

  return NULL;
}

static int test_concurrent_insert_remove(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  /* Preload all keys 1..IR_KEY_RANGE */
  for (bsl_key_t k = 1; k <= IR_KEY_RANGE; k++)
    bsl_insert(list, k, k);

  barrier_init();
  pthread_t threads[NUM_THREADS];
  thread_ctx_t ctx[NUM_THREADS];
  atomic_int err = 0;

  /* Half inserters (re-insert odd), half removers (remove even) */
  for (int i = 0; i < NUM_THREADS; i++) {
    ctx[i].list = list;
    ctx[i].thread_id = i;
    ctx[i].err = &err;

    if (i < NUM_THREADS / 2)
      pthread_create(&threads[i], NULL, inserter_worker, &ctx[i]);
    else
      pthread_create(&threads[i], NULL, remover_worker, &ctx[i]);
  }

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  barrier_destroy();

  /* After: odd keys should exist, even keys should be removed */
  for (bsl_key_t k = 1; k <= IR_KEY_RANGE; k++) {
    bsl_val_t v;
    int rc = bsl_get(list, k, &v);
    if (k % 2 == 1) {
      EXPECT(rc);
      EXPECT(v == k);
    } else {
      EXPECT(rc == 0);
    }
  }

  EXPECT(verify_structure(list, NULL) == 0);

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Test 3: Concurrent Read-Write (Linearizability)                    */
/* ================================================================== */

#define RW_KEY_RANGE 50000
static atomic_int g_rw_stop;

static void *rw_writer(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  unsigned int seed = (unsigned int)(ctx->thread_id * 1103515245 + 12345);
  while (!atomic_load(&g_rw_stop)) {
    bsl_key_t k = (bsl_key_t)(rand_r(&seed) % RW_KEY_RANGE + 1);
    if (rand_r(&seed) % 2 == 0)
      bsl_insert(ctx->list, k, k * 7);
    else
      bsl_delete(ctx->list, k);
  }
  return NULL;
}

static void *rw_reader(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  unsigned int seed = (unsigned int)(ctx->thread_id * 7654321 + 99999);
  int reads = 0;
  while (!atomic_load(&g_rw_stop)) {
    bsl_key_t k = (bsl_key_t)(rand_r(&seed) % RW_KEY_RANGE + 1);
    bsl_val_t v = 0;
    int rc = bsl_get(ctx->list, k, &v);

    /* Linearizability check: if found, value must be k * 7.
       Any other value indicates a concurrency bug (stale/corrupt read). */
    if (rc && v != k * 7) {
      fprintf(
          stderr,
          "    READ-WRITE BUG: key=%llu expected_val=%llu actual_val=%llu\n",
          (unsigned long long)k, (unsigned long long)(k * 7),
          (unsigned long long)v);
      atomic_store(ctx->err, 1);
    }

    reads++;
    if (reads > 500000)
      break; /* safety limit */
  }
  return NULL;
}

static int test_concurrent_read_write(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  atomic_store(&g_rw_stop, 0);
  atomic_int err = 0;

  barrier_init();
  pthread_t threads[NUM_THREADS];
  thread_ctx_t ctx[NUM_THREADS];

  /* Half writers, half readers */
  for (int i = 0; i < NUM_THREADS; i++) {
    ctx[i].list = list;
    ctx[i].thread_id = i;
    ctx[i].err = &err;

    if (i < NUM_THREADS / 2)
      pthread_create(&threads[i], NULL, rw_writer, &ctx[i]);
    else
      pthread_create(&threads[i], NULL, rw_reader, &ctx[i]);
  }

  /* Let it run for ~2 seconds */
  struct timespec ts = {.tv_sec = 2, .tv_nsec = 0};
  nanosleep(&ts, NULL);

  atomic_store(&g_rw_stop, 1);

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  barrier_destroy();

  EXPECT(atomic_load(&err) == 0);
  EXPECT(verify_structure(list, NULL) == 0);

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Test 4: Concurrent Overlapping Inserts                             */
/* ================================================================== */

#define OI_KEY_RANGE 10000

static void *overlapping_insert_worker(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  bsl_val_t my_val = (bsl_val_t)(ctx->thread_id + 1) * 1000;

  for (bsl_key_t k = 1; k <= OI_KEY_RANGE; k++)
    bsl_insert(ctx->list, k, my_val + k);

  return NULL;
}

static int test_concurrent_overlapping_insert(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  atomic_int err = 0;

  barrier_init();
  pthread_t threads[NUM_THREADS];
  thread_ctx_t ctx[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    ctx[i].list = list;
    ctx[i].thread_id = i;
    ctx[i].err = &err;
    pthread_create(&threads[i], NULL, overlapping_insert_worker, &ctx[i]);
  }

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  barrier_destroy();

  /* Each key should exist exactly once with a value from one of the threads */
  for (bsl_key_t k = 1; k <= OI_KEY_RANGE; k++) {
    bsl_val_t v;
    EXPECT(bsl_get(list, k, &v));

    /* Value must be thread_val + k for some thread */
    int valid = 0;
    for (int t = 0; t < NUM_THREADS; t++) {
      bsl_val_t expected = (bsl_val_t)(t + 1) * 1000 + k;
      if (v == expected) {
        valid = 1;
        break;
      }
    }
    EXPECT(valid);
  }

  /* Structural check: exactly OI_KEY_RANGE + 1 (sentinel) elements */
  long count = 0;
  EXPECT(verify_structure(list, &count) == 0);
  EXPECT(count == OI_KEY_RANGE + 1);

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Test 5: Concurrent Scan During Writes                              */
/* ================================================================== */

#define SDW_KEY_RANGE 30000
static atomic_int g_sdw_stop;

static void *sdw_writer(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  unsigned int seed = (unsigned int)(ctx->thread_id * 314159 + 271828);
  while (!atomic_load(&g_sdw_stop)) {
    bsl_key_t k = (bsl_key_t)(rand_r(&seed) % SDW_KEY_RANGE + 1);
    bsl_insert(ctx->list, k, k);
  }
  return NULL;
}

/* Batch scan callback: verify sorted and values match keys */
typedef struct {
  bsl_key_t last_key;
  int sorted;
  int values_ok;
  size_t total;
} scan_check_t;

static void scan_check_batch_cb(bsl_range_t range, void *arg) {
  scan_check_t *c = (scan_check_t *)arg;
  for (size_t i = 0; i < range.count; i++) {
    bsl_key_t key = range.keys[i];
    bsl_val_t val = range.vals[i];

    if (key <= c->last_key && c->last_key != 0)
      c->sorted = 0;
    if (val != key)
      c->values_ok = 0;
    c->last_key = key;
    c->total++;
  }
}

static void *sdw_scanner(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  unsigned int seed = (unsigned int)(ctx->thread_id * 999983 + 1);
  int scans = 0;
  while (!atomic_load(&g_sdw_stop)) {
    bsl_key_t start = (bsl_key_t)(rand_r(&seed) % SDW_KEY_RANGE + 1);
    scan_check_t check = {
        .last_key = 0, .sorted = 1, .values_ok = 1, .total = 0};
    bsl_limit_scan_batch(ctx->list, start, 100, scan_check_batch_cb, &check);

    THREAD_EXPECT(check.sorted, ctx->err);
    THREAD_EXPECT(check.values_ok, ctx->err);

    scans++;
    if (scans > 50000)
      break;
  }
  return NULL;
}

static int test_concurrent_scan_during_writes(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  /* Preload */
  for (bsl_key_t k = 1; k <= SDW_KEY_RANGE / 2; k++)
    bsl_insert(list, k, k);

  atomic_store(&g_sdw_stop, 0);
  atomic_int err = 0;

  barrier_init();
  pthread_t threads[NUM_THREADS];
  thread_ctx_t ctx[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    ctx[i].list = list;
    ctx[i].thread_id = i;
    ctx[i].err = &err;

    if (i < NUM_THREADS / 2)
      pthread_create(&threads[i], NULL, sdw_writer, &ctx[i]);
    else
      pthread_create(&threads[i], NULL, sdw_scanner, &ctx[i]);
  }

  struct timespec ts = {.tv_sec = 2, .tv_nsec = 0};
  nanosleep(&ts, NULL);
  atomic_store(&g_sdw_stop, 1);

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  barrier_destroy();

  EXPECT(atomic_load(&err) == 0);
  EXPECT(verify_structure(list, NULL) == 0);

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Test 6: Concurrent All-Ops Mixed                                   */
/* ================================================================== */

#define AO_KEY_RANGE 50000
static atomic_int g_ao_stop;

typedef struct {
  bsl_key_t keys;
  bsl_val_t vals;
  size_t count;
} batch_sum_ctx_t;

static void batch_sum_cb(bsl_range_t range, void *arg) {
  batch_sum_ctx_t *c = (batch_sum_ctx_t *)arg;
  c->count += range.count;
  (void)range; /* consume */
}

static void *ao_worker(void *arg) {
  thread_ctx_t *ctx = (thread_ctx_t *)arg;
  barrier_wait();

  unsigned int seed = (unsigned int)(ctx->thread_id * 7919 + 6563);
  while (!atomic_load(&g_ao_stop)) {
    bsl_key_t k = (bsl_key_t)(rand_r(&seed) % AO_KEY_RANGE + 1);
    int op = rand_r(&seed) % 100;

    if (op < 30) {
      bsl_insert(ctx->list, k, k);
    } else if (op < 50) {
      bsl_delete(ctx->list, k);
    } else if (op < 80) {
      bsl_val_t v;
      int rc = bsl_get(ctx->list, k, &v);
      if (rc)
        THREAD_EXPECT(v == k, ctx->err);
    } else {
      batch_sum_ctx_t bctx = {.count = 0};
      bsl_limit_scan_batch(ctx->list, k, 50, batch_sum_cb, &bctx);
    }
  }
  return NULL;
}

static int test_concurrent_all_ops(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  /* Preload */
  for (bsl_key_t k = 1; k <= AO_KEY_RANGE / 2; k++)
    bsl_insert(list, k, k);

  atomic_store(&g_ao_stop, 0);
  atomic_int err = 0;

  barrier_init();
  pthread_t threads[NUM_THREADS];
  thread_ctx_t ctx[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    ctx[i].list = list;
    ctx[i].thread_id = i;
    ctx[i].err = &err;
    pthread_create(&threads[i], NULL, ao_worker, &ctx[i]);
  }

  struct timespec ts = {.tv_sec = 3, .tv_nsec = 0};
  nanosleep(&ts, NULL);
  atomic_store(&g_ao_stop, 1);

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  barrier_destroy();

  EXPECT(atomic_load(&err) == 0);
  EXPECT(verify_structure(list, NULL) == 0);

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Test 7: High-Volume Stress                                         */
/* ================================================================== */

#define STRESS_OPS_PER_THREAD 1000000
#define STRESS_KEY_RANGE 200000

typedef struct {
  bsl_t *list;
  int thread_id;
  atomic_int *err;
} stress_ctx_t;

static void *stress_worker(void *arg) {
  stress_ctx_t *ctx = (stress_ctx_t *)arg;
  barrier_wait();

  unsigned int seed = (unsigned int)(ctx->thread_id * 48271 + 1);
  for (int i = 0; i < STRESS_OPS_PER_THREAD; i++) {
    bsl_key_t k = (bsl_key_t)(rand_r(&seed) % STRESS_KEY_RANGE + 1);
    int op = rand_r(&seed) % 100;

    if (op < 40)
      bsl_insert(ctx->list, k, k);
    else if (op < 60)
      bsl_delete(ctx->list, k);
    else if (op < 90) {
      bsl_val_t v;
      int rc = bsl_get(ctx->list, k, &v);
      if (rc)
        THREAD_EXPECT(v == k, ctx->err);
    } else {
      batch_sum_ctx_t bctx = {.count = 0};
      bsl_limit_scan_batch(ctx->list, k, 20, batch_sum_cb, &bctx);
    }
  }
  return NULL;
}

static int test_concurrent_stress(void) {
  bsl_t *list = bsl_new();
  EXPECT(list != NULL);

  /* Preload half the keys */
  for (bsl_key_t k = 1; k <= STRESS_KEY_RANGE / 2; k++)
    bsl_insert(list, k, k);

  atomic_int err = 0;

  barrier_init();
  pthread_t threads[NUM_THREADS];
  stress_ctx_t ctx[NUM_THREADS];

  struct timespec t0, t1;
  clock_gettime(CLOCK_MONOTONIC, &t0);

  for (int i = 0; i < NUM_THREADS; i++) {
    ctx[i].list = list;
    ctx[i].thread_id = i;
    ctx[i].err = &err;
    pthread_create(&threads[i], NULL, stress_worker, &ctx[i]);
  }

  for (int i = 0; i < NUM_THREADS; i++)
    pthread_join(threads[i], NULL);

  clock_gettime(CLOCK_MONOTONIC, &t1);
  barrier_destroy();

  double secs = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
  long total_ops = (long)NUM_THREADS * STRESS_OPS_PER_THREAD;
  printf("\n    [stress: %.2fs, %.1f Mops/s] ", secs, total_ops / secs / 1e6);

  EXPECT(atomic_load(&err) == 0);

  /* Final structural integrity check */
  long count = 0;
  EXPECT(verify_structure(list, &count) == 0);
  printf("[%ld elements] ", count);

  bsl_destroy(list);
  return 0;
}

/* ================================================================== */
/*  Main                                                               */
/* ================================================================== */

int main(void) {
  printf("\n================================================\n");
  printf("  hocc64-bskiplist  Concurrent Correctness Tests\n");
  printf("  Threads: %d\n", NUM_THREADS);
  printf("================================================\n\n");

  printf("--- Disjoint Inserts ---\n");
  RUN_TEST(test_concurrent_disjoint_insert);

  printf("\n--- Insert + Remove ---\n");
  RUN_TEST(test_concurrent_insert_remove);

  printf("\n--- Read-Write Conflict ---\n");
  RUN_TEST(test_concurrent_read_write);

  printf("\n--- Overlapping Inserts ---\n");
  RUN_TEST(test_concurrent_overlapping_insert);

  printf("\n--- Scan During Writes ---\n");
  RUN_TEST(test_concurrent_scan_during_writes);

  printf("\n--- All-Ops Mixed ---\n");
  RUN_TEST(test_concurrent_all_ops);

  printf("\n--- Stress Test ---\n");
  RUN_TEST(test_concurrent_stress);

  printf("\n================================================\n");
  printf("  Results:  %d/%d passed", g_tests_passed, g_tests_run);
  if (g_tests_failed > 0)
    printf("  (\033[1;31m%d FAILED\033[0m)", g_tests_failed);
  else
    printf("  (\033[1;32mALL PASSED\033[0m)");
  printf("\n================================================\n\n");

  return g_tests_failed ? 1 : 0;
}
