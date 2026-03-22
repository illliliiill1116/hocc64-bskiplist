#include "epoch.h"
#include "util.h"
#include "node.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <pthread.h>

#ifndef CACHE_LINE_SIZE
#  define CACHE_LINE_SIZE 64
#endif

#define CACHE_ALIGNED __attribute__((aligned(CACHE_LINE_SIZE)))

/*
 * GC_NEXT - access the intrusive next pointer embedded at the tail of a node.
 *
 * Instead of a wrapper struct, we reuse NODE_GC_NEXT_OFFSET bytes at the end
 * of each node to link the pending reclamation list. This eliminates per-retire
 * malloc overhead and indirect function calls through a free callback.
 * node.h guarantees via STATIC_ASSERT that this region does not overlap
 * with valid keys, values, or children.
 */
#define GC_NEXT(ptr) (*(void**)((char*)(ptr) + NODE_GC_NEXT_OFFSET))

/* ------------------------------------------------------------------ */
/* Lock-free MPSC stack                                               */
/*                                                                    */
/* Used to collect pending nodes from threads that exit before GC     */
/* runs. Multiple threads may push concurrently (Treiber CAS loop);   */
/* only the GC winner calls pop_all (single consumer, no ABA risk).   */
/* ------------------------------------------------------------------ */

typedef struct
{
    void* volatile head;
    char _pad[CACHE_LINE_SIZE - sizeof(void* volatile)];
} CACHE_ALIGNED gc_stack_t;

STATIC_ASSERT(sizeof(void*) <= CACHE_LINE_SIZE);

static void gc_stack_push(gc_stack_t *s, void *node)
{
    void *old = LOAD_RELAXED(s->head);
    do
    {
        GC_NEXT(node) = old;
    }
    while (!CAS_WEAK(s->head, &old, node, __ATOMIC_RELEASE, __ATOMIC_RELAXED));
}

static void *gc_stack_pop_all(gc_stack_t *s)
{
    return EXCHANGE_ACQUIRE(s->head, NULL);
}


typedef struct slot_node {
    int index;
    struct slot_node *next;
} slot_node_t;

static slot_node_t g_slot_nodes[EBR_MAX_THREADS];
static slot_node_t * volatile  g_free_slot_head = NULL;

static void free_slot_push(int index)
{
    slot_node_t *node = &g_slot_nodes[index];
    node->index = index;
    slot_node_t *old;
    do {
        old = LOAD_RELAXED(g_free_slot_head);
        node->next = old;
    }
    while (!CAS_WEAK(g_free_slot_head, &old, node,
                     __ATOMIC_RELEASE, __ATOMIC_RELAXED));
}

static int free_slot_pop(void)
{
    slot_node_t *old;
    do {
        old = LOAD_ACQUIRE(g_free_slot_head);
        if (!old) return -1;
    }
    while (!CAS_WEAK(g_free_slot_head, &old, old->next,
                     __ATOMIC_ACQUIRE, __ATOMIC_RELAXED));
    return old->index;
}


typedef struct
{
    /* cache line 0 - read by remote threads during GC scan */
    volatile int in_critical;
    volatile int local_epoch;
    char _pad0[CACHE_LINE_SIZE - sizeof(int) * 2];

    /* cache line 1 - written only by the owning thread */
    int   op_count;
    int   epoch_tick;
    int   slot_index;
    void *pending[EBR_EPOCH_COUNT];
    char  _pad1[CACHE_LINE_SIZE
                - sizeof(int) * 3
                - sizeof(void*) * EBR_EPOCH_COUNT];
} CACHE_ALIGNED thread_ctx_t;

STATIC_ASSERT(sizeof(int) * 2 <= CACHE_LINE_SIZE);
STATIC_ASSERT(sizeof(int) * 3 + sizeof(void*) * EBR_EPOCH_COUNT <= CACHE_LINE_SIZE);

/* ------------------------------------------------------------------ */
/* Global state                                                       */
/* ------------------------------------------------------------------ */

static volatile int           g_epoch              = 0;
static volatile int           g_n_threads          = 0;
static thread_ctx_t* volatile g_contexts[EBR_MAX_THREADS];
static gc_stack_t             g_retired[EBR_EPOCH_COUNT];

#ifdef DEBUG
static volatile long g_stat_retired   = 0;
static volatile long g_stat_reclaimed = 0;
#endif

static pthread_key_t  g_tls_key;
static pthread_once_t g_tls_once = PTHREAD_ONCE_INIT;

static __thread thread_ctx_t *my_ctx = NULL;

/* ------------------------------------------------------------------ */
/* Internal: reclaim a linked list of nodes                           */
/* ------------------------------------------------------------------ */

static void reclaim_list(void *list)
{
    while (list)
    {
        void *next = GC_NEXT(list);
        bsl_node_destroy(list);
#ifdef DEBUG
        FETCH_ADD_RELAXED(g_stat_reclaimed, 1L);
#endif
        list = next;
    }
}

/* ------------------------------------------------------------------ */
/* Internal: attempt to advance the epoch and reclaim safe nodes      */
/* ------------------------------------------------------------------ */

static void try_gc(void)
{
    int cur = LOAD_ACQUIRE(g_epoch);
    int n   = LOAD_ACQUIRE(g_n_threads);
    int i;

    /*
     * Scan all registered threads. If any thread is inside a critical
     * section with local_epoch != cur, it may hold a pointer into the
     * previous epoch's nodes. Abort to avoid premature reclamation.
     */
    for (i = 0; i < n; i++)
    {
        thread_ctx_t *ctx = LOAD_ACQUIRE(g_contexts[i]);
        if (!ctx) continue;

        if (LOAD_ACQUIRE(ctx->in_critical))
        {
            if (LOAD_ACQUIRE(ctx->local_epoch) != cur)
                return;
        }
    }

    /*
     * Race to advance the epoch. Only one thread wins; all others
     * return and retry on their next GC_THRESHOLD boundary.
     */
    {
        int expected = cur;
        if (!CAS_STRONG(g_epoch, &expected, cur + 1,
                        __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
            return;
    }

    /*
     * Reclaim slot (cur - 1) % EPOCH_COUNT.
     *
     * After advancing to cur+1, slot cur%3 is still accumulating new
     * retires. Slot (cur-1)%3 is safe: the scan above confirmed every
     * active thread has already observed epoch cur, so no thread can
     * hold a pointer into that slot's nodes.
     *
     * Edge case: cur=0 gives slot (0-1+3)%3 = 2, which is empty at
     * startup, so reclaiming it is harmless.
     */
    {
        int   rslot  = ((cur - 1) % EBR_EPOCH_COUNT + EBR_EPOCH_COUNT)
                       % EBR_EPOCH_COUNT;
        void *to_free;
        void *local;
        void *tail;

        to_free = gc_stack_pop_all(&g_retired[rslot]);

        local                   = my_ctx->pending[rslot];
        my_ctx->pending[rslot]  = NULL;

        if (local)
        {
            if (!to_free)
            {
                to_free = local;
            }
            else
            {
                tail = to_free;
                while (GC_NEXT(tail)) tail = GC_NEXT(tail);
                GC_NEXT(tail) = local;
            }
        }

        reclaim_list(to_free);
    }
}

/* ------------------------------------------------------------------ */
/* TLS destructor - called automatically by pthread on thread exit    */
/* ------------------------------------------------------------------ */

static void thread_destructor(void *val)
{
    thread_ctx_t *ctx = (thread_ctx_t *)val;
    int e;

    if (!ctx) return;

    /*
     * Clear the slot so try_gc() no longer waits for this thread,
     * then return the index to the free list for reuse.
     */
    STORE_RELEASE(g_contexts[ctx->slot_index], (thread_ctx_t *)NULL);
    free_slot_push(ctx->slot_index);

    /*
     * Transfer pending nodes to the global retired stacks so the
     * next GC winner can reclaim them.
     */
    for (e = 0; e < EBR_EPOCH_COUNT; e++)
    {
        void *node = ctx->pending[e];
        while (node)
        {
            void *next = GC_NEXT(node);
            gc_stack_push(&g_retired[e], node);
            node = next;
        }
        ctx->pending[e] = NULL;
    }

    free(ctx);
    my_ctx = NULL;
}

static void tls_key_init(void)
{
    int rc = pthread_key_create(&g_tls_key, thread_destructor);
    assert(rc == 0);
    (void)rc;
}

/* ------------------------------------------------------------------ */
/* Thread context initialisation                                      */
/* ------------------------------------------------------------------ */

static void thread_init(void)
{
    thread_ctx_t *ctx;
    int idx;

    pthread_once(&g_tls_once, tls_key_init);

    if (posix_memalign((void **)&ctx, CACHE_LINE_SIZE,
                       sizeof(thread_ctx_t)) != 0)
        abort();
    memset(ctx, 0, sizeof(thread_ctx_t));

    /*
     * Try to reuse a freed slot first; only allocate a new one if
     * the free list is empty. This keeps g_n_threads stable under
     * steady-state thread churn and bounds try_gc() scan cost.
     */
    idx = free_slot_pop();
    if (idx == -1)
    {
        idx = (int)FETCH_ADD_RELAXED(g_n_threads, 1);
        if (idx >= EBR_MAX_THREADS)
        {
            free(ctx);
            abort();
        }
    }

    ctx->slot_index = idx;
    STORE_RELEASE(g_contexts[idx], ctx);
    my_ctx = ctx;

    pthread_setspecific(g_tls_key, ctx);
}

/* ------------------------------------------------------------------ */
/* Public API                                                         */
/* ------------------------------------------------------------------ */

void epoch_enter(void)
{
    if (unlikely(!my_ctx))
        thread_init();

    /*
     * Hot-path optimisation: local_epoch is refreshed lazily in
     * epoch_exit every EBR_GC_THRESHOLD operations, so epoch_enter
     * only needs a single STORE_RELEASE to announce entry.
     *
     * The RELEASE ordering ensures local_epoch (written by a prior
     * exit) is visible to any thread that observes in_critical == 1
     * via an ACQUIRE load, satisfying the scanner's happens-before
     * requirement without a separate fence or per-enter atomic load.
     */
    STORE_RELEASE(my_ctx->in_critical, 1);
}

void epoch_exit(void)
{
    assert(my_ctx && "epoch_exit called without matching epoch_enter");

    STORE_RELEASE(my_ctx->in_critical, 0);

    /*
     * Amortised local_epoch refresh: read g_epoch and attempt GC only
     * every EBR_GC_THRESHOLD exits. This keeps the common read path
     * down to a single STORE_RELEASE and two counter increments.
     */
    if (++(my_ctx->epoch_tick) % EBR_GC_THRESHOLD == 0)
    {
        STORE_RELAXED(my_ctx->local_epoch, LOAD_ACQUIRE(g_epoch));
        if (++(my_ctx->op_count) % EBR_GC_THRESHOLD == 0)
            try_gc();
    }
}

void ebr_retire(void *ptr)
{
    int cur, slot;

    assert(ptr    && "ebr_retire: ptr must not be NULL");
    assert(my_ctx && "ebr_retire: thread not registered");

    /*
     * Use the current global epoch to select the destination slot.
     * Using local_epoch would be incorrect: it may lag behind g_epoch,
     * causing a node to land in a slot that has already been reclaimed.
     */
    cur  = LOAD_ACQUIRE(g_epoch);
    slot = cur % EBR_EPOCH_COUNT;

    GC_NEXT(ptr)          = my_ctx->pending[slot];
    my_ctx->pending[slot] = ptr;

#ifdef DEBUG
    FETCH_ADD_RELAXED(g_stat_retired, 1L);
#endif
}

void ebr_sync(void)
{
    int target;

    assert(my_ctx && "ebr_sync requires a registered thread");

    /*
     * Drive the epoch forward by EPOCH_COUNT steps to guarantee that
     * every slot has been reclaimed at least once since this call began.
     */
    target = LOAD_ACQUIRE(g_epoch) + EBR_EPOCH_COUNT;
    while (LOAD_ACQUIRE(g_epoch) < target)
    {
        epoch_enter();
        epoch_exit();
    }
}

void ebr_get_stats(ebr_stats_t *out)
{
    if (!out) return;
    out->global_epoch   = LOAD_RELAXED(g_epoch);
    out->active_threads = LOAD_RELAXED(g_n_threads);
#ifdef DEBUG
    out->total_retired   = LOAD_RELAXED(g_stat_retired);
    out->total_reclaimed = LOAD_RELAXED(g_stat_reclaimed);
#else
    out->total_retired   = 0;
    out->total_reclaimed = 0;
#endif
}