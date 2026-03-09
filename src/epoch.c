#include "epoch.h"
#include "util.h"
#include "node.h"
#include <stdlib.h>
#include <string.h>

static int global_epoch = 0;
static int thread_idx_counter = 0;
static thread_context_t* all_contexts[64]; 

__thread thread_context_t* my_ctx = NULL;

static void auto_thread_init()
{
    my_ctx = (thread_context_t*)calloc(1, sizeof(thread_context_t));
    int idx = __atomic_fetch_add(&thread_idx_counter, 1, __ATOMIC_RELEASE);
    if (idx < 64)
        STORE_RELEASE(all_contexts[idx], my_ctx);
}

void epoch_enter()
{
    if (unlikely(!my_ctx)) 
        auto_thread_init();

    int g_epoch = LOAD_ACQUIRE(global_epoch);
    STORE_RELAXED(my_ctx->local_epoch, g_epoch);
    STORE_RELEASE(my_ctx->in_critical, 1);
}

void epoch_exit()
{
    STORE_RELEASE(my_ctx->in_critical, 0);

    if (++(my_ctx->op_count) % GC_THRESHOLD == 0) {
        try_gc();
    }
}

void* bsl_alloc_node()
{
    void* ptr;
    if (posix_memalign(&ptr, CACHE_LINE_SIZE, NODE_SIZE) != 0) return NULL;
    memset(ptr, 0, NODE_SIZE);
    return ptr;
}

void bsl_free_node(void* ptr)
{
    if (!ptr) return;
    
    void** next_slot = (void**)((char*)ptr + NODE_GC_NEXT_OFFSET);
    *next_slot = my_ctx->pending_gc[my_ctx->local_epoch % EPOCH_COUNT];
    my_ctx->pending_gc[my_ctx->local_epoch % EPOCH_COUNT] = ptr;
}

void try_gc()
{
    int g_epoch = LOAD_ACQUIRE(global_epoch);
    int active_threads = LOAD_RELAXED(thread_idx_counter);

    for (int i = 0; i < active_threads; i++)
    {
        thread_context_t* other_ctx = LOAD_RELAXED(all_contexts[i]);
        if (!other_ctx) continue;

        if (LOAD_ACQUIRE(other_ctx->in_critical))
        {
            if (LOAD_ACQUIRE(other_ctx->local_epoch) < g_epoch)
                return;
        }
    }

    __atomic_fetch_add(&global_epoch, 1, __ATOMIC_RELEASE);

    int reclaim_idx = (g_epoch + 2) % EPOCH_COUNT;
    void* to_reclaim = my_ctx->pending_gc[reclaim_idx];
    my_ctx->pending_gc[reclaim_idx] = NULL;

    while (to_reclaim)
    {
        void* next = *(void**)((char*)to_reclaim + NODE_GC_NEXT_OFFSET);
        free(to_reclaim);
        to_reclaim = next;
    }
}