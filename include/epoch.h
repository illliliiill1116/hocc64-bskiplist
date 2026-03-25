/*
 * epoch.h - Epoch-Based Reclamation (EBR) for lock-free data structures
 */

#ifndef EPOCH_H
#define EPOCH_H

/* Number of epoch_exit() calls between GC attempts. */
#ifndef EBR_GC_THRESHOLD
#  define EBR_GC_THRESHOLD 256
#endif

/* Maximum number of concurrently registered threads. */
#ifndef EBR_MAX_THREADS
#  define EBR_MAX_THREADS 256
#endif

/* Number of epoch slots. Must be 3; do not change. */
#define EBR_EPOCH_COUNT 3

/* Statistics (available only when compiled with -DDEBUG). */
typedef struct {
    int  global_epoch;
    int  active_threads;
    long total_retired;
    long total_reclaimed;
} ebr_stats_t;

/*
 * epoch_enter - enter a read/write critical section
 *
 * Must be called before accessing any node that may be concurrently
 * deleted. Initialises per-thread state on first call. Non-reentrant:
 * each epoch_enter must be paired with exactly one epoch_exit.
 */
void epoch_enter(void);

/*
 * epoch_exit - leave the critical section
 *
 * Must be paired with epoch_enter. Periodically advances the global
 * epoch and reclaims nodes that are no longer reachable.
 */
void epoch_exit(void);

/*
 * ebr_retire - mark a node for deferred reclamation
 *
 * Call after the node has been unlinked from the data structure.
 * The node's tail bytes (NODE_GC_NEXT_OFFSET) are used as an
 * intrusive link; no wrapper allocation or free callback is needed.
 * ptr must not be NULL.
 */
void ebr_retire(void *ptr);

/*
 * ebr_sync - block until all previously retired nodes are freed
 *
 * For use in tests and cleanup paths only; not safe on the hot path.
 */
void ebr_sync(void);

/*
 * ebr_get_stats - fill *out with current counters
 *
 * total_retired and total_reclaimed are zero unless compiled with -DDEBUG.
 */
void ebr_get_stats(ebr_stats_t *out);

#endif /* EPOCH_H */