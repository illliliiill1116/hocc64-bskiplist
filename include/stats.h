#include <stdint.h>

#ifdef STATS
    #define MAX_RETRY_POINTS 128

    extern int global_retry_point_counter;
    extern int point_to_line[MAX_RETRY_POINTS];
    extern const char* point_to_file[MAX_RETRY_POINTS];
    extern const char* point_to_note[MAX_RETRY_POINTS];

    extern __thread uint64_t local_retry_counts[MAX_RETRY_POINTS];
    #define RECORD_RETRY(...) _RECORD_RETRY_INTERNAL("" __VA_ARGS__)

    #define _RECORD_RETRY_INTERNAL(note) do {          \
        static int __retry_idx = -1;                   \
        if (unlikely(__retry_idx == -1))               \
        {                                              \
            __retry_idx = __atomic_fetch_add(          \
                    &global_retry_point_counter,       \
                    1, __ATOMIC_RELAXED);              \
            if (__retry_idx < MAX_RETRY_POINTS)        \
            {                                          \
                point_to_line[__retry_idx] = __LINE__; \
                point_to_file[__retry_idx] = __FILE__; \
                point_to_note[__retry_idx] = (note);   \
            }                                          \
        }                                              \
        if (likely(__retry_idx < MAX_RETRY_POINTS))    \
            local_retry_counts[__retry_idx]++;         \
    } while (0)

    void bsl_stats_collect();
    void bsl_stats_cleanup();
    void bsl_stats_print_report();

#else
    #define RECORD_RETRY(note) ((void)0)
    static inline void bsl_stats_collect() {}
    static inline void bsl_stats_cleanup() {}
    static inline void bsl_stats_print_report() {}
#endif