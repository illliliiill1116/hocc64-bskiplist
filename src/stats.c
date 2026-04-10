#include "stats.h"
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>

#ifdef STATS

int global_retry_point_counter = 0;
int point_to_line[MAX_RETRY_POINTS] = {0};
const char* point_to_file[MAX_RETRY_POINTS] = {NULL};
const char* point_to_note[MAX_RETRY_POINTS] = {NULL};
uint64_t global_final_counts[MAX_RETRY_POINTS] = {0};

__thread uint64_t local_retry_counts[MAX_RETRY_POINTS] = {0};

static pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

void bsl_stats_collect()
{
    pthread_mutex_lock(&stats_mutex);
    for (int i = 0; i < global_retry_point_counter && i < MAX_RETRY_POINTS; i++)
    {
        global_final_counts[i] += local_retry_counts[i];
        local_retry_counts[i] = 0;
    }
    pthread_mutex_unlock(&stats_mutex);
}

void bsl_stats_cleanup()
{
    pthread_mutex_lock(&stats_mutex);
    
    for (int i = 0; i < MAX_RETRY_POINTS; i++)
        global_final_counts[i] = 0;

    pthread_mutex_unlock(&stats_mutex);
}

typedef struct {
    int id;
    uint64_t count;
} sort_node_t;

static int _compare_stats(const void *a, const void *b)
{
    sort_node_t *nodeA = (sort_node_t *)a;
    sort_node_t *nodeB = (sort_node_t *)b;
    if (nodeA->count < nodeB->count) return 1;
    if (nodeA->count > nodeB->count) return -1;
    return 0;
}

void bsl_stats_print_report()
{
    int num_points = global_retry_point_counter;
    if (num_points > MAX_RETRY_POINTS) num_points = MAX_RETRY_POINTS;

    sort_node_t to_sort[MAX_RETRY_POINTS];
    int active_count = 0;

    for (int i = 0; i < num_points; i++)
    {
        if (global_final_counts[i] > 0)
        {
            to_sort[active_count].id = i;
            to_sort[active_count].count = global_final_counts[i];
            active_count++;
        }
    }

    if (active_count > 0)
        qsort(to_sort, active_count, sizeof(sort_node_t), _compare_stats);

    printf("\n");
    printf("================================================================================\n");
    printf("                              Retry Report\n");
    printf("================================================================================\n");
    printf("%-20s | %-6s | %-12s | %-30s\n", "File", "Line", "Count", "Note");
    printf("--------------------------------------------------------------------------------\n");

    uint64_t total_retries = 0;
    for (int i = 0; i < active_count; i++)
    {
        int idx = to_sort[i].id;
        printf("%-20s | %-6d | %-12llu | %-30s\n", 
               point_to_file[idx], 
               point_to_line[idx], 
               (unsigned long long)to_sort[i].count,
               point_to_note[idx]);
        
        total_retries += to_sort[i].count;
    }

    printf("--------------------------------------------------------------------------------\n");
    printf("Total Retries: %llu\n", (unsigned long long)total_retries);
    printf("================================================================================\n\n");
}

#endif

