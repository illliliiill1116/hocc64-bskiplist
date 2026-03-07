#include "random_level.h"
#include "params.h"

static inline uint64_t hash_64(uint64_t x)
{
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

int bsl_random_level(bsl_key_t key)
{
    int result = 0;
    uint64_t h = hash_64((uint64_t)key);

    uint64_t threshold_64 = 0xFFFFFFFFFFFFFFFFULL / B_INTERNAL;

    while (h < threshold_64)
    {
        result++;
        if (result >= MAX_LEVEL - 1) break;
        h = hash_64(h);
    }

    return result;
}