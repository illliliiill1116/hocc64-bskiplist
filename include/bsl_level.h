#ifndef BSL_LEVEL
#define BSL_LEVEL

#include "node.h"
#include "params.h"
#include <stdint.h>

#define BSL_P_LEVEL ((uint64_t)(B_INTERNAL / PROMOTE_RATE_SCALE))

static inline uint64_t hash_64(uint64_t x)
{
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

static inline int bsl_level_for_key(bsl_key_t key)
{
    int result = 0;
    uint64_t h = hash_64((uint64_t)key);

    const uint64_t P = BSL_P_LEVEL;

    while (result < MAX_LEVEL - 1)
    {
        if ((h % P) == 0)
        {
            result++;
            h /= P;
            if (h < P) h = hash_64(h + result);
        }
        else break;
    }
    return result;
}

#endif /* BSL_LEVEL */