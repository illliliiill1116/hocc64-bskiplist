#ifndef BSL_PARAMS_H
#define BSL_PARAMS_H
#include <stdint.h>

typedef uint64_t bsl_key_t;
typedef uint64_t bsl_val_t;

#define BSL_KEY_MIN ((bsl_key_t)0)
#define BSL_KEY_MAX (~(bsl_key_t)0)

#define MAX_LEVEL 5
#define NODE_SIZE 2048
#define CACHE_LINE_SIZE 64

#define PROMOTE_RATE_SCALE 1

#endif