#ifndef BSL_UTIL_H
#define BSL_UTIL_H

#define CONCAT_IMPL(a, b) a##b
#define CONCAT(a, b) CONCAT_IMPL(a, b)
#define STATIC_ASSERT(cond) \
    typedef char CONCAT(__static_assertion_, __LINE__)[(cond) ? 1 : -1]

#define LOAD_ACQUIRE(ptr)       __atomic_load_n(&(ptr), __ATOMIC_ACQUIRE)
#define STORE_RELEASE(ptr, val) __atomic_store_n(&(ptr), (val), __ATOMIC_RELEASE)
#define LOAD_RELAXED(ptr)       __atomic_load_n(&(ptr), __ATOMIC_RELAXED)
#define STORE_RELAXED(ptr, val) __atomic_store_n(&(ptr), (val), __ATOMIC_RELAXED)

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#endif /* BSL_UTIL_H */