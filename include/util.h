#ifndef BSL_UTIL_H
#define BSL_UTIL_H

#ifndef __GNUC__
#  error "util.h: requires GCC or Clang (__atomic_* built-ins)"
#endif

#define CONCAT_IMPL(a, b) a##b
#define CONCAT(a, b)      CONCAT_IMPL(a, b)

#define STATIC_ASSERT(cond) \
    typedef char CONCAT(__static_assertion_, __LINE__)[(cond) ? 1 : -1]

#define likely(x)   __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define LOAD_ACQUIRE(ptr) \
    __atomic_load_n(&(ptr), __ATOMIC_ACQUIRE)

#define LOAD_RELAXED(ptr) \
    __atomic_load_n(&(ptr), __ATOMIC_RELAXED)

#define STORE_RELEASE(ptr, val) \
    __atomic_store_n(&(ptr), (val), __ATOMIC_RELEASE)

#define STORE_RELAXED(ptr, val) \
    __atomic_store_n(&(ptr), (val), __ATOMIC_RELAXED)

#define FETCH_ADD_RELAXED(ptr, val) \
    __atomic_fetch_add(&(ptr), (val), __ATOMIC_RELAXED)

#define FETCH_ADD_RELEASE(ptr, val) \
    __atomic_fetch_add(&(ptr), (val), __ATOMIC_RELEASE)

#define EXCHANGE_ACQUIRE(ptr, val) \
    __atomic_exchange_n(&(ptr), (val), __ATOMIC_ACQUIRE)

#define CAS_STRONG(ptr, expected, desired, s_mo, f_mo) \
    __atomic_compare_exchange_n(&(ptr), (expected), (desired), \
                                0, (s_mo), (f_mo))

#define CAS_WEAK(ptr, expected, desired, s_mo, f_mo) \
    __atomic_compare_exchange_n(&(ptr), (expected), (desired), \
                                1, (s_mo), (f_mo))

#define THREAD_FENCE(mo) \
    __atomic_thread_fence(mo)

#endif /* BSL_UTIL_H */