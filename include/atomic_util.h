#ifndef ATOMIC_UTIL_H
#define ATOMIC_UTIL_H 

#define LOAD_ACQUIRE(ptr) __atomic_load_n(&(ptr), __ATOMIC_ACQUIRE)
#define STORE_RELEASE(ptr, val) __atomic_store_n(&(ptr), (val), __ATOMIC_RELEASE)
#define LOAD_RELAXED(ptr) __atomic_load_n(&(ptr), __ATOMIC_RELAXED)
#define STORE_RELAXED(ptr, val) __atomic_store_n(&(ptr), (val), __ATOMIC_RELAXED)

#endif /* ATOMIC_UTIL_H */