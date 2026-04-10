#ifndef HOCC64_H
#define HOCC64_H

#include <stdint.h>

typedef uint64_t hocc64_t;
#define HOCC_INIT            (hocc64_t)(0)

#define HOCC_WRITER_BIT      (hocc64_t)(0x1)
#define HOCC_VERSION_UNIT    (hocc64_t)(0x2)
#define HOCC_VERSION_MASK    (hocc64_t)(~HOCC_WRITER_BIT)

#ifndef likely
    #if defined(__GNUC__) || defined(__clang__)
        #define unlikely(x) __builtin_expect(!!(x), 0)
        #define likely(x)   __builtin_expect(!!(x), 1)
    #else
        #define unlikely(x) (x)
        #define likely(x)   (x)
    #endif
#endif

#ifndef cpu_relax
    #if defined(__x86_64__) || defined(__i386__)
        #define cpu_relax() __builtin_ia32_pause()
    #elif defined(__aarch64__) || defined(__arm__)
        #define cpu_relax() __asm__ volatile("yield" ::: "memory")
    #else
        #define cpu_relax() do { } while (0)
    #endif
#endif


static inline hocc64_t hocc_load(hocc64_t *ptr) 
{
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}


static inline int hocc_validate(hocc64_t *ptr, hocc64_t old_v)
{
    hocc64_t curr_v = __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
    return curr_v == old_v;
}

static inline void hocc_write_lock(hocc64_t *ptr)
{
    hocc64_t v;
    int backoff = 64;
    while (1)
    {
        v = __atomic_load_n(ptr, __ATOMIC_RELAXED);
        if (!(v & HOCC_WRITER_BIT) && 
            __atomic_compare_exchange_n(ptr, &v, v | HOCC_WRITER_BIT,
                                        0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
            break;

        for (int i = 0; i < backoff; i++) cpu_relax();
        backoff = (backoff < 1024) ? backoff << 1 : 1024;
    }
}

static inline void hocc_write_unlock(hocc64_t *ptr)
{
    hocc64_t v = __atomic_load_n(ptr, __ATOMIC_RELAXED);
    __atomic_store_n(ptr, (v + HOCC_VERSION_UNIT) & HOCC_VERSION_MASK, __ATOMIC_RELEASE);
}

#endif /* HOCC64_H */