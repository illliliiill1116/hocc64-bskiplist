#ifndef HOCC64_H
#define HOCC64_H

#include <stdint.h>

/**
 * HOCC-64 (64-bit Hybrid Optimistic Concurrency Control)
 * * A synchronization primitive for hierarchical concurrent data structures.
 * * 64-bit State Word Layout:
 * [ 63 : 16 ] - Version Counter : 48-bit epoch, increments on write unlock.
 * [ 15 : 2  ] - Reader Count    : 14-bit shared reader count.
 * [    1    ] - Retry Bit       : Triggers hocc_read_trylock() abort.
 * [    0    ] - Writer Bit      : Exclusive lock indicator.
 */
typedef uint64_t hocc64_t;
#define HOCC_INIT            (hocc64_t)(0)

#define HOCC_WRITER_BIT      (hocc64_t)(0x1)
#define HOCC_RETRY_BIT       (hocc64_t)(0x2)
#define HOCC_READER_UNIT     (hocc64_t)(0x4)
#define HOCC_READER_MASK     (hocc64_t)((HOCC_VERSION_UNIT - 1) & ~(HOCC_WRITER_BIT | HOCC_RETRY_BIT))
#define HOCC_VERSION_UNIT    (hocc64_t)(1 << 16)
#define HOCC_VERSION_MASK    (hocc64_t)(~(HOCC_READER_MASK | HOCC_RETRY_BIT | HOCC_WRITER_BIT))

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
    /* Fails if version counter changed or writer bit is set */
    return !((curr_v ^ old_v) & (HOCC_VERSION_MASK | HOCC_WRITER_BIT));
}


static inline void hocc_read_lock(hocc64_t *ptr)
{
    hocc64_t v;
    while (1) 
    {
        v = __atomic_load_n(ptr, __ATOMIC_ACQUIRE);

        if (unlikely(v & HOCC_WRITER_BIT))
        {
            cpu_relax();
            continue;
        }

        if (__atomic_compare_exchange_n(ptr, &v, v + HOCC_READER_UNIT,
                                       0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
            return;
    }
}


static inline int hocc_read_trylock(hocc64_t *ptr)
{
    hocc64_t v;
    while (1)
    {
        v = __atomic_load_n(ptr, __ATOMIC_ACQUIRE);

        if (unlikely(v & HOCC_RETRY_BIT)) return 0;

        if (unlikely(v & HOCC_WRITER_BIT))
        {
            cpu_relax();
            continue;
        }

        if (__atomic_compare_exchange_n(ptr, &v, v + HOCC_READER_UNIT,
                                       0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
            return 1;
    }
}


static inline void hocc_read_unlock(hocc64_t *ptr)
{
    __atomic_fetch_sub(ptr, HOCC_READER_UNIT, __ATOMIC_RELEASE);
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

    /* Wait for all readers to finish */
    while (__atomic_load_n(ptr, __ATOMIC_RELAXED) & HOCC_READER_MASK) cpu_relax();
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}


static inline void hocc_write_lock_evicting(hocc64_t *ptr)
{
    hocc64_t v;
    int backoff = 64;
    while (1)
    {
        v = __atomic_load_n(ptr, __ATOMIC_RELAXED);

        if (!(v & HOCC_WRITER_BIT) && 
            __atomic_compare_exchange_n(ptr, &v, v | HOCC_WRITER_BIT | HOCC_RETRY_BIT,
                                       0, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
            break;

        for (int i = 0; i < backoff; i++) cpu_relax();
        backoff = (backoff < 1024) ? backoff << 1 : 1024;
    }
    
    /* Wait for all readers to finish */ 
    while (__atomic_load_n(ptr, __ATOMIC_RELAXED) & HOCC_READER_MASK) cpu_relax();
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}


static inline void hocc_write_unlock(hocc64_t *ptr)
{
    hocc64_t v = __atomic_load_n(ptr, __ATOMIC_RELAXED);
    /* Increment version and clear all status bits */
    __atomic_store_n(ptr, (v + HOCC_VERSION_UNIT) & (HOCC_VERSION_MASK), __ATOMIC_RELEASE);
}

#endif /* HOCC64_H */