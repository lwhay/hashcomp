//
// Created by Michael on 11/28/19.
//

#ifndef HASHCOMP_BATCH_HAZARD_H
#define HASHCOMP_BATCH_HAZARD_H

#include <atomic>
#include <bitset>
#include <cassert>
#include <cstring>
#include "memory_hazard.h"

#define BIG_CONSTANT(x) (x##LLU)

uint64_t HashFunc(const void *key, int len, uint64_t seed) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^(len * m);

    const uint64_t *data = (const uint64_t *) key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *) data;

    switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

#define strategy 0 // 0: batch in/out; 1: hash batch in/out; 2: one out; 3: half one out.

#define with_cache 1

#define with_stdbs 0

#if strategy == 0
constexpr size_t batch_size = (1llu << 6);

thread_local uint64_t lrulist[batch_size];

thread_local uint64_t freebit[(batch_size < 64) ? 1 : batch_size / 64];

thread_local size_t idx = 0;

thread_local uint64_t thread_id = 0;

#elif strategy == 1

constexpr size_t holder_size = (1llu << 10);

constexpr size_t hash_volume = holder_size / batch_size;

#define hash_idx1(x) (HashFunc(x, 8, 234324324231llu) % hash_volume)

//#define hash_idx(key) ((((uint64_t) key >> 7) ^ ((uint64_t) key >> 5) ^ ((uint64_t) key >> 3) ^ (uint64_t) key) % hash_volume)

#define hash_idx(key) (((uint64_t) key >> 4) % hash_volume)

thread_local uint64_t lrucell[hash_volume][batch_size];

thread_local size_t lruidx[hash_volume];

thread_local uint64_t recent_hash;

thread_local uint64_t circile_queue[holder_size];

thread_local size_t counter[hash_volume];
#else
#endif

template<typename T>
class batch_hazard : public memory_hazard {
private:
#if strategy == 0
    holder holders[thread_limit];
#elif strategy == 1
    holder cells[hash_volume][thread_limit];
#else
#endif

    class circulequeue {
        constexpr static size_t limit = (batch_size * 2);
        volatile size_t head = 0, tail = 0;
        volatile uint64_t queue[limit];
    public:
        inline bool push(uint64_t e) {
            if ((head + limit - tail) % limit == 1) return false;
            queue[tail] = e;
            tail = (tail + 1) % limit;
            return true;
        }

        inline uint64_t pop() {
            if (head == tail) return 0;
            else {
                uint64_t now = queue[head];
                head = (head + 1) % limit;
                return now;
            }
        }
    } cache[thread_limit];

public:
    void registerThread() {
        holders[thread_number++].init();
        /*std::cout << thread_number << ": " << sizeof(freebit) << ", " << std::bitset<sizeof(freebit) * 8>(0).size()
                  << std::endl;*/
    }

    void initThread(size_t tid = 0) { thread_id = tid; }

    inline uint64_t allocate(size_t tid) {
#if with_cache == 0
        return (uint64_t) std::malloc(sizeof(T));
#else
        uint64_t e = cache[tid].pop();
        if (e == 0) return (uint64_t) std::malloc(sizeof(T)); else return e;
#endif
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        uint64_t address;
        do {
            address = (uint64_t) res.load(std::memory_order_relaxed);
#if strategy == 1
            recent_hash = hash_idx((const void *) address);
            cells[recent_hash][tid].store(address);
#else
            if (!address) {
                return nullptr;
            }

            if (is_safe((T *) address)) {
                return filter((T *) address);
            }

            holders[tid].store(address);
#endif
        } while (address != (uint64_t) res.load(std::memory_order_relaxed));
        return (T *) address;
    }

    inline uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address;
        do {
            address = ptr.load(std::memory_order_relaxed);
#if strategy == 1
            recent_hash = hash_idx((const void *) address);
            cells[recent_hash][tid].store(address);
#else
            holders[tid].store(address);
#endif
        } while (address != ptr.load(std::memory_order_relaxed));
        return address;
    }

    inline void read(size_t tid) {
#if strategy == 1
        cells[recent_hash][tid].store(0);
#else
        holders[tid].store(0);
#endif
    }

    inline bool free(uint64_t ptr) {
#if strategy == 0
        assert(ptr != 0);
        assert(idx >= 0 && idx < batch_size);
        lrulist[idx++] = ptr;
        if (idx == batch_size) {
#if with_stdbs
            std::bitset<sizeof(freebit) * 8> bs(0);
#else
            std::memset(freebit, 0, sizeof(freebit));
#endif
            for (size_t t = 0; t < thread_number; t++) {
                const uint64_t target = holders[t].load();
                if (target != 0) {
                    for (size_t i = 0; i < batch_size; i++) {
                        if (lrulist[i] == target)
#if with_stdbs
                            bs.set(i);
#else
                            freebit[i / 64] |= (1llu << (i % 64));
#endif
                    }
                }
            }
            idx = 0;
            for (size_t i = 0; i < batch_size; i++) {
#if with_stdbs
                if ((bs.test(i)) == 0) {
#else
                if ((freebit[i / 64] & (1llu << (i % 64))) == 0) {
#endif
#if with_cache == 0
                    std::free((void *) lrulist[i]);
#else
                    if (!cache[thread_id].push(lrulist[i])) std::free((void *) lrulist[i]);
#endif
                } else lrulist[idx++] = lrulist[i];
            }
        }
#elif strategy == 1
        uint64_t token = hash_idx((const void *) ptr);
        assert(ptr != 0);
        assert(lruidx[token] >= 0 && lruidx[token] < batch_size);
        lrucell[token][lruidx[token]++] = ptr;
        if (lruidx[token] == batch_size) {
#if 0
            if (counter[token]++ % holder_size == 0 && token == 0) {
                std::cout << token << "\t";
                for (uint64_t id = 0; id < hash_volume; id++) { std::cout << counter[id] << "\t"; }
                std::cout << std::endl;
            }
#endif
            uint64_t freebit = 0;
            for (size_t t = 0; t < thread_number; t++) {
                const uint64_t target = cells[token][t].load();
                if (target != 0) {
                    for (size_t i = 0; i < batch_size; i++) {
                        if (lrucell[token][i] == target) freebit |= (1llu << i);
                    }
                }
            }
            lruidx[token] = 0;
            for (size_t i = 0; i < batch_size; i++) {
                if ((freebit & (1llu << i)) == 0) std::free((void *) lrucell[token][i]);
                else lrucell[token][lruidx[token]++] = lrucell[token][i];
            }
        }
#else
#endif
        return true;
    }

    const

    char *info() { return "batch_hazard"; }

};

#endif //HASHCOMP_BATCH_HAZARD_H
