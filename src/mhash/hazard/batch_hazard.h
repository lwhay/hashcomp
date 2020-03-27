//
// Created by Michael on 11/28/19.
//

#ifndef HASHCOMP_BATCH_HAZARD_H
#define HASHCOMP_BATCH_HAZARD_H

#include <atomic>
#include <cassert>
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

constexpr size_t batch_size = (1llu << 6);

thread_local uint64_t lrulist[batch_size];

constexpr size_t holder_size = (1llu << 10);

constexpr size_t hash_volume = holder_size / batch_size;

#define hash_idx1(x) (HashFunc(x, 8, 234324324231llu) % hash_volume)

//#define hash_idx(key) ((((uint64_t) key >> 7) ^ ((uint64_t) key >> 5) ^ ((uint64_t) key >> 3) ^ (uint64_t) key) % hash_volume)

#define hash_idx(key) (((uint64_t) key >> 4) % hash_volume)

thread_local uint64_t lrucell[hash_volume][batch_size];

thread_local size_t lruidx[hash_volume];

thread_local uint64_t recent_hash;

thread_local uint64_t circile_queue[holder_size];

thread_local uint64_t freebit = 0;

thread_local size_t idx = 0;

thread_local size_t counter[hash_volume];

class batch_hazard : public memory_hazard {
private:
    holder holders[thread_limit];
    holder cells[hash_volume][thread_limit];

public:
    void registerThread() {
        holders[thread_number++].init();
    }

    void initThread() {}

    uint64_t allocate(size_t tid) { return -1; }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
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

    void read(size_t tid) {
#if strategy == 1
        cells[recent_hash][tid].store(0);
#else
        holders[tid].store(0);
#endif
    }

    bool free(uint64_t ptr) {
#if strategy == 0
        assert(ptr != 0);
        assert(idx >= 0 && idx < batch_size);
        lrulist[idx++] = ptr;
        if (idx == batch_size) {
            freebit = 0;
            for (size_t t = 0; t < thread_number; t++) {
                const uint64_t target = holders[t].load();
                if (target != 0) {
                    for (size_t i = 0; i < batch_size; i++) {
                        if (lrulist[i] == target) freebit |= (1llu << i);
                    }
                }
            }
            idx = 0;
            for (size_t i = 0; i < batch_size; i++) {
                if ((freebit & (1llu << i)) == 0) std::free((void *) lrulist[i]);
                else lrulist[idx++] = lrulist[i];
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

    const char *info() { return "batch_hazard"; }
};

#endif //HASHCOMP_BATCH_HAZARD_H
