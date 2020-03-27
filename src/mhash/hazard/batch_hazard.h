//
// Created by Michael on 11/28/19.
//

#ifndef HASHCOMP_BATCH_HAZARD_H
#define HASHCOMP_BATCH_HAZARD_H

#include <atomic>
#include <cassert>
#include "memory_hazard.h"

#define strategy 0 // 0: batch in/out; 1: one out; 2: half one out.

constexpr size_t batch_size = (1llu << 6);

thread_local uint64_t freebit = 0;

thread_local uint64_t lrulist[batch_size];

thread_local size_t idx = 0;

class batch_hazard : public memory_hazard {
private:
    holder holders[thread_limit];

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
            holders[tid].store(address);
        } while (address != ptr.load(std::memory_order_relaxed));
        return address;
    }

    void read(size_t tid) { holders[tid].store(0); }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        assert(idx >= 0 && idx < batch_size);
        lrulist[idx++] = ptr;
#if strategy == 0
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
#else
#endif
        return true;
    }

    const char *info() { return "batch_hazard"; }
};

#endif //HASHCOMP_BATCH_HAZARD_H
