//
// Created by Michael on 11/28/19.
//

#ifndef HASHCOMP_BATCH_HAZARD_H
#define HASHCOMP_BATCH_HAZARD_H

#include <atomic>
#include <cassert>
#include "memory_hazard.h"

constexpr size_t batch_size = (1 << 6);

thread_local uint64_t freebit = 0;

thread_local uint64_t lrulist[batch_size];

thread_local size_t idx = 0;

class batch_hazard : public memory_hazard {
private:
    holder holders[thread_limit];

public:
    void registerThread() {
        holders[thread_number].init();
        thread_number++;
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load();
        holders[tid].store(address);
        return address;
    }

    void read(size_t tid) { holders[tid].store(0); }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        assert(idx >= 0 && idx < batch_size);
        lrulist[idx++] = ptr;
        if (idx == batch_size) {
            for (size_t t = 0; t < thread_number; t++) {
                uint64_t target = holders[t].load();
                for (size_t i = 0; i < batch_size; i++) {
                    if (lrulist[i] == target) freebit |= (1llu << i);
                }
            }
            idx = 0;
            for (size_t i = 0; i < batch_size; i++) {
                if ((freebit & (1llu << i)) == 0) std::free((void *) lrulist[i]);
                else lrulist[idx++] = lrulist[i];
            }
            freebit = 0;
            for (size_t i = 0; i < idx; i++) freebit |= (1llu << i);
        }
        return true;
    }

    char *info() { return "batch_hazard"; }
};

#endif //HASHCOMP_BATCH_HAZARD_H
