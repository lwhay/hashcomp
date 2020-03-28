//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_MEMORY_HAZARD_H
#define HASHCOMP_MEMORY_HAZARD_H

#include <atomic>
#include <cassert>
#include "ihazard.h"

constexpr size_t thread_limit = (1 << 8);

struct holder {
    alignas(128) std::atomic<uint64_t> address;
public:
    void init() { address.store(0, std::memory_order_relaxed); }

    void store(uint64_t ptr) { address.store(ptr, std::memory_order_relaxed); }

    uint64_t load() { return address.load(std::memory_order_relaxed); }
};

class memory_hazard : public ihazard {
protected:
    holder holders[thread_limit];

public:
    void registerThread() {
        holders[thread_number].init();
        thread_number++;
    }

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load(std::memory_order_relaxed);
        holders[tid].store(address);
        return address;
    }

    void read(size_t tid) { holders[tid].store(0); }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        bool busy = false;
        for (size_t t = 0; t < thread_number; t++) {
            if (holders[t].load() == ptr) {
                busy = true;
                break;
            }
        }
        if (busy) return false;
        else {
            std::free((void *) ptr);
            return true;
        }
    }

    const char *info() { return "memory_hazard"; }
};

#endif //HASHCOMP_MEMORY_HAZARD_H
