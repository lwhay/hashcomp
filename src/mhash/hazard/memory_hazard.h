//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_MEMORY_HAZARD_H
#define HASHCOMP_MEMORY_HAZARD_H

#include <atomic>
#include <cassert>
#include "ihazard.h"

constexpr size_t thread_limit = (1 << 8);

constexpr size_t default_step = (1 << 4);

#define IDX(tid) (tid * default_step)

struct holder {
    alignas(64) std::atomic<uint64_t> address;
public:
    void init() { address.store(0); }

    void store(uint64_t ptr) { address.store(ptr); }

    uint64_t load() { return address.load(); }
};

class memory_hazard : public ihazard {
private:
    holder holders[thread_limit * default_step];
    size_t thread_number = 0;

public:
    void registerThread() {
        holders[IDX(thread_number)].init();
        thread_number++;
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        holders[IDX(tid)].store(ptr.load());
        return ptr.load();
    }

    void read(size_t tid) { holders[IDX(tid)].store(0); }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        bool busy = false;
        for (size_t t = 0; t < thread_number; t++) {
            if (holders[IDX(t)].load() == ptr) {
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
};

#endif //HASHCOMP_MEMORY_HAZARD_H
