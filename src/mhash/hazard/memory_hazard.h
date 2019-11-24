//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_MEMORY_HAZARD_H
#define HASHCOMP_MEMORY_HAZARD_H

#include <atomic>
#include <cassert>

constexpr size_t thread_limit = (1 << 8);

struct holder {
    alignas(64) std::atomic<uint64_t> address;
public:
    void init() { address.store(0); }

    void store(uint64_t ptr) { address.store(ptr); }

    uint64_t load() { return address.load(); }
};

class memory_hazard {
private:
    holder holders[thread_limit];
    size_t thread_number = 0;

public:
    void registerThread() {
        holders[thread_number++].init();
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        holders[tid].store(ptr.load());
        return holders[tid].load();
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
};

#endif //HASHCOMP_MEMORY_HAZARD_H
