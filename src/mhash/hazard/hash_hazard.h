//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_HASH_HAZARD_H
#define HASHCOMP_HASH_HAZARD_H

#include <atomic>
#include <cassert>
#include "ihazard.h"
#include "City.h"

size_t total_hash_keys = (1 << 20);

class indicator {
    alignas(64) std::atomic<uint64_t> counter;
public:
    indicator() { counter.store(0); }

    void init() { counter.store(0); }

    void store(uint64_t ptr) { counter.store(ptr); }

    uint64_t load() { return counter.load(); }
};

class hash_hazard : public ihazard {
private:
    size_t thread_number = 0;
    size_t total_holders = 0;
    indicator *holders;
    uint64_t *caches;

public:
    hash_hazard(size_t total_thread) : thread_number(total_thread),
                                       total_holders(total_hash_keys * thread_number),
                                       holders(new indicator[total_holders]),
                                       caches(new uint64_t[total_thread]) {}

    ~hash_hazard() {
        delete[] holders;
        delete[] caches;
    }

    void registerThread() {}

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        caches[tid] = ptr.load();
        uint64_t hashkey = CityHash64((char *) caches[tid], sizeof(uint64_t)) % total_hash_keys * thread_number + tid;
        holders[hashkey].store(1);
        return ptr.load();
    }

    void read(size_t tid) {
        uint64_t hashkey = CityHash64((char *) caches[tid], sizeof(uint64_t)) % total_hash_keys * thread_number + tid;
        holders[hashkey].store(0);
    }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        bool busy = false;
        uint64_t hashbase = CityHash64((char *) ptr, sizeof(uint64_t)) % total_hash_keys * thread_number;
        for (size_t t = 0; t < thread_number; t++) {
            if (holders[hashbase + t].load() == 1) {
                //std::cout << ptr << " " << hashbase << std::endl;
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

#endif //HASHCOMP_HASH_HAZARD_H
