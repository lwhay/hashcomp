//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_HASH_HAZARD_H
#define HASHCOMP_HASH_HAZARD_H

#include <atomic>
#include <cassert>
#include "ihazard.h"

#undef CITY3

#ifdef CITY3

#include "City.h"

#define hash(x) (CityHash64((char*)x, sizeof(uint64_t)))
#else

#define BIG_CONSTANT(x) (x##LLU)

constexpr uint64_t module = 1llu << 63;

#define hash(key) (((uint32_t) key >> 3) ^ (uint32_t) key)

//inline uint64_t hash(uint64_t key) {
//    uint32_t h = (uint32_t) key * 6722461;
//    //h *= 0x85ebca6b;
//    return h;
//    /*key ^= key >> 33;
//    key *= BIG_CONSTANT(0xff51afd7ed558ccd);
//    key ^= key >> 33;
//    key *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
//    key ^= key >> 33;
//    return key;*/
//    /*key = (~key) + (key << 21); // key = (key << 21) - key - 1;
//    key = key ^ (key >> 24);
//    key = (key + (key << 3)) + (key << 8); // key * 265
//    key = key ^ (key >> 14);
//    key = (key + (key << 2)) + (key << 4); // key * 21
//    key = key ^ (key >> 28);
//    key = key + (key << 31);
//    return key;*/
//}

/*inline uint64_t hash(uint64_t x) {
    uint64_t result;
    uint64_t lresult;
    result = (151261303 * x) + 6722461;
    result = ((result >> 63) + result) & module;
    lresult = result;
    return (lresult);
}*/
#endif

constexpr size_t total_hash_keys = (1 << 20);

constexpr size_t maximum_threads = (1 << 7);

class indicator {
    alignas(64) std::atomic<uint64_t> counter;
public:
    indicator() { counter.store(0); }

    void init() { counter.store(0); }

    void store(uint64_t ptr) { counter.store(ptr); }

    uint64_t fetch_add(uint64_t inc = 1) { return counter.fetch_add(inc); }

    uint64_t fetch_sub(uint64_t dec = 1) { return counter.fetch_sub(dec); }

    uint64_t load() { return counter.load(); }
};

class hash_hazard : public ihazard {
private:
    size_t thread_number = 0;
    size_t total_holders = 0;
    indicator holders[total_hash_keys];
    uint64_t caches[maximum_threads];
    uint64_t hashkeys[maximum_threads];

public:
    hash_hazard(size_t total_thread) : thread_number(total_thread), total_holders(total_hash_keys) {
        for (size_t i = 0; i < total_hash_keys; i++) holders[i].init();
        std::cout << "Hash hazard" << std::endl;
    }

    ~hash_hazard() {}

    void registerThread() {}

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        caches[tid] = ptr.load();
        hashkeys[tid] = hash(caches[tid]) % total_hash_keys;
        holders[hashkeys[tid]].fetch_add();
        return caches[tid];
    }

    void read(size_t tid) {
        holders[hashkeys[tid]].fetch_sub();
    }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        bool busy;
        uint64_t hashbase = hash(ptr) % total_hash_keys;
        busy = (holders[hashbase].load() != 0);
        if (busy) return false;
        else {
            std::free((void *) ptr);
            return true;
        }
    }
};

#endif //HASHCOMP_HASH_HAZARD_H
