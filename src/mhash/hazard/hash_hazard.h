//
// Created by iclab on 11/24/19.
//

#ifndef HASHCOMP_HASH_HAZARD_H
#define HASHCOMP_HASH_HAZARD_H

#include <atomic>
#include <cassert>
#include <iostream>
#include "ihazard.h"

constexpr size_t total_hash_keys = (1 << 20);

#undef CITY3

#ifdef CITY3

#include "City.h"

#define hash(x) (CityHash64((char*)x, sizeof(uint64_t)))
#else

#define simplehash(key) ((((uint32_t) key >> 3) ^ (uint32_t) key) % total_hash_keys)

//#define BIG_CONSTANT(x) (x##LLU)
//
//constexpr uint64_t module = 1llu << 63;
//
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

class indicator {
    alignas(128) std::atomic<uint64_t> counter;
public:
    indicator() { counter.store(0, std::memory_order_relaxed); }

    void init() { counter.store(0, std::memory_order_relaxed); }

    void store(uint64_t ptr) { counter.store(ptr, std::memory_order_relaxed); }

    uint64_t fetch_add(uint64_t inc = 1) { return counter.fetch_add(inc, std::memory_order_relaxed); }

    uint64_t fetch_sub(uint64_t dec = 1) { return counter.fetch_sub(dec, std::memory_order_relaxed); }

    uint64_t load() { return counter.load(std::memory_order_relaxed); }
};

thread_local uint64_t hashkey;

template<class T, class D = T>
class hash_hazard : public ihazard<T, D> {
protected:
    size_t total_holders = 0;
    indicator indicators[total_hash_keys];
    using ihazard<T, D>::thread_number;

public:
    hash_hazard<T, D>(size_t total_thread) : total_holders(total_hash_keys) {
        thread_number = total_thread;
        for (size_t i = 0; i < total_hash_keys; i++) indicators[i].init();
        std::cout << "Hash hazard" << std::endl;
    }

    ~hash_hazard() {}

    void registerThread() {}

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load(std::memory_order_relaxed);
        hashkey = simplehash(address);
        indicators[hashkey].fetch_add();
        return address;
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        return (T *) load(tid, (std::atomic<uint64_t> &) res);
    }

    void read(size_t tid) {
        indicators[hashkey].fetch_sub();
    }

    bool free(uint64_t ptr) {
        assert(ptr != 0);
        bool busy;
        uint64_t hk = simplehash(ptr);
        busy = (indicators[hk].load() != 0);
        if (busy) return false;
        else {
            std::free((void *) ptr);
            return true;
        }
    }

    const char *info() { return "hash_hazard"; }
};

#endif //HASHCOMP_HASH_HAZARD_H
