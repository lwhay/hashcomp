//
// Created by Michael on 11/26/2019.
//

#ifndef HASHCOMP_ADAPTIVE_HAZARD_H
#define HASHCOMP_ADAPTIVE_HAZARD_H

#include <cstring>
#include "hash_hazard.h"
#include "memory_hazard.h"

thread_local uint64_t skew = 1;

thread_local uint64_t tick = 0;

char information[255];

class adaptive_hazard : public hash_hazard {
private:
    holder lrulist[thread_limit];
    holder holders[thread_limit];
    uint64_t intensive_high;

    indicator readintensive[thread_limit], writeintensive[thread_limit];

    alignas(64) std::atomic<uint64_t> intensive{0};
public:
    adaptive_hazard(size_t total_thread) : hash_hazard(total_thread) {
        for (size_t i = 0; i < total_thread; i++) {
            holders[i].init();
            lrulist[i].init();
            readintensive[i].init();
            writeintensive[i].init();
        }
        intensive_high = 0;
        for (int i = 0; i < total_thread; i++) {
            intensive_high |= (1llu << i);
        }
        std::cout << "adaptive scheme enabled: " << intensive_high << std::endl;
    }

    ~adaptive_hazard() {}

    char *info() {
        ::memset(information, 0, 255);
        uint64_t totalreadintensive = 0, totalwriteintensive = 0;
        for (size_t i = 0; i < thread_number; i++) {
            totalreadintensive += readintensive[i].load();
            totalwriteintensive += writeintensive[i].load();
        }
        std::sprintf(information, "%llu, %llu", totalreadintensive, totalwriteintensive);
        return information;
    }

    void registerThread() {}

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load();
        if (tick++ % thread_number == 0) {
            lrulist[tid].store(address);
            uint64_t other = lrulist[(tid + thread_number / 2) % thread_number].load();
            if (other == address) {
                if (skew == 0) {
                    uint64_t bit = 1llu << tid;
                    intensive.fetch_or(bit);
                    skew = 1;
                }
            } else {
                if (skew != 0) {
                    uint64_t bit = ((1llu << tid) xor (uint64_t) (-1));
                    intensive.fetch_and(bit);
                    skew = 0;
                }
            }
        }
        if (skew != 0) {
            readintensive[tid].fetch_add(1);
            holders[tid].store(address);
        } else {
            writeintensive[tid].fetch_add(1);
            hashkey = hash(address);
            indicators[hashkey].fetch_add();
        }
        return address;
    }

    void read(size_t tid) {
        if (skew != 0) holders[tid].store(0);
        else indicators[hashkey].fetch_sub();
    }

    bool free(uint64_t ptr) {
        uint64_t intention = intensive.load();
        assert(ptr != 0);
        bool busy = false;
        if (intention > 0) {
            for (size_t t = 0; t < thread_number; t++) {
                if ((intention & (1llu << t)) != 0 && holders[t].load() == ptr) {
                    busy = true;
                    break;
                }
            }
            if (busy) return false;
        }
        uint64_t hk = hash(ptr);
        busy = (indicators[hk].load() != 0);
        if (busy) return false;
        else {
            std::free((void *) ptr);
            return true;
        }
    }
};

#endif //HASHCOMP_ADAPTIVE_HAZARD_H
