//
// Created by Michael on 11/26/2019.
//

#ifndef HASHCOMP_ADAPTIVE_HAZARD_H
#define HASHCOMP_ADAPTIVE_HAZARD_H

#include "hash_hazard.h"

class adaptive_hazard : public hash_hazard, memory_hazard {
private:
    uint64_t **lrulists;

    std::atomic<bool> intensive{0};
public:
    adaptive_hazard(size_t total_thread) : hash_hazard(total_thread) {
        for (size_t i = 0; i < total_thread; i++)registerThread();
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {

    }

    void read(size_t ptr) {

    }

    bool free(uint64_t ptr) {

    }
};

#endif //HASHCOMP_ADAPTIVE_HAZARD_H
