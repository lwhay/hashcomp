//
// Created by Michael on 3/21/20.
//

#ifndef HASHCOMP_OPTHAZARD_POINTER_H
#define HASHCOMP_OPTHAZARD_POINTER_H

#include "ihazard.h"
#include "my_haz_ptr/haz_ptr.h"

template<typename T>
class opt_hazard : public ihazard {
protected:
    static const int OPT_MAX_THREAD = 128;
    static const int HOLDER_ALIGNAS = 32;
    int thread_cnt;
    HazPtrHolder holders[OPT_MAX_THREAD * HOLDER_ALIGNAS];

public:
    void registerThread() {}

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    opt_hazard(int thread_num) : thread_cnt(thread_num) {
        HazPtrInit(thread_cnt);
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        /*T *node;
        do {
            node = holders[tid].Repin((std::atomic<T *> &) ptr);
        } while (!node);
        return (uint64_t) node;*/
        return (uint64_t) holders[tid * HOLDER_ALIGNAS].Pin((std::atomic<T *> &) ptr);
    }

    void read(size_t tid) { holders[tid * HOLDER_ALIGNAS].Reset(); }

    bool free(uint64_t ptr) { HazPtrRetire((T *) ptr); }

    const char *info() { return "opthazard_pointer"; }
};

#endif //HASHCOMP_OPTHAZARD_POINTER_H
