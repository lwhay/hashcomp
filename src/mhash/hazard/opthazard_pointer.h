//
// Created by Michael on 3/21/20.
//

#ifndef HASHCOMP_OPTHAZARD_POINTER_H
#define HASHCOMP_OPTHAZARD_POINTER_H

#include "ihazard.h"
#include "my_haz_ptr/haz_ptr.h"

template<typename T, typename D = T>
class opt_hazard : public ihazard<T, D> {
protected:
    static const int OPT_MAX_THREAD = 128;
    int thread_cnt;
    HazPtrHolder holders
    alignas(128)[OPT_MAX_THREAD];

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
        return (uint64_t) holders[tid].Pin((std::atomic<T *> &) ptr);
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        return (T *) load(tid, (std::atomic<uint64_t> &) res);
    }

    void read(size_t tid) { holders[tid].Reset(); }

    bool free(uint64_t ptr) { HazPtrRetire((T *) ptr); }

    const char *info() { return "opthazard_pointer"; }
};

#endif //HASHCOMP_OPTHAZARD_POINTER_H
