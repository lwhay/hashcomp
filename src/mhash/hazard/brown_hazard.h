//
// Created by iclab on 12/29/19.
//

#ifndef HASHCOMP_BROWN_HAZARD_H
#define HASHCOMP_BROWN_HAZARD_H

#include <atomic>
#include <cassert>
#include "ihazard.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "record_manager.h"
#include "reclaimer_hazardptr.h"
#include "memory_hazard.h"

thread_local uint64_t holder;

template<typename T>
class brown_hazard : public ihazard {
    typedef reclaimer_hazardptr<T, pool_none<T, allocator_once<T>>> Reclaimer;
    typedef allocator_once<T> Allocator;
    typedef pool_none<T, allocator_once<T>> Pool;
private:
    size_t thread_num;
    Allocator *alloc;
    Pool *pool;
    Reclaimer *reclaimer;

public:
    brown_hazard(size_t total_thread) : thread_num(total_thread) {
        alloc = new Allocator(thread_num, nullptr);
        pool = new Pool(thread_num, alloc, nullptr);
        reclaimer = new Reclaimer(thread_num, pool, nullptr);
    }

    void registerThread() {
        ftid = thread_number++;
        alloc->initThread(ftid);
        reclaimer->initThread(ftid);
    }

    T *allocate(size_t tid) {
        return alloc->allocate(tid);
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load();
        reclaimer->protect(tid, (T *) address, callbackReturnTrue, nullptr, false);
        holder = address;
        return address;
    }

    void read(size_t tid) {
        reclaimer->unprotect(tid, (T *) holder);
    }

    bool free(uint64_t ptr) {
        reclaimer->retire(ftid, (T *) ptr);
        //std::free((T *) ptr);
        alloc->deallocate(ftid, (T *) ptr);
        return true;
    }

    char *info() {
        return "brown hazard";
    }
};

#endif //HASHCOMP_BROWN_HAZARD_H
