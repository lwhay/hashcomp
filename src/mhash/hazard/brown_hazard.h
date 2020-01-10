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

template<typename T>
class brown_hazard : public ihazard {
    typedef reclaimer_hazardptr<T, pool_interface<T>> Reclaimer;
    typedef allocator_new<T> Allocator;
    typedef pool_perthread_and_shared<T> Pool;
private:
    size_t thread_num;
    Allocator *alloc;
    Pool *pool;
    Reclaimer *reclaimer;
    static thread_local uint64_t holder;
    static thread_local int ftid;

public:
    brown_hazard(size_t total_thread) : thread_num(total_thread) {
        alloc = new Allocator(thread_num, nullptr);
        pool = new Pool(thread_num, alloc, nullptr);
        reclaimer = new Reclaimer(thread_num, pool, nullptr);
    }

    void registerThread() { ftid = thread_number++; }

    uint64_t load(size_t tid, std::atomic<T> &ptr) {
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
        return true;
    }

    char *info() {
        return "brown hazard";
    }
};

#endif //HASHCOMP_BROWN_HAZARD_H
