//
// Created by iclab on 12/29/19.
//

#ifndef HASHCOMP_BROWN_RECLAIM_H
#define HASHCOMP_BROWN_RECLAIM_H

#include <atomic>
#include <cassert>
#include "ihazard.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "record_manager.h"
#include "reclaimer_hazardptr.h"
#include "reclaimer_ebr_token.h"
#include "reclaimer_ebr_tree.h"
#include "reclaimer_ebr_tree_q.h"
#include "reclaimer_debra.h"
#include "reclaimer_debraplus.h"
#include "reclaimer_debracap.h"
#include "memory_hazard.h"

thread_local uint64_t holder;

template<typename T, class N, class P, class R>
class brown_reclaim : public ihazard {
    typedef typename N::template rebind<T>::other Allocator;
    typedef typename P::template rebind2<T, Allocator>::other Pool;
    typedef typename R::template rebind2<T, Pool>::other Reclaimer;
    /*typedef reclaimer_hazardptr<T, pool_none<T, allocator_new<T>>> Reclaimer;
    typedef allocator_new<T> Allocator;
    typedef pool_none<T, allocator_new<T>> Pool;*/
private:
    size_t thread_num;
    Allocator *alloc;
    Pool *pool;
    Reclaimer *reclaimer;
    std::atomic<bool> lock{false};

public:
    brown_reclaim(size_t total_thread) : thread_num(total_thread + 1) {
        alloc = new Allocator(thread_num, nullptr);
        pool = new Pool(thread_num, alloc, nullptr);
        reclaimer = new Reclaimer(thread_num, pool, nullptr);
    }

    void registerThread() {
    }

    void initThread() {
        bool expect = false;
        while (!lock.compare_exchange_strong(expect, true));
        ftid = thread_number++;
        alloc->initThread(ftid);
        reclaimer->initThread(ftid);
        lock.store(false);
    }

    uint64_t allocate(size_t tid) {
        return (uint64_t) alloc->allocate(ftid);
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        uint64_t address = ptr.load();
        reclaimer->protect(ftid, (T *) address, callbackReturnTrue, nullptr, false);
        holder = address;
        return address;
    }

    void read(size_t tid) {
        reclaimer->unprotect(ftid, (T *) holder);
    }

    bool free(uint64_t ptr) {
        //std::cout << ftid << std::endl;
        reclaimer->retire(ftid, (T *) ptr);
        //std::free((T *) ptr);
        alloc->deallocate(ftid, (T *) ptr);
        return true;
    }

    const char *info() {
        return typeid(R).name();
    }
};

#endif //HASHCOMP_BROWN_RECLAIM_H
