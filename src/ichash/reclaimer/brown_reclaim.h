//
// Created by iclab on 12/29/19.
//

#ifndef HASHCOMP_BROWN_RECLAIM_H
#define HASHCOMP_BROWN_RECLAIM_H

#include <atomic>
#include <cassert>
#include "ihazard.h"
#include "brown/pool_none.h"
#include "brown/allocator_new.h"
#include "brown/pool_perthread_and_shared.h"
#include "brown/record_manager.h"
#include "brown/reclaimer_hazardptr.h"
#include "brown/reclaimer_ebr_token.h"
#include "brown/reclaimer_ebr_tree.h"
#include "brown/reclaimer_ebr_tree_q.h"
#include "brown/reclaimer_debra.h"
#include "brown/reclaimer_debraplus.h"
#include "brown/reclaimer_debracap.h"
#include "brown/reclaimer_batchhp.h"

#include "def.h"
#include "../entry_def.h"

thread_local uint64_t brown_tid;
thread_local uint64_t holder{0};
static int free_type = -1; // 0: hazard; 1: debraplus; 2: others





template<typename T, class N, class P, class R, typename D = T>
class brown_reclaim : public ihazard<T, D> {
    typedef typename N::template rebind<D>::other Allocator;
    typedef typename P::template rebind2<D, Allocator>::other Pool;
    typedef typename R::template rebind2<D, Pool>::other Reclaimer;
    /*typedef reclaimer_hazardptr<T, pool_none<T, allocator_new<T>>> Reclaimer;
    typedef allocator_new<T> Allocator;
    typedef pool_none<T, allocator_new<T>> Pool;*/
private:
    size_t thread_num;
    Allocator *alloc;
    Pool *pool;
    Reclaimer *reclaimer;
    std::atomic<bool> lock{false};
protected:
    using ihazard<T, D>::thread_number;

public:
    brown_reclaim(size_t total_thread) : thread_num(total_thread) {
        alloc = new Allocator(thread_num, nullptr);
        pool = new Pool(thread_num, alloc, nullptr);
        reclaimer = new Reclaimer(thread_num, pool, nullptr);
        if (typeid(R) == typeid(reclaimer_hazardptr<>)) free_type = 0;
        else if (typeid(R) == typeid(reclaimer_debraplus<>)) free_type = 1;
        else free_type = 2;
    }

    ~brown_reclaim() {
        for (int tid = 0; tid < thread_number - 1; tid++) {
            reclaimer->deinitThread(tid);
            alloc->deinitThread(tid);
        }
        delete reclaimer;
        //delete pool;
        delete alloc;
    }

    void registerThread() {}

    void initThread(int tid = 0) {
        bool expect = false;
        while (!lock.compare_exchange_strong(expect, true));
        thread_number++;
        brown_tid = tid;
        //std::cout << "initThread: " << brown_tid << std::endl;
        alloc->initThread(brown_tid);
        reclaimer->initThread(brown_tid);
        lock.store(false);
    }

    //length not used
    D * allocate(int tid, uint64_t len) {
        return (D * ) pool->get(tid);
    }

    bool deallocate(int tid,T * ptr) {
        reclaimer->retire(tid, ptr);
        return true;
    }

    uint64_t read(int tid, std::atomic<uint64_t> &atomic_entry) {
        uint64_t entry = 0, entry_check = 0;
        do {
            entry = atomic_entry.load();
            if (entry == 0) break;
            reclaimer->protect(brown_tid, (D *) libcuckoo::con_hp_store(entry) , callbackReturnTrue, nullptr, true);
            entry_check= atomic_entry.load();
        } while (entry != entry_check ) ;
        return entry;
    }

    void clear(int tid) {;}

    bool startOp(int tid){reclaimer->template startOp<T>(brown_tid, (void *const *const) &reclaimer, 1);};
    void endOp(int tid){reclaimer->endOp(tid);};


    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        uint64_t address = 0;
        do {
            if (address != 0)
                if (free_type == 0) reclaimer->unprotect(brown_tid, (D *) address);
                else reclaimer->endOp(brown_tid);

            address = (uint64_t) res.load(std::memory_order_relaxed);
            if (!address) {
                return nullptr;
            }

            if (is_safe((T *) address)) {
                return filter((T *) address);
            }
            if (free_type == 0) reclaimer->protect(brown_tid, (D *) address, callbackReturnTrue, nullptr, true);
            else reclaimer->template startOp<T>(brown_tid, (void *const *const) &reclaimer, 1);
        } while (address != (uint64_t) res.load(std::memory_order_relaxed));
        holder = address;
        //std::cout << "beginOp" << std::endl;
        return (T *) address;
    }

    const char *info() {
        return typeid(R).name();
    }
};

#endif //HASHCOMP_BROWN_RECLAIM_H
