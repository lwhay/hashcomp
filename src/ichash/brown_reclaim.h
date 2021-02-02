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
    brown_reclaim(size_t total_thread) : thread_num(total_thread + 1) {
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

    void initThread(size_t tid = 0) {
        bool expect = false;
        while (!lock.compare_exchange_strong(expect, true));
        thread_number++;
        brown_tid = tid;
        //std::cout << "initThread: " << brown_tid << std::endl;
        alloc->initThread(brown_tid);
        reclaimer->initThread(brown_tid);
        lock.store(false);
    }

    uint64_t allocate(size_t tid) {
        return (uint64_t) pool->get(brown_tid);
        //return (uint64_t) std::malloc(sizeof(D));
        //return (uint64_t) alloc->allocate(brown_tid);
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        if (free_type == 0) {
            uint64_t address = 0;
            do {
                if (address != 0) reclaimer->unprotect(brown_tid, (D *) address);
                address = ptr.load(std::memory_order_relaxed);
                if (address == 0) break;
                reclaimer->protect(brown_tid, (D *) address, callbackReturnTrue, nullptr, false);
            } while (address != ptr.load(std::memory_order_relaxed));
            holder = address;
            return address;
        } else {
            holder = ptr.load(std::memory_order_relaxed);
            if (holder == 0) return holder;
            //std::cout << "start: " << tid << " " << address << std::endl;
            reclaimer->template startOp<T>(brown_tid, (void *const *const) &reclaimer, 1);
            return ptr.load(std::memory_order_relaxed);
        }
    }

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
            if (free_type == 0) reclaimer->protect(brown_tid, (D *) address, callbackReturnTrue, nullptr, false);
            else reclaimer->template startOp<T>(brown_tid, (void *const *const) &reclaimer, 1);
        } while (address != (uint64_t) res.load(std::memory_order_relaxed));
        holder = address;
        //std::cout << "beginOp" << std::endl;
        return (T *) address;
    }

    void read(size_t tid) {
        if (holder != 0) {
            if (free_type == 0) {
                reclaimer->unprotect(brown_tid, (D *) holder);
            } else {
                reclaimer->endOp(brown_tid);
                holder = 0;
                //std::cout << "endOp" << std::endl;
                //reclaimer->rotateEpochBags(ftid);
            }
        }
    }

    bool free(uint64_t ptr) {
        //std::cout << ftid << std::endl;
        reclaimer->retire(brown_tid, (D *) ptr);
        if (free_type != 0) {
            //alloc->deallocate(ftid, (T *) ptr);
            /*reclaimer->template startOp<T>(ftid, (void *const *const) &reclaimer, 1);
            reclaimer->endOp(ftid);*/
            reclaimer->rotateEpochBags(brown_tid);
        }
        //std::free((T *) ptr);
        return true;
    }

    const char *info() {
        return typeid(R).name();
    }
};

#endif //HASHCOMP_BROWN_RECLAIM_H
