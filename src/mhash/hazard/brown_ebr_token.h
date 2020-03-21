//
// Created by Michael on 3/22/20.
//

#ifndef HASHCOMP_BROWN_EBR_TOKEN_H
#define HASHCOMP_BROWN_EBR_TOKEN_H

#include <atomic>
#include <cassert>
#include "ihazard.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "record_manager.h"
#include "reclaimer_ebr_token.h"
#include "memory_hazard.h"

template<typename T>
class brown_ebr_token : public ihazard {
    typedef reclaimer_ebr_token<T, pool_none<T, allocator_new<T>>> Reclaimer;
    typedef allocator_new<T> Allocator;
    typedef pool_none<T, allocator_new<T>> Pool;
private:
    size_t thread_num;
    Allocator *alloc;
    Pool *pool;
    Reclaimer *reclaimer;
    std::atomic<bool> lock{false};

public:
    brown_ebr_token(size_t total_thread) : thread_num(total_thread) {
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

    T *allocate(size_t tid) {
        return alloc->allocate(ftid);
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
        /*if (ftid == 0) return true;
        std::cout << ftid << std::endl;*/
        reclaimer->retire(ftid, (T *) ptr);
        //std::free((T *) ptr);
        //alloc->deallocate(ftid, (T *) ptr);
        return true;
    }

    char *info() {
        return "brown ebr token";
    }
};

#endif //HASHCOMP_BROWN_EBR_TOKEN_H