#ifndef RECLAIM_BATCHHP_STACK_H
#define RECLAIM_BATCHHP_STACK_H

#include <cassert>
#include <iostream>
#include <sstream>
#include <string>
#include "blockbag.h"
#include "plaf.h"
#include "allocator_interface.h"
#include "hashtable.h"
#include "reclaimer_interface.h"
#include "arraylist.h"

#define MAX_HAZARDPTRS_PER_THREAD 16

#ifndef HASHCOMP_BATCH_HAZARD_H
// Carefully choose reservior to be between thread_number and batch_size, and cache by batch_size * 2;
constexpr size_t batch_size = (1llu << 9);

constexpr size_t reservior = (1llu << 6);

constexpr size_t lru_volume = batch_size + reservior;

thread_local uint64_t lrulist[lru_volume];

thread_local size_t startPos = 0, endPos = 0;

thread_local uint64_t thread_id = 0;

thread_local uint64_t recent_address = 0;
#endif

template<typename T = void, class Pool = pool_interface<T> >
class reclaimer_batchhp : public reclaimer_interface<T, Pool> {
private:
    struct holder {
        alignas(128) std::atomic<uint64_t> address;
    public:
        void init() { address.store(0, std::memory_order_relaxed); }

        void store(uint64_t ptr) { address.store(ptr, std::memory_order_relaxed); }

        uint64_t load() { return address.load(std::memory_order_relaxed); }
    };

    PAD; // post padding for superclass layout
    struct holder holders[MAX_THREADS_POW2];

    /*class circulequeue {
        constexpr static size_t limit = (batch_size * 2);
        volatile size_t head = 0, tail = 0;
        volatile uint64_t queue[limit];
    public:
        inline bool push(uint64_t e) {
            if ((head + limit - tail) % limit == 1) return false;
            queue[tail] = e;
            tail = (tail + 1) % limit;
            return true;
        }

        inline uint64_t pop() {
            if (head == tail) return 0;
            else {
                uint64_t now = queue[head];
                head = (head + 1) % limit;
                return now;
            }
        }
    } cache[thread_limit];*/

public:

    template<typename _Tp1>
    struct rebind {
        typedef reclaimer_batchhp<_Tp1, Pool> other;
    };
    template<typename _Tp1, typename _Tp2>
    struct rebind2 {
        typedef reclaimer_batchhp<_Tp1, _Tp2> other;
    };

    std::string getDetailsString() { return "batchhp reclaimer"; }

    std::string getSizeString() { return "batchhp reclaimer"; }

    inline static bool shouldHelp() {
        return true;
    }

    inline static bool isQuiescent(const int tid) {
        return true;
    }

    inline bool isProtected(const int tid, T *const obj) {
        return true;
    }

    inline bool isQProtected(const int tid, T *const obj) {
        return false;
    }

    // for hazard pointers (and reference counting)
    inline bool protect(const int tid, T *const ptr, CallbackType notRetiredCallback, CallbackArg callbackArg,
                        bool memoryBarrier = true) {
        holders[tid].store((uint64_t) ptr);
        recent_address = (uint64_t) ptr;
        return true;
    }

    inline void unprotect(const int tid, T *const obj) {
        if (recent_address != 0) holders[tid].store(0);
    }

    inline bool qProtect(const int tid, T *const obj, CallbackType notRetiredCallback, CallbackArg callbackArg,
                         bool memoryBarrier = true) {
        return true;
    }

    inline void qUnprotectAll(const int tid) {}

    // rotate the epoch bags and reclaim any objects retired two epochs ago.
    inline void rotateEpochBags(const int tid) {
    }

    // invoke this at the beginning of each operation that accesses
    // objects reclaimed by this epoch manager.
    // returns true if the call rotated the epoch bags for thread tid
    // (and reclaimed any objects retired two epochs ago).
    // otherwise, the call returns false.
    template<typename First, typename... Rest>
    inline bool
    startOp(const int tid, void *const *const reclaimers, const int numReclaimers, const bool readOnly = false) {
        return false;
    }

    inline void endOp(const int tid) {
    }

    // for all schemes except reference counting
    inline void retire(const int tid, T *ptr) {
        assert(ptr != 0);
        lrulist[endPos] = (uint64_t) ptr;
        endPos = ++endPos % lru_volume;
        if (endPos == startPos) {
            std::bitset<batch_size> bs(0);
            for (size_t t = 0; t < this->NUM_PROCESSES; t++) {
                if (t == tid) continue;
                const uint64_t target = holders[t].load();
                if (target != 0) {
                    for (size_t i = 0; i < batch_size; i++) {
                        if (lrulist[(startPos + i) % lru_volume] == target)
                            bs.set(i);
                    }
                }
            }
            for (size_t i = 0; i < batch_size; i++) {
                size_t idx = (startPos + i) % lru_volume;
                if (!bs.test(i)) {
                    uint64_t element = lrulist[idx];
                    this->pool->add(tid, (T *) element);
                    //if (!cache[tid].push(element)) std::free((void *) element);
                } else {
#if TRACE_CONFLICTS == 1
                    conflicts++;
#endif
                    lrulist[endPos] = lrulist[idx];
                    endPos = ++endPos % lru_volume;
                }
            }
            startPos = (startPos + batch_size) % lru_volume;
        }
    }

    void debugPrintStatus(const int tid) {
    }

//    set_of_bags<T> getBlockbags() {
//        set_of_bags<T> empty = {.bags = NULL, .numBags = 0};
//        return empty;
//    }
//
//    void getOldestTwoBlockbags(const int tid, blockbag<T> ** oldest, blockbag<T> ** secondOldest) {
//        *oldest = *secondOldest = NULL;
//    }
//
//    int getOldestBlockbagIndexOffset(const int tid) {
//        return -1;
//    }

    void getSafeBlockbags(const int tid, blockbag<T> **bags) {
        bags[0] = NULL;
    }

    void initThread(const int tid) {}

    void deinitThread(const int tid) {}

    reclaimer_batchhp(const int numProcesses, Pool *_pool, debugInfo *const _debug,
                      RecoveryMgr<void *> *const _recoveryMgr = NULL)
            : reclaimer_interface<T, Pool>(numProcesses, _pool, _debug, _recoveryMgr) {
        VERBOSE DEBUG std::cout << "constructor reclaimer_none" << std::endl;
    }

    ~reclaimer_batchhp() {
        VERBOSE DEBUG std::cout << "destructor reclaimer_none" << std::endl;
    }

};

#endif //RECLAIM_BATCHHP_STACK_H
