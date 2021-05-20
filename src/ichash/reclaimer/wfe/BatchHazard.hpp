//
// Created by iclab on 4/9/21.
//

#ifndef WFE_BATCHHAZARD_H
#define WFE_BATCHHAZARD_H

#ifndef _REENTRANT
#define _REENTRANT
#endif

#include <queue>
#include <list>
#include <vector>
#include <atomic>
#include "ConcurrentPrimitives.hpp"
#include "arraylist.h"
//#include "RAllocator.hpp"

#include "BaseTracker.hpp"

#define PRINT_CONFLICT       0

template<class T>
class BatchHazardTracker : public BaseTracker<T> {
private:
    static const size_t cacheLimit = 2048;
    int task_num;
    int slotsPerThread;
    int freq;
    bool collect;

    //RAllocator *mem;

    AtomicArrayList<T> **announce;
    PAD;

    paddedAtomic<T *> *slots;
    padded<int> *cntrs;
    padded<std::list<T *>> *retired; // TODO: use different structure to prevent malloc locking....
    uint64_t *conflicts;

    struct CircleQueue {
        T *cache[cacheLimit];
        size_t head = 0, tail = 0;
    public:
        bool push(T *e) {
            if ((tail + 1) % cacheLimit == head) return false;
            cache[tail] = e;
            tail = (tail + 1) % cacheLimit;
            return true;
        }

        T *pop() {
            if (head == tail) return nullptr;
            T *cur = cache[head];
            head = (head + 1) % cacheLimit;
            return cur;
        }

        T *fetch(size_t off) {
            if (off >= size()) return nullptr;
            return cache[(head + off) % cacheLimit];
        }

        void shift(size_t count) {
            head = (head + count) % cacheLimit;
        }

        size_t size() {
            return (tail + cacheLimit - head) % cacheLimit;
        }
    };

    CircleQueue caches[MAX_DEFAULT_THREAD_NUMBER];
    bool needPin[MAX_DEFAULT_THREAD_NUMBER][cacheLimit];

    void empty(int tid) {
        std::list<T *> *myTrash = &(retired[tid].ui);

        size_t total = cacheLimit / 2;
        std::memset(needPin[tid], 0, total * sizeof(bool));
//        for (int i = 0; i < task_num * slotsPerThread; i++) {
//            T *wanted = slots[i].ui.load(std::memory_order_acquire);
//            if (wanted == NULL) continue;
//            for (int i = 0; i < total; i++) {
//                if (caches[tid].fetch(i) == wanted) needPin[tid][i] = true;
//            }
//            /*for (typename std::list<T *>::iterator iterator = myTrash->begin(); count < total; count++) {
//                auto ptr = *iterator;
//                if (ptr == wanted) reEnter[count] = true;
//            }*/
//        }
        for (int otherTid = 0; otherTid < this->task_num; ++otherTid) {
            int sz = announce[otherTid]->size();
                for (int ixHP = 0; ixHP < sz; ++ixHP) {
                    T *wanted =announce[otherTid]->get(ixHP);
                    if (wanted == NULL) continue;
                    for (int i = 0; i < total; i++) {
                        if (caches[tid].fetch(i) == wanted) needPin[tid][i] = true;
                    }
            }

        }


        for (int i = 0; i < total; i++) {
            auto ptr = caches[tid].pop();
            if (needPin[tid][i]) {
#if PRINT_CONFLICT
                std::cout << tid << ":" << conflicts[tid] /*<< ":" << caches[tid].size()*/ << std::endl;
#endif
                caches[tid].push(ptr);
                conflicts[tid]++;
            } else {
                this->reclaim(ptr);
                this->dec_retired(tid);
            }
        }

        return;
    }

public:
    ~BatchHazardTracker() {
        delete[] conflicts;
    };

    BatchHazardTracker(int task_num, int slotsPerThread = 0, int emptyFreq = 100, bool collect=true) : BaseTracker<T>(task_num) {
        announce = new AtomicArrayList<T> *[task_num];
        this->task_num = task_num;
        this->slotsPerThread = slotsPerThread;
        this->freq = emptyFreq;
        slots = new paddedAtomic<T *>[task_num * slotsPerThread];
        conflicts = new uint64_t[task_num];
        for (int i = 0; i < task_num * slotsPerThread; i++) {
            slots[i] = NULL;
        }
        retired = new padded<std::list<T *>>[task_num];
        cntrs = new padded<int>[task_num];
        for (int i = 0; i < task_num; i++) {
            announce[i] = new AtomicArrayList<T>(MAX_HAZARDPTRS_PER_THREAD);
            cntrs[i] = 0;
            conflicts[i] = 0;
            retired[i].ui = std::list<T *>();
        }
        this->collect = collect;
    }

    BatchHazardTracker(int task_num, int slotsPerThread, int emptyFreq) :
            BatchHazardTracker(task_num, slotsPerThread, emptyFreq, true) {}

    void initThread(int tid = 0){};
    bool startOp(int tid){};
    void endOp(int tid){};

    uint64_t read(int tid,std::atomic<uint64_t>& atomic_entry){
        uint64_t entry = 0, entry_check = 0;
        do {
            entry = atomic_entry.load(std::memory_order_relaxed);
            if (entry == 0) break;
            announce[tid]->add((T*)libcuckoo::con_hp_store(entry));
            entry_check= atomic_entry.load(std::memory_order_relaxed);
        } while (entry != entry_check ) ;
        return entry;
    }

    uint64_t get_conflict_cnt(int tid) { return conflicts[tid]; }

    void reserve_slot(T *ptr, int slot, int tid) {
        slots[tid * slotsPerThread + slot] = ptr;
    }

    void reserve_slot(T *ptr, int slot, int tid, T *node) {
        slots[tid * slotsPerThread + slot] = ptr;
    }

    void clearSlot(int slot, int tid) {
        slots[tid * slotsPerThread + slot] = NULL;
    }

    void clearAll(int tid) {
        for (int i = 0; i < slotsPerThread; i++) {
            slots[tid * slotsPerThread + i] = NULL;
        }
    }

    void clear_all(int tid) {
        clearAll(tid);
    }

    void clear(int tid){
        announce[tid]->clear();
    };

    inline storeType * allocate(int tid, uint64_t len){
        return static_cast<storeType *>(malloc(len));
    };

    bool deallocate(int tid, storeType * ptr){
        if (ptr == nullptr) { return false; }

        if (caches[tid].size() == cacheLimit - 1) empty(tid);
        caches[tid].push(ptr);

    }

    bool collecting() { return collect; }

};

#endif //WFE_BATCHHAZARD_H
