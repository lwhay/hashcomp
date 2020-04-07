//
// Created by Michael on 11/27/19.
//

#ifndef HASHCOMP_WRAPPER_EPOCH_H
#define HASHCOMP_WRAPPER_EPOCH_H

#include <ihazard.h>
#include "LocalWriteEM.h"
#include "GlobalWriteEM.h"

#define uselocal 1
#if uselocal == 0
thread_local void *curepoch;
#endif

template<typename T, typename D = T>
class epoch_wrapper : public ihazard<T, D> {
private:
#if uselocal == 1
    LocalWriteEM<T> *lwm;
#else
    GlobalWriteEM<T> *gwm;
#endif

protected:
    using ihazard<T, D>::thread_number;

public:
    epoch_wrapper(size_t thread_count) {
        thread_number = thread_count;
#if uselocal
        lwm = new LocalWriteEM<T>{thread_count};
        lwm->StartGCThread();
#else
        gwm = new GlobalWriterEM<T>();
        gwm->StartGCThread();
#endif
        std::cout << "Epoch wrapper" << std::endl;
    }

    ~epoch_wrapper() {
#if uselocal == 1
        delete lwm;
#else
        delete gwm;
#endif
    }

    void registerThread() {}

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
#if uselocal == 1
        lwm->AnnounceEnter(tid);
#endif
        return ptr.load();
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        return (T *) load(tid, res);
    }

    void read(size_t tid) {}

    bool free(uint64_t ptr) {
#if uselocal == 1
        lwm->AddGarbageNode((T *) ptr);
#else
        gwm->LeaveEpoch((void *) ptr);
#endif
        return true;
    }

    const char *info() { return "epoch wrapper"; }

    uint64_t get() {
#if uselocal == 1
        return 0;
#else
        return (uint64_t) gwm->JoinEpoch();
#endif
    }
};

#endif //HASHCOMP_WRAPPER_EPOCH_H
