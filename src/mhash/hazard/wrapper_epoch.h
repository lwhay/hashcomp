//
// Created by Michael on 11/27/19.
//

#ifndef HASHCOMP_WRAPPER_EPOCH_H
#define HASHCOMP_WRAPPER_EPOCH_H

#include <ihazard.h>
#include "GlobalWriteEM.h"

template<typename T>
class epoch_wrapper : public ihazard {
private:
    GlobalWriteEM<T> *gwm;

public:
    epoch_wrapper(size_t thread_count) {
        thread_number = thread_count;
        gwm->StartGCThread();
        std::cout << "Epoch wrapper" << std::endl;
    }

    ~epoch_wrapper() { delete gwm; }

    void registerThread() {}

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) { return ptr.load(); }

    void read(size_t tid) {}

    bool free(uint64_t ptr) {
        gwm->LeaveEpoch((void *) ptr);
        return true;
    }

    char *info() { return "epoch wrapper"; }

    uint64_t get() { return (uint64_t) gwm->JoinEpoch(); }
};

#endif //HASHCOMP_WRAPPER_EPOCH_H
