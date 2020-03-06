//
// Created by Michael on 3/5/20.
//

#ifndef HASHCOMP_FASTER_EPOCH_H
#define HASHCOMP_FASTER_EPOCH_H

#include <vector>
#include "ihazard.h"
#include "faster/light_epoch.h"

template<typename T>
class faster_epoch : public ihazard {
    FASTER::core::LightEpoch epoch;

    class NodeQueue {
        std::vector<T> vect;
    public:
        void Push(T t) { vect.push_back(t); }

        T *Pop() { return vect.erase(vect.front()); }
    };

    //using DataNodeQueue = NodeQueue<DataNode>;
    //std::vector<DataNodeQueue> data_pool_;
    //epoch::MemoryEpoch <T, std::vector<DataNodeQueue>, DataNode> epoch_;

public:
    faster_epoch(uint64_t thread_count) : epoch(thread_count) {}

    void registerThread() {}

    T *allocate() {
        //return (T *) allocator.Allocate().control();
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        //epoch.ReentrantProtect();
        return ptr.load();
    }

    void read(size_t tid) {
        //epoch.ReentrantUnprotect();
    }

    bool free(uint64_t ptr) {
        //allocator.FreeAtEpoch(ptr, 0);
    }

    char *info() { return "faster_epoch"; }
};

#endif //HASHCOMP_FASTER_EPOCH_H