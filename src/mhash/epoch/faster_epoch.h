//
// Created by Michael on 3/5/20.
//

#ifndef HASHCOMP_FASTER_EPOCH_H
#define HASHCOMP_FASTER_EPOCH_H

#include <vector>
#include <queue>
#include "ihazard.h"
#include "faster/light_epoch.h"

template<typename T, typename D = T>
class faster_epoch : public ihazard<T, D> {
private:
    /*class NodeQueue {
        std::vector<T> vect;
    public:
        void Push(T t) { vect.push_back(t); }

        T *Pop() { return vect.erase(vect.front()); }
    };*/

    using DataNodeQueue = std::queue<T *>;
    using Pool = std::vector<DataNodeQueue>;

    class Delete_Context : public FASTER::core::IAsyncContext {
    public:
        Delete_Context(T *ptr_, Pool *pool_) : ptr(ptr_), pool(pool_) {}

        Delete_Context(const Delete_Context &other) : ptr(other.ptr), pool(other.pool) {}

    protected:
        FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext *&context_copy) final {
            return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
        }

    public:
        T *ptr;
        Pool *pool;
    };

    static void delete_callback(FASTER::core::IAsyncContext *ctxt) {
        FASTER::core::CallbackContext<Delete_Context> context(ctxt);
        uint32_t tid = FASTER::core::Thread::id();
        assert(tid <= context->pool->size());
        auto history = context->pool->operator[](tid);
        history.push(context->ptr);
    }

public:
    faster_epoch(uint64_t thread_count) : epoch(thread_count), data_pool{thread_count + 2} {}

    ~faster_epoch() {
        epoch.Unprotect();
    }

    void registerThread() {}

    void initThread(size_t tid = 0) {}

    uint64_t allocate(size_t tid) { return -1; }

    T *allocate() {
        //return (T *) allocator.Allocate().control();
    }

    template<typename IS_SAFE, typename FILTER>
    T *Repin(size_t tid, std::atomic<T *> &res, IS_SAFE is_safe, FILTER filter) {
        return (T *) load(tid, (std::atomic<uint64_t> &) res);
    }

    uint64_t load(size_t tid, std::atomic<uint64_t> &ptr) {
        epoch.ReentrantProtect();
        return ptr.load();
    }

    void read(size_t tid) {
        epoch.ReentrantUnprotect();
    }

    bool free(uint64_t ptr) {
        Delete_Context deleteContext(reinterpret_cast<T *>(ptr), &data_pool);
        FASTER::core::IAsyncContext *contextCopy;
        deleteContext.DeepCopy(contextCopy);
        epoch.BumpCurrentEpoch(delete_callback, contextCopy);
        return true;
    }

    const char *info() { return "faster_epoch"; }

private:
    Pool data_pool;
    FASTER::core::LightEpoch epoch;
};

#endif //HASHCOMP_FASTER_EPOCH_H
