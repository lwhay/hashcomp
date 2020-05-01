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
static thread_local std::queue<T *> cache;

template<typename T, typename D = T>
class faster_epoch : public ihazard<T, D> {
    class SelfCleanQueue;

    using DataNodeQueue = SelfCleanQueue;
    using Pool = std::vector<DataNodeQueue>;

private:
    Pool data_pool;
    FASTER::core::LightEpoch epoch;
private:
    class SelfCleanQueue {
    private:
        std::queue<T *> history;
    public:
        ~SelfCleanQueue() {
            while (!history.empty()) {
                T *e = history.front();
                history.pop();
                delete e;
                //cache<T>.push(e);
            }
        }

        void Push(T *t) { history.push(t); }
    };

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
        history.Push(context->ptr);
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
        return -1;
        /*T *e;
        if (!cache<T>.empty()) {
            e = cache<T>.front();
            cache<T>.pop();
        } else {
            e = new T;
        }
        return e;*/
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
};

#endif //HASHCOMP_FASTER_EPOCH_H
