#ifndef MY_RECLAIMER_RECLAIMER_DEBRA_H
#define MY_RECLAIMER_RECLAIMER_DEBRA_H

#include "buffer_stack.h"

#include "def.h"
#include <atomic>

#define DEBRA_DISABLE_READONLY_OPT

#define EPOCH_INCREMENT 2
#define BITS_EPOCH(ann) ((ann)&~(EPOCH_INCREMENT-1))
#define QUIESCENT(ann) ((ann)&1)
#define GET_WITH_QUIESCENT(ann) ((ann)|1)

#define MIN_OPS_BEFORE_READ DEBRA_SETTING_MIN_OPS_BEFORE_READ

#define NUMBER_OF_EPOCH_BAGS 3 // 9 for range query support
#define NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS 0 // 3 for range query support


typedef BufferStack LimboBag;

class Reclaimer_debra{

private:
    class ThreadData {
    private:
        PAD;
    public:
        std::atomic_long announcedEpoch;
        long localvar_announcedEpoch; // copy of the above, but without the volatile tag, to try to make the read in enterQstate more efficient
    private:
        PAD;
    public:
        LimboBag *epochbags[NUMBER_OF_EPOCH_BAGS];
        // note: oldest bag is number (index+1)%NUMBER_OF_EPOCH_BAGS
        int index; // index of currentBag in epochbags for this process
    private:
        PAD;
    public:
        LimboBag *currentBag;  // pointer to current epoch bag for this process
        int checked;               // how far we've come in checking the announced epochs of other threads
        int opsSinceRead;
        //MultiLevelQueue<storeType> multiLevelQueue;
        ThreadData() {}

    private:
        PAD;
    };

    PAD;
    ThreadData threadData[MAX_THREADS_POW2];
    PAD;
    Alloc * alloc;
    PAD;

//    PAD; // not needed after superclass layout
    volatile long epoch;
    PAD;

    const int NUM_PROCESSES;



    void retire(int tid,storeType * ptr);
    void rotate_epoch_bag(int tid);

public:

    Reclaimer_debra(int thread_num);
    void initThread(int tid = 0);

    bool startOp(int tid);
    void endOp(int tid);

    //Not doing the actual work
    inline uint64_t read(int tid,atomic<uint64_t> & a){return a.load();};
    void clear(int tid){};

    inline storeType * allocate(int tid, uint64_t len);
    bool deallocate(int tid, storeType * ptr);

    //void dump();
};

Reclaimer_debra::Reclaimer_debra(int thread_num) : NUM_PROCESSES(thread_num) {
    alloc = new Alloc;
    epoch = 0;
    for (int tid = 0; tid < NUM_PROCESSES; ++tid) {
        threadData[tid].index = 0;
        threadData[tid].localvar_announcedEpoch = GET_WITH_QUIESCENT(0);
        threadData[tid].announcedEpoch.store(GET_WITH_QUIESCENT(0), std::memory_order_relaxed);
        for (int i = 0; i < NUMBER_OF_EPOCH_BAGS; ++i) {
            threadData[tid].epochbags[i] = NULL;
        }
    }
}

void Reclaimer_debra::initThread(int tid) {
    for (int i = 0; i < NUMBER_OF_EPOCH_BAGS; ++i) {
        if (threadData[tid].epochbags[i] == NULL) {
            threadData[tid].epochbags[i] = new LimboBag ;
        }
    }
    threadData[tid].currentBag = threadData[tid].epochbags[0];
    threadData[tid].opsSinceRead = 0;
    threadData[tid].checked = 0;
}

storeType *Reclaimer_debra::allocate(int tid, uint64_t len) {
    return static_cast<storeType *>(alloc->allocate(len));
}

bool Reclaimer_debra::deallocate(int tid, storeType *ptr) {
    //TODO can rm mask here, just confirm that input ptr,not entry
    //TODO pay attention to op time
    //startOp(tid);
    retire(tid, ptr);
    //endOp(tid);
}

void Reclaimer_debra::rotate_epoch_bag(int tid) {
    int nextIndex = (threadData[tid].index + 1) % NUMBER_OF_EPOCH_BAGS;
    LimboBag *const freeable = threadData[tid].epochbags[(nextIndex + NUMBER_OF_ALWAYS_EMPTY_EPOCH_BAGS) %
                                                            NUMBER_OF_EPOCH_BAGS];
    llog.bag_rotate_times ++;
    llog.bag_rotate_length += freeable->total_size;
    freeable->free(alloc);
    SOFTWARE_BARRIER;

    threadData[tid].currentBag = threadData[tid].epochbags[nextIndex];
}

bool Reclaimer_debra::startOp(int tid) {
    SOFTWARE_BARRIER; // prevent any bookkeeping from being moved after this point by the compiler.
    bool result = false;

    long readEpoch = epoch;
    const long ann = threadData[tid].localvar_announcedEpoch;
    threadData[tid].localvar_announcedEpoch = readEpoch;

    // if our announced epoch was different from the current epoch
    if (readEpoch != ann /* invariant: ann is not quiescent */) {
        // rotate the epoch bags and
        // reclaim any objects retired two epochs ago.
        threadData[tid].checked = 0;

        rotate_epoch_bag(tid);
        result = true;
    }
    // we should announce AFTER rotating bags if we're going to do so!!
    // (very problematic interaction with lazy dirty page purging in jemalloc triggered by bag rotation,
    //  which causes massive non-quiescent regions if non-Q announcement happens before bag rotation)
    SOFTWARE_BARRIER;
    threadData[tid].announcedEpoch.store(readEpoch,
                                         std::memory_order_relaxed); // note: this must be written, regardless of whether the announced epochs are the same, because the quiescent bit will vary

    SOFTWARE_BARRIER;

    // note: readEpoch, when written to announcedEpoch[tid],
    //       sets the state to non-quiescent and non-neutralized

    // incrementally scan the announced epochs of all threads
    if (++threadData[tid].opsSinceRead == MIN_OPS_BEFORE_READ) {
        threadData[tid].opsSinceRead = 0;
        int otherTid = threadData[tid].checked;
        long otherAnnounce = threadData[otherTid].announcedEpoch.load(std::memory_order_relaxed);
        if (BITS_EPOCH(otherAnnounce) == readEpoch || QUIESCENT(otherAnnounce)) {
            const int c = ++threadData[tid].checked;
            if (c >= this->NUM_PROCESSES /*&& c > MIN_OPS_BEFORE_CAS_EPOCH*/) {
                if (__sync_bool_compare_and_swap(&epoch, readEpoch, readEpoch + EPOCH_INCREMENT)) {
                    llog.epoch_go_forward_times ++;
                }
            }
        }
    }
    return result;
}

void Reclaimer_debra::endOp(int tid) {
    threadData[tid].announcedEpoch.store(GET_WITH_QUIESCENT(threadData[tid].localvar_announcedEpoch),
                                         std::memory_order_relaxed);
}

void Reclaimer_debra::retire(int tid, storeType *ptr) {
    threadData[tid].currentBag->add(ptr);
}


#endif //MY_RECLAIMER_RECLAIMER_DEBRA_H
