#ifndef MY_RECLAIMER_RECLAIMER_TOKEN_H
#define MY_RECLAIMER_RECLAIMER_TOKEN_H

#include "buffer_stack.h"

#include "def.h"
#include <atomic>

typedef BufferStack LimboBag;

class Reclaimer_ebr_token{

private:

    class ThreadData {
    private:
        PAD;
    public:
        volatile int token;
        uint64_t tokenCount; // how many times this thread has had the token

        uint64_t empty_limbobag_pass_token;

        LimboBag *curr;
        LimboBag *last;
        //MultiLevelQueue<storeType> multiLevelQueue;

    public:
        ThreadData() {}
    };

    const int NUM_PROCESSES;
    ThreadData threadData[MAX_THREADS_POW2];
    PAD;


    void rotate_epoch_bag(int tid);
    void retire(int tid, storeType *ptr);

    Alloc * alloc;

public:
    Reclaimer_ebr_token(int thread_num);
    void initThread(int tid = 0);

    bool startOp(int tid);
    void endOp(int tid);

    inline uint64_t read(int tid, std::atomic<uint64_t> &a);
    void clear(int tid);

    inline storeType * allocate(int tid, uint64_t len);
    bool deallocate(int tid, storeType * ptr);

    void dump();


};

Reclaimer_ebr_token::Reclaimer_ebr_token(int thread_num) : NUM_PROCESSES(thread_num) {
    for (int tid = 0; tid < NUM_PROCESSES; ++tid) {
        threadData[tid].token = (tid == 0 ? 1 : 0); // first thread starts with the token
        threadData[tid].tokenCount = 0; // thread with token will update this itself
        threadData[tid].empty_limbobag_pass_token = 0;
        threadData[tid].curr = nullptr;
        threadData[tid].last = nullptr;
    }
    //multi_level_queues is initialized automatically
}

void Reclaimer_ebr_token::initThread(int tid) {
    if (threadData[tid].curr == nullptr) {
        threadData[tid].curr = new LimboBag();
    }
    if (threadData[tid].last == nullptr) {
        threadData[tid].last = new LimboBag();
    }
}


inline storeType *Reclaimer_ebr_token::allocate(int tid, uint64_t len) {
    return static_cast<storeType *>(alloc->allocate(len));
}


bool Reclaimer_ebr_token::deallocate(int tid, storeType *ptr) {
    retire(tid,ptr);
}

bool Reclaimer_ebr_token::startOp(int tid) {
    SOFTWARE_BARRIER; // prevent token passing from happening before we are really quiescent

    bool result = false;
    if (threadData[tid].token) {
        //cout<<tid<<" pass token "<< this->NUM_PROCESSES<<endl;
        ++threadData[tid].tokenCount;

        // pass token
        threadData[tid].token = 0;
        threadData[(tid + 1) % this->NUM_PROCESSES].token = 1;
        __sync_synchronize();

        rotate_epoch_bag(tid);

        result = true;


    }

    // in common case, this is lightning fast...
    return result;
}

void Reclaimer_ebr_token::endOp(int tid) {}



void Reclaimer_ebr_token::rotate_epoch_bag(int tid) {
    LimboBag * freeable = threadData[tid].last;

    if(freeable->get_size() == 0) ++threadData[tid].empty_limbobag_pass_token;
    //threadData[tid].multiLevelQueue.free_limbobag(freeable);
    llog.bag_rotate_times ++;
    llog.bag_rotate_length += freeable->total_size;
    freeable->free(alloc);
    SOFTWARE_BARRIER;

    // swap curr and last
    threadData[tid].last = threadData[tid].curr;
    threadData[tid].curr = freeable;
}

void Reclaimer_ebr_token::retire(int tid, storeType *ptr) {
    threadData[tid].curr->add(ptr);
}

uint64_t Reclaimer_ebr_token::read(int tid, std::atomic<uint64_t> &a) {
    return a.load();
}

void Reclaimer_ebr_token::clear(int tid)  {
    //do nothing
    //endOp(tid);
}

void Reclaimer_ebr_token::dump() {
    cout<<"dump reclaimer data >> "<<endl;
    for( int i = 0; i < NUM_PROCESSES ; i++  ){
        ThreadData  & td  = threadData[i];
        cout<<"* therad "<<i<<endl;
        cout<<"token "<<td.token<<" token_count "<<td.tokenCount<<" empty pass: "<<td.empty_limbobag_pass_token<<endl;
        cout<<"currbag size: "<<td.curr->get_size()<<endl;
        cout<<"lastbag size: "<<td.last->get_size()<<endl;
        //td.multiLevelQueue.dump();
    }
}

#endif //MY_RECLAIMER_RECLAIMER_TOKEN_H
