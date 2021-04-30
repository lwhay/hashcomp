#ifndef MY_RECLAIMER_DEF_H
#define MY_RECLAIMER_DEF_H

#include <cmath>
#include <iostream>
#include <assert.h>
#include <mutex>

//The size of the smallest buffer
#define BASE_SIZE 100
//The growth rate
#define FACTOR  1.5
//Maximum number of increases
#define MAX_ORDER 5

#define ORDER(i) BASE_SIZE * pow(FACTOR,i)

#define MAX_UNIF_SIZE  BASE_SIZE * pow(FACTOR,MAX_ORDER-1)




#ifndef SOFTWARE_BARRIER
#   define SOFTWARE_BARRIER asm volatile("": : :"memory")
#endif


#define CAT2(x, y) x##y
#define CAT(x, y) CAT2(x, y)

#define PAD volatile char CAT(___padding, __COUNTER__)[128]

#define MAX_THREADS_POW2 512



thread_local int debug_tid;

struct Debug_thread_work_info{
    uint64_t num_new_item_malloc;
    uint64_t num_mlq_reclaim;
};

thread_local Debug_thread_work_info tw_info;

std::mutex debug_mtx;
static void dump_debug_thread_work_info(){
    debug_mtx.lock();
    cout<<"thread: "<<debug_tid<<endl;
    cout<<"num_new_item_alloc  "<<tw_info.num_new_item_malloc<<endl;
    cout<<"num_mlq_reclaimer  "<<tw_info.num_mlq_reclaim<<endl;
    debug_mtx.unlock();
}

#endif //MY_RECLAIMER_DEF_H
