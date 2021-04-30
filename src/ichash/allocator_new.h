#ifndef MY_RECLAIMER_ALLOCATOR_NEW_H
#define MY_RECLAIMER_ALLOCATOR_NEW_H


#include "item.h"
#include "buffer_stack.h"

template <typename T>
class AllocatorNew{
public:
    AllocatorNew();

    T * allocate(uint64_t len);
    void deallocate(T * ptr);
    void free_limbobag(BufferStack<T> * freebag);

//    uint64_t get_total_size();
//    void dump();

private:

//    uint64_t size; //total number of items in this multi level queue
//    BufferQueue<T> BFQS[MAX_ORDER];

};

template<typename T>
AllocatorNew<T>::AllocatorNew() {}


template<typename T>
T * AllocatorNew<T>::allocate(uint64_t len) {

    void * tp=malloc(len + sizeof(POINTER)); //buffer size + two pointer spaces used to construct lists
    ((Listhead *)tp)->next= nullptr;
    return (T*)((POINTER)tp+1);

}


//only used in reclaimering freebag
template<typename T>
void AllocatorNew<T>::deallocate(T *ptr) {

    void * tp = (POINTER)ptr-1;
    free(tp);
    return;

}


template<typename T>
void AllocatorNew<T>::free_limbobag(BufferStack<T> * freebag) {

    while(freebag->get_size() > 0){
        T * p = freebag->pop();
        tw_info.num_mlq_reclaim++;
        deallocate(p);
    }
}



#endif //MY_RECLAIMER_ALLOCATOR_NEW_H
