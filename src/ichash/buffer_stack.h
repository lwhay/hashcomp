#ifndef MY_RECLAIMER_BUFFER_QUEUE_H
#define MY_RECLAIMER_BUFFER_QUEUE_H

#include <cstdint>
#include "reclaimer_def.h"


struct Listhead{
    Listhead * next;
};


#define POINTER void **


#define GET_QUEUE_NODE(p) \
            (Listhead *)((POINTER)p-1)

#define GET_MEM(listnode) \
            (void *)(listnode+1)

#define GET_MALLOC_RETURN(len) \
            (void *)((void **)malloc(len) + 2)
template <typename T>
class BufferStack{
public:
    BufferStack();

    void add(void * ptr);

    T * pop(); //used in freebag

    inline uint64_t get_size();

private:

    uint64_t size;     //number of block memory the buffer has
    Listhead listhead; //queue head
    PAD;
};

template <typename T>
BufferStack<T>::BufferStack():size(0){
    listhead.next = nullptr ;
}

template <typename T>
inline uint64_t BufferStack<T>::get_size() {
    return this->size;
}

//only used when freeing freebag
template<typename T>
T * BufferStack<T>::pop() {
    assert(size > 0 );
    Listhead *head= & listhead;
    Listhead *node = head->next;
    head->next = node->next;
    size --;
    node->next= nullptr;
    return (T *)GET_MEM(node);
}

//both used in multi_level_queue and limbo bag
template<typename T>
void BufferStack<T>::add(void *ptr) {
    Listhead * head= & listhead;
    Listhead * node= GET_QUEUE_NODE(ptr);

    node->next = head->next;
    head->next = node;

    size++;
}


#endif //MY_RECLAIMER_BUFFER_QUEUE_H
