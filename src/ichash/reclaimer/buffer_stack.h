#ifndef MY_RECLAIMER_BUFFER_QUEUE_H
#define MY_RECLAIMER_BUFFER_QUEUE_H

#include <cstdint>
#include "cdrbea_def.h"
#include "allocator_new.h"
#include "../assert_msg.h"

//default block size
#define BLOCK_SIZE 65535

//
struct Block{
    void* ptrs[BLOCK_SIZE];
    Block * next;
    size_t offset;
    Block(){
        next = nullptr;
        offset = 0;
    }

    //frees all pointers from the block,return free number of ptrs
    size_t free(AllocatorNew * alloc){
        for(size_t i = 0 ; i < offset; i++){
            alloc->deallocate(ptrs[i]);
        }
        size_t ret = offset;
        offset =0;
        return ret;
    }

    //Hold a new ptr in the block
    //Make sure the upper limit is not exceeded when called
    void push(void * ptr){
        cdebug ASSERT(offset < BLOCK_SIZE,"block full");
        ptrs[offset++] = ptr;
    }
};


class BufferStack{
public:
    BufferStack();

    void add(void * ptr);

    void free(AllocatorNew * alloc);

    size_t get_size(){return total_size;}

    uint64_t total_size;     //number of blocks

private:

    Block * head_block;
    Block * curr_block;
    PAD;
};


BufferStack::BufferStack(){
    total_size = 0;
    head_block = new Block();
    curr_block = head_block;
}


void BufferStack::add(void *ptr) {
    if(curr_block->offset == BLOCK_SIZE){
        //add new block
        Block * tmp = new Block ();
        curr_block->next = tmp;
        curr_block = tmp;
    }

    curr_block->push(ptr);
    total_size ++;
}

void BufferStack::free(AllocatorNew * alloc){
    Block * b = head_block;
    while( b != curr_block){
        total_size -= b->free(alloc);
        Block * tmp = b->next;
        delete b;
        b = tmp;
    }
    total_size -= b->free(alloc);
    head_block = curr_block;
    cdebug ASSERT(total_size == 0,"Not cleaned up")
}


#endif //MY_RECLAIMER_BUFFER_QUEUE_H
