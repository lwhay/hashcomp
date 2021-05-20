#include "cdebra.h"
#include "cdebra_group.h"
#include "cebr_token.h"
#include "brown_reclaim.h"

#include "wfe/HazardTracker.hpp"
#include "wfe/BatchHazard.hpp"


class Empty{
public:
    Empty(int thread_num){};
    void initThread(int tid = 0){};

    bool startOp(int tid){};
    void endOp(int tid){};

    inline uint64_t read(int tid,atomic<uint64_t> & a){
        return a.load();
    };
    void clear(int tid){};

    inline storeType * allocate(int tid, uint64_t len){
        return static_cast<storeType *>(malloc(len));
    };

    bool deallocate(int tid, storeType * ptr){
        if(ptr!= nullptr)
            free(ptr);
    };
};

#define brown_new_once 1
#define brown_use_pool 1

#if brown_new_once == 1
#define balloc allocator_new
#elif brown_new_once == 0
#define balloc allocator_once
#else
#define balloc allocator_bump
#endif

#if brown_use_pool == 1
#define pool pool_perthread_and_shared
#else
#define pool pool_none
#endif


/** reclaimer list **/
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_hazardptr<>> brown6;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_ebr_token<>> brown7;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_ebr_tree<>> brown8;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_ebr_tree_q<>> brown9;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_debra<>> brown10;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_debraplus<>> brown11;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_debracap<>> brown12;
typedef brown_reclaim<storeType, balloc<storeType>, pool<>, reclaimer_none<>> brown13;

typedef Empty                           rc_empty;
typedef Reclaimer_debra                 rc_debra;
typedef Reclaimer_debra_group           rc_debra_group;
typedef Reclaimer_ebr_token             rc_ebr_token;

typedef HazardTracker<storeType>        wfe_hazardptr;
typedef BatchHazardTracker<storeType>   wfe_batch_hazardptr;

/** setting reclaimer **/
using Reclaimer = rc_ebr_token;

