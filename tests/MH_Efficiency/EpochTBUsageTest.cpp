//
// Created by iclab on 1/6/20.
//

#include <iostream>
#include <thread>
#include "tracer.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "record_manager.h"
#include "reclaimer_hazardptr.h"
#include "reclaimer_debra.h"
#include "reclaimer_debraplus.h"
#include "reclaimer_debracap.h"
#include "reclaimer_ebr_tree.h"
#include "reclaimer_ebr_tree_q.h"
#include "reclaimer_ebr_token.h"

const int NUM_THREADS = 1;

const int NUM_ROUNDS = (1 << 30); // For tb_hazard, number of rounds should be larger than that of elements.

const int NUM_ELEMENTS = (1 << 4/*19*/);

// Rather than bz_hazard, other reclaimers are dummy classes in case of (un)protect
template<typename type, template<typename, typename> class tb_reclaimer>
class testcase {
    typedef tb_reclaimer<type, pool_interface<type>> Reclaimer;
    typedef allocator_new<type> Allocator;
    typedef pool_perthread_and_shared<type> Pool;
public:
    void bz_test() {
        Tracer tracer;
        tracer.startTime();
        Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
        Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
        Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
        std::cout << "\033[32m" << typeid(Reclaimer).name() << "\033[0m" << std::endl;
        std::cout << "Reclaimer create: " << tracer.getRunTime() << std::endl;
        type *cache[NUM_ELEMENTS];

        for (int i = 0; i < NUM_ELEMENTS; i++) cache[i] = alloc->allocate(0);
        std::cout << "Reclaimer allocate: " << tracer.getRunTime() << std::endl;
        for (int k = 0; k < NUM_ROUNDS; k++) {
            for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->protect(0, cache[i], callbackReturnTrue, nullptr, false);

            for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->unprotect(0, cache[i]);

            //for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->retire(0, cache[i]);
        }
        std::cout << "Reclaimer (un)protect: " << tracer.getRunTime() << std::endl;
        for (int i = 0; i < NUM_ELEMENTS; i++) alloc->deallocate(0, cache[i]);

        std::cout << "Reclaimer deallocate: " << tracer.getRunTime() << std::endl;
    }
};

void tb_hazard_original() {
    typedef reclaimer_hazardptr<uint64_t, pool_none<uint64_t, allocator_once<uint64_t>>> Reclaimer;
    typedef allocator_once<uint64_t> Allocator;
    typedef pool_none<uint64_t, allocator_once<uint64_t>> Pool;

    Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
    Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
    Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
    /*record_manager<Reclaimer, Allocator, Pool, uint64_t> *recordManager = new record_manager<Reclaimer, Allocator, Pool, uint64_t>(
            NUM_THREADS);*/
    Tracer tracer;
    tracer.startTime();
    //recordManager->initThread(0);

    uint64_t *cache[NUM_ELEMENTS];

    std::cout << "Reclaimer allocate: " << tracer.getRunTime() << std::endl;
    for (int k = 0; k < NUM_ROUNDS; k++) {
        for (int i = 0; i < NUM_ELEMENTS; i++) cache[i] = alloc->allocate(0);
        for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->protect(0, cache[i], callbackReturnTrue, nullptr, false);

        for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->unprotect(0, cache[i]);

        for (int i = 0; i < NUM_ELEMENTS; i++) {
            reclaimer->retire(0, cache[i]);
            alloc->deallocate(0, cache[i]);
        }
        if (k % 10000000 == 0) {
            std::cout << pool->computeSize(0) << std::endl;
        }
    }
    std::cout << "Reclaimer (un)protect: " << tracer.getRunTime() << std::endl;

    std::cout << "Reclaimer deallocate: " << tracer.getRunTime() << std::endl;
    /*recordManager->deinitThread(0);
    std::cout << "Reclaimer deinitThread: " << tracer.getRunTime() << std::endl;
    delete recordManager;*/
    //delete reclaimer;
    //delete pool;
    delete alloc;
}

int main(int argc, char **argv) {
    tb_hazard_original();
    for (int i = 0; i < 0; i++) {
        testcase<uint64_t, reclaimer_debra> debra;
        debra.bz_test();
        testcase<uint64_t, reclaimer_debraplus> debraplus;
        debraplus.bz_test();
        testcase<uint64_t, reclaimer_debracap> debracap;
        debracap.bz_test();
        testcase<uint64_t, reclaimer_ebr_tree> ebrtree;
        ebrtree.bz_test();
        testcase<uint64_t, reclaimer_ebr_tree_q> ebrtreeq;
        ebrtreeq.bz_test();
        testcase<uint64_t, reclaimer_ebr_token> ebrtoken;
        ebrtoken.bz_test();
        testcase<uint64_t, reclaimer_hazardptr> hazard;
        hazard.bz_test();
    }
    return 0;
}