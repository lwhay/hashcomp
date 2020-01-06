//
// Created by iclab on 1/6/20.
//

#include <iostream>
#include <thread>
#include "tracer.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "reclaimer_hazardptr.h"
#include "reclaimer_debra.h"
#include "reclaimer_debraplus.h"
#include "reclaimer_debracap.h"
#include "reclaimer_ebr_tree.h"
#include "reclaimer_ebr_tree_q.h"
#include "reclaimer_ebr_token.h"

const int NUM_THREADS = 1;

const int NUM_ROUNDS = (1 << 20); // For tb_hazard, number of rounds should be larger than that of elements.

const int NUM_ELEMENTS = (1 << 8/*19*/);

CallbackReturn callbackReturnTrue(CallbackArg arg) {
    return true;
}

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
    typedef reclaimer_hazardptr<uint64_t> Reclaimer;
    typedef allocator_new<uint64_t> Allocator;
    typedef pool_perthread_and_shared<uint64_t> Pool;

    Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
    Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
    Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
    //delete reclaimer;
}

int main(int argc, char **argv) {
    tb_hazard_original();
    for (int i = 0; i < 1; i++) {
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