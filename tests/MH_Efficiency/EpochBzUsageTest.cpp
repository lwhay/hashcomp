//
// Created by iclab on 1/1/20.
//

#include <iostream>
#include <thread>
#include "tracer.h"
#include "bz_ebr.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "reclaimer_hazardptr.h"
#include "reclaimer_debra.h"
#include "reclaimer_debraplus.h"

const int NUM_THREADS = 1;

const int NUM_DUMMIES = (1 << 4);

const int NUM_ELEMENTS = (1 << 20);

CallbackReturn callbackReturnTrue(CallbackArg arg) {
    return true;
}

void bz_hazard_simple() {
    typedef reclaimer_hazardptr<uint64_t> Reclaimer;
    typedef allocator_new<uint64_t> Allocator;
    typedef pool_perthread_and_shared<uint64_t> Pool;

    Tracer tracer;
    tracer.startTime();
    Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
    Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
    Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
    std::cout << std::endl << "Hazard Reclaimer create: " << tracer.getRunTime() << std::endl;
    uint64_t *cache[NUM_DUMMIES];

    for (int k = 0; k < NUM_ELEMENTS; k++) {
        for (int i = 0; i < NUM_DUMMIES; i++) cache[i] = alloc->allocate(0);

        for (int i = 0; i < NUM_DUMMIES; i++) reclaimer->protect(0, cache[i], callbackReturnTrue, nullptr, false);

        for (int i = 0; i < NUM_DUMMIES; i++) reclaimer->unprotect(0, cache[i]);

        //for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->retire(0, cache[i]);

        for (int i = 0; i < NUM_DUMMIES; i++) alloc->deallocate(0, cache[i]);
    }

    std::cout << std::endl << "Hazard Reclaimer total: " << tracer.getRunTime() << std::endl;
}

void bz_debra_simple() {
    typedef reclaimer_debra <uint64_t> Reclaimer;
    typedef allocator_new<uint64_t> Allocator;
    typedef pool_perthread_and_shared<uint64_t> Pool;

    Tracer tracer;
    tracer.startTime();
    Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
    Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
    Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
    std::cout << std::endl << "Debra Reclaimer create: " << tracer.getRunTime() << std::endl;
    uint64_t *cache[NUM_DUMMIES];

    for (int k = 0; k < NUM_ELEMENTS; k++) {
        for (int i = 0; i < NUM_DUMMIES; i++) cache[i] = alloc->allocate(0);

        for (int i = 0; i < NUM_DUMMIES; i++) reclaimer->protect(0, cache[i], callbackReturnTrue, nullptr, false);

        for (int i = 0; i < NUM_DUMMIES; i++) reclaimer->unprotect(0, cache[i]);

        //for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->retire(0, cache[i]);

        for (int i = 0; i < NUM_DUMMIES; i++) alloc->deallocate(0, cache[i]);
    }

    std::cout << std::endl << "Debra Reclaimer total: " << tracer.getRunTime() << std::endl;
}

void bz_debraplus_simple() {
    typedef reclaimer_debraplus <uint64_t> Reclaimer;
    typedef allocator_new<uint64_t> Allocator;
    typedef pool_perthread_and_shared<uint64_t> Pool;

    Tracer tracer;
    tracer.startTime();
    Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
    Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
    Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
    std::cout << std::endl << "Debraplus Reclaimer create: " << tracer.getRunTime() << std::endl;
    uint64_t *cache[NUM_DUMMIES];

    for (int k = 0; k < NUM_ELEMENTS; k++) {
        for (int i = 0; i < NUM_DUMMIES; i++) cache[i] = alloc->allocate(0);

        for (int i = 0; i < NUM_DUMMIES; i++) reclaimer->protect(0, cache[i], callbackReturnTrue, nullptr, false);

        for (int i = 0; i < NUM_DUMMIES; i++) reclaimer->unprotect(0, cache[i]);

        //for (int i = 0; i < NUM_ELEMENTS; i++) reclaimer->retire(0, cache[i]);

        for (int i = 0; i < NUM_DUMMIES; i++) alloc->deallocate(0, cache[i]);
    }

    std::cout << std::endl << "Debraplus Reclaimer total: " << tracer.getRunTime() << std::endl;
}

void bz_hazard_original() {
    typedef reclaimer_hazardptr<uint64_t> Reclaimer;
    typedef allocator_new<uint64_t> Allocator;
    typedef pool_perthread_and_shared<uint64_t> Pool;

    Allocator *alloc = new Allocator(NUM_THREADS, nullptr);
    Pool *pool = new Pool(NUM_THREADS, alloc, nullptr);
    Reclaimer *reclaimer = new Reclaimer(NUM_THREADS, pool, nullptr);
    //delete reclaimer;
}

void singleTest() {
    Tracer tracer;
    tracer.startTime();
    ebr_t *deallocator = ebr_create();
    std::cout << std::endl << "EBR create: " << tracer.getRunTime() << std::endl;
    ebr_register(deallocator);
    std::cout << "EBR register: " << tracer.getRunTime() << std::endl;
    for (int i = 0; i < NUM_ELEMENTS; i++) {
        ebr_enter(deallocator);
    }
    std::cout << "EBR enter: " << tracer.getRunTime() << std::endl;
    ebr_exit(deallocator);
    std::cout << "EBR exit: " << tracer.getRunTime() << std::endl;
    ebr_destroy(deallocator);
    std::cout << "EBR destroy: " << tracer.getRunTime() << std::endl;
}

void multiTest() {
    ebr_t *deallocator = ebr_create();
    std::thread reader = std::thread([](ebr_t *deallocator) {
        Tracer tracer;
        tracer.startTime();
        ebr_register(deallocator);
        std::cout << std::endl << "EBR mregister: " << tracer.getRunTime() << std::endl;
        for (int i = 1; i < NUM_ELEMENTS; i++) {
            ebr_enter(deallocator);
            double d = .1;
            //for (int j = 0; j < (1 << 10); j++) d *= ((j % 2 == 0) ? j : ((double) 1 / j));
        }
        std::cout << "EBR menter: " << tracer.getRunTime() << std::endl;
        ebr_exit(deallocator);
        std::cout << "EBR mexit: " << tracer.getRunTime() << std::endl;
    }, deallocator);
    reader.join();
    ebr_destroy(deallocator);
}

int main(int argc, char **argv) {
    singleTest();
    multiTest();
    bz_hazard_original();
    for (int i = 0; i < 3; i++) {
        bz_hazard_simple();
        bz_debra_simple();
        bz_debraplus_simple();
    }
    return 0;
}