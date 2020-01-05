//
// Created by iclab on 1/1/20.
//

#include <iostream>
#include <thread>
#include "tracer.h"
#include "bz_ebr.h"
#include "reclaimer_hazardptr.h"

const int NUM_THREADS = 1;

void bz_hazard_simple() {
    reclaimer_hazardptr<uint64_t> reclaimer(NUM_THREADS, nullptr, nullptr);
}

void singleTest() {
    Tracer tracer;
    tracer.startTime();
    ebr_t *deallocator = ebr_create();
    std::cout << std::endl << "EBR create: " << tracer.getRunTime() << std::endl;
    ebr_register(deallocator);
    std::cout << "EBR register: " << tracer.getRunTime() << std::endl;
    for (int i = 0; i < (1 << 20); i++) {
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
        for (int i = 1; i < (1 << 20); i++) {
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
    bz_hazard_simple();
    return 0;
}