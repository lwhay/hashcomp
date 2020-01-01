//
// Created by iclab on 1/1/20.
//

#include <thread>
#include "bz_ebr.h"

void singleTest() {
    ebr_t *deallocator = ebr_create();
    for (int i = 0; i < (1 << 20); i++) {
        ebr_register(deallocator);
        ebr_enter(deallocator);
        ebr_exit(deallocator);
    }
    ebr_destroy(deallocator);
}

void multiTest() {
    ebr_t *deallocator = ebr_create();
    std::thread reader = std::thread([](ebr_t *deallocator) {
        for (int i = 1; i < (1 << 20); i++) {
            ebr_register(deallocator);
            ebr_enter(deallocator);
            double d = .1;
            //for (int j = 0; j < (1 << 10); j++) d *= ((j % 2 == 0) ? j : ((double) 1 / j));
            ebr_exit(deallocator);
        }
    }, deallocator);
    reader.join();
    ebr_destroy(deallocator);
}

int main(int argc, char **argv) {
    //singleTest();
    multiTest();
    return 0;
}