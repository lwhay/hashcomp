//
// Created by Michael on 11/24/19.
//

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>
#include "tracer.h"

size_t align_width = (1 << 4);

size_t thread_number = (1 << 2);

size_t total_count = (1 << 20);

void dummyCAS() {
    alignas(64) std::atomic<uint64_t> controller[thread_number];
    std::vector<std::thread> threads;
    Tracer tracer;
    tracer.startTime();
    for (size_t t = 0; t < thread_number; t++)
        threads.push_back(std::thread([](std::atomic<uint64_t> *pcon, size_t tid) {
            for (size_t i = 0; i < total_count / thread_number; i++) {
                uint64_t d = pcon->load();
                while (!pcon->compare_exchange_strong(d, i));
            }
        }, controller + t, t));
    for (size_t t = 0; t < thread_number; t++) threads[t].join();
    std::cout << "alignsCAS: " << (double) total_count / tracer.getRunTime() << std::endl;
}

int main(int argc, char **argv) {
    dummyCAS();
    return 0;
}