//
// Created by Michael on 11/24/19.
//

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>
#include "tracer.h"

size_t align_width = (1 << 4);

size_t thrd_number = (1 << 2);

size_t total_count = (1 << 20);

constexpr size_t default_align = (1 << 6);

void casworker(std::atomic<uint64_t> *pcon, size_t tid) {
    for (size_t i = 0; i < total_count / thrd_number; i++) {
        uint64_t d = pcon->load();
        while (!pcon->compare_exchange_strong(d, i));
    }
}

void addworker(std::atomic<uint64_t> *pcon, size_t tid) {
    for (size_t i = 0; i < total_count / thrd_number; i++) {
        while (!pcon->fetch_add(1));
    }
}

void loadworker(std::atomic<uint64_t> *pcon, size_t tid) {
    for (size_t i = 0; i < total_count / thrd_number; i++) {
        pcon->load();
    }
}

void storeworker(std::atomic<uint64_t> *pcon, size_t tid) {
    for (size_t i = 0; i < total_count / thrd_number; i++) {
        pcon->store(i);
    }
}

struct align_controller {
    alignas(default_align) std::atomic<uint64_t> controller;
};

void run(void (*func)(std::atomic<uint64_t> *, size_t), char *name) {
    std::cout << name << std::endl;
    std::vector<std::thread> threads;
    Tracer tracer;
    alignas(default_align) std::atomic<uint64_t> controller[thrd_number];
    tracer.startTime();
    for (size_t t = 0; t < thrd_number; t++) threads.push_back(std::thread(func, controller + t, t));
    for (size_t t = 0; t < thrd_number; t++) threads[t].join();
    std::cout << "\taligns: " << (double) total_count / tracer.getRunTime() << std::endl;
    threads.clear();

    align_controller aontroller[thrd_number];
    tracer.startTime();
    for (size_t t = 0; t < thrd_number; t++) threads.push_back(std::thread(func, &aontroller[t].controller, t));
    for (size_t t = 0; t < thrd_number; t++) threads[t].join();
    std::cout << "\talignas: " << (double) total_count / tracer.getRunTime() << std::endl;
    threads.clear();

    std::atomic<uint64_t> *sontroller = new std::atomic<uint64_t>[align_width * thrd_number];
    tracer.getRunTime();
    for (size_t t = 0; t < thrd_number; t++) threads.push_back(std::thread(func, sontroller + t * align_width, t));
    for (size_t t = 0; t < thrd_number; t++) threads[t].join();
    std::cout << "\tseparate: " << (double) total_count / tracer.getRunTime() << std::endl;
    delete[] sontroller;
}

int main(int argc, char **argv) {
    if (argc == 4) {
        align_width = std::atol(argv[1]);
        thrd_number = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
    }
    run(casworker, "cas");
    run(addworker, "add");
    run(storeworker, "store");
    run(loadworker, "load");
    return 0;
}