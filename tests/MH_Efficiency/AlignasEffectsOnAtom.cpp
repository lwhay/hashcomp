//
// Created by Michael on 11/24/19.
//

#include <atomic>
#include <iostream>
#include <thread>
#include <vector>
#include "tracer.h"

#define reverse 0

size_t align_width = (1 << 4);

size_t thrd_number = (1 << 2);

size_t total_count = (1 << 20);

constexpr size_t default_align = (1 << 6);

template<typename T>
void casworker(T *pcon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        uint64_t d;
        if (loc) do { d = pcon->load(); } while (!pcon->compare_exchange_strong(d, i));
        else
            do { d = (pcon + i % thrd_number * aln)->load(); }
            while (!(pcon + i % thrd_number * aln)->compare_exchange_strong(d, i));
    }
}

template<typename T>
void addworker(T *pcon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        if (loc) while (!pcon->fetch_add(1));
        else while (!(pcon + i % thrd_number * aln)->fetch_add(1));
    }
}

template<typename T>
void loadworker(T *pcon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        if (loc) pcon->load();
        else (pcon + i % thrd_number * aln)->load();
    }
}

template<typename T>
void storeworker(T *pcon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        if (loc) pcon->store(i);
        else (pcon + i % thrd_number * aln)->store(i);
    }
}

template<typename T>
void exchworker(T *pcon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        if (loc) pcon->exchange(i);
        else (pcon + i % thrd_number * aln)->exchange(i);
    }
}

template<typename T>
void readworker(T *acon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        if (loc) uint64_t v = *acon;
        else uint64_t v = *(acon + i % thrd_number * aln);
    }
}

template<typename T>
void writeworker(T *acon, size_t tid, bool loc = true, uint64_t aln = 1) {
#if reverse
    for (size_t i = total_count / thrd_number - 1; i != 0; i--) {
#else
    for (size_t i = 0; i < total_count / thrd_number; i++) {
#endif
        if (loc) *acon = i;
        else *(acon + i % thrd_number * aln) = i;
    }
}

template<typename T>
struct align_controller {
    alignas(default_align) T controller;
};

template<typename T>
void run(void (*func)(T *, size_t, bool, uint64_t), char *name, bool loc = true) {
    std::cout << name << std::endl;
    std::vector<std::thread> threads;
    Tracer tracer;

    alignas(default_align) T controller[thrd_number];
    tracer.startTime();
    for (size_t t = 0; t < thrd_number; t++)
        if (loc) threads.push_back(std::thread(func, controller + t, t, loc, 1));
        else threads.push_back(std::thread(func, controller + 0, t, loc, 1));
    for (size_t t = 0; t < thrd_number; t++) threads[t].join();
    std::cout << ((loc) ? "\taligns: " : "\talignd: ") << (double) total_count / tracer.getRunTime() << std::endl;
    threads.clear();

    if (loc) {
        align_controller<T> aontroller[thrd_number];
        tracer.startTime();
        for (size_t t = 0; t < thrd_number; t++)
            threads.push_back(std::thread(func, &aontroller[t].controller, t, loc, 1));
        for (size_t t = 0; t < thrd_number; t++) threads[t].join();
        std::cout << "\talignas: " << (double) total_count / tracer.getRunTime() << std::endl;
        threads.clear();
    }

    T *sontroller = new T[align_width * thrd_number];
    tracer.getRunTime();
    for (size_t t = 0; t < thrd_number; t++)
        if (loc) threads.push_back(std::thread(func, sontroller + t * align_width, t, loc, align_width));
        else threads.push_back(std::thread(func, sontroller, t, loc, align_width));
    for (size_t t = 0; t < thrd_number; t++) threads[t].join();
    std::cout << ((loc) ? "\tseparate: " : "\tdisperse: ") << (double) total_count / tracer.getRunTime() << std::endl;
    delete[] sontroller;
}

int main(int argc, char **argv) {
    if (argc == 4) {
        align_width = std::atol(argv[1]);
        thrd_number = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
    }
    std::cout << "align: " << align_width << " thrd: " << thrd_number << " total: " << total_count << std::endl;
    run<std::atomic<uint64_t>>(casworker, "cas", true);
    run<std::atomic<uint64_t>>(addworker, "add", true);
    run<std::atomic<uint64_t>>(exchworker, "exchange", true);
    run<std::atomic<uint64_t>>(storeworker, "store", true);
    run<std::atomic<uint64_t>>(loadworker, "load", true);
    run<std::atomic<uint64_t>>(writeworker, "sstore", true);
    run<std::atomic<uint64_t>>(readworker, "sload", true);
    run<uint64_t>(readworker, "read", true);
    run<uint64_t>(writeworker, "write", true);

    run<std::atomic<uint64_t>>(casworker, "wcas", false);
    run<std::atomic<uint64_t>>(addworker, "wadd", false);
    run<std::atomic<uint64_t>>(exchworker, "wexchange", false);
    run<std::atomic<uint64_t>>(storeworker, "wstore", false);
    run<std::atomic<uint64_t>>(loadworker, "wload", false);
    run<std::atomic<uint64_t>>(writeworker, "wsstore", false);
    run<std::atomic<uint64_t>>(readworker, "wsload", false);
    run<uint64_t>(readworker, "wread", false);
    run<uint64_t>(writeworker, "wwrite", false);
    return 0;
}