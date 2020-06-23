//
// Created by iclab on 6/23/20.
//

#include <iostream>
#include <cstring>
#include <numa.h>
#include <thread>
#include <tracer.h>

size_t operations = (1ull << 27);
size_t **loads;

void pin_to_core(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void initialize(int threads) {
    loads = new size_t *[threads];
    for (int i = 0; i < threads; i++) {
        pin_to_core(i);
        loads[i] = new size_t[operations];
        std::memset(loads[i], 0xf, operations * sizeof(size_t));
    }
}

void copier(int tid, size_t *from, size_t *to) {
    pin_to_core(tid);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < operations; i++)
        for (int r = 0; r < operations / 128; r++) {
            size_t idx = r * 128 + i;
            to[idx] = from[idx];
        }
    std::cout << "Tpt: " << (double) operations * sizeof(size_t) * 1e6 / tracer.getRunTime() / (1024.0 * 1024 * 1024)
              << " GB/s" << std::endl;
}

void serialTest(int threads) {
    for (int i = 0; i < threads; i++) {
        std::thread worker = std::thread(copier, i, loads[i], loads[0]);
        worker.join();
    }
}

void deinitialize(int threads) {
    for (int i = 0; i < threads; i++) {
        delete[] loads[i];
    }
    delete[] loads;
}

int main(int argc, char **argv) {
    int num_cpus = numa_num_task_cpus();
    numa_set_localalloc();
    std::cout << "Threads: " << num_cpus << std::endl;
    initialize(num_cpus);
    for (int r = 0; r < 6; r++) {
        serialTest(num_cpus);
        std::cout << "------------------------------------------------------" << std::endl;
    }
    deinitialize(num_cpus);
    return 0;
}