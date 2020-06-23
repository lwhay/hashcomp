//
// Created by iclab on 6/23/20.
//

#include <iostream>
#include <cstring>
#include <numa.h>
#include <thread>
#include <tracer.h>

size_t operations = (1ull << 25);
size_t **loads;
size_t **target;

void pin_to_core(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void initialize(int threads) {
    pin_to_core(0);
    target = new size_t *[threads];
    for (int i = 0; i < threads; i++) {
        target[i] = new size_t[operations];
        std::memset(target[i], 0xf, operations * sizeof(size_t));
    }
    loads = new size_t *[threads];
    for (int i = 0; i < threads; i++) {
        pin_to_core(i);
        loads[i] = new size_t[operations];
        std::memset(loads[i], 0xf, operations * sizeof(size_t));
    }
}

void copier(int tid, size_t *from, size_t *to, long *time) {
    pin_to_core(tid);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < 128; i++)
        for (int r = 0; r < operations / 128; r++) {
            size_t idx = (r * 128 + i) % operations;
            to[idx] = from[idx];
        }
    *time = tracer.getRunTime();
}

std::atomic<uint64_t> total_dummy{0};

void reader(int tid, size_t *from, long *time) {
    pin_to_core(tid);
    Tracer tracer;
    tracer.startTime();
    uint64_t dummy = 0;
    for (int i = 0; i < 128; i++)
        for (int r = 0; r < operations / 128; r++) {
            size_t idx = (r * 128 + i) % operations;
            dummy += from[idx];
        }
    *time = tracer.getRunTime();
    total_dummy.fetch_add(dummy);
}

void serialTest(int threads) {
    long runtime[threads];
    for (int i = 0; i < threads; i++) {
        std::thread worker = std::thread(copier, i, loads[i], loads[0], runtime + i);
        worker.join();
        std::cout << i << "->" << 0 << " WTpt: " << (double) operations * 64 * 1e6 / runtime[i] / (1024.0 * 1024 * 1024)
                  << " GB/s" << std::endl;
    }
    std::cout << "---------------------" << std::endl;
    for (int i = 0; i < threads; i++) {
        std::thread worker = std::thread(reader, i, loads[i], runtime + i);
        worker.join();
        std::cout << i << "->" << "  RTpt: " << (double) operations * 64 * 1e6 / runtime[i] / (1024.0 * 1024 * 1024)
                  << " GB/s" << " dummy: " << total_dummy << std::endl;
    }
}

void parallelTest(int threads) {
    long runtime[threads];
    std::vector<std::thread> workers;
    for (int i = 0; i < threads; i++) {
        workers.push_back(std::thread(copier, i, loads[i], target[i], runtime + i));
    }
    for (int i = 0; i < threads; i++) {
        workers[i].join();
        std::cout << i << "->" << 0 << " PWTpt: "
                  << (double) operations * 64 * 1e6 / runtime[i] / (1024.0 * 1024 * 1024) << " GB/s" << std::endl;
    }
    workers.clear();
    std::cout << "---------------------" << std::endl;
    for (int i = 0; i < threads; i++) {
        workers.push_back(std::thread(reader, i, loads[i], runtime + i));
    }
    for (int i = 0; i < threads; i++) {
        workers[i].join();
        std::cout << i << "->" << "  PRTpt: " << (double) operations * 64 * 1e6 / runtime[i] / (1024.0 * 1024 * 1024)
                  << " GB/s" << " dummy: " << total_dummy << std::endl;
    }
}

void deinitialize(int threads) {
    for (int i = 0; i < threads; i++) {
        delete[] loads[i];
        delete[] target[i];
    }
    delete[] loads;
    delete[] target;
}

int main(int argc, char **argv) {
    int num_cpus = numa_num_task_cpus();
    numa_set_localalloc();
    std::cout << "Threads: " << num_cpus << std::endl;
    initialize(num_cpus);
    for (int r = 0; r < 3; r++) {
        serialTest(num_cpus);
        std::cout << "------------------------------------------------------" << std::endl;
    }
    for (int r = 0; r < 3; r++) {
        parallelTest(num_cpus);
        std::cout << "------------------------------------------------------" << std::endl;
    }
    deinitialize(num_cpus);
    return 0;
}