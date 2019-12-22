//
// Created by iclab on 12/22/19.
//

#include <iostream>
#include <mutex>
#include <thread>
#include <numa.h>
#include "gtest/gtest.h"

TEST(NumaTest, UnionStructTest) {
    constexpr unsigned num_threads = 16;
    // A mutex ensures orderly access to std::cout from multiple threads.
    std::mutex iomutex;
    std::vector<std::thread> threads(num_threads);
    unsigned num_cpus = std::thread::hardware_concurrency();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (size_t i = 1; i < num_cpus; i++) CPU_SET(i, &cpuset);
    std::cout << num_cpus << "\t" << *(cpuset.__bits) << std::endl;
    for (unsigned i = 0; i < num_threads; ++i) {
        threads[i] = std::thread([&iomutex, i] {
            unsigned tick = 0;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            while (1) {
                {
                    // Use a lexical scope and lock_guard to safely lock the mutex only
                    // for the duration of std::cout usage.
                    std::lock_guard<std::mutex> iolock(iomutex);
                    std::cout << tick++ << "\tThread #" << i << ": on CPU " << sched_getcpu() << "\n";
                }

                // Simulate important work done by the tread by sleeping for a bit...
                std::this_thread::sleep_for(std::chrono::milliseconds(900));
            }
        });
        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
        // only CPU i as set.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i % 8, &cpuset);
        int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }

    for (auto &t : threads) {
        t.join();
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}