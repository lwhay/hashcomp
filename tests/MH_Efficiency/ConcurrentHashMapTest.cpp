//
// Created by Michael on 4/1/20.
//

#include <vector>
#include <thread>
#include "hash_hazard.h"
#include "tracer.h"
#include "trick_concurrent_hash_map.h"

uint64_t thread_number = 4;
uint64_t total_count = 1llu << 20;

void SimpleTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, batch_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
    maptype map(total_count, 10, thread_number);
    uint64_t v;
    Tracer tracer;
    tracer.startTime();
    for (uint64_t i = 0; i < total_count; i++) {
        map.Insert(i, i);
    }
    std::cout << "Insert: " << tracer.getRunTime() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) {
        bool ret = map.Find(i, v);
        if (ret) assert(i == v);
    }
    std::cout << "Find: " << tracer.getRunTime() << std::endl;
}

void MultiWriteTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>> maptype;
    maptype map(1llu << 27, 10, thread_number);
    Tracer tracer;
    tracer.startTime();
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            for (uint64_t i = tid; i < total_count; i += thread_number) {
                map.Insert(i, i);
                if (tid == 0 && i % 1000000 == 0) std::cout << i << std::endl;
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "MultiInsert: " << tracer.getRunTime() << std::endl;
}

void MultiReadTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>> maptype;
    maptype map(1llu << 27, 10, thread_number);
    Tracer tracer;
    tracer.startTime();
    for (uint64_t i = 0; i < total_count; i++) map.Insert(i, i);
    std::cout << "MultiInsert: " << tracer.getRunTime() << std::endl;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            uint64_t v;
            for (uint64_t i = tid; i < total_count; i += thread_number) {
                map.Find(i, v);
                assert(i == v);
                if (tid == 0 && i % 1000000 == 0) {
                    if (i != v) std::cout << "\t" << i << ":" << v << std::endl;
                    std::cout << i << std::endl;
                }
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "MultiFind: " << tracer.getRunTime() << std::endl;
}

int main(int argc, char **argv) {
    if (argc > 2) {
        thread_number = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
    }
    std::cout << "-------------------------" << std::endl;
    SimpleTest();
    std::cout << "-------------------------" << std::endl;
    MultiWriteTest();
    std::cout << "-------------------------" << std::endl;
    MultiReadTest();
    std::cout << "-------------------------" << std::endl;
    return 0;
}