//
// Created by Michael on 4/1/20.
//

#include <vector>
#include <thread>
#include "trick_concurrent_hash_map.h"

constexpr uint64_t thread_number = 1;
constexpr uint64_t total_count = 1llu << 20;

void SimpleTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>> maptype;
    maptype map(1llu << 27, 10, 64);
    for (uint64_t i = 0; i < 1llu << 20; i++) map.Insert(i, i);
    uint64_t v;
    for (uint64_t i = 0; i < 1llu << 20; i++) map.Find(i, v);
}

void MultiWriteTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>> maptype;
    maptype map(1llu << 27, 10, 64);

    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            for (uint64_t i = tid; i < total_count; i += thread_number) {
                map.Insert(i, i);
                if (tid == 0 && i % 100000 == 0) std::cout << i << std::endl;
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
}

void MultiReadTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>> maptype;
    maptype map(1llu << 27, 10, 64);
    for (uint64_t i = 0; i < total_count; i++) map.Insert(i, i);
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            uint64_t v;
            for (uint64_t i = tid; i < total_count; i += thread_number) {
                map.Find(i, v);
                //assert(i == v);
                if (tid == 0 && i % 100000 == 0) {
                    if (i != v) std::cout << "\t" << i << ":" << v << std::endl;
                    std::cout << i << std::endl;
                }
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
}

int main(int argc, char **argv) {
    //SimpleTest();
    //MultiWriteTest();
    MultiReadTest();
    return 0;
}