//
// Created by iclab on 11/28/19.
//

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include "gtest/gtest.h"
#include "concurrent_hash_map.h"

TEST(awlmapTests, UnitOperations) {
    ConcurrentHashMap<int, int, std::hash<int>, std::equal_to<>> map(16, 3, 1);
    for (int i = 0; i < 100; i++) {
        map.Insert(1, 1);
    }
    int v;

    for (int i = 0; i < 100; i++) {
        map.Find(1, v);
        ASSERT_EQ(v, 1);
    }
}

TEST(awlmapTests, ParallelOperations) {
    using maptype = ConcurrentHashMap<int, int, std::hash<int>, std::equal_to<>>;
    maptype map(16, 3, 4);
    for (int i = 0; i < 100; i++) {
        map.Insert(1, 1);
    }

    std::thread workers[4];
    for (size_t t = 0; t < 4; t++) {
        workers[t] = std::thread([](maptype &map) {
            int v;
            for (int i = 0; i < 100; i++) {
                map.Find(1, v);
                ASSERT_EQ(v, 1);
            }
        }, std::ref(map));
    }
    for (size_t t = 0; t < 4; t++) workers[t].join();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}