//
// Created by iclab on 11/28/19.
//

#include <atomic>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include "gtest/gtest.h"
#include "concurrent_hash_map.h"

TEST(awlmapTests, UnionTest) {
    struct dummy {
        union {
            uint64_t body;
            std::atomic<uint64_t> ctrl;
        };
    };
    dummy dum{0};
    ASSERT_EQ(dum.body, 0);
    ASSERT_EQ(dum.ctrl.load(), 0);
    dum.ctrl.store(1);
    ASSERT_EQ(dum.body, 1);
    ASSERT_EQ(dum.ctrl.load(), 1);
    dum.body = 0;
    ASSERT_EQ(dum.body, 0);
    ASSERT_EQ(dum.ctrl.load(), 0);
}

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