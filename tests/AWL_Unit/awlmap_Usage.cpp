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
#include "trick_concurrent_hash_map.h"

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
        map.Insert(i, i);
    }
    int v;

    for (int i = 0; i < 100; i++) {
        map.Find(i, v);
        ASSERT_EQ(v, i);
    }

    for (int i = 0; i < 100; i++) {
        map.Delete(i);
    }

    for (int i = 0; i < 100; i++) {
        bool ret = map.Find(i, v);
        ASSERT_EQ(ret, false);
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

TEST(awlmapTests, SerialMap) {
    const int paral = 2;
    using maptype = trick::ConcurrentHashMap<int, int, std::hash<int>, std::equal_to<>>;
    maptype *map = new maptype(1ull << 20, 20, 4);
    std::thread workers[paral];
    for (int k = 0; k < paral; k++) {
        workers[k] = std::thread([](maptype *map) {
            for (int i = 0; i < 1000000; i++) map->Insert(i, i);
        }, map);
    }
    for (int k = 0; k < paral; k++) workers[k].join();
    delete map;
}

TEST(awlmapTests, ParallelMap) {
    const int degree = 1, paral = 4;
    using maptype = trick::ConcurrentHashMap<int, int, std::hash<int>, std::equal_to<>>;
    maptype *maps[degree];
    std::thread workers[degree * paral];
    for (size_t t = 0; t < degree; t++) {
        maps[t] = new maptype(1 << 24, 20, 4);
        for (int k = 0; k < paral; k++) {
            workers[t * degree + k] = std::thread([](maptype *map) {
                for (int i = 0; i < 1000000; i++) map->Insert(i, i);
            }, std::ref(maps[t]));
        }
    }
    for (size_t t = 0; t < degree * paral; t++) workers[t].join();
    for (size_t t = 0; t < degree; t++) delete maps[t];
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}