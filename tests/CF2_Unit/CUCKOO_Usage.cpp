//
// Created by Michael on 2019-10-14.
//

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include "gtest/gtest.h"
#include "libcuckoo/cuckoohash_map.hh"

TEST(CuckooTests, UnitOperations) {
    libcuckoo::cuckoohash_map<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>,
            std::allocator<std::pair<const uint64_t, uint64_t>>, 8> cmap(128);
    auto neg = [](uint64_t &val) {};
    cmap.upsert(1, neg, 1);
    ASSERT_EQ(cmap.find(1), 1);
    cmap.update(1, 2);
    ASSERT_EQ(cmap.find(1), 2);
    cmap.erase(1);
    ASSERT_EQ(cmap.contains(1), false);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}