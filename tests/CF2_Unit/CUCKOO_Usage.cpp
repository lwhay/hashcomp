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

TEST(CuckooTests, StringTest) {
    libcuckoo::cuckoohash_map<char *, char *, std::hash<char *>, std::equal_to<char *>,
            std::allocator<std::pair<const char *, char *>>, 8> cmap(128);
    char *key = "123456789";
    char *val = "123456789";
    cmap.insert_or_assign(key, val);
    char *ksh = "123456789";
    ksh = "012345678";
    ASSERT_EQ(cmap.contains(ksh), 0);
    ASSERT_STREQ(cmap.find(key), "123456789");
    /*ASSERT_STREQ(cmap.find(ksh), "123456789");*/

    {
        uint64_t ik = 234234234151234323llu;
        cmap.insert_or_assign((char *) &ik, (char *) &ik);
    }
    uint64_t uk = 234234234151234323llu;
    ASSERT_EQ(*(uint64_t *) (char *) &uk, 234234234151234323llu);
    ASSERT_EQ(cmap.contains((char *) &uk), 0);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}