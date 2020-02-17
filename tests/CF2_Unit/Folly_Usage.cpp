//
// Created by Michael on 2019-10-15.
//

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include <stdlib.h>
#include "gtest/gtest.h"
#include "folly/AtomicHashMap.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "folly/AtomicUnorderedMap.h"

TEST(FollyTest, AtomicHashMapOperation) {
    folly::AtomicHashMap<uint64_t, uint64_t> fmap(128);
    fmap.insert(1, 1);
    ASSERT_EQ(fmap.find(1)->second, 1);
    fmap.insert(1, 2);
    ASSERT_EQ(fmap.find(1)->second, 1);
    fmap.erase(1);
    auto ret = fmap.find(1);
    //ASSERT_ANY_THROW(fmap.find(1)->second);
}

TEST(FollyTest, AtomicHashMapMultiThreadTest) {
    folly::AtomicHashMap<uint64_t, uint64_t> fmap(128);
    fmap.insert(1, 0);
    std::thread feeder = std::thread([](folly::AtomicHashMap<uint64_t, uint64_t> &fmap) {
        for (int i = 1; i < 10000000; i++) {
            /*auto ret = fmap.insert(1, 1);
            ASSERT_EQ(ret.first->second, i - 1);
            __sync_lock_test_and_set(&ret.first->second, i);*/
            auto ret = fmap.find(1);
            __sync_lock_test_and_set(&ret->second, i / i);
            //__sync_lock_release(&ret->second);
        }
    }, std::ref(fmap));
    std::thread reader = std::thread([](folly::AtomicHashMap<uint64_t, uint64_t> &fmap) {
        for (int i = 0; i < 10000000; i++) {
            //__sync_synchronize();
            auto ret = fmap.find(1);
            ASSERT_EQ(ret->second, 1);
            //ASSERT_TRUE(ret->second > 0 && ret->second < 10000000);
        }
    }, std::ref(fmap));
    reader.join();
    feeder.join();
}

TEST(FollyTest, ConcurrentHashMapMultiThreadTest) {
    folly::ConcurrentHashMap<uint64_t, uint64_t> fmap(128);
    fmap.insert(1, 0);
    std::thread feeder = std::thread([](folly::ConcurrentHashMap<uint64_t, uint64_t> &fmap) {
        for (int i = 1; i < 10000000; i++) {
            auto ret = fmap.assign(1, i);
        }
    }, std::ref(fmap));
    std::thread reader = std::thread([](folly::ConcurrentHashMap<uint64_t, uint64_t> &fmap) {
        for (int i = 0; i < 10000000; i++) {
            auto ret = fmap.find(1);
            ASSERT_TRUE(ret->second > 0 && ret->second < 10000000);
        }
    }, std::ref(fmap));
    reader.join();
    feeder.join();
}

TEST(FollyTest, ConcurrentHashMapSIMDMultiThreadTest) {
    folly::ConcurrentHashMapSIMD<uint64_t, uint64_t> fmap(128);
    fmap.insert(1, 0);
    std::thread feeder = std::thread([](folly::ConcurrentHashMapSIMD<uint64_t, uint64_t> &fmap) {
        for (int i = 1; i < 10000000; i++) {
            auto ret = fmap.assign(1, i);
        }
    }, std::ref(fmap));
    std::thread reader = std::thread([](folly::ConcurrentHashMapSIMD<uint64_t, uint64_t> &fmap) {
        for (int i = 0; i < 10000000; i++) {
            auto ret = fmap.find(1);
            ASSERT_TRUE(ret->second > 0 && ret->second < 10000000);
        }
    }, std::ref(fmap));
    reader.join();
    feeder.join();
}

/*TEST(FollyTest, AtomicUnorderedMapOperation) {
    folly::AtomicUnorderedInsertMap<uint64_t, uint64_t> fmap(128);
    fmap.findOrConstruct(1, 1);
    ASSERT_EQ(fmap.find(1)->second, 1);
    fmap.emplace(1, 2);
    ASSERT_EQ(fmap.find(1)->second, 1);
}*/

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}