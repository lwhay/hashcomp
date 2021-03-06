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
#ifndef FOLLY_DEBUG
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
#endif
}

/*TEST(FollyTest, AtomicUnorderedMapOperation) {
    folly::AtomicUnorderedInsertMap<uint64_t, uint64_t> fmap(128);
    fmap.findOrConstruct(1, 1);
    ASSERT_EQ(fmap.find(1)->second, 1);
    fmap.emplace(1, 2);
    ASSERT_EQ(fmap.find(1)->second, 1);
}*/

TEST(FollyTest, StringTest1) {
#ifdef FOLLY_DEBUG
    folly::ConcurrentHashMap<char *, char *> cmap;
#else
    folly::ConcurrentHashMapSIMD<char *, char *> cmap(128);
#endif
    char *key = "123456789";
    char *val = "123456789";
    cmap.insert_or_assign(key, val);
    char *ksh = "123456789";
    ksh = "012345678";
    ASSERT_STREQ(cmap.find(key)->second, "123456789");

    {
        char *skey = new char[11];
        std::memset(skey, 0, 11);
        std::strcpy(skey, "0123456789");
        char *sval = "1234567890";
        ASSERT_EQ(cmap.insert(skey, sval).second, true);
        ASSERT_EQ(cmap.size(), 2);
        delete skey;
        skey = nullptr;
        char *qkey = new char[11];
        char *qval;
        std::memset(qkey, 0, 11);
        std::strcpy(qkey, "0123456789");
        ASSERT_STREQ(qval = cmap.find(qkey)->second, sval);
        ASSERT_STREQ(qkey, "0123456789");
        ASSERT_STREQ(qval, sval);
    }
}

TEST(FollyTest, StringTest2) {
#ifdef FOLLY_DEBUG
    folly::ConcurrentHashMap<char *, char *> cmap;
#else
    folly::ConcurrentHashMapSIMD<char *, char *> cmap(128);
#endif
    {
        uint64_t ik = 234234234151234323llu;
        cmap.insert_or_assign((char *) &ik, (char *) &ik);
    }
    uint64_t uk = 234234234151234323llu;
    ASSERT_EQ(*(uint64_t *) (char *) &uk, 234234234151234323llu);
    ASSERT_EQ(cmap.find((char *) &uk), cmap.end());
}

TEST(FollyTest, StringTest3) {
#ifdef FOLLY_DEBUG
    folly::ConcurrentHashMap<std::string, std::string> smap(128);
#else
    folly::ConcurrentHashMapSIMD<std::string, std::string> smap(128);
#endif
    uint64_t uk = 234234234151234323llu;
    {
        uint64_t ik = 234234234151234323llu;
        smap.insert((char *) &ik, (char *) &ik);
    }
    /*uint64_t tk = 234234234151234323llu;
    std::string value = smap.find((char *) &uk)->second;
    ASSERT_EQ(smap.find((char *) &tk)->second.compare((char *) tk), 0);*/
#ifndef FOLLY_DEBUG
    ASSERT_STREQ(smap.find((char *) &uk)->second.c_str(), (char *) &uk);
#endif
    {
        uint64_t ik = 234234234151234323llu;
        smap.insert(std::string((char *) &ik), std::string((char *) &ik));
    }
    ASSERT_EQ(smap.find(std::string((char *) &uk))->second.compare((char *) &uk), 0);
    uint64_t ik = 234234234151234323llu;
    ASSERT_EQ(smap.find(std::string((char *) &ik)), smap.end());
}

TEST(FollyTest, StringTest4) {
#ifdef FOLLY_DEBUG
    folly::ConcurrentHashMap<std::string, std::string> smap(128);
#else
    folly::ConcurrentHashMapSIMD<std::string, std::string> smap(128);
#endif
    {
        char *skey = new char[11];
        std::memset(skey, 0, 11);
        std::strcpy(skey, "0123456789");
        char *sval = "1234567890";
        ASSERT_EQ(smap.insert(skey, sval).second, true);
#ifdef FOLLY_DEBUG
        ASSERT_EQ(smap.size(), 3);
#else
        ASSERT_EQ(smap.size(), 2);
#endif
        delete skey;
        skey = nullptr;
        char *qkey = new char[11];
        std::memset(qkey, 0, 11);
        std::strcpy(qkey, "0123456789");
        ASSERT_EQ(smap.find(qkey)->second.compare(sval), 0);
        ASSERT_STREQ(qkey, "0123456789");
    }
    {
        char *skey = new char[11];
        std::memset(skey, 0, 11);
        std::strcpy(skey, "1234567890");
        char *sval = "1234567890";
        ASSERT_EQ(smap.insert(std::string(skey), std::string(sval)).second, true);
#ifdef FOLLY_DEBUG
        ASSERT_EQ(smap.size(), 4);
#else
        ASSERT_EQ(smap.size(), 3);
#endif
        delete skey;
        skey = nullptr;
        char *qkey = new char[11];
        std::memset(qkey, 0, 11);
        std::strcpy(qkey, "1234567890");
        ASSERT_EQ(smap.find(std::string(qkey))->second.compare(sval), 0);
        ASSERT_STREQ(qkey, "1234567890");
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}