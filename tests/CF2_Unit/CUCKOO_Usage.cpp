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

#define BIG_CONSTANT(x) (x##LLU)

uint64_t MurmurHash64A(const void *key, int len, uint64_t seed) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^(len * m);

    const uint64_t *data = (const uint64_t *) key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *) data;

    switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

TEST(CuckooTests, HasherDetail) {
    char *tc = "wpoeiruwepoiruewir";
    std::hash<char *> hasher;
    std::hash<uint64_t> hashei;
    std::cout << hasher(tc) << " " << (uint64_t) tc << " " << hashei((uint64_t) tc) << std::endl;
    std::string ts(tc);
    std::hash<std::string> hashes;
    char *tt = new char[std::strlen(tc)];
    std::strcpy(tt, tc);
    std::hash<const char *> hasheu;
    std::string td(tc);
    std::cout << hashes(ts) << " " << hashes(td) << std::endl;
    std::cout << hasheu(ts.c_str()) << " " << hasher(tt) << " " << hasher(tc) << std::endl;
    std::cout << MurmurHash64A((void *) ts.c_str(), std::strlen(ts.c_str()), 23) << " "
              << MurmurHash64A((void *) tc, std::strlen(tc), 23) << " "
              << MurmurHash64A((void *) tt, std::strlen(tt), 23) << std::endl;
}

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
        char *skey = new char[11];
        std::memset(skey, 0, 11);
        std::strcpy(skey, "0123456789");
        char *sval = "1234567890";
        ASSERT_EQ(cmap.insert(skey, sval), true);
        ASSERT_EQ(cmap.size(), 2);
        delete skey;
        skey = nullptr;
        char *qkey = new char[11];
        char *qval;
        std::memset(qkey, 0, 11);
        std::strcpy(qkey, "0123456789");
        ASSERT_EQ(cmap.find(qkey, qval), true);
        ASSERT_STREQ(qkey, "0123456789");
        ASSERT_STREQ(qval, sval);
    }

    {
        uint64_t ik = 234234234151234323llu;
        cmap.insert_or_assign((char *) &ik, (char *) &ik);
    }
    uint64_t uk = 234234234151234323llu;
    ASSERT_EQ(*(uint64_t *) (char *) &uk, 234234234151234323llu);
    ASSERT_EQ(cmap.contains((char *) &uk), false);

    libcuckoo::cuckoohash_map<std::string, std::string> smap;
    {
        uint64_t ik = 234234234151234323llu;
        smap.insert((char *) &ik, (char *) &ik);
    }
    ASSERT_EQ(smap.contains((char *) &uk), false);
    {
        uint64_t ik = 234234234151234323llu;
        smap.insert(std::string((char *) &ik), std::string((char *) &ik));
        ASSERT_EQ(smap.contains(std::string((char *) &uk)), false);
        ASSERT_EQ(smap.size(), 2);
    }
    uint64_t ik = 234234234151234323llu;
    std::string value;
    ASSERT_EQ(smap.find(std::string((char *) &ik), value), false);
    ASSERT_EQ(smap.contains(std::string((char *) &uk)), false);

    {
        char *skey = new char[11];
        std::memset(skey, 0, 11);
        std::strcpy(skey, "0123456789");
        char *sval = "1234567890";
        ASSERT_EQ(smap.insert(skey, sval), true);
        ASSERT_EQ(smap.size(), 3);
        delete skey;
        skey = nullptr;
        char *qkey = new char[11];
        std::memset(qkey, 0, 11);
        std::strcpy(qkey, "0123456789");
        ASSERT_EQ(smap.find(qkey, value), true);
        ASSERT_STREQ(qkey, "0123456789");
        ASSERT_STREQ(value.c_str(), sval);
    }
    {
        char *skey = new char[11];
        std::memset(skey, 0, 11);
        std::strcpy(skey, "1234567890");
        char *sval = "1234567890";
        ASSERT_EQ(smap.insert(std::string(skey), std::string(sval)), true);
        ASSERT_EQ(smap.size(), 4);
        delete skey;
        skey = nullptr;
        char *qkey = new char[11];
        std::memset(qkey, 0, 11);
        std::strcpy(qkey, "1234567890");
        ASSERT_EQ(smap.find(std::string(qkey), value), true);
        ASSERT_STREQ(qkey, "1234567890");
        ASSERT_STREQ(value.c_str(), sval);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}