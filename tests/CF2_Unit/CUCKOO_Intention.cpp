//
// Created by Michael on 2/26/20.
//

#include <cstdint>
#include <cstring>
#include <deque>
#include <vector>
#include <functional>
#include <thread>
#include "tracer.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "gtest/gtest.h"

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

constexpr uint64_t hashseedA = 151261303;
constexpr uint64_t hashseedB = 6722461;
constexpr uint64_t esize = 32;
constexpr double eta = 0.5;
constexpr uint64_t tsize = (uint64_t) ((1 + eta) * esize);
constexpr uint64_t tries = 64;

bool tableinsert(uint64_t *key, uint64_t *lefttable, uint64_t *righttable) {
    return true;
}

TEST(CUCKOOIntention, NaiveTest) {
    uint64_t *keys = new uint64_t[esize];
    uint64_t *lefttable = new uint64_t[tsize];
    uint64_t *righttable = new uint64_t[tsize];
    std::memset(keys, 0, esize * sizeof(uint64_t));
    std::memset(lefttable, 0, tsize * sizeof(uint64_t));
    std::memset(righttable, 0, tsize * sizeof(uint64_t));
    size_t totaltries = 0;
    for (int i = 0; i < esize; i++) {
        uint64_t curkey = keys[i];
        uint64_t curtry = 0;
        do {
            totaltries++;
        } while (curtry < tries && !tableinsert(&curkey, lefttable, righttable));
    }
    delete[] lefttable;
    delete[] righttable;
    delete[] keys;
    std::cout << totaltries << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}