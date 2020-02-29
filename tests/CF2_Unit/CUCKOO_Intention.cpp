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

constexpr uint64_t esize = 10000000;
constexpr double eta = 0.5;
constexpr uint64_t dim = 2;
constexpr uint64_t hashseeds[dim] = {15126133284972304873llu, 2398702347823893llu};
constexpr uint64_t tsize = (uint64_t) ((1 + eta) * esize);
constexpr uint64_t tries = 64;
constexpr std::hash<uint64_t> hasher;

bool tableinsert(uint64_t *key, uint64_t *hashtable, uint64_t seed) {
    uint64_t hash = MurmurHash64A(key, std::strlen((char *) key), seed) % tsize;
    //std::cout << hash << " ";
    uint64_t tmp = *key;
    *key = hashtable[hash];
    hashtable[hash] = tmp;
    return (*key == 0);
}

TEST(CUCKOOIntention, NaiveTest) {
    uint64_t *keys = new uint64_t[esize];
    uint64_t *twintables[dim];
    twintables[0] = new uint64_t[tsize];
    twintables[1] = new uint64_t[tsize];
    std::memset(keys, 0, esize * sizeof(uint64_t));
    std::memset(twintables[0], 0, tsize * sizeof(uint64_t));
    std::memset(twintables[1], 0, tsize * sizeof(uint64_t));
    uint64_t cache[dim];
    std::memset(cache, 0, dim * sizeof(uint64_t));
    size_t totaltries = 0;
    for (int i = 0; i < esize; i++) {
        keys[i] = i;
        uint64_t curkey = keys[i];
        uint64_t curtry = 0;
        cache[0] = keys[i];
        do {
            curtry++;
        } while (curtry < tries &&
                 !tableinsert(cache, twintables[(curtry - 1) % dim], hashseeds[(curtry - 1) % dim]));
        //std::cout << std::endl;
#if 0
        std::cout << i << " " << curkey << " " << curtry << " "
                  << MurmurHash64A(&keys[i], std::strlen((char *) &keys[i]), hashseeds[0]) % tsize << std::endl;
#endif
        totaltries += curtry;
    }
    delete[] twintables[0];
    delete[] twintables[1];
    delete[] keys;
    std::cout << esize << " " << tsize << " " << totaltries << " " << esize * tries << std::endl;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}