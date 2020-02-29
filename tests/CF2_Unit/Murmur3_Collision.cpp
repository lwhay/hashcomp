//
// Created by iclab on 2/29/20.
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

constexpr uint64_t key_range = 1000000000;
constexpr uint64_t key_count = 100000000;
constexpr uint64_t root_size = 100000000;

typedef folly::ConcurrentHashMap<uint64_t, uint64_t> fmap;

void printstat(fmap &store, uint64_t root_size) {
    std::vector<std::pair<uint64_t, uint64_t >> cmparray;
    for (uint64_t i = 0; i < root_size; i++) {
        if (store.find(i) != store.end()) cmparray.push_back(std::make_pair(i, store.find(i)->second));
    }
    std::cout << "root: " << root_size << " volume: " << cmparray.size() << std::endl;
    std::sort(cmparray.begin(), cmparray.end(),
              [](std::pair<uint64_t, uint64_t> &a, std::pair<uint64_t, uint64_t> &b) { return b.second < a.second; });
    for (int i = 0; i < cmparray.size(); i++) {
        if (i < 1024 || i > (cmparray.size() / 32 * 32) - 1024) {
            std::cout << cmparray[i].first << ":" << cmparray[i].second << " ";
            if (i % 32 == 0) {
                std::cout << std::endl << i / 32 << ":\t";
            }
        }
    }
}

TEST(CUCKOOIntention, IntHashBalance) {
    constexpr double skew = 0;
    uint64_t *loads = new uint64_t[key_count];
    RandomGenerator<uint64_t>::generate(loads, key_range, key_count, skew);
    fmap store(root_size);
    for (int i = 0; i < key_count; i++) {
        uint64_t key = loads[i] % root_size;
        if (store.end() == store.find(key)) {
            store.insert(key, 0);
        }
        store.assign(key, store.find(key)->second + 1);
    }
    printstat(store, root_size);
}

uint64_t key_number = 100000000;

TEST(CUCKOOIntention, StringHashBalance) {
    ycsb::YCSBLoader loader("./tests/YCSBBench/load.dat", key_range);
    std::vector<ycsb::YCSB_request *> loads = loader.load();
    key_number = loader.size();
    fmap store(root_size);
    for (int i = 0; i < key_number; i++) {
        uint64_t key =
                MurmurHash64A(loads[i]->getKey(), std::strlen(loads[i]->getKey()), std::strlen(loads[i]->getKey())) %
                root_size;

        if (store.end() == store.find(key)) {
            store.insert(key, 0);
        }
        store.assign(key, store.find(key)->second + 1);
    }
    printstat(store, root_size);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
