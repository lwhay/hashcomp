//
// Created by Michael on 3/2/20.
//
#include <algorithm>
#include <iostream>
#include <cstring>
#include <unordered_set>
#include <vector>
#include "tracer.h"

using namespace std;
using namespace ycsb;

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

int main(int argc, char **argv) {
    YCSBLoader loader(loadpath, 1000000000);
    std::vector<YCSB_request *> loads = loader.load();
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < loader.size(); i++) {
        uint64_t hk = MurmurHash64A(loads[i]->getKey(), std::strlen(loads[i]->getKey()), 23423423234234234llu);
    }
    cout << "MurHasher: " << tracer.getRunTime() << " with " << loader.size() << endl;

    std::hash<std::string> hash;
    tracer.startTime();
    for (int i = 0; i < loader.size(); i++) {
        uint64_t hk = hash(loads[i]->getKey());
    }
    cout << "STDHasher: " << tracer.getRunTime() << " with " << loader.size() << endl;

    std::unordered_set<uint64_t> murset;
    tracer.startTime();
    for (int i = 0; i < loader.size(); i++) {
        murset.insert(MurmurHash64A(loads[i]->getKey(), std::strlen(loads[i]->getKey()), 23423423234234234llu));
    }
    cout << "MurHasher: " << tracer.getRunTime() << " with " << murset.size() << " out of " << loader.size() << endl;

    std::unordered_set<uint64_t> stdset;
    tracer.startTime();
    for (int i = 0; i < loader.size(); i++) {
        stdset.insert(hash(loads[i]->getKey()));
    }
    cout << "STDHasher: " << tracer.getRunTime() << " with " << stdset.size() << " out of " << loader.size() << endl;

    return 0;
}