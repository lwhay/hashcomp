//
// Created by iclab on 11/18/19.
//

#include <algorithm>
#include <iostream>
#include <random>
#include <vector>
#include <unordered_set>
#include "GeneralLazySS.h"
#include "RealKVKeyToHash.h"
#include "tracer.h"

#define increasing 0

#define freshinput 0

#define datasource 2  // 0: random, 1: ycsb, 2: clickstream

size_t key_range = (1llu << 32);

size_t total_count = (1 << 27);

size_t counter_size = (1 << 16);

double zipf = 0.99f;

struct counter {
    uint64_t key;
    uint64_t cnt;
};

template<typename T>
bool binarySearch(T array[], uint64_t size, uint64_t key) {
    int start = 1, end = size;
    int mid = (start + end) / 2;

    while (start <= end && array[mid].getItem() != key) {
        if (array[mid].getItem() < key) {
            start = mid + 1;
        } else {
            end = mid - 1;
        }
        mid = (start + end) / 2;
    } // While Loop End

    if (array[mid].getItem() == key)
        return true; //Returnig to main
    else
        return false; //Returnig to main
} // binarySearch Function Ends Here

int main(int argc, char **argv) {
    if (argc == 5) {
        key_range = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        counter_size = std::atol(argv[3]);
        zipf = std::atof(argv[4]);
    }
    uint64_t *keys = new uint64_t[total_count];

#if freshinput
    std::remove(existingFilePath);
#endif

    Tracer tracer;
    tracer.startTime();
#if datasource == 0
    RandomGenerator<uint64_t>::generate(keys, key_range, total_count, zipf);
#elif datasource == 1
    delete[] keys;
    keys = RealWorkload::ycsbKeyToUint64(total_count);
#elif datasource == 2
    delete[] keys;
    keys = RealWorkload::clickstreamKeyToUint64(total_count);
#else
    std::perror("0: random, 1: ycsb, 2: clickstream");
#endif
    std::cout << "Generate: " << tracer.getRunTime() << std::endl;

    std::unordered_set<uint64_t> noters;
    tracer.startTime();
    for (int i = 0; i < total_count; i++) noters.insert(keys[i]);
    std::cout << "Distinct: " << tracer.getRunTime() << " number: " << noters.size() << " out of: " << total_count
              << std::endl;

    GeneralLazySS<uint64_t> glss(0.00001);
    tracer.startTime();
    for (int i = 0; i < total_count; i++) glss.put(keys[i]);
    std::cout << "Quantile: " << tracer.getRunTime() << std::endl;

    tracer.startTime();
    Item<uint64_t> *bins = glss.output(true);
    std::cout << "Output: " << tracer.getRunTime() << std::endl;

    counter *counters = new counter[counter_size];
    std::memset(counters, 0, sizeof(counter) * counter_size);
#if increasing
    for (int i = 0; i < /*glss.volume()*/counter_size; i++) {
#else
    for (int i = /*glss.volume()*/counter_size - 1; i >= 0; i--) {
#endif
        uint64_t fastkey = bins[i].getItem() % counter_size;
        if (counters[fastkey].key != bins[i].getItem()) {
            counters[fastkey].key = bins[i].getItem();
        }
        counters[fastkey].cnt++;
    }

    uint64_t keyhit = 0;
    uint64_t keyhit2 = 0;
    uint64_t keyhit3 = 0;
    uint64_t keyhit4 = 0;
    uint64_t keyhit5 = 0;
    for (int i = 0; i < counter_size; i++) {
        if (counters[i].cnt > 0) keyhit++;
        if (counters[i].cnt > 1) keyhit2++;
        if (counters[i].cnt > 2) keyhit3++;
        if (counters[i].cnt > 3) keyhit4++;
        if (counters[i].cnt > 3) keyhit5++;
    }
    std::cout << "Hitkeys: " << keyhit << " hitkey2: " << keyhit2 << " hitkey3: " << keyhit3 << " hitkey4: " << keyhit4
              << " hitkey4: " << keyhit4 << std::endl;

    double averagehit = 0.0;
    for (int i = 0; i < counter_size; i++) counters[i].cnt = 0;
    for (int i = 0; i < total_count; i++) {
        uint64_t fastkey = keys[i] % counter_size;
        if (counters[fastkey].key == keys[i]) counters[fastkey].cnt++;
    }
    for (int i = 0; i < counter_size; i++) {
        averagehit += counters[i].cnt;
    }
    std::cout << "Averagehit: " << (averagehit / total_count) << std::endl;

    tracer.startTime();
    std::sort(&bins[0], &bins[counter_size - 1], Item<uint64_t>::prec);
    std::cout << "SortingByKey: " << tracer.getRunTime() << std::endl;

    averagehit = 0;
    std::vector<Item<uint64_t>> intends;
    intends.push_back(Item<uint64_t>());
    tracer.startTime();
    for (int i = 0; i < total_count; i++) {
        intends[0].setitem(keys[i]);
        //if (std::find(&bins[0], &bins[counter_size - 1], intend)) {
        //if (std::binary_search(&bins[0], &bins[counter_size - 1], intends.at(0), Item<uint64_t>::prec)) {
        if (binarySearch<Item<uint64_t>>(bins, counter_size, keys[i])) {
            averagehit++;
        }
    }
    std::cout << "Actualhit: " << (averagehit / total_count) << " search time: " << tracer.getRunTime() << std::endl;

    delete[] keys;
    delete[] counters;
}