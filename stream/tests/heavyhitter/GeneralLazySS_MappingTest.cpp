//
// Created by iclab on 11/18/19.
//

#include <algorithm>
#include <functional>
#include <iostream>
#include <random>
#include <vector>
#include <unordered_set>
#include "GeneralLazySS.h"
#include "../../src/mhash/util/City.h"
#include "RealKVKeyToHash.h"
#include "tracer.h"

#define increasing 0

#define freshinput 0

#define datasource 0  // 0: random, 1: ycsb, 2: clickstream

#define segmenting 1

size_t key_range = (1llu << 32);

size_t total_count = (1 << 27);

size_t counter_size = (1 << 16);

size_t merge_round = (1 << 4);

double zipf = 0.99f;

struct counter {
    uint64_t key;
    uint64_t cnt;
};

uint64_t getfastkey(uint64_t key) {
    //return key % counter_size;
    return CityHash64((const char *) (&key), sizeof(uint64_t)) % counter_size;
}

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

void averageHitTest(Item<uint64_t> *bins, uint64_t *const keys) {
    Tracer tracer;
    counter *counters = new counter[counter_size];
    std::memset(counters, 0, sizeof(counter) * counter_size);
    tracer.startTime();
#if increasing
    for (int i = 0; i < /*glss.volume()*/counter_size; i++) {
#else
    for (int i = /*glss.volume()*/counter_size - 1; i >= 0; i--) {
#endif
        uint64_t fastkey = getfastkey(bins[i].getItem());
        if (counters[fastkey].key != bins[i].getItem()) {
            counters[fastkey].key = bins[i].getItem();
        }
        counters[fastkey].cnt++;
    }
    std::cout << "Moving bins: " << tracer.getRunTime() << std::endl;

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
        if (counters[i].cnt > 4) keyhit5++;
    }
    std::cout << "Hitkeys: " << keyhit << " hitkey2: " << keyhit2 << " hitkey3: " << keyhit3 << " hitkey4: " << keyhit4
              << " hitkey5: " << keyhit5 << std::endl;

    double averagehit = 0.0;
    for (int i = 0; i < counter_size; i++) counters[i].cnt = 0;
    for (int i = 0; i < total_count; i++) {
        uint64_t fastkey = getfastkey(keys[i]);
        if (counters[fastkey].key == keys[i]) counters[fastkey].cnt++;
    }
    for (int i = 0; i < counter_size; i++) {
        averagehit += counters[i].cnt;
    }
    std::cout << "Averagehit: " << (averagehit / total_count) << std::endl;

    delete[] counters;
}

void actualHitTest(Item<uint64_t> *bins, uint64_t *keys) {
    Tracer tracer;
    tracer.startTime();
    std::sort(&bins[0], &bins[counter_size - 1], Item<uint64_t>::prec);
    //for (int i = 0; i < 1000; i++) { std::cout << "\t" << bins[i].getItem() << ":" << bins[i].getCount() << std::endl; }
    std::cout << "SortingByKey: " << tracer.getRunTime() << std::endl;
    double averagehit = 0;
    //std::vector<Item<uint64_t>> intends;
    //intends.push_back(Item<uint64_t>());
    tracer.startTime();
    for (int i = 0; i < total_count; i++) {
        //intends[0].setitem(keys[i]);
        //if (std::find(&bins[0], &bins[counter_size - 1], intend)) {
        //if (std::binary_search(&bins[0], &bins[counter_size - 1], intends.at(0), Item<uint64_t>::prec)) {
        if (binarySearch<Item<uint64_t>>(bins, counter_size, keys[i])) {
            averagehit++;
        }
    }
    std::cout << "Actualhit: " << (averagehit / total_count) << " search time: " << tracer.getRunTime() << std::endl;
}

void singleMapping(uint64_t *keys) {
    GeneralLazySS<uint64_t> glss(0.00001);
    Tracer tracer;
    tracer.startTime();
    std::unordered_set<uint64_t> noters;
    tracer.startTime();
    for (int i = 0; i < total_count; i++) noters.insert(keys[i]);
    std::cout << "Distinct: " << tracer.getRunTime() << " number: " << noters.size() << " out of: " << total_count
              << std::endl;

    tracer.startTime();
    for (int i = 0; i < total_count; i++) glss.put(keys[i]);
    std::cout << "Quantile: " << tracer.getRunTime() << std::endl;

    tracer.startTime();
    Item<uint64_t> *bins = glss.output(true);
    std::cout << "Output: " << tracer.getRunTime() << std::endl;

    averageHitTest(bins, keys);
    actualHitTest(bins, keys);
}

void findAfterSort(uint64_t *keys) {
    GeneralLazySS<uint64_t> glss(0.00001);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < total_count; i++) glss.put(keys[i]);
    for (int i = 0; i < 1024; i++) {
        if (i != 0 && i % 128 == 0) std::cout << std::endl;
        if (glss.find(keys[i])) std::cout << "+";
        else std::cout << "-";
    }
    std::cout << std::endl;
    Item<uint64_t> *bins = glss.output(true);
    for (int i = 0; i < 1024; i++) {
        if (i != 0 && i % 128 == 0) std::cout << std::endl;
        if (glss.find(keys[i])) std::cout << "+";
        else std::cout << "-";
    }
    std::cout << std::endl;
}

void mergeMapping(uint64_t *keys) {
    size_t count_per_round = total_count / merge_round;
    GeneralLazySS<uint64_t> first(0.00002);
    Item<uint64_t> *bins;
    size_t i, r = 0;
    Tracer tracer;
    tracer.startTime();
    for (i = 0; i < count_per_round; i++) first.put(keys[i]);
    std::cout << "\tr: " << r << " " << tracer.getRunTime() << std::endl;
    for (r = 1; r < merge_round; r++) {
        GeneralLazySS<uint64_t> current(0.00002);
        tracer.startTime();
        for (; i < count_per_round * (r + 1); i++) current.put(keys[i]);
        std::cout << "\tr: " << r << " " << tracer.getRunTime();
        tracer.startTime();
        bins = first.merge(current);
        std::cout << " " << " " << tracer.getRunTime() << std::endl;
    }

    averageHitTest(bins, keys);
    actualHitTest(bins, keys);
}

int main(int argc, char **argv) {
    if (argc == 6) {
        key_range = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        counter_size = std::atol(argv[3]);
        merge_round = std::atol(argv[4]);
        zipf = std::atof(argv[6]);
    }
    uint64_t *keys = new uint64_t[total_count];

#if freshinput
    std::remove(existingFilePath);
#endif

    Tracer tracer;
    tracer.startTime();
#if datasource == 0
#if segmenting
    size_t range_per_round = (key_range / merge_round);
    RandomGenerator<uint64_t>::generate(keys, range_per_round, total_count, zipf);
    size_t count_per_round = total_count / merge_round;
    for (int i = 0; i < merge_round; i++) {
        for (int j = 0; j < count_per_round; j++) {
            keys[i * count_per_round + j] += i * range_per_round;
        }
    }
#else
    RandomGenerator<uint64_t>::generate(keys, key_range, total_count, zipf);
#endif
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

    singleMapping(keys);

    findAfterSort(keys);

    mergeMapping(keys);

    delete[] keys;
}