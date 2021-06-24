//
// Created by Michael on 6/24/21.
//

#include <cassert>
#include <iostream>
#include <unordered_map>
#include "RandomAlgorithm.h"
#include "ReplacementAlgorithm.h"
#include "FastLRUAlgorithm.h"
#include "FastARCAlgorithm.h"
#include "ARCAlgorithm.h"
#include "LRUAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 1000000000
#define DATA_SKEW 0.99

size_t HIT_COUNT = (MAX_COUNT / 10);

std::vector<uint64_t> keys;

void generate(int removingK) {
    keys.clear();
    uint64_t *_keys = (uint64_t *) calloc(MAX_COUNT, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint64_t>::generate(_keys, (1llu << 32), MAX_COUNT, DATA_SKEW);
    cout << tracer.getRunTime() << " with " << MAX_COUNT << endl;
    for (int i = 0; i < MAX_COUNT; i++) {
        if (_keys[i] > removingK) keys.push_back(_keys[i]);
    }
}

template<class T>
void efficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    T lss(HIT_COUNT);
    tracer.startTime();
    for (int i = 0; i < keys.size(); i++) {
        lss.put(keys[i]);
    }
    cout << typeid(T).name() << ":" << tracer.getRunTime() << ":" << lss.size();
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    for (int i = 0; i < keys.size(); i++) {
        if (nullptr == lss.find(keys[i])) miss++;
        else hit++;
    }
    cout << " find: " << "\033[33m" << tracer.getRunTime() << "\033[0m" << "\t" << "\033[34m" << miss << "\033[0m"
         << "\t" << hit << "\t" << lss.size() << endl;
}

void FastARCEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    FastARCAlgorithm<uint64_t> lss(HIT_COUNT);
    tracer.startTime();
    std::vector<uint64_t> queue;
    for (int i = 0; i < keys.size(); i++) {
        lss.add(keys[i]);
    }
    cout << "FastARC-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << "\t" << "\033[34m"
         << lss.getTotalMiss() << "\033[0m" << "\t" << (size_t) (lss.getHitRatio() * keys.size() / 100) << "\t"
         << lss.getTotalRequest() << endl;
}

void FastLRUEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    FastLRUAlgorithm<uint64_t> lss(HIT_COUNT);
    tracer.startTime();
    std::vector<uint64_t> queue;
    for (int i = 0; i < keys.size(); i++) {
        lss.add(keys[i]);
    }
    cout << "FastLRU-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << "\t" << "\033[34m"
         << lss.getTotalMiss() << "\033[0m" << "\t" << (size_t) (lss.getHitRatio() * keys.size() / 100) << "\t"
         << lss.getTotalRequest() << endl;
}

template<class T>
void replaceEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    T lss(HIT_COUNT);
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    for (int i = 0; i < keys.size(); i++) {
        if (lss.find(keys[i]) == nullptr) miss++; else hit++;
        lss.put(keys[i]);
    }
    cout << "Replace-" << typeid(T).name() << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << lss.size()
         << "\t" << "\033[34m" << miss << "\033[0m" << "\t" << hit << "\t" << lss.size() << endl;
}

int main(int argc, char **argv) {
    for (int i = 0; i <= 10000;) {
        generate(i);
        for (int c = 100; c <= 1000000; c *= 10) {
            cout << "\033[33m" << "--------------------------------------------------------------" << endl;
            cout << "\t\t\t\t" << "Removing" << i << "\tc:" << c << "\033[0m" << endl;
            HIT_COUNT = c;
            HIT_COUNT = MAX_COUNT / 10;
            efficiencyTest<RandomAlgorithm<uint64_t >>();
            replaceEfficiencyTest<RandomAlgorithm<uint64_t >>();
            efficiencyTest<LRUAlgorithm<uint64_t >>();
            replaceEfficiencyTest<LRUAlgorithm<uint64_t >>();
            efficiencyTest<GeneralReplacement<uint64_t >>();
            replaceEfficiencyTest<GeneralReplacement<uint64_t >>();
            FastLRUEfficiencyTest();
            FastARCEfficiencyTest();
        }
        i = (i == 0) ? 1 : i *= 10;
    }
}