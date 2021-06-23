//
// Created by iclab on 6/17/21.
//
#include <cassert>
#include <iostream>
#include <unordered_map>
#include "RandomAlgorithm.h"
#include "ReplacementAlgorithm.h"
#include "FastLRUAlgorithm.h"
#include "FastARCAlgorithm.h"
#include "DefaultLRUAlgorithm.h"
#include "ARCAlgorithm.h"
#include "LRUAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 100000000
#define DATA_SKEW 0.99

size_t HIT_COUNT = (MAX_COUNT / 10);

std::vector<uint64_t> keys;

void generate(size_t num = MAX_COUNT, size_t range = (1LLU << 32)) {
    keys.clear();
    zipf_distribution<uint64_t> gen(range, DATA_SKEW);
    std::mt19937 mt;
    Tracer tracer;
    tracer.startTime();
    std::unordered_map<uint64_t, int> freq;
    for (int i = 0; i < num; i++) {
        uint64_t key = gen(mt);
        keys.push_back(key);
        if (freq.end() == freq.find(key)) freq.insert(make_pair(key, 0));
        freq.find(key)->second++;
    }
    cout << tracer.getRunTime() << " with " << keys.size() << " " << freq.size() << endl;
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
         << ":" << "\033[34m" << miss << "\033[0m" << ":" << hit << ":" << lss.size() << endl;
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
    cout << "FastARC-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << "\033[34m"
         << lss.getTotalMiss() << "\033[0m" << ":" << (size_t) (lss.getHitRatio() * keys.size() / 100) << ":"
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
    cout << "FastLRU-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << "\033[34m"
         << lss.getTotalMiss() << "\033[0m" << ":" << (size_t) (lss.getHitRatio() * keys.size() / 100) << ":"
         << lss.getTotalRequest() << endl;
}

void DefaultLRUEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    DefaultLRUAlgorithm<uint64_t> lss(HIT_COUNT);
    tracer.startTime();
    std::vector<uint64_t> queue;
    for (int i = 0; i < keys.size(); i++) {
        lss.findAndReplace(queue, keys[i]);
    }
    cout << "DefaultLRU-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << "\033[34m" << lss.getMiss()
         << "\033[0m" << ":" << lss.getHit() << endl;
}

void ARCEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    ARCAlgorithm<uint64_t> lss(HIT_COUNT);
    tracer.startTime();
    for (int i = 0; i < keys.size(); i++) {
        lss.arc_lookup(keys[i]);
    }
    cout << "ARC-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << "\033[34m" << lss.getMiss()
         << "\033[0m" << ":" << lss.getHit() << endl;
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
    cout << " find: " << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << "\033[34m" << miss << "\033[0m" << ":"
         << hit << ":" << lss.size() << endl;
}

template<class T>
void detailHeavyHitterTest() {
    T gr(10); // LFU
    int miss = 0, total = 0;
    for (int j = 0; j < 100; j++) {
        int ret = gr.put(keys[j]);
        gr.print();
    }
    for (int j = 0; j < 100; j++) {
        if (!gr.find(j)) miss++;
        total++;
    }
    cout << typeid(T).name() << ":" << miss << ":" << total << endl;
}

void testAsReplace() {
    // GeneralReplacement<int, 1> gr(10); // FIFO/LRU does not work
    GeneralReplacement<int> gr(10); // LFU
    for (int j = 0; j < 100; j++) {
        if (j == 9)
            int k = 0;
        int i = j; //(j * (5 - j > 0 ? j - 1 : 1)) % 20;
        int exists = (gr.find(i) != nullptr);
        int ret = gr.put(i);
        for (int k = 0; k < 135; k++)std::cout << "-";
        cout << endl;
        if (!exists) assert(gr.find(ret) == nullptr);
        assert(gr.find(i) != nullptr);
    }
    int t = 5;
    std::cout << (((t + 1) & 0x7fffffff) - 1) << std::endl;
}

int main(int argc, char **argv) {
    generate(100);
    detailHeavyHitterTest<RandomAlgorithm<uint64_t>>();
    detailHeavyHitterTest<LRUAlgorithm<uint64_t>>();
    detailHeavyHitterTest<GeneralReplacement<uint64_t>>();
    cout << "\033[33m" << "--------------------------------------------------------------" << "\033[0m" << endl;
    // small-scale test
    for (size_t c = 1000; c <= 100000; c *= 10) {
        generate(c, c);
        HIT_COUNT = c / 10;
        efficiencyTest<RandomAlgorithm<uint64_t>>();
        replaceEfficiencyTest<RandomAlgorithm<uint64_t>>();
        efficiencyTest<LRUAlgorithm<uint64_t>>();
        replaceEfficiencyTest<LRUAlgorithm<uint64_t>>();
        efficiencyTest<GeneralReplacement<uint64_t>>();
        replaceEfficiencyTest<GeneralReplacement<uint64_t>>();
        FastLRUEfficiencyTest();
        FastARCEfficiencyTest();
        DefaultLRUEfficiencyTest();
        ARCEfficiencyTest();
        cout << "\033[33m" << "--------------------------------------------------------------" << "\033[0m" << endl;
    }
    // large-scale test
    generate(MAX_COUNT, MAX_COUNT);
    HIT_COUNT = MAX_COUNT / 10;
    efficiencyTest<RandomAlgorithm<uint64_t>>();
    replaceEfficiencyTest<RandomAlgorithm<uint64_t>>();
    efficiencyTest<LRUAlgorithm<uint64_t>>();
    replaceEfficiencyTest<LRUAlgorithm<uint64_t>>();
    efficiencyTest<GeneralReplacement<uint64_t>>();
    replaceEfficiencyTest<GeneralReplacement<uint64_t>>();
    FastLRUEfficiencyTest();
    FastARCEfficiencyTest();

    // testAsReplace();
    return 0;
}