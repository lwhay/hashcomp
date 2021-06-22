//
// Created by iclab on 6/17/21.
//
#include <cassert>
#include <iostream>
#include <unordered_map>
#include "RandomAlgorithm.h"
#include "ReplacementAlgorithm.h"
#include "LRUAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 100000000
#define HIT_COUNT (MAX_COUNT / 10)
#define DATA_SKEW 0.49

std::vector<uint64_t> keys;

void generate(size_t num = MAX_COUNT) {
    keys.clear();
    zipf_distribution<uint64_t> gen((1LLU << 32), DATA_SKEW);
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
    for (int i = 0; i < MAX_COUNT; i++) {
        if (lss.find(keys[i]) == nullptr) miss++; else hit++;
        lss.put(keys[i]);
    }
    cout << "Replace-" << typeid(T).name() << ":" << tracer.getRunTime() << ":" << lss.size() << ":" << miss << ":"
         << hit << lss.size() << endl;
}

template<class T>
void efficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    T lss(HIT_COUNT);
    tracer.startTime();
    for (int i = 0; i < MAX_COUNT; i++) {
        lss.put(keys[i]);
    }
    cout << typeid(T).name() << ":" << tracer.getRunTime() << ":" << lss.size();
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    for (int i = 0; i < MAX_COUNT; i++) {
        if (nullptr == lss.find(keys[i])) miss++;
        else hit++;
    }
    cout << " find: " << tracer.getRunTime() << ":" << miss << ":" << hit << ":" << lss.size() << endl;
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
    generate();
    efficiencyTest<RandomAlgorithm<uint64_t>>();
    replaceEfficiencyTest<RandomAlgorithm<uint64_t>>();
    efficiencyTest<LRUAlgorithm<uint64_t>>();
    replaceEfficiencyTest<LRUAlgorithm<uint64_t>>();
    efficiencyTest<GeneralReplacement<uint64_t>>();
    replaceEfficiencyTest<GeneralReplacement<uint64_t>>();
    // testAsReplace();
    return 0;
}