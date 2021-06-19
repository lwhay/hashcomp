//
// Created by iclab on 6/17/21.
//
#include <cassert>
#include <iostream>
#include <unordered_map>
#include "ReplacementAlgorithm.h"
#include "LRUAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 100
#define HIT_COUNT (MAX_COUNT / 10)
#define DATA_SKEW 0.99

void testLRU() {
    zipf_distribution<uint64_t> gen((1LLU << 32), DATA_SKEW);
    std::mt19937 mt;
    std::vector<uint64_t> keys;
    Tracer tracer;
    tracer.startTime();
    std::unordered_map<uint64_t, int> freq;
    for (int i = 0; i < MAX_COUNT; i++) {
        uint64_t key = gen(mt);
        keys.push_back(key);
        if (freq.end() == freq.find(key)) freq.insert(make_pair(key, 0));
        freq.find(key)->second++;
    }
    cout << tracer.getRunTime() << " with " << keys.size() << " " << freq.size() << endl;
    tracer.getRunTime();
    LRUAlgorithm<int, 0> lss(HIT_COUNT);
    cout << lss.size() << endl;
    tracer.startTime();
    for (int i = 0; i < MAX_COUNT; i++) {
        lss.put(keys[i]);
    }
    cout << "LRUReplacement: " << tracer.getRunTime() << ":" << lss.size() << endl;
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    for (int i = 0; i < MAX_COUNT; i++) {
        if (nullptr == lss.find(keys[i])) miss++;
        else hit++;
    }
    cout << "Run find: " << tracer.getRunTime() << ":" << miss << ":" << hit << endl;
    cout << lss.size() << endl;
}

void testAsHeavyHitter() {
    zipf_distribution<uint64_t> gen((1LLU << 32), DATA_SKEW);
    std::mt19937 mt;
    std::vector<uint64_t> keys;
    Tracer tracer;
    tracer.startTime();
    std::unordered_map<uint64_t, int> freq;
    for (int i = 0; i < MAX_COUNT; i++) {
        uint64_t key = gen(mt);
        keys.push_back(key);
        if (freq.end() == freq.find(key)) freq.insert(make_pair(key, 0));
        freq.find(key)->second++;
    }
    cout << tracer.getRunTime() << " with " << keys.size() << " " << freq.size() << endl;
    tracer.getRunTime();
    GeneralReplacement<int, 0> lss(HIT_COUNT);
    tracer.startTime();
    for (int i = 0; i < MAX_COUNT; i++) {
        lss.put(keys[i]);
    }
    cout << "GeneralReplacement: " << tracer.getRunTime() << ":" << lss.size() << endl;
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    for (int i = 0; i < MAX_COUNT; i++) {
        if (nullptr == lss.find(keys[i])) miss++;
        else hit++;
    }
    cout << "Run find: " << tracer.getRunTime() << ":" << miss << ":" << hit << endl;
}

void testAsReplace() {
    // GeneralReplacement<int, 1> gr(10); // FIFO/LRU does not work
    GeneralReplacement<int, 0> gr(10); // LFU
    for (int j = 0; j < 100; j++) {
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
    /*testLRU();
    testAsHeavyHitter();*/
    testAsReplace();
    return 0;
}