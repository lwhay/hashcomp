//
// Created by iclab on 6/17/21.
//
#include <cassert>
#include <iostream>
#include <unordered_map>
#include "ReplacementAlgorithm.h"
#include "LRUAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 100000000

void testLRU() {
    zipf_distribution<uint64_t> gen((1LLU << 32), 0.99);
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
    LRUAlgorithm<int, 0> lss(100000);
    cout << lss.size() << endl;
    tracer.startTime();
    for (int i = 0; i < MAX_COUNT; i++) {
        lss.put(keys[i]);
    }
    cout << "LRUReplacement: " << tracer.getRunTime() << ":" << lss.size() << endl;
    cout << lss.size() << endl;
}

void testAsHeavyHitter() {
    zipf_distribution<uint64_t> gen((1LLU << 32), 0.99);
    std::mt19937 mt;
    std::vector<uint64_t> keys;
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < MAX_COUNT; i++) {
        keys.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << " with " << keys.size() << endl;
    tracer.getRunTime();
    GeneralReplacement<int, 0> lss(100000);
    tracer.startTime();
    for (int i = 0; i < MAX_COUNT; i++) {
        lss.put(keys[i]);
    }
    cout << "GeneralReplacement: " << tracer.getRunTime() << ":" << lss.size() << endl;
}

void testAsReplace() {
    // GeneralReplacement<int, 1> gr(10); // FIFO/LRU does not work
    GeneralReplacement<int, 0> gr(10); // LFU
    for (int j = 0; j < 100; j++) {
        int i = j; //(j * (5 - j > 0 ? j - 1 : 1)) % 20;
        int exists = (gr.find(i) != nullptr);
        int ret = gr.put(i);
        std::cout << ret << " " << i << std::endl;
        gr.print();
        if (!exists) assert(gr.find(ret) == nullptr);
        assert(gr.find(i) != nullptr);
    }
    int t = 5;
    std::cout << (((t + 1) & 0x7fffffff) - 1) << std::endl;
}

int main(int argc, char **argv) {
    testLRU();
    testAsHeavyHitter();
    // testAsReplace();
    return 0;
}