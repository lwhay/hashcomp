//
// Created by Michael on 6/24/21.
//
#include <algorithm>
#include <iostream>
#include <unordered_map>
#include "RandomAlgorithm.h"
#include "ReplacementAlgorithm.h"
#include "FastLRUAlgorithm.h"
#include "FastARCAlgorithm.h"
#include "DefaultARCAlgorithm.h"
#include "LRUAlgorithm.h"
#include "ARCAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 1000000000
#define DATA_SKEW 0.99

size_t HIT_COUNT = (MAX_COUNT / 10);

std::vector<uint64_t> keys;

std::unordered_set<uint64_t> top[6];

void generate() {
    keys.clear();
    uint64_t *_keys = (uint64_t *) calloc(MAX_COUNT, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint64_t>::generate(_keys, (1llu << 32), MAX_COUNT, DATA_SKEW);
    cout << "gen: " << tracer.getRunTime() << " with " << MAX_COUNT << " ";
    unordered_map<uint64_t, uint64_t> freq;
    for (int i = 0; i < MAX_COUNT; i++) {
        if (freq.find(_keys[i]) == freq.end()) freq.insert(make_pair(_keys[i], 0));
        freq.find(_keys[i])->second++;
    }
    cout << "map: " << tracer.getRunTime() << " with " << freq.size() << " ";
    vector<pair<uint64_t, uint64_t>> sorted;
    for (auto kv : freq) sorted.push_back(kv);
    sort(sorted.begin(), sorted.end(),
         [](pair<uint64_t, uint64_t> a, pair<uint64_t, uint64_t> b) { return a.second > b.second; });
    cout << "sort: " << tracer.getRunTime() << " with " << freq.size() << " ";
    freq.clear();
    top[0].insert(0);
    for (int i = 0; i < MAX_COUNT; i++) keys.push_back(_keys[i]);
    for (int k = 0; k < 6; k++) {
        for (int i = 0; i < pow(10, k); i++) {
            if (i < sorted.size()) top[k].insert(sorted.at(i).first);
        }
    }

    cout << "put: " << tracer.getRunTime() << " with " << freq.size() << " ";
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
    uint64_t hits[12];
    std::memset(hits, 0, sizeof(uint64_t) * 12);
    for (int i = 0; i < keys.size(); i++) {
        if (nullptr == lss.find(keys[i])) {
            miss++;
            for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k]++;

        } else {
            hit++;
            for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k + 1]++;
        }
    }
    cout << "find:" << "\033[33m" << tracer.getRunTime() << "\033[0m" << "\t" << "\033[34m" << miss << "\033[0m"
         << "\t" << hit;
    for (int i = 0; i < 6; i++) cout << "\t" << hits[i * 2] << "\t" << hits[i * 2 + 1];
    cout << endl;
}

void ARCEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    ARCAlgorithm<uint64_t> lss(HIT_COUNT);
    tracer.startTime();
    std::vector<uint64_t> queue;
    uint64_t miss = 0, hit = 0;
    uint64_t hits[12];
    std::memset(hits, 0, sizeof(uint64_t) * 12);
    for (int i = 0; i < keys.size(); i++) {
        bool contains = lss.add(keys[i]);
        if (contains) {
            hit++;
            for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k + 1]++;
        } else {
            miss++;
            for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k]++;
        }
    }
    cout << "ARC-" << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << lss.getTotalMiss() << ":"
         << (size_t) (lss.getHitRatio() * keys.size() / 100) << ":" << lss.getTotalRequest() << "\t" << "\033[34m"
         << miss << "\033[0m" << "\t" << hit << "\t";
    for (int i = 0; i < 6; i++) cout << "\t" << hits[i * 2] << "\t" << hits[i * 2 + 1];
    cout << endl;
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
void rollingEfficiencyTest(int round = 10) {
    Tracer tracer;
    tracer.getRunTime();
    T lss1(HIT_COUNT);
    T lss2(HIT_COUNT);
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    uint64_t hits[12];
    std::memset(hits, 0, sizeof(uint64_t) * 12);
    for (int k = 0; k < round; k++) {
        size_t begin = k * keys.size() / round;
        size_t end = (k + 1) * keys.size() / round;
        // approximate for round 0 by end round
        if (k == 0) for (int i = keys.size() - begin; i < keys.size(); i++) lss2.put(keys[i]);
        for (int i = begin; i < end; i++) {
            if (k % 2 == 0) {
                if (lss2.find(keys[i]) == nullptr) {
                    miss++;
                    for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k]++;
                } else {
                    hit++;
                    for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k + 1]++;
                }
                lss1.put(keys[i]);
            } else {
                if (lss1.find(keys[i]) == nullptr) {
                    miss++;
                    for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k]++;
                } else {
                    hit++;
                    for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k + 1]++;
                }
                lss2.put(keys[i]);
            }
        }
        if (k % 2 == 0) lss2.reset(); else lss1.reset();
    }
    cout << "Roll-" << typeid(T).name() << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << lss1.size()
         << "\t" << "\033[34m" << miss << "\033[0m" << "\t" << hit;
    for (int i = 0; i < 6; i++) cout << "\t" << hits[i * 2] << "\t" << hits[i * 2 + 1];
    cout << endl;
}

template<class T>
void replaceEfficiencyTest() {
    Tracer tracer;
    tracer.getRunTime();
    T lss(HIT_COUNT);
    tracer.startTime();
    uint64_t miss = 0, hit = 0;
    uint64_t hits[12];
    std::memset(hits, 0, sizeof(uint64_t) * 12);
    for (int i = 0; i < keys.size(); i++) {
        if (lss.find(keys[i]) == nullptr) {
            miss++;
            for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k]++;
        } else {
            hit++;
            for (int k = 0; k < 6; k++) if (top[k].find(keys[i]) != top[k].end()) hits[2 * k + 1]++;
        }
        lss.put(keys[i]);
    }
    cout << "Replace-" << typeid(T).name() << ":" << "\033[33m" << tracer.getRunTime() << "\033[0m" << ":" << lss.size()
         << "\t" << "\033[34m" << miss << "\033[0m" << "\t" << hit;
    for (int i = 0; i < 6; i++) cout << "\t" << hits[i * 2] << "\t" << hits[i * 2 + 1];
    cout << endl;
}

int main(int argc, char **argv) {
    generate();
    for (int c = 100; c <= 10000000; c *= 10) {
        cout << "\033[33m" << "--------------------------------------------------------------" << endl;
        cout << "\t\t\t\t" << "Capacity c: " << c << "\033[0m" << endl;
        HIT_COUNT = c;
        efficiencyTest<RandomAlgorithm<uint64_t>>();
        replaceEfficiencyTest<RandomAlgorithm<uint64_t>>();
        rollingEfficiencyTest<RandomAlgorithm<uint64_t>>();
        efficiencyTest<LRUAlgorithm<uint64_t>>();
        replaceEfficiencyTest<LRUAlgorithm<uint64_t>>();
        rollingEfficiencyTest<LRUAlgorithm<uint64_t>>();
        ARCEfficiencyTest();
        efficiencyTest<GeneralReplacement<uint64_t>>();
        replaceEfficiencyTest<GeneralReplacement<uint64_t>>();
        rollingEfficiencyTest<GeneralReplacement<uint64_t>>();
    }
}