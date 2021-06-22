//
// Created by iclab on 6/22/21.
//
#include <functional>
#include <iostream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <queue>
#include "tracer.h"

using namespace std;

#define MAX_COUNT 100000000
#define HIT_COUNT (MAX_COUNT / 10)
#define DATA_SKEW 0.0f

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

void sortEfficiencyTest() {
    Tracer tracer;
    tracer.startTime();
    sort(keys.begin(), keys.end(), greater<uint64_t>());
    cout << "sort: " << tracer.getRunTime() << ":" << keys.size() << endl;
}

void pqEfficiencyTest() {
    Tracer tracer;
    tracer.startTime();
    priority_queue<uint64_t, vector<uint64_t>, greater<uint64_t>> pq;
    for (auto k:keys) pq.push(k);
    cout << "pq: " << tracer.getRunTime() << ":" << pq.size() << endl;
}

void limitPQEfficiencyTest(int ratio = 10) {
    Tracer tracer;
    tracer.startTime();
    priority_queue<uint64_t, vector<uint64_t>, greater<uint64_t>> pq;
    for (auto k:keys) {
        if (pq.size() >= keys.size() / ratio) {
            uint64_t t = pq.top();
            pq.pop();
        }
        pq.push(k);
    }
    cout << "limited pq" << ((float) 1 / ratio) << ": " << tracer.getRunTime() << ":" << pq.size() << endl;
}

int main() {
    generate();
    for (size_t r = 10000; r >= 1; r /= 10) {
        limitPQEfficiencyTest(r);
        cout << "\033[33m" << "--------------------------------------------------------------" << "\033[0m" << endl;
    }
    for (size_t s = MAX_COUNT / 10000; s <= MAX_COUNT; s *= 10) {
        generate(s);
        sortEfficiencyTest();
        generate(s);
        pqEfficiencyTest();
        generate(s);
        limitPQEfficiencyTest();
        cout << "\033[33m" << "--------------------------------------------------------------" << "\033[0m" << endl;
    }
}