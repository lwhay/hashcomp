//
// Created by Michael on 7/1/21.
//

#include <atomic>
#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <thread>
//#includede <mutex>
#include "tracer.h"

#define MAX_COUNT 1000000 //000000
#define DATA_SKEW 0.99

using namespace std;

size_t HIT_COUNT = (MAX_COUNT / 10);

std::vector<uint64_t> hkeys;

std::vector<uint64_t> lkeys;

alignas(64) atomic<uint64_t> *core;

size_t unique_keys = 0;

atomic<int> stopMeasure(0);

class spin_mutex {
    std::atomic<bool> flag = ATOMIC_VAR_INIT(false);
public:
    spin_mutex() = default;

    spin_mutex(const spin_mutex &) = delete;

    spin_mutex &operator=(const spin_mutex &) = delete;

    void lock() {
        while (flag.exchange(true, std::memory_order_acquire));
    }

    void unlock() {
        flag.store(false, std::memory_order_release);
    }
};

void generate(int removingK) {
    hkeys.clear();
    lkeys.clear();
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
    unique_keys = freq.size();
    cout << "map: " << tracer.getRunTime() << " with " << freq.size() << " ";
    vector<pair<uint64_t, uint64_t>> sorted;
    for (auto kv : freq) sorted.push_back(kv);
    sort(sorted.begin(), sorted.end(),
         [](pair<uint64_t, uint64_t> a, pair<uint64_t, uint64_t> b) { return a.second > b.second; });
    cout << "sort: " << tracer.getRunTime() << " with " << freq.size() << " ";
    freq.clear();
    for (int i = 0; i < removingK; i++) freq.insert(sorted.at(i));
    for (int i = 0; i < MAX_COUNT; i++) {
        if (freq.find(_keys[i]) == freq.end()) hkeys.push_back(_keys[i]); else lkeys.push_back(_keys[i]);
    }
    cout << "put: " << tracer.getRunTime() << " with " << freq.size() << " ";

    if (core == nullptr) core = new atomic<uint64_t>[unique_keys];
}

void primitiveFA(vector<uint64_t> loads, size_t trd) {
    vector<thread> workers;
    stopMeasure.store(0, memory_order_relaxed);
    uint64_t opcounts[trd];
    std::memset(opcounts, 0, sizeof(uint64_t) * trd);
    Timer timer;
    timer.start();
    for (int i = 0; i < trd; i++) {
        workers.push_back(thread([](vector<uint64_t> &loads, uint64_t &optcount, int tid, int trd) {
            for (int i = tid; i < loads.size(); i += trd) {
                core[loads.at(i) % unique_keys].fetch_add(1);
                optcount++;
            }
        }, loads, opcounts[i], i, trd));
    }
    while (timer.elapsedSeconds() < 60) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < trd; i++) {
        workers.at(i).join();
        if (i > 1) opcounts[0] += opcounts[i];
    }
    cout << "FA:\t" << trd << "\t" << opcounts[0] << endl;
}

void primitiveCAS(vector<uint64_t> loads, size_t trd) {
    vector<thread> workers;
    stopMeasure.store(0, memory_order_relaxed);
    uint64_t opcounts[trd];
    std::memset(opcounts, 0, sizeof(uint64_t) * trd);
    Timer timer;
    timer.start();
    for (int i = 0; i < trd; i++) {
        workers.push_back(thread([](vector<uint64_t> &loads, uint64_t &optcount, int tid, int trd) {
            for (int i = tid; i < loads.size(); i += trd) {
                size_t idx = loads.at(i) % unique_keys;
                uint64_t old;
                do {
                    old = core[idx].load();
                } while (!core[idx].compare_exchange_strong(old, old + 1));
                optcount++;
            }
        }, loads, opcounts[i], i, trd));
    }
    while (timer.elapsedSeconds() < 60) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < trd; i++) {
        workers.at(i).join();
        if (i > 1) opcounts[0] += opcounts[i];
    }
    cout << "CAS:\t" << trd << "\t" << opcounts[0] << endl;
}

void primitiveSpin() {

}

void primitiveMutex() {

}

int main(int argc, char **argv) {
    for (int i = 100000; i <= 100000;) {
        generate(i);
        //for (int t = 1; t <= 100; t *= 3) primitiveFA(hkeys, t);
    }

}