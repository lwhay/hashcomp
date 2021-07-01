//
// Created by Michael on 7/1/21.
//

#include <atomic>
#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <cstring>
#include <memory>
#include <mutex>
#include "tracer.h"

#define MAX_COUNT 1000000000
#define ALIGN_CNT 8
#define DATA_SKEW 0.99

using namespace std;

std::vector<uint64_t> hkeys;

std::vector<uint64_t> lkeys;

alignas(64) atomic<uint64_t> *core;

uint64_t *nore;

size_t unique_keys = 0;

atomic<int> stopMeasure(0);

size_t test_duration = 30;

size_t max_count = MAX_COUNT;

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
    if (core != nullptr) delete[] core;
    if (nore != nullptr) delete[] nore;
    hkeys.clear();
    lkeys.clear();
    uint64_t *_keys = (uint64_t *) calloc(max_count, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint64_t>::generate(_keys, (1llu << 32), max_count, DATA_SKEW);
    cout << "gen: " << tracer.getRunTime() << " with " << max_count << " ";
    unordered_map<uint64_t, uint64_t> freq;
    for (int i = 0; i < max_count; i++) {
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
    for (int i = 0; i < max_count; i++) {
        if (freq.find(_keys[i]) == freq.end()) hkeys.push_back(_keys[i]); else lkeys.push_back(_keys[i]);
    }
    cout << "put: " << tracer.getRunTime() << " with " << freq.size() << " " << endl;

    if (core == nullptr) core = new atomic<uint64_t>[unique_keys * ALIGN_CNT];
    if (nore == nullptr) nore = new uint64_t[unique_keys * ALIGN_CNT];
}

void primitiveFA(vector<uint64_t> &loads, size_t trd) {
    for (int i = 0; i < unique_keys; i++) core[i * ALIGN_CNT].store(0);
    vector<thread> workers;
    stopMeasure.store(0, memory_order_relaxed);
    uint64_t opcounts[trd];
    std::memset(opcounts, 0, sizeof(uint64_t) * trd);
    cout << loads.size() << "\t";
    Timer timer;
    timer.start();
    for (int i = 0; i < trd; i++) {
        workers.push_back(thread([](vector<uint64_t> &loads, uint64_t &optcount, int tid, int trd) {
            size_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (int i = tid % loads.size(); i < loads.size(); i += trd) {
                    core[(loads.at(i) % unique_keys) * ALIGN_CNT].fetch_add(1);
                    tick++;
                    if (tick % 100000 == 0 && stopMeasure.load() == 1) break;
                }
            }
            optcount = tick;
        }, ref(loads), ref(opcounts[i]), i, trd));
    }
    while (timer.elapsedSeconds() < test_duration) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < trd; i++) {
        workers.at(i).join();
        if (i > 0) opcounts[0] += opcounts[i];
    }
    cout << "FA:\t" << trd << "\t" << opcounts[0] << "\t" << timer.elapsedMilliseconds() << endl;
}

void primitiveCAS(vector<uint64_t> &loads, size_t trd) {
    for (int i = 0; i < unique_keys; i++) core[i * ALIGN_CNT].store(0);
    vector<thread> workers;
    stopMeasure.store(0, memory_order_relaxed);
    uint64_t opcounts[trd];
    std::memset(opcounts, 0, sizeof(uint64_t) * trd);
    cout << loads.size() << "\t";
    Timer timer;
    timer.start();
    for (int i = 0; i < trd; i++) {
        workers.push_back(thread([](vector<uint64_t> &loads, uint64_t &optcount, int tid, int trd) {
            size_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (int i = tid % loads.size(); i < loads.size(); i += trd) {
                    size_t idx = (loads.at(i) % unique_keys) * ALIGN_CNT;
                    uint64_t old;
                    do {
                        old = core[idx].load();
                    } while (!core[idx].compare_exchange_strong(old, old + 1));
                    tick++;
                    if (tick % 100000 == 0 && stopMeasure.load() == 1) break;
                }
            }
            optcount = tick;
        }, ref(loads), ref(opcounts[i]), i, trd));
    }
    while (timer.elapsedSeconds() < test_duration) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < trd; i++) {
        workers.at(i).join();
        if (i > 0) opcounts[0] += opcounts[i];
    }
    cout << "CAS:\t" << trd << "\t" << opcounts[0] << "\t" << timer.elapsedMilliseconds() << endl;
}

void primitiveSpin(vector<uint64_t> &loads, size_t trd) {
    std::memset(nore, 0, sizeof(uint64_t) * (unique_keys * ALIGN_CNT));
    spin_mutex *locks = new spin_mutex[unique_keys * ALIGN_CNT];
    for (int i = 0; i < unique_keys; i++) {
        core[i].store(0);
        locks->unlock();
    }
    vector<thread> workers;
    stopMeasure.store(0, memory_order_relaxed);
    uint64_t opcounts[trd];
    std::memset(opcounts, 0, sizeof(uint64_t) * trd);
    cout << loads.size() << "\t";
    Timer timer;
    timer.start();
    for (int i = 0; i < trd; i++) {
        workers.push_back(thread([](vector<uint64_t> &loads, spin_mutex *&locks, uint64_t &optcount, int tid, int trd) {
            size_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (int i = tid % loads.size(); i < loads.size(); i += trd) {
                    size_t idx = (loads.at(i) % unique_keys) * ALIGN_CNT;
                    locks[idx].lock();
                    nore[idx]++;
                    locks[idx].unlock();
                    tick++;
                    if (tick % 100000 == 0 && stopMeasure.load() == 1) break;
                }
            }
            optcount = tick;
        }, ref(loads), ref(locks), ref(opcounts[i]), i, trd));
    }
    while (timer.elapsedSeconds() < test_duration) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < trd; i++) {
        workers.at(i).join();
        if (i > 0) opcounts[0] += opcounts[i];
    }
    cout << "Spin:\t" << trd << "\t" << opcounts[0] << "\t" << timer.elapsedMilliseconds() << endl;
    delete[] locks;
}

void primitiveMutex(vector<uint64_t> &loads, size_t trd) {
    std::memset(nore, 0, sizeof(uint64_t) * (unique_keys * ALIGN_CNT));
    mutex *locks = new mutex[unique_keys * ALIGN_CNT];
    for (int i = 0; i < unique_keys; i++) {
        core[i].store(0);
        //locks->unlock();
    }
    vector<thread> workers;
    stopMeasure.store(0, memory_order_relaxed);
    uint64_t opcounts[trd];
    std::memset(opcounts, 0, sizeof(uint64_t) * trd);
    cout << loads.size() << "\t";
    Timer timer;
    timer.start();
    for (int i = 0; i < trd; i++) {
        workers.push_back(thread([](vector<uint64_t> &loads, mutex *&locks, uint64_t &optcount, int tid, int trd) {
            size_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (int i = tid % loads.size(); i < loads.size(); i += trd) {
                    size_t idx = (loads.at(i) % unique_keys) * ALIGN_CNT;
                    locks[idx].lock();
                    nore[idx]++;
                    locks[idx].unlock();
                    tick++;
                    if (tick % 100000 == 0 && stopMeasure.load() == 1) break;
                }
            }
            optcount = tick;
        }, ref(loads), ref(locks), ref(opcounts[i]), i, trd));
    }
    while (timer.elapsedSeconds() < test_duration) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < trd; i++) {
        workers.at(i).join();
        if (i > 0) opcounts[0] += opcounts[i];
    }
    cout << "Mutex:\t" << trd << "\t" << opcounts[0] << "\t" << timer.elapsedMilliseconds() << endl;
    delete[] locks;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        max_count = std::atol(argv[1]);
        test_duration = std::atol(argv[2]);
    }
    for (int i = 1; i <= 10000000; i *= 100) {
        generate(i);
        for (int t = 1; t <= 100; t += 3) primitiveFA(hkeys, t);
        for (int t = 1; t <= 100; t += 3) primitiveCAS(hkeys, t);
        for (int t = 1; t <= 100; t += 3) primitiveSpin(hkeys, t);
        for (int t = 1; t <= 100; t += 3) primitiveMutex(hkeys, t);
    }

    for (int i = 1; i <= 1000000; i *= 100) {
        generate(i);
        for (int t = 1; t <= 100; t += 3) primitiveFA(lkeys, t);
        for (int t = 1; t <= 100; t += 3) primitiveCAS(lkeys, t);
        for (int t = 1; t <= 100; t += 3) primitiveSpin(lkeys, t);
        for (int t = 1; t <= 100; t += 3) primitiveMutex(lkeys, t);
    }
}