//
// Created by Michael on 1/4/20.
//

#include <iostream>
#include <sstream>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_set>
#include "tracer.h"
#include "concurrent_hash_map.h"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
//#define DEFAULT_KEY_LENGTH 8
#define PARTIAL_DATA       2   // 1: < 65536; 2: >= 65536; 0: ALL
#define REDO_INCASEOF_FAIL 0
#if REDO_INCASEOF_FAIL

#define CACHE_SIZE (1llu << 8)
thread_local uint64_t cache[CACHE_SIZE];

#endif

#define COUNT_HASH         1

struct Value {
    uint64_t value;
public:
    Value(uint64_t v) : value(v) {}

    ~Value() {}

    uint64_t get() { return value; }
};

typedef ConcurrentHashMap<uint64_t, /*Value **/uint64_t, std::hash<uint64_t>, std::equal_to<>> maptype;

maptype *store;

uint64_t *loads;

long total_time;

uint64_t exists = 0;

uint64_t success = 0;

uint64_t failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

uint64_t timer_range = default_timer_range;

int thread_number = DEFAULT_THREAD_NUM;

int key_range = DEFAULT_KEYS_RANGE;

double skew = 0.0;

int root_capacity = (1 << 16);

stringstream *output;

atomic<int> stopMeasure(0);

struct target {
    int tid;
    uint64_t *insert;
    maptype *store;
};

pthread_t *workers;

struct target *parms;

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    unordered_set<uint64_t> set;
    for (int i = 0; i < total_count; i++) {
        store->Insert(loads[i], loads[i]/*new Value(loads[i])*/);
        set.insert(loads[i]);
        inserted++;
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
}

void *insertWorker(void *args) {
    //struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = 0; i < total_count; i++) {
        store->Insert(loads[i], loads[i]/*new Value(loads[i])*/);
        inserted++;
    }
    __sync_fetch_and_add(&exists, inserted);
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    uint64_t hit = 0;
    uint64_t fail = 0;
#if REDO_INCASEOF_FAIL
    uint64_t cursor = 0;
#endif
    try {
        while (stopMeasure.load(memory_order_relaxed) == 0) {
#if INPUT_METHOD == 0
            for (int i = 0; i < total_count; i++) {
#elif INPUT_METHOD == 1
                for (int i = work->tid; i < total_count; i += thread_number) {
#else
                for (int i = work->tid * total_count / thread_number;
                     i < (work->tid + 1) * total_count / thread_number; i++) {
#endif
#if TEST_LOOKUP
                uint64_t /*Value **/value;
#if PARTIAL_DATA == 1
                if (loads[i] < 65536) {
#elif PARTIAL_DATA == 2
                if (loads[i] >= 65536) {
#endif
#ifndef DISABLE_FAST_TABLE
                    maptype::AsyncReturnCode ret = store->AsyncFind(loads[i], value);
#if REDO_INCASEOF_FAIL
                    if (maptype::AsyncReturnCode::Pending == ret) {
                        if (cursor == CACHE_SIZE) {
                            for (int j = 0; j < CACHE_SIZE; j++) {
                                bool reret = store->Find(cache[j], value);
                                if (reret && value == cache[j]) hit++;
                                else fail++;
                            }
                            cursor = 0;
                        }
                        cache[cursor] = loads[i];
                        cursor++;
                    } else if (maptype::AsyncReturnCode::Ok == ret) hit++;
                    else fail++;
#else
                    if (ret == maptype::AsyncReturnCode::Ok && value/*->get()*/ == loads[i])
                        hit++;
                    else
                        fail++;
#endif
#else
                    bool ret = store->Find(loads[i], value);
                    if (ret && value == loads[i]) hit++;
                    else fail++;
#endif
#if PARTIAL_DATA > 0
                }
#endif
#else
#ifndef DISABLE_FAST_TABLE
                maptype::AsyncReturnCode ret = store->AsyncInsert(loads[i], loads[i]);
#if REDO_INCASEOF_FAIL
                if (maptype::AsyncReturnCode::Pending == ret) {
                    if (cursor == CACHE_SIZE) {
                        for (int j = 0; j < CACHE_SIZE; j++) {
                            bool reret = store->Insert(cache[j], cache[j]);
                            if (reret) hit++;
                            else fail++;
                        }
                        cursor = 0;
                    }
                    cache[cursor] = loads[i];
                    cursor++;
                } else if (maptype::AsyncReturnCode::Ok == ret) hit++;
                else fail++;
#else
                if (ret == maptype::AsyncReturnCode::Ok)
                    hit++;
                else
                    fail++;
#endif
#else
                bool ret = store->Insert(loads[i], loads[i]/*new Value(loads[i])*/);
                if (ret)
                    hit++;
                else
                    fail++;
#endif
#endif
            }
        }
#ifndef DISABLE_FAST_TABLE
#if REDO_INCASEOF_FAIL
        for (int i = 0; i < cursor; i++) {
#if TEST_LOOKUP
            uint64_t value;
            bool reret = store->Find(cache[i], value);
            if (reret && value == cache[i]) hit++;
            else fail++;
#else
            bool reret = store->Insert(cache[i], cache[i]);
            if (reret) hit++;
            else fail++;
#endif
        }
#endif
#endif
    } catch (exception e) {
        cout << work->tid << endl;
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << hit << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&success, hit);
    __sync_fetch_and_add(&failure, fail);
}

void prepare() {
    cout << "prepare" << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        parms[i].store = store;
        parms[i].insert = (uint64_t *) calloc(total_count / thread_number, sizeof(uint64_t *));
        char buf[DEFAULT_STR_LENGTH];
        for (int j = 0; j < total_count / thread_number; j++) {
            std::sprintf(buf, "%d", i + j * thread_number);
            parms[i].insert[j] = j;
        }
    }
}

void finish() {
    cout << "finish" << endl;
    for (int i = 0; i < thread_number; i++) {
        delete[] parms[i].insert;
    }
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void multiWorkers() {
    output = new stringstream[thread_number];
    Tracer tracer;
    tracer.startTime();
    /*for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
    }*/
    cout << "Insert " << exists << " " << tracer.getRunTime() << endl;
    Timer timer;
    timer.start();
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, measureWorker, &parms[i]);
#if WITH_NUMA == 1
        fixedThread(i, workers[i]);
#elif WITH_NUMA == 2
        maskThread(i, workers[i]);
#endif
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
        string outstr = output[i].str();
        cout << outstr;
    }
    cout << "Gathering ..." << endl;
}

int main(int argc, char **argv) {
    if (argc > 5) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        skew = std::stof(argv[5]);
    }
    if (argc > 6)
        root_capacity = std::atoi(argv[6]);
    store = new maptype(root_capacity, 20, thread_number);
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << endl;
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, skew);
    prepare();
    cout << "simple" << endl;
    simpleInsert();
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "operations: " << success << " failure: " << failure << " throughput: "
         << (double) (success /*+ failure*/) * thread_number / total_time << endl;
    free(loads);
    finish();
    delete store;
    return 0;
}