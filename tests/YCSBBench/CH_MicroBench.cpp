//
// Created by iclab on 10/12/19.
//

#include <iostream>
#include <sstream>
#include <chrono>
#include <cstring>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_set>
#include "tracer.h"
#include "libcuckoo/cuckoohash_map.hh"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define COUNT_HASH         1

typedef libcuckoo::cuckoohash_map<char *, char *, std::hash<char *>, std::equal_to<char *>,
        std::allocator<std::pair<const char *, char *>>, 8> cmap;

cmap *store;

uint64_t *loads;

long total_time;

uint64_t exists = 0;

uint64_t read_success = 0, modify_success = 0;

uint64_t read_failure = 0, modify_failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

uint64_t timer_range = default_timer_range;

int thread_number = DEFAULT_THREAD_NUM;

int key_range = DEFAULT_KEYS_RANGE;

double skew = 0.0;

int root_capacity = (1 << 16);

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int ereasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - ereasePercentage);

struct target {
    int tid;
    uint64_t *insert;
    cmap *store;
};

pthread_t *workers;

struct target *parms;

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    unordered_set<uint64_t> set;
    for (int i = 0; i < total_count; i++) {
        store->insert((char *) &loads[i], (char *) &loads[i]);
        set.insert(loads[i]);
        inserted++;
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = work->tid * total_count / thread_number; i < (work->tid + 1) * total_count / thread_number; i++) {
        store->insert((char *) &loads[i], (char *) &loads[i]);
        inserted++;
    }
    __sync_fetch_and_add(&exists, inserted);
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    int evenRound = 0;
    uint64_t inserts = 0;
    uint64_t ereased = 0;
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
                if (updatePercentage > 0 && i % (totalPercentage / updatePercentage) == 0) {
                    bool ret = store->update((char *) &loads[i], (char *) &loads[i]);
                    if (ret) mhit++;
                    else mfail++;
                } else if (ereasePercentage > 0 && (i + 1) % (totalPercentage / ereasePercentage) == 0) {
                    bool ret;
                    if (evenRound % 2 == 0) {
                        uint64_t key = inserts++ + (work->tid + 1) * key_range + evenRound / 2;
                        ret = store->insert((char *) &key, (char *) &key);
                    } else {
                        uint64_t key = ereased++ + (work->tid + 1) * key_range + evenRound / 2;
                        ret = store->erase((char *) &key);
                    }
                    if (ret) mhit++;
                    else mfail++;
                } else {
                    char *value = store->find((char *) &loads[i]);
                    if (std::strcmp(value, (char *) &loads[i]) == 0) rhit++;
                    else rfail++;
                }
            }
            if (evenRound++ % 2 == 0) ereased = 0;
            else inserts = 0;
        }
    } catch (exception e) {
        cout << work->tid << ":" << ereased << endl;
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&read_success, rhit);
    __sync_fetch_and_add(&read_failure, rfail);
    __sync_fetch_and_add(&modify_success, mhit);
    __sync_fetch_and_add(&modify_failure, mfail);
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
    Timer timer;
    timer.start();
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, measureWorker, &parms[i]);
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
    if (argc > 7) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        skew = std::atof(argv[5]);
        updatePercentage = std::atoi(argv[6]);
        ereasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - ereasePercentage;
    }
    if (argc > 8)
        root_capacity = std::atoi(argv[8]);
    store = new cmap(root_capacity);
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << ereasePercentage << ":"
         << readPercentage << endl;
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, skew);
    prepare();
    cout << "simple" << endl;
    simpleInsert();
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << " hash size: " << store->bucket_count() << " capacity: " << store->capacity() << " load factor: "
         << store->load_factor() << endl;
    free(loads);
    finish();
    //delete mhash;
    return 0;
}