//
// Created by iclab on 10/12/19.
//

#include <iostream>
#include <sstream>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_set>
#include "tracer.h"
#include "concurrent_hash_map.h"
#include "junction/ConcurrentMap_Leapfrog.h"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
//#define DEFAULT_KEY_LENGTH 8

#define COUNT_HASH         1

struct Value {
    uint64_t value;
public:
    Value(uint64_t v) : value(v) {}

    uint64_t get() { return value; }
};

using namespace ycsb;

typedef junction::ConcurrentMap_Leapfrog<char *, char *> maptype;

maptype *store;

std::vector<YCSB_request *> loads;

std::vector<YCSB_request *> runs;

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
    maptype *store;
};

pthread_t *workers;

struct target *parms;

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < key_range; i++) {
        store->assign(loads[i]->getKey(), loads[i]->getVal());
        inserted++;
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = work->tid * key_range / thread_number; i < (work->tid + 1) * key_range / thread_number; i++) {
        bool ret = false;
        while (!ret) ret = store->assign(loads[i]->getKey(), loads[i]->getVal());
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
    try {
        while (stopMeasure.load(memory_order_relaxed) == 0) {
            for (int i = work->tid * total_count / thread_number;
                 i < (work->tid + 1) * total_count / thread_number; i++) {

                switch (static_cast<int>(runs[i]->getOp())) {
                    case 0: {
                        bool ret = store->get(runs[i]->getKey());
                        if (ret /*&& (dummyVal.compare(runs[i]->getVal()) == 0)*/) rhit++;
                        else rfail++;
                        break;
                    }
                    case 1: {
                        bool ret = store->assign(runs[i]->getKey(), runs[i]->getVal());
                        if (ret) mhit++;
                        else mfail++;
                        break;
                    }
                    case 2: {
                        bool ret = store->erase(runs[i]->getKey());
                        if (ret) mhit++;
                        else mfail++;
                        break;
                    }
                    case 3: {
                        bool ret = store->exchange(runs[i]->getKey(), runs[i]->getVal());
                        if (ret) mhit++;
                        else mfail++;
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    } catch (exception e) {
        cout << work->tid << endl;
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
    }
}

void finish() {
    cout << "finish" << endl;
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void multiWorkers() {
    output = new stringstream[thread_number];
    Tracer tracer;
    tracer.startTime();
    cout << "Insert " << exists << " " << tracer.getRunTime() << endl;
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
    store = new maptype(util::nextPowerOf2(root_capacity));
    YCSBLoader loader(loadpath, key_range);
    loads = loader.load();
    key_range = loader.size();
    prepare();
    cout << "simple" << endl;
    simpleInsert();
    YCSBLoader runner(runpath, total_count);
    runs = runner.load();
    total_count = runner.size();
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << ereasePercentage << ":"
         << readPercentage << endl;
    cout << "multiinsert" << endl;
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    loads.clear();
    runs.clear();
    finish();
    delete store;
    return 0;
}