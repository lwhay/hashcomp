//
// Created by Michael on 2/21/20.
//

#include <iostream>
#include <sstream>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unordered_set>
#include "tracer.h"
#include "dict/concurrent_dict.h"
#include "dict/slice.h"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 20)

#define INPLACE            0

#define COUNT_HASH         1

struct Value {
    uint64_t value;
public:
    Value(uint64_t v) : value(v) {}

    ~Value() {}

    uint64_t get() { return value; }
};

using namespace concurrent_dict;

typedef ConcurrentDict maptype;

maptype *store;

uint64_t *loads;

long total_time;

uint64_t exists = 0;

uint64_t read_success = 0, modify_success = 0;

uint64_t read_failure = 0, modify_failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

uint64_t timer_range = default_timer_range;

int thread_number = DEFAULT_THREAD_NUM;

size_t key_range = DEFAULT_KEYS_RANGE;

double skew = 0.0;

int root_capacity = (1 << 16);

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int erasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - erasePercentage);

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
    unordered_set<uint64_t> set;
    unordered_set<string> stset;
    double avglength = .0;
    for (int i = 0; i < total_count; i++) {
#if WITH_STRING == 1
        store->Insert(Slice(string((char *) &loads[i])), Slice(string((char *) &loads[i])));
        stset.insert(string((char *) &loads[i]));
        avglength += string((char *) &loads[i]).size();
#elif  WITH_STRING == 2
        string kv((char *) &loads[i], UNIT_SIZE);
        store->Insert(Slice(kv), Slice(kv));
        stset.insert(kv);
        avglength += kv.size();
#else
        store->Insert(Slice((char *) &loads[i]), Slice((char *) &loads[i]));
#endif
        set.insert(loads[i]);
        inserted++;
    }
    cout << inserted << " " << set.size() << " " << avglength / total_count << " " << tracer.getRunTime() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = work->tid * total_count / thread_number; i < (work->tid + 1) * total_count / thread_number; i++) {
#if WITH_STRING == 1
        store->Insert(Slice(string((char *) &loads[i])), Slice(string((char *) &loads[i])));
#elif WITH_STRING == 2
        store->Insert(Slice(string((char *) &loads[i], UNIT_SIZE)), Slice(string((char *) &loads[i], UNIT_SIZE)));
#else
        store->Insert(Slice((char *) &loads[i]), Slice((char *) &loads[i]));
#endif
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
    uint64_t erased = 0, inserts = 0;
    std::string dummyVal;
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
#if WITH_STRING == 1
                    bool ret = store->Insert(Slice(string((char *) &loads[i])), Slice(string((char *) &loads[i])));
#elif WITH_STRING == 2
                    bool ret = store->Insert(Slice(string((char *) &loads[i], UNIT_SIZE)),
                                             Slice(string((char *) &loads[i], UNIT_SIZE)));
#else
                    bool ret = store->Insert(Slice((char *) &loads[i]), Slice((char *) &loads[i]),
                                             InsertType::MUST_EXIST);
#endif
                    if (ret)
                        mhit++;
                    else
                        mfail++;
                } else if (erasePercentage > 0 && (i + 1) % (totalPercentage / erasePercentage) == 0) {
                    bool ret;
                    if (evenRound % 2 == 0) {
                        uint64_t key = thread_number * inserts++ + work->tid + (evenRound / 2 + 1) * key_range;
#if WITH_STRING == 1
                        ret = store->Insert(Slice(string((char *) &key)), Slice(string((char *) &key)));
#elif WITH_STRING == 2
                        ret = store->Insert(Slice(string((char *) &key, UNIT_SIZE)),
                                            Slice(string((char *) &key, UNIT_SIZE)));
#else
                        ret = store->Insert(Slice((char *) &key, 8), Slice((char *) &key, 8));
#endif
                    } else {
                        uint64_t key = thread_number * erased++ + work->tid + (evenRound / 2 + 1) * key_range;
#if WITH_STRING == 1
                        ret = store->Delete(Slice(string((char *) &key)), &dummyVal);
#elif WITH_STRING == 2
                        ret = store->Delete(Slice(string((char *) &key, UNIT_SIZE)), &dummyVal);
#else
                        ret = store->Delete(Slice((char *) &key, 8), &dummyVal);
#endif
                    }
                    if (ret) mhit++;
                    else mfail++;
                } else {
#if WITH_STRING == 1
                    bool ret = store->Find(Slice(string((char *) &loads[i])), &dummyVal);
                    if (ret && (dummyVal.compare((char *) &loads[i]) == 0))
                        rhit++;
                    else
                        rfail++;
#elif WITH_STRING == 2
                    bool ret = store->Find(Slice(string((char *) &loads[i], UNIT_SIZE)), &dummyVal);
                    if (ret && (dummyVal.compare(string((char *) &loads[i], UNIT_SIZE)) == 0))
                        rhit++;
                    else
                        rfail++;
#else
                    bool ret = store->Find(Slice((char *) &loads[i]), &dummyVal);
                    if (ret && (dummyVal.compare((char *) &loads[i]) == 0))
                        rhit++;
                    else
                        rfail++;
#endif
                }
            }
            if (evenRound++ % 2 == 0) erased = 0;
            else inserts = 0;
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
        skew = std::stof(argv[5]);
        updatePercentage = std::atoi(argv[6]);
        erasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - erasePercentage;
    }
    if (argc > 8)
        root_capacity = std::atoi(argv[8]);
    store = new maptype(root_capacity, 20, thread_number);
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << erasePercentage << ":"
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
         << endl;
    free(loads);
    finish();
    delete store;
    return 0;
}