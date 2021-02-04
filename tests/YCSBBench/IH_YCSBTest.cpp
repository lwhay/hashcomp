//
// Created by iclab on 2/1/21.
//

#include <iostream>
#include <sstream>
#include <chrono>
#include <cstring>
#include <unordered_set>
#include <vector>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <bitset>
#include "tracer.h"
#include "ichash/new_map.hh"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 20)

#define COUNT_HASH         1

using namespace ycsb;

constexpr uint32_t kHashSeed = 7079;

uint64_t MurmurHash64A(const void *key, size_t len) {
    const uint64_t m = 0xc6a4a7935bd1e995ull;
    const size_t r = 47;
    uint64_t seed = kHashSeed;

    uint64_t h = seed ^(len * m);

    const auto *data = (const uint64_t *) key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const auto *data2 = (const unsigned char *) data;

    switch (len & 7ull) {
        case 7:
            h ^= uint64_t(data2[6]) << 48ull;
        case 6:
            h ^= uint64_t(data2[5]) << 40ull;
        case 5:
            h ^= uint64_t(data2[4]) << 32ull;
        case 4:
            h ^= uint64_t(data2[3]) << 24ull;
        case 3:
            h ^= uint64_t(data2[2]) << 16ull;
        case 2:
            h ^= uint64_t(data2[1]) << 8ull;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

template<typename T>
struct str_equal_to {
    bool operator()(const T first, T second) {
        if (std::strcmp(first, second) == 0) return true;
        else return false;
    }
};

template<typename T>
struct str_hash {
    std::size_t operator()(const T &str) const noexcept {
        return MurmurHash64A((char *) str, std::strlen(str));
    }
};

typedef libcuckoo::new_cuckoohash_map cmap;

cmap *store;

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

int root_capacity = (25);

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int erasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - erasePercentage);

struct target {
    int tid;
    cmap *store;
};

pthread_t *workers;

struct target *parms;

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < key_range; i++, inserted++) {
        // cout << inserted << endl;
        store->insert_or_assign(loads[i]->getKey(), loads[i]->keyLength(), loads[i]->getVal(), loads[i]->valLength());
    }
    cout << inserted << " " << tracer.getRunTime() << " " << store->get_item_num() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = work->tid * key_range / thread_number; i < (work->tid + 1) * key_range / thread_number; i++) {
        store->insert_or_assign(loads[i]->getKey(), loads[i]->keyLength(), loads[i]->getVal(), loads[i]->valLength());
        inserted++;
    }
    __sync_fetch_and_add(&exists, inserted);
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    libcuckoo::cuckoo_thread_id = work->tid;
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    try {
        while (stopMeasure.load(memory_order_relaxed) == 0) {
            for (int i = work->tid * total_count / thread_number;
                 i < (work->tid + 1) * total_count / thread_number; i++) {
                switch (static_cast<int>(runs[i]->getOp())) {
                    case 0: {
                        YCSB_request *req = runs[i];
                        bool ret = store->find(runs[i]->getKey(), runs[i]->keyLength());
                        if (ret) rhit++;
                        else rfail++;
                        break;
                    }
                    case 1: {
                        bool ret = store->insert_or_assign(runs[i]->getKey(), runs[i]->keyLength(), runs[i]->getVal(),
                                                           runs[i]->valLength());
                        mhit++;
                        break;
                    }
                    case 2: {

                        bool ret = store->erase(runs[i]->getKey(), runs[i]->keyLength());
                        if (ret) mhit++;
                        else mfail++;
                        break;
                    }
                    case 3: {
                        bool ret = store->insert_or_assign(runs[i]->getKey(), runs[i]->keyLength(), runs[i]->getVal(),
                                                           runs[i]->valLength());
                        mhit++;
                        break;
                    }
                    default:
                        break;
                }
            }
        }
    } catch (exception e) {
        //cout << work->tid << endl;
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << mfail << " " << rhit << " " << rfail
                      << endl;
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
        erasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - erasePercentage;
    }
    if (argc > 8)
        root_capacity = std::atoi(argv[8]);
    uint64_t power = 0, root_size = root_capacity;
    do { power++; } while (root_size = (root_size >> 1));
    cmap tmp(25);
    cmap engine(1);
    store = &engine;
    store->swap(tmp);
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
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << erasePercentage << ":"
         << readPercentage << " item: " << store->get_item_num() << " capacity: " << store->bucket_num() << endl;
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << " hash size: " << " item: " << store->get_item_num() << " capacity: " << store->bucket_num() << endl;
    loads.clear();
    runs.clear();
    finish();
    //delete mhash;
    return 0;
}
