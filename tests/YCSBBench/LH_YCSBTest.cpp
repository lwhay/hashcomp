#include <iostream>
#include <sstream>
#include <cstring>
#include <unordered_set>
#include "tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include "level_hashing.h"

/* Here, we introduce the following parameters instead of the default ones in level_hashing.h.
 #define KEY_LEN 20
 #define VALUE_LEN 32
 * */

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
#define DEFAULT_KEY_LENGTH 8

std::vector<ycsb::YCSB_request *> loads;

std::vector<ycsb::YCSB_request *> runs;

long total_time;

uint64_t exists = 0;

uint64_t read_success = 0, modify_success = 0;

uint64_t read_failure = 0, modify_failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

uint64_t timer_range = default_timer_range;

double skew = 0.0;

int root_capacity = (1 << 16);

int thread_number = DEFAULT_THREAD_NUM;

size_t key_range = DEFAULT_KEYS_RANGE;

level_hash *levelHash;

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int erasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - erasePercentage);

struct target {
    int tid;
    level_hash *levelHash;
};

pthread_t *workers;

struct target *parms;

using namespace std;

int level_calculate(size_t count) {
    int ret = 0;
    while (count >>= 1) {
        ret++;
    }
    return ret + 1;
}

void prepare() {
    cout << "prepare" << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        parms[i].levelHash = levelHash;
    }
}

void finish() {
    cout << "finish" << endl;
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < key_range; i++) {
        if (level_insert(levelHash, (uint8_t *) loads[i]->getKey(), (uint8_t *) loads[i]->getVal()) == 0) {
            inserted++;
        }
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    uint8_t value[DEFAULT_STR_LENGTH];
    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = work->tid * total_count / thread_number;
             i < (work->tid + 1) * total_count / thread_number; i++) {
            switch (static_cast<int>(runs[i]->getOp())) {
                case 0: {
                    int ret = level_query(work->levelHash, (uint8_t *) runs[i]->getKey(), value);
                    if (ret == 0) rhit++;
                    else rfail++;
                    break;
                }
                case 1: {
                    int ret = level_insert(work->levelHash, (uint8_t *) runs[i]->getKey(),
                                           (uint8_t *) runs[i]->getVal());
                    if (ret == 0) mhit++;
                    else mfail++;
                    break;
                }
                case 2: {
                    int ret = level_delete(work->levelHash, (uint8_t *) runs[i]->getKey());
                    if (ret == 0) mhit++;
                    else mfail++;
                    break;
                }
                case 3: {
                    int ret = level_update(work->levelHash, (uint8_t *) runs[i]->getKey(),
                                           (uint8_t *) runs[i]->getVal());
                    if (ret == 0) mhit++;
                    else mfail++;
                    break;
                }
                default:
                    break;
            }
        }
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&read_success, rhit);
    __sync_fetch_and_add(&read_failure, rfail);
    __sync_fetch_and_add(&modify_success, mhit);
    __sync_fetch_and_add(&modify_failure, mfail);
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
    finish();
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
    levelHash = level_init(level_calculate(root_capacity / 16));
    ycsb::YCSBLoader loader(ycsb::loadpath, key_range);
    loads = loader.load();
    key_range = loader.size();
    prepare();
    cout << "simple" << endl;
    simpleInsert();
    ycsb::YCSBLoader runner(ycsb::runpath, total_count);
    runs = runner.load();
    total_count = runner.size();
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << erasePercentage << ":"
         << readPercentage << endl;
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    loads.clear();
    runs.clear();
    level_destroy(levelHash);
    return 0;
}