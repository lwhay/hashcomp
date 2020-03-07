#include <iostream>
#include <sstream>
#include <cstring>
#include "tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include "level_hashing.h"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
#define DEFAULT_KEY_LENGTH 8

uint8_t *loads;

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

int ereasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - ereasePercentage);

struct target {
    int tid;
    uint8_t **insert;
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
        parms[i].insert = (uint8_t **) calloc(total_count / thread_number, sizeof(uint8_t *));
        char buf[DEFAULT_STR_LENGTH];
        for (int j = 0; j < total_count / thread_number; j++) {
            std::sprintf(buf, "%d", i + j * thread_number);
            parms[i].insert[j] = (uint8_t *) malloc(DEFAULT_KEY_LENGTH * sizeof(uint8_t));
            memcpy(parms[i].insert[j], buf, DEFAULT_KEY_LENGTH);
        }
    }
}

void restart() {
    cout << "\tdestroy" << endl;
    level_destroy(levelHash);
    cout << "\treinit" << endl;
    levelHash = level_init(level_calculate(root_capacity / 16));
    for (int i = 0; i < thread_number; i++) {
        parms[i].levelHash = levelHash;
    }
    cout << "\trestarted" << endl;
}

void finish() {
    cout << "finish" << endl;
    for (int i = 0; i < thread_number; i++) {
        for (int j = 0; j < total_count / thread_number; j++) {
            delete[] parms[i].insert[j];
        }
        delete[] parms[i].insert;
    }
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < total_count; i++) {
        if (!level_insert(levelHash, &loads[i * DEFAULT_KEY_LENGTH], &loads[i * DEFAULT_KEY_LENGTH])) {
            inserted++;
        }
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
    tracer.startTime();
    uint64_t found = 0;
    uint8_t value[DEFAULT_KEY_LENGTH];
    for (int i = 0; i < total_count; i++) {
        found += (0 == level_query(levelHash, &loads[i * DEFAULT_KEY_LENGTH], value));
    }
    cout << found << " " << tracer.getRunTime() << endl;
    tracer.startTime();
    found = 0;
    for (int i = 0; i < total_count; i++) {
        found += (0 == level_delete(levelHash, &loads[i * DEFAULT_KEY_LENGTH]));
    }
    cout << found << " " << tracer.getRunTime() << endl;
}

void uniqueInsert() {
    uint8_t *uniques = (uint8_t *) calloc(DEFAULT_KEY_LENGTH * total_count, sizeof(uint8_t));
    char buf[DEFAULT_STR_LENGTH];
    for (int i = 0; i < total_count; i++) {
        std::sprintf(buf, "%d", i);
        memcpy(uniques + i * DEFAULT_KEY_LENGTH, buf, DEFAULT_KEY_LENGTH);

    }
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < total_count; i++) {
        if (!level_insert(levelHash, &uniques[i * DEFAULT_KEY_LENGTH], &uniques[i * DEFAULT_KEY_LENGTH])) {
            inserted++;
        }
        /*if (false && (i % 100000 == 0 || (i - 10000000) % 100 == 1)) {
            memset(buf, 0, DEFAULT_STR_LENGTH);
            memcpy(buf, &uniques[i * DEFAULT_KEY_LENGTH], DEFAULT_KEY_LENGTH);
            cout << i << " " << buf << " " << uniques[i * DEFAULT_KEY_LENGTH] << endl;
        }*/
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
    tracer.startTime();
    uint64_t found = 0;
    uint8_t value[DEFAULT_KEY_LENGTH];
    for (int i = 0; i < total_count; i++) {
        found += (0 == level_query(levelHash, &uniques[i * DEFAULT_KEY_LENGTH], value));
    }
    cout << found << " " << tracer.getRunTime() << endl;
    tracer.startTime();
    found = 0;
    for (int i = 0; i < total_count; i++) {
        found += (0 == level_delete(levelHash, &uniques[i * DEFAULT_KEY_LENGTH]));
    }
    cout << found << " " << tracer.getRunTime() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t fail = 0;
    for (int i = 0; i < total_count /*/ thread_number*/; i++) { // Here, we found a bug in multi-thread settings.
        if (!level_insert(work->levelHash, &loads[i * DEFAULT_KEY_LENGTH], &loads[i * DEFAULT_KEY_LENGTH])) {
            fail++;
        }
    }
    __sync_fetch_and_add(&exists, fail);
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
    uint8_t value[DEFAULT_KEY_LENGTH];
    char newkey[DEFAULT_KEY_LENGTH];
    char newvalue[DEFAULT_KEY_LENGTH];
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
                int ret = level_update(work->levelHash, &loads[i * DEFAULT_KEY_LENGTH], &loads[i * DEFAULT_KEY_LENGTH]);
                if (ret == 0) mhit++;
                else mfail++;
            } else if (ereasePercentage > 0 && (i + 1) % (totalPercentage / ereasePercentage) == 0) {
                int ret;
                if (evenRound % 2 == 0) {
                    uint64_t key = thread_number * inserts++ + work->tid + (evenRound / 2 + 1) * key_range;
                    std::memset(newkey, '0', DEFAULT_KEY_LENGTH);
                    std::sprintf(newkey, "%llu", key);
                    std::memcpy(newvalue, newkey, DEFAULT_KEY_LENGTH);
                    ret = level_insert(work->levelHash, (uint8_t *) newkey, (uint8_t *) newvalue);
                } else {
                    uint64_t key = thread_number * ereased++ + work->tid + (evenRound / 2 + 1) * key_range;
                    std::memset(newkey, '0', DEFAULT_KEY_LENGTH);
                    std::sprintf(newkey, "%llu", key);
                    level_delete(work->levelHash, (uint8_t *) newkey);
                }
                if (ret == 0) mhit++;
                else mfail++;
            } else {
                int ret = level_query(work->levelHash, &loads[i * DEFAULT_KEY_LENGTH], value);
                if (ret == 0) rhit++;
                else rfail++;
            }
        }
        if (evenRound++ % 2 == 0) ereased = 0;
        else inserts = 0;
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
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < 1/*thread_number*/; i++) {
        pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
    }
    for (int i = 0; i < 1/*thread_number*/; i++) {
        pthread_join(workers[i], nullptr);
    }
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
        ereasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - ereasePercentage;
    }
    if (argc > 8)
        root_capacity = std::atoi(argv[8]);
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << ereasePercentage << ":"
         << readPercentage << endl;
    loads = (uint8_t *) calloc(DEFAULT_KEY_LENGTH * total_count, sizeof(uint8_t));
    RandomGenerator<uint8_t>::generate(loads, DEFAULT_KEY_LENGTH, key_range, total_count, skew);
    levelHash = level_init(level_calculate(root_capacity / 16));
    cout << "simple" << endl;
    simpleInsert();
    prepare();
    cout << "restart" << endl;
    restart();
    cout << "unique" << endl;
    //uniqueInsert();
    //prepare();
    cout << "restart" << endl;
    restart();
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    free(loads);
    level_destroy(levelHash);
    return 0;
}