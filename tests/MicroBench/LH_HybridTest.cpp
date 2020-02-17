#include <iostream>
#include <sstream>
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

uint64_t success = 0;

uint64_t failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

double skew = 0.0;

int root_capacity = (1 << 16);

int thread_number = DEFAULT_THREAD_NUM;

int key_range = DEFAULT_KEYS_RANGE;

level_hash *levelHash;

stringstream *output;

atomic<int> stopMeasure(0);

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
    levelHash = level_init(level_calculate(key_range));
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
    uint64_t hit = 0;
    uint64_t fail = 0;
    uint8_t value[DEFAULT_KEY_LENGTH];
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
            if (0 == level_query(work->levelHash, &loads[i * DEFAULT_KEY_LENGTH], value)) {
                hit++;
            } else {
                fail++;
            }
#else
            if (0 == level_update(work->levelHash, &loads[i * DEFAULT_KEY_LENGTH], &loads[i * DEFAULT_KEY_LENGTH])) {
                hit++;
            } else {
                fail++;
            }
#endif
        }
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&success, hit);
    __sync_fetch_and_add(&failure, fail);
}

void multiWorkers() {
    output = new stringstream[thread_number];
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < 1/*thread_number*/; i++) {
        pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
#if WITH_NUMA == 1
        fixedThread(i, workers[i]);
#elif WITH_NUMA == 2
        maskThread(i, workers[i]);
#endif
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
    while (timer.elapsedSeconds() < default_timer_range) {
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
    if (argc > 4) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        skew = std::atof(argv[4]);
    }
    if (argc > 6)
        root_capacity = std::atoi(argv[6]);
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " skew: " << skew
         << endl;
    loads = (uint8_t *) calloc(DEFAULT_KEY_LENGTH * total_count, sizeof(uint8_t));
    RandomGenerator<uint8_t>::generate(loads, DEFAULT_KEY_LENGTH, key_range, total_count, skew);
    levelHash = level_init(level_calculate(root_capacity));
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
    cout << "operations: " << success << " failure: " << failure << " throughput: "
         << (double) (success + failure) * thread_number / total_time << endl;
    free(loads);
    level_destroy(levelHash);
    return 0;
}