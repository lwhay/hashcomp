#include <iostream>
#include <sstream>
#include "tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include "faster.h"
#include "kvcontext.h"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 20)

#define DEFAULT_STR_LENGTH 256
//#define DEFAULT_KEY_LENGTH 8

#define TEST_LOOKUP        1

#define DEFAULT_STORE_BASE 10000LLU

using namespace FASTER::api;

#ifdef _WIN32
typedef hreadPoolIoHandler handler_t;
#else
typedef QueueIoHandler handler_t;
#endif
typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

using store_t = FasterKv<Key, Value, disk_t>;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t store{init_size, 17179869184, "storage"};

uint64_t *loads;

long total_time;

uint64_t exists = 0;

uint64_t success = 0;

uint64_t failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

int thread_number = DEFAULT_THREAD_NUM;

int key_range = DEFAULT_KEYS_RANGE;

stringstream *output;

atomic<int> stopMeasure(0);

struct target {
    int tid;
    uint64_t *insert;
    store_t *store;
};

pthread_t *workers;

struct target *parms;


void prepare() {
    cout << "prepare" << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        parms[i].store = &store;
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
}

void *insertWorker(void *args) {
    //struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    store.StartSession();
    for (int i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{loads[i], loads[i]};
        Status stat = store.Upsert(context, callback, 1);
        inserted++;
    }
    __sync_fetch_and_add(&exists, inserted);
    store.StopSession();
}

void initWorkers() {
    Tracer tracer;
    tracer.startTime();
    store.StartSession();
    cout << "\tTill SessionStart " << tracer.fetchTime() << endl;
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
    }
    store.StopSession();
    cout << "\tTill SessionStop " << tracer.fetchTime() << endl;
    cout << "Insert " << exists << " " << tracer.getRunTime() << endl;
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    uint64_t hit = 0;
    uint64_t fail = 0;
    store.StartSession();
    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = 0; i < total_count; i++) {
#if TEST_LOOKUP
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            ReadContext context{loads[i]};

            Status result = store.Read(context, callback, 1);
            if (result == Status::Ok)
                hit++;
            else
                fail++;
#else

            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            UpsertContext context{loads[i], loads[i]};
            Status stat = store.Upsert(context, callback, 1);
            if (stat == Status::NotFound)
                fail++;
            else
                hit++;
#endif
        }
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << "\t" << work->tid << " " << elipsed << " " << hit << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&success, hit);
    __sync_fetch_and_add(&failure, fail);
    store.StopSession();
}

void multiWorkers() {
    output = new stringstream[thread_number];
    total_time = 0;
    success = 0;
    failure = 0;
    Tracer tracer;
    tracer.startTime();
    stopMeasure.store(0, memory_order_relaxed);
    Timer timer;
    timer.start();
    store.StartSession();
    cout << "\tTill SessionStart " << tracer.fetchTime() << endl;
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
    store.StopSession();
    cout << "\tTill SessionStop " << tracer.fetchTime() << endl;
    cout << "Gathering ..." << endl;
    cout << "\tRound 0: " << store.Size() << " " << tracer.getRunTime() << endl;
    for (int d = 1; init_size <= total_count * 2; init_size *= 2, d++) {
        tracer.startTime();
        store.StartSession();
        cout << "\t\tTill SessionStart " << tracer.fetchTime() << endl;
        static std::atomic<bool> grow_done{false};
        auto callback = [](uint64_t new_size) {
            grow_done = true;
        };
        store.GrowIndex(callback);
        while (!grow_done) {
            store.Refresh();
            std::this_thread::yield();
        }
        cout << "\t\tTill SessionStop " << tracer.fetchTime() << endl;
        store.StopSession();
        cout << "\tRound " << d << ": " << store.Size() << " " << tracer.getRunTime() << endl;
    }
    cout << "operations: " << success << " failure: " << failure << " throughput: "
         << (double) (success + failure) * thread_number / total_time << endl;
    delete[] output;
}

int main(int argc, char **argv) {
    if (argc > 3) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
    }
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << endl;
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count);
    prepare();
    cout << "multiinsert" << endl;
    initWorkers();
    cout << "operations: " << success << " failure: " << failure << " throughput: "
         << (double) (success + failure) * thread_number / total_time << endl;
    for (int r = 0; r < 3; r++) {
        multiWorkers();
    }
    free(loads);
    finish();
    return 0;
}