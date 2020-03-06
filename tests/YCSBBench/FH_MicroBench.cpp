#include <iostream>
#include "tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include "faster.h"


#include "stringcontext.h"

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 20)

using namespace FASTER::api;

#ifdef _WIN32
typedef hreadPoolIoHandler handler_t;
#else
typedef QueueIoHandler handler_t;
#endif
typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

using store_t = FasterKv<Key, Value, disk_t>;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t *store;

uint64_t *loads;

#if CONTEXT_TYPE == 2
uint64_t *content;
#endif

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

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int ereasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - ereasePercentage);

struct target {
    int tid;
    store_t *store;
};

pthread_t *workers;

struct target *parms;

void simpleInsert() {
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    for (int i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{Key((uint8_t *) &loads[i], UNIT_SIZE), Value((uint8_t *) &loads[i], UNIT_SIZE)};
        Status stat = store->Upsert(context, callback, 1);
        inserted++;
    }
    cout << inserted << " " << tracer.getRunTime() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = work->tid * total_count / thread_number; i < (work->tid + 1) * total_count / thread_number; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{Key((uint8_t *) &loads[i], UNIT_SIZE), Value((uint8_t *) &loads[i], UNIT_SIZE)};
        Status stat = store->Upsert(context, callback, 1);
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
    uint64_t ereased = 0, inserts = 0;
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
                auto callback = [](IAsyncContext *ctxt, Status result) {
                    CallbackContext<UpsertContext> context{ctxt};
                };
                UpsertContext context{Key((uint8_t *) &loads[i], UNIT_SIZE), Value((uint8_t *) &loads[i], UNIT_SIZE)};
                Status stat = store->Upsert(context, callback, 1);
                if (stat == Status::NotFound)
                    mfail++;
                else
                    mhit++;
            } else if (ereasePercentage > 0 && (i + 1) % (totalPercentage / ereasePercentage) == 0) {
                bool ret;
                if (evenRound % 2 == 0) {
                    uint64_t key = inserts++ + (work->tid + 1) * key_range + evenRound / 2;
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<UpsertContext> context{ctxt};
                    };
                    UpsertContext context{Key((uint8_t *) key, UNIT_SIZE), Value((uint8_t *) key, UNIT_SIZE)};
                    Status stat = store->Upsert(context, callback, 1);
                    ret = (stat == Status::Ok);
                } else {
                    uint64_t key = ereased++ + (work->tid + 1) * key_range + evenRound / 2;
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<DeleteContext> context{ctxt};
                    };
                    DeleteContext context{Key((uint8_t *) key, UNIT_SIZE)};
                    Status stat = store->Delete(context, callback, 1);
                    ret = (stat == Status::Ok);
                }
                ereased++;
                if (ret)
                    mhit++;
                else
                    mfail++;
            } else {
                auto callback = [](IAsyncContext *ctxt, Status result) {
                    CallbackContext<ReadContext> context{ctxt};
                };

                ReadContext context{Key((uint8_t *) &loads[i], UNIT_SIZE)};

                Status result = store->Read(context, callback, 1);
                if (result == Status::Ok /*&& *(uint64_t *) &context.output_bytes == loads[i]*/)
                    rhit++;
                else
                    rfail++;
            }
            if (evenRound++ % 2 == 0) ereased = 0;
            else inserts = 0;
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

void prepare() {
    cout << "prepare" << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        parms[i].store = store;
    }
#if CONTEXT_TYPE == 2
    content = new uint64_t[total_count];
    for (long i = 0; i < total_count; i++) {
        content[i] = total_count - loads[i];
    }
#endif
}

void finish() {
    cout << "finish" << endl;
    delete[] parms;
    delete[] workers;
    delete[] output;
#if CONTEXT_TYPE == 2
    delete[] content;
#endif
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
    init_size = next_power_of_two(root_capacity / 2);
    store = new store_t(init_size, 17179869184, "storage");
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
         << endl;
    free(loads);
    finish();
    delete store;
    return 0;
}