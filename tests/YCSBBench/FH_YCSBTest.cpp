#include <iostream>
#include "tracer.h"
#include <stdio.h>
#include <stdlib.h>
#include "faster.h"

#define SERIALIZABLE_CONTEXT

#ifdef SERIALIZABLE_CONTEXT

#include "serializablecontext.h"

#else

#include "stringcontext.h"

#endif

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define DEFAULT_STR_LENGTH 256
#define DEFAULT_KEY_LENGTH 8

using namespace FASTER::api;

#ifdef _WIN32
typedef hreadPoolIoHandler handler_t;
#else
typedef QueueIoHandler handler_t;
#endif
typedef FileSystemDisk<handler_t, 68719476736ull> disk_t;

using store_t = FasterKv<Key, Value, disk_t>;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t *store;

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

int key_range = DEFAULT_KEYS_RANGE;

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int ereasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - ereasePercentage);

struct target {
    int tid;
    store_t *fmap;
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
        parms[i].fmap = store;
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
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{Key((uint8_t *) loads[i]->getKey(), std::strlen(loads[i]->getKey())),
                              Value((uint8_t *) loads[i]->getVal(), std::strlen(loads[i]->getVal()))};
        Status stat = store->Upsert(context, callback, 1);
        if (stat == Status::Ok) {
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
#if LIGHT_CT_COPY == 1
    ReadContext rc{Key((uint8_t *) runs[0]->getKey(), std::strlen(runs[0]->getKey()))};
    UpsertContext uc{Key((uint8_t *) runs[0]->getKey(), std::strlen(runs[0]->getKey())),
                     Value(value, DEFAULT_STR_LENGTH)};
#endif
    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = work->tid * total_count / thread_number;
             i < (work->tid + 1) * total_count / thread_number; i++) {
            switch (static_cast<int>(runs[i]->getOp())) {
                case 0: {
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<ReadContext> context{ctxt};
                    };
#if LIGHT_CT_COPY == 1
                    Key key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()));
                    rc.init(key);
#else
                    ReadContext rc{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
#endif
                    Status stat = store->Read(rc, callback, 1);
                    if (stat == Status::Ok) rhit++;
                    else rfail++;
                    break;
                }
                case 1:
                case 3: {
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<UpsertContext> context{ctxt};
                    };
#if LIGHT_CT_COPY == 1
                    Key key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()));
                    Value value((uint8_t *) runs[i]->getVal(), std::strlen(runs[i]->getVal()));
                    uc.init(key, value);
#else
                    UpsertContext uc{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey())),
                                     Value((uint8_t *) runs[i]->getVal(), std::strlen(runs[i]->getVal()))};
#endif
                    Status stat = store->Upsert(uc, callback, 1);
                    if (stat == Status::Ok) mhit++;
                    else mfail++;
                    break;
                }
                case 2: {
                    auto callback = [](IAsyncContext *ctxt, Status result) {
                        CallbackContext<DeleteContext> context{ctxt};
                    };
                    DeleteContext context{Key((uint8_t *) runs[i]->getKey(), std::strlen(runs[i]->getKey()))};
                    Status stat = store->Delete(context, callback, 1);
                    if (stat == Status::Ok) mhit++;
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
#ifdef TRACE
    size_t counter = 0;
#endif
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
#ifdef TRACE
        counter += store->GetConflict(i);
        cout << store->GetConflict(i) << " " << counter << " ";
#endif
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
    store = new store_t(next_power_of_two(root_capacity / 2), 68719476736ull, "storage");
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
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << ereasePercentage << ":"
         << readPercentage << endl;
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    loads.clear();
    runs.clear();
    delete store;
    return 0;
}