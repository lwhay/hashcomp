#include <iostream>
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

#define ITERATION_MODE      0
#define PERMUTATE_MODE      1
#define REPLICATE_MODE      2

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

int resize_round = 1;

store_t *store;

const int max_iter_per_round = 1;

bool oneshotresize = false;

int operatemode = ITERATION_MODE;

uint64_t *loads;

long total_time;

long insert_time;

int iter_round = 0;

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
    bool type;
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
        parms[i].type = false;
        parms[i].store = store;
    }
}

void finish() {
    cout << "finish" << endl;
    delete[] parms;
    delete[] workers;
}

void *operateWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    uint64_t hit = 0;
    uint64_t fail = 0;
    store->StartSession();
    for (int i = 0; i < total_count; i++) {
        int idx = -1;
        switch (operatemode) {
            case ITERATION_MODE:
                if (i < total_count / thread_number) {
                    idx = work->tid * total_count / thread_number + i;
                }
                break;
            case PERMUTATE_MODE:
                if (i < total_count / thread_number) {
                    idx = work->tid + i * thread_number;
                }
                break;
            case REPLICATE_MODE:
                idx = i;
                break;
            default:
                break;
        }
        if (idx == -1) {
            break;
        }
        if (work->type) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            UpsertContext context{loads[idx], loads[idx]};
            Status stat = store->Upsert(context, callback, 1);
            if (iter_round == 0 && work->type && stat == Status::Ok)
                hit++;
            else
                fail++;
        } else {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<DeleteContext> context(ctxt);
            };
            DeleteContext context{loads[idx]};
            store->Delete(context, callback, 1);
        }
    }

    long elipsed = tracer.getRunTime();
    if (iter_round == 0)
        output[work->tid] << "\t" << work->tid << " " << iter_round << " " << elipsed << " " << hit << endl;
    if (work->type) {
        __sync_fetch_and_add(&insert_time, elipsed);
    }
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&success, hit);
    __sync_fetch_and_add(&failure, fail);
    store->StopSession();
}

void scheduleWorkers() {
    insert_time = 0;
    total_time = 0;
    success = 0;
    failure = 0;
    Tracer tracer;
    tracer.startTime();
    iter_round = 0;
    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = 0; i < thread_number; i++) {
            parms[i].type = !parms[i].type;
            pthread_create(&workers[i], nullptr, operateWorker, &parms[i]);
        }
        for (int i = 0; i < thread_number; i++) {
            pthread_join(workers[i], nullptr);
        }
        iter_round++;
    }
}

void operateWorkers() {
    output = new stringstream[thread_number];
    Tracer tracer;
    tracer.startTime();
    stopMeasure.store(0, memory_order_relaxed);
    Timer timer;
    timer.start();
    store->StartSession();
    thread t = thread(scheduleWorkers);
    while (timer.elapsedSeconds() < default_timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    t.join();
    store->StopSession();
    cout << "\tTill SessionStop " << tracer.fetchTime() << endl;
    cout << "Gathering ..." << endl;
    for (int t = 0; t < thread_number; t++) {
        string outstr = output[t].str();
        cout << outstr;
    }
    double total_tpt = (double) (success + failure) * thread_number / total_time;
    double insert_tpt = 0;
    double delete_tpt = 0;
    if (iter_round % 2 == 0) {
        insert_tpt = (double) (success + failure) / 2 * thread_number / insert_time;
        delete_tpt = (double) (success + failure) / 2 * thread_number / (total_time - insert_time);
    } else {
        insert_tpt = (double) ((success + failure) / 2 + 1) * thread_number / insert_time;
        delete_tpt = (double) (success + failure) / 2 * thread_number / (total_time - insert_time);
    }
    cout << "Total round: " << iter_round << " " << store->Size() << " " << tracer.getRunTime() << endl;
    cout << "insert time: " << insert_time << " delete time: " << (total_time - insert_time) << endl;
    cout << "operations: " << success << " failure: " << failure << endl;
    cout << "total tpt: " << total_tpt << " insert tpt: " << insert_tpt << " delete tpt: " << delete_tpt << endl;
    delete[] output;
}

bool resizeStore(int max_resize = std::numeric_limits<int>::max()) {
    Tracer tracer;
    bool resized = false;
    for (int d = 1; init_size <= total_count * 2 & d <= max_resize; init_size *= 2, d++) {
        resized = true;
        tracer.startTime();
        store->StartSession();
        cout << "\tTill SessionStart " << tracer.fetchTime() << endl;
        static std::atomic<bool> grow_done{false};
        auto callback = [](uint64_t new_size) {
            grow_done = true;
        };
        store->GrowIndex(callback);
        while (!grow_done) {
            store->Refresh();
            std::this_thread::yield();
        }
        cout << "\tTill SessionStop " << tracer.fetchTime() << endl;
        store->StopSession();
        cout << "Resize round: " << resize_round++ << " keyspace: " << init_size << " storesize: " << store->Size()
             << " elipsed: " << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    }
    return resized;
}

int main(int argc, char **argv) {
    if (argc > 4) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        operatemode = std::atoi(argv[4]);
    }
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " type: "
         << operatemode << endl;
    Tracer tracer;
    tracer.startTime();
    store = new store_t{init_size, 17179869184, "storage"};
    cout << "Before insertion: " << -1 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count);
    prepare();
    for (int r = 0; r < max_iter_per_round; r++) {
        operateWorkers();
    }
    cout << "After insertion: " << 0 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    while (true) {
        int resized;
        if (oneshotresize)
            resized = resizeStore();
        else
            resized = resizeStore(1);
        if (!resized)
            break;
        for (int r = 0; r < max_iter_per_round; r++) {
            operateWorkers();
        }
    }
    free(loads);
    finish();
    delete store;
    return 0;
}