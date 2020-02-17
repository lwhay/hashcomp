//
// Created by iclab on 9/9/19.
//
#include <iostream>
#include "tracer.h"
#include <stdio.h>
#include "faster.h"

#define CONTEXT_TYPE 1

#if CONTEXT_TYPE == 0

#include "kvcontext.h"

#elif CONTEXT_TYPE == 1

#include "ckvcontext.h"

#elif CONTEXT_TYPE == 2

#include "cvkvcontext.h"

#else

#error "Context not supported"

#endif

#define DEFAULT_STORE_BASE (1LLU << 27)

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

uint64_t total_count = (1LLU << 20);

uint64_t dummy_count = (1LLU << 4);

uint64_t thread_count = 8;

uint64_t total_round = 8;

bool needHit = true;

void *singleInsert(void *args) {
    int tid = *((int *) args);
    for (uint64_t i = tid; i < total_count; i += thread_count) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
#if CONTEXT_TYPE == 0
        UpsertContext context{i, i};
#elif CONTEXT_TYPE == 1
        UpsertContext context(i);
#elif CONTEXT_TYPE == 2
        UpsertContext context(i, sizeof(uint64_t));
#endif
        Status stat = store->Upsert(context, callback, 1);
    }
}

void *singleDelete(void *args) {
    int tid = *((int *) args);
    for (uint64_t i = tid; i < total_count; i += thread_count) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<DeleteContext> context{ctxt};
        };
        Status stat;
        DeleteContext context{i};
        do {
            stat = store->Delete(context, callback, 1);
        } while (stat != Status::Ok);
    }
}

void *singleRead(void *args) {
    int tid = *((int *) args);
    for (uint64_t i = tid; i < total_count; i += thread_count) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
        ReadContext context(i);
        Status stat = store->Read(context, callback, 1);
        if (needHit) {
            assert(stat == Status::Ok);
        } else {
            if (stat != Status::NotFound) {
                cout << i << ":" << Utility::retStatus(stat) << "<->" << Utility::retStatus(Status::NotFound) << endl;
            }
            assert(stat == Status::NotFound);
        }
    }
}

void steppingOperations() {
    Tracer tracer;
    tracer.startTime();
    int tids[thread_count];
    for (uint64_t r = 0; r < total_round; r++) {
        cout << "Round: " << r << endl;
        tracer.startTime();
        cout << "\tBefore insertion" << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
             << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
        pthread_t threads[thread_count];
        tracer.startTime();
        store->StartSession();
        for (int i = 0; i < thread_count; i++) {
            tids[i] = i;
            pthread_create(&threads[i], nullptr, singleInsert, tids + i);
        }
        for (int i = 0; i < thread_count; i++) {
            pthread_join(threads[i], nullptr);
        }
        store->StopSession();
        cout << "\tAfter insertion" << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
             << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
        tracer.startTime();
        store->StartSession();
        needHit = true;
        for (int i = 0; i < thread_count; i++) {
            pthread_create(&threads[i], nullptr, singleRead, tids + i);
        }
        for (int i = 0; i < thread_count; i++) {
            pthread_join(threads[i], nullptr);
        }
        store->StopSession();
        cout << "\tAfter read true" << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
             << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
        tracer.startTime();
        store->StartSession();
        for (int i = 0; i < thread_count; i++) {
            pthread_create(&threads[i], nullptr, singleDelete, tids + i);
        }
        for (int i = 0; i < thread_count; i++) {
            pthread_join(threads[i], nullptr);
        }
        store->StopSession();
        cout << "\tAfter deletion" << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
             << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
        tracer.startTime();
        store->StartSession();
        needHit = false;
        for (int i = 0; i < thread_count; i++) {
            pthread_create(&threads[i], nullptr, singleRead, tids + i);
        }
        for (int i = 0; i < thread_count; i++) {
            pthread_join(threads[i], nullptr);
        }
        store->StopSession();
        cout << "\tAfter read false" << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
             << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    }
}

void *dummyInsert(void *args) {
    for (uint64_t i = 0; i < dummy_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
#if CONTEXT_TYPE == 0
        UpsertContext context{i, i};
#elif CONTEXT_TYPE == 1
        UpsertContext context(i);
#elif CONTEXT_TYPE == 2
        UpsertContext context{i, sizeof(uint64_t)};
#endif
        Status stat = store->Upsert(context, callback, 1);
        //cout << store->Size() << endl;
    }
}

void *dummyDelete(void *args) {
    for (uint64_t i = 0; i < dummy_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<DeleteContext> context{ctxt};
        };
        DeleteContext context{i};
        Status stat = store->Delete(context, callback, 1);
        //cout << store->Size() << endl;
    }
}

void verifyStore() {
    for (uint64_t i = 0; i < dummy_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
        ReadContext context(i);
        Status stat = store->Read(context, callback, 1);
        if (needHit) {
            assert(stat == Status::Ok);
        } else {
            if (stat != Status::NotFound) {
                cout << i << ":" << Utility::retStatus(stat) << "<->" << Utility::retStatus(Status::NotFound) << endl;
            }
            assert(stat == Status::NotFound);
        }
    }
}

void dummyOperations() {
    pthread_t threads[thread_count];
    cout << "Dummy insert:\t\t" << store->Size() << ":" << store->entrySize() << endl;
    for (int i = 0; i < thread_count; i++) {
        pthread_create(&threads[i], nullptr, dummyInsert, nullptr);
    }
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], nullptr);
    }
    cout << "Dummy read true:\t" << store->Size() << ":" << store->entrySize() << endl;
    needHit = true;
    verifyStore();
    cout << "Dummy delete:\t\t" << store->Size() << ":" << store->entrySize() << endl;
    for (int i = 0; i < thread_count; i++) {
        pthread_create(&threads[i], nullptr, dummyDelete, nullptr);
    }
    for (int i = 0; i < thread_count; i++) {
        pthread_join(threads[i], nullptr);
    }
    cout << "Dummy read false:\t" << store->Size() << ":" << store->entrySize() << endl;
    needHit = false;
    verifyStore();
    needHit = true;
    cout << "Dummy complete:\t\t" << store->Size() << ":" << store->entrySize() << endl;
}

int main(int argc, char **argv) {
    if (argc > 3) {
        thread_count = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        total_round = std::atol(argv[3]);
    }
    cout << " threads: " << thread_count << " count: " << total_count << " round: " << total_round << endl;

    Tracer tracer;
    tracer.startTime();
    store = new store_t{init_size, 17179869184, "storage"};
    cout << "New store: " << tracer.getRunTime() << endl;
    dummyOperations();
    //steppingOperations();
    delete store;
}