//
// Created by iclab on 9/9/19.
//
#include <iostream>
#include "tracer.h"
#include <stdio.h>
#include "faster.h"
#include "kvcontext.h"

#define DEFAULT_STORE_BASE (1LLU << 26)

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

uint64_t total_count = (1LLU << 26);

void simpleInsert() {
    for (uint64_t i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{i, i};
        Status stat = store->Upsert(context, callback, 1);
    }
}

void simpleDelete() {
    for (uint64_t i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<DeleteContext> context{ctxt};
        };
        DeleteContext context{i};
        Status stat = store->Delete(context, callback, 1);
    }
}

void verifyRead(bool needHit) {
    for (uint64_t i = 0; i < total_count; i++) {
        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
        ReadContext context(i);
        Status stat = store->Read(context, callback, 1);
        if (needHit) {
            assert(stat == Status::Ok);
        } else {
            assert(stat == Status::NotFound);
        }
    }
}

int main(int argc, char **argv) {
    Tracer tracer;
    tracer.startTime();
    store = new store_t{init_size, 17179869184, "storage"};
    cout << "Before insertion: " << -1 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    tracer.startTime();
    simpleInsert();
    cout << "After insertion: " << -1 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    tracer.startTime();
    verifyRead(true);
    cout << "Verify read: " << -1 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    tracer.startTime();
    simpleDelete();
    cout << "After deletion: " << -1 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    tracer.startTime();
    verifyRead(false);
    cout << "Verify read: " << -1 << " keyspace: " << init_size << " storesize: " << store->Size() << " elipsed: "
         << tracer.getRunTime() << " " << store->hlog.GetTailAddress().offset() << endl;
    delete store;
}