//
// Created by lwh on 19-7-12.
//

#include <iostream>
#include <algorithm>
#include <functional>
#include "tracer.h"
#include "lock_free_hash_table.h"
#include "concurrent_hash_table.h"
#include "basic_hash_table.h"
#include "atomic_shared_ptr.h"

#if defined(__linux__)

#include <cpuid.h>

#endif

uint64_t total = 10000;
int pdegree = 2;
long total_runtime = 0;
long max_runtime = 0;
long min_runtime = std::numeric_limits<long>::max();

#define testingFixed 0
#if testingFixed
#define type int
#else
#define type char*
type*sinput;
#endif

#define testingType  1 // 0: basic; 1: nhash; 2: chash; 3: lhash.
#define kvLength     4

using equalTo = equal_to<type>;
#if (testingType == 0)
neatlib::BasicHashTable<type, type, std::hash<type>, std::equal_to<type>,
        std::allocator<std::pair<const type, type>>, 4> *store;
#elif (testingType == 1)
neatlib::ConcurrentHashTable<type, type, std::hash<type>, 4, 4,
        neatlib::unsafe::atomic_shared_ptr, neatlib::unsafe::shared_ptr> *store;
#elif (testingType == 2)
neatlib::ConcurrentHashTable<type, type, std::hash<type>, 4, 4> *store;
#else if (testingType == 3)
neatlib::LockFreeHashTable<type, type, std::hash<type>, 4, 4> *store;
#endif

struct paramstruct {
    int tid;
    long runtime = 0;
};

void *worker(void *args) {
    paramstruct *param = static_cast<paramstruct *>(args);
    Tracer tracer;
    tracer.startTime();
    for (int i = param->tid; i < total; i += pdegree) {
#if testingFixed
        store->Insert(i, i);
#else
        store->Insert(sinput[i], sinput[i]);
#endif
    }
    param->runtime = tracer.getRunTime();
}

void multiInsert() {
    total_runtime = 0;
    pthread_t threads[pdegree];
    paramstruct params[pdegree];
    for (int i = 0; i < pdegree; i++) {
        params[i].tid = i;
        pthread_create(&threads[i], nullptr, worker, &params[i]);
    }
    for (int i = 0; i < pdegree; i++) {
        pthread_join(threads[i], nullptr);
        total_runtime += params[i].runtime;
        if (params[i].runtime > max_runtime) {
            max_runtime = params[i].runtime;
        }
        if (params[i].runtime < min_runtime) {
            min_runtime = params[i].runtime;
        }
    }
}

void verify() {
    size_t right = 0;
    size_t wrong = 0;
    for (int i = 0; i < total; i++) {
#if testingFixed
        pair<type, type> p = store->Get(i);
        assert(p.first == i);
#else
        pair<type, type> p = store->Get(sinput[i]);
        //equalTo(p.first, sinput[i]);
        if (strcmp(p.first, sinput[i]) == 0) {
            right++;
        } else {
            wrong++;
        }
#endif
    }
    cout << "Right: " << right << " wrong: " << wrong << endl;
}

int main(int argc, char **argv) {
#if defined(__linux__)
    if (sysconf(_SC_NPROCESSORS_ONLN) > 8) {
        size_t total_num = 100000000;
        int thread_num = 32;
        if (argc >= 2) {
            total_num = std::atol(argv[1]);
            thread_num = std::atoi(argv[2]);
        }
        total = total_num;
        pdegree = thread_num;
    }
#endif
#if !testingFixed
    sinput = new type[total];
    for (int i = 0; i < total; i++) {
        sinput[i] = new char[kvLength];
        memset(sinput[i], static_cast<char>(i), kvLength - 1);
    }
#endif
    cout << total << "/" << pdegree << endl;
#if (testingType == 0)
    store = new neatlib::BasicHashTable<type, type, std::hash<type>, std::equal_to<type>,
            std::allocator<std::pair<const type, type>>, 4>();
#elif (testingType == 1)
    store = new neatlib::ConcurrentHashTable<type, type, std::hash<type>, 4, 4,
            neatlib::unsafe::atomic_shared_ptr, neatlib::unsafe::shared_ptr>();
#elif (testingType == 2)
    store = new neatlib::ConcurrentHashTable<type, type, std::hash<type>, 4, 4>();
#elif (testingType == 3)
    store = new neatlib::LockFreeHashTable<type, type, std::hash<type>, 4, 4>(pdegree);
#endif
    Tracer tracer;
    tracer.startTime();
    multiInsert();
    cout << "Insert: " << tracer.getRunTime() << " minTime: " << min_runtime << " maxTime: " << max_runtime
         << " avgTime: " << ((double) total_runtime / pdegree) << " avgTpt: "
         << ((double) total * pdegree / total_runtime) << endl;
    tracer.startTime();
    verify();
    cout << "Verify: " << tracer.getRunTime() << endl;
    return 0;
}