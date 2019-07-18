//
// Created by lwh on 19-7-18.
//

#include <iostream>
#include "tracer.h"
#include "bztree.h"

using namespace std;
using namespace bztree;
using namespace pmwcas;

uint64_t total = 1000000;
int pdegree = 2;
int simple = 0;
int pagesize = 0;
int split = 0;
int reserve = 0;
long total_runtime = 0;
long total_counter = 0;
long max_runtime = 0;
long min_runtime = std::numeric_limits<long>::max();
atomic<int> stopMeasure(0);

uint64_t *loads = nullptr;
pmwcas::DescriptorPool *pool;
bztree::BzTree::ParameterSet *param;
BzTree *zt;

void initLoads() {
    loads = new uint64_t[total];

    for (int i = 0; i < total; i++) {
        loads[i] = i;
    }
}

void simpleOperationTests() {
    Tracer tracer;
    tracer.startTime();
    pmwcas::DescriptorPool *pool = new pmwcas::DescriptorPool(total, 1, false);
    bztree::BzTree::ParameterSet *param = new bztree::BzTree::ParameterSet(split, reserve, total);
    BzTree *zt = bztree::BzTree::New(*param, pool);
    cout << "Table init: " << tracer.getRunTime() << endl;
    tracer.startTime();

    for (int i = 0; i < total; i++) {
        std::string key = std::to_string(i);
        zt->Insert(key.c_str(), key.size(), i);
    }

    cout << "Table insert: " << tracer.getRunTime() << endl;
    tracer.startTime();

    for (int i = 0; i < total; i++) {
        std::string key = std::to_string(i);
        zt->Delete(key.c_str(), key.size());
    }
    cout << "Table empty: " << tracer.getRunTime() << endl;
    delete zt;
}

struct paramstruct {
    int tid;
    BzTree *zt;
    long runtime = 0;
    long counter = 0;
};

void *insertWorker(void *args) {
    paramstruct *param = static_cast<paramstruct *>(args);
    Tracer tracer;
    tracer.startTime();

    for (int i = param->tid; i < total; i += pdegree) {
        std::string key = std::to_string(i);
        param->zt->Insert(key.c_str(), key.size(), i);
    }

    param->runtime = tracer.getRunTime();
}

void insert() {
    total_runtime = 0;
    pthread_t threads[pdegree];
    paramstruct params[pdegree];

    for (int i = 0; i < pdegree; i++) {
        params[i].tid = i;
        params[i].zt = zt;
        pthread_create(&threads[i], nullptr, insertWorker, &params[i]);
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

void *updateWorker(void *args) {
    paramstruct *param = static_cast<paramstruct *>(args);
    Tracer tracer;
    tracer.startTime();

    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = param->tid; i < total; i += pdegree) {
            std::string key = std::to_string(i);
            param->zt->Update(key.c_str(), key.size(), total - i);
            param->counter++;
        }
    }

    param->runtime = tracer.getRunTime();
}

void *searchWorker(void *args) {
    paramstruct *param = static_cast<paramstruct *>(args);
    Tracer tracer;
    tracer.startTime();

    while (stopMeasure.load(memory_order_relaxed) == 0) {
        for (int i = param->tid; i < total; i += pdegree) {
            std::string key = std::to_string(i);
            uint64_t payload;
            param->zt->Read(key.c_str(), key.size(), &payload);
            param->counter++;
        }
    }

    param->runtime = tracer.getRunTime();
}

void upSearch(bool insert) {
    total_runtime = 0;
    total_counter = 0;
    min_runtime = 0;
    max_runtime = 0;
    pthread_t threads[pdegree];
    paramstruct params[pdegree];

    Timer timer;
    timer.start();

    stopMeasure.store(0, memory_order_relaxed);
    for (int i = 0; i < pdegree; i++) {
        params[i].tid = i;
        params[i].zt = zt;
        if (insert) {
            pthread_create(&threads[i], nullptr, updateWorker, &params[i]);
        } else {
            pthread_create(&threads[i], nullptr, searchWorker, &params[i]);
        }
    }
    while (timer.elapsedSeconds() < default_timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (int i = 0; i < pdegree; i++) {
        pthread_join(threads[i], nullptr);
        total_runtime += params[i].runtime;
        total_counter += params[i].counter;
        if (params[i].runtime > max_runtime) {
            max_runtime = params[i].runtime;
        }
        if (params[i].runtime < min_runtime) {
            min_runtime = params[i].runtime;
        }
    }
}

int main(int argc, char **argv) {
    if (argc > 3) {
        total = std::atol(argv[1]);
        pdegree = std::atoi(argv[2]);
        simple = std::atoi(argv[3]);
        pagesize = std::atoi(argv[4]);
        split = pagesize * 3 / 4;
        reserve = pagesize - split;
    }

    cout << "Thread: " << pdegree << " total: " << total << " page: " << pagesize << " split: " << split << " reserve: "
         << reserve << endl;

    Tracer tracer;
    tracer.startTime();
    initLoads();
    cout << "Init: " << tracer.getRunTime() << endl;
    tracer.startTime();
    pmwcas::InitLibrary(pmwcas::DefaultAllocator::Create,
                        pmwcas::DefaultAllocator::Destroy,
                        pmwcas::LinuxEnvironment::Create,
                        pmwcas::LinuxEnvironment::Destroy);
    pool = new pmwcas::DescriptorPool(total, pdegree, false);
    param = new bztree::BzTree::ParameterSet(split, reserve, pagesize);
    cout << "Memory init: " << tracer.getRunTime() << endl;
    tracer.startTime();
    zt = bztree::BzTree::New(*param, pool);
    cout << "Tree init: " << tracer.getRunTime() << endl;
    tracer.startTime();

    if (simple) {
        simpleOperationTests();
    } else {
        insert();
    }

    cout << "Insert: " << tracer.getRunTime() << " minTime: " << min_runtime << " maxTime: " << max_runtime
         << " avgTime: " << ((double) total_runtime / pdegree) << " avgTpt: "
         << ((double) total * pdegree / total_runtime) << endl;

    if (!simple) {
        upSearch(true);
    }

    cout << "Update: " << tracer.getRunTime() << " minTime: " << min_runtime << " maxTime: " << max_runtime
         << " avgTime: " << ((double) total_runtime / pdegree) << " avgTpt: "
         << ((double) total_counter * pdegree / total_runtime) << endl;

    if (!simple) {
        upSearch(false);
    }

    cout << "Search: " << tracer.getRunTime() << " minTime: " << min_runtime << " maxTime: " << max_runtime
         << " avgTime: " << ((double) total_runtime / pdegree) << " avgTpt: "
         << ((double) total_counter * pdegree / total_runtime) << endl;

    delete zt;
    delete[] loads;
    return 0;
}