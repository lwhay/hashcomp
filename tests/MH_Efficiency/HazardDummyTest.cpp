//
// Created by iclab on 11/24/19.
//

#include <iostream>
#include <thread>
#include <queue>
#include "ihazard.h"
#include "adaptive_hazard.h"
#include "memory_hazard.h"
#include "hash_hazard.h"
#include "mshazrd_pointer.h"
#include "wrapper_epoch.h"
#include "batch_hazard.h"
#include "brown_hazard.h"
#include "tracer.h"

#define high_intensive 0

struct node {
    uint64_t key;
    uint64_t value;
};

size_t align_width = (1 << 6);

size_t list_volume = (1 << 20);

size_t thrd_number = (1 << 3);

size_t total_count = (1 << 20);

size_t queue_limit = (1 << 16);

size_t hash_freent = 1;

atomic<int> stopMeasure(0);

size_t worker_gran = thrd_number / 2;

ihazard *deallocator;

long *runtime;

uint64_t *operations;

uint64_t *conflict;

void reader(std::atomic<uint64_t> *bucket, size_t tid) {
    uint64_t total = 0;
    Tracer tracer;
    tracer.startTime();
    while (stopMeasure.load() == 0) {
#if high_intensive
        for (size_t i = 0; i < total_count / thrd_number; i++) {
#else
        for (size_t i = tid; i < total_count / thrd_number; i += thrd_number) {
#endif
            size_t idx = i % (list_volume / align_width) * align_width;
            node *ptr = (node *) deallocator->load(tid, std::ref(bucket[idx]));
            total += ptr->value;
            deallocator->read(tid);
            //std::cout << "r" << tid << i << std::endl;
        }
    }
    runtime[tid] = tracer.getRunTime();
    operations[tid] = total;
}

void init(std::atomic<uint64_t> *bucket) {
    for (size_t i = 0; i < total_count / thrd_number; i++) {
        node *ptr;
#if uselocal == 0
        if (hash_freent == 4)
            ptr = (node *) ((epoch_wrapper<node> *) deallocator)->get();
        else
#endif
        if (hash_freent == 6) ptr = ((brown_hazard<node> *) deallocator)->allocate(0);
        else ptr = (node *) std::malloc(sizeof(node));
        size_t idx = i % (list_volume / align_width) * align_width;
        ptr->key = idx;
        ptr->value = 1;
        bucket[idx].store((uint64_t) ptr);
#if uselocal == 1
        if (hash_freent == 4) {
            deallocator->load(0, std::ref(bucket[idx]));
        }
#endif
    }
}

void print(std::atomic<uint64_t> *bucket) {
    for (size_t i = 0; i < 64; i++) {
        size_t idx = i % (list_volume / align_width) * align_width;
        node *ptr = (node *) bucket[idx].load();
        std::cout << ptr << " " << ptr->key << " " << ptr->value << std::endl;
    }
}

void writer(std::atomic<uint64_t> *bucket, size_t tid) {
    if (hash_freent == 2) ftid = tid;
    uint64_t total = 0, hitting = 0;
    std::queue<uint64_t> oldqueue;
    Tracer tracer;
    tracer.startTime();
    while (stopMeasure.load() == 0) {
#if high_intensive
        for (size_t i = 0; i < total_count / thrd_number; i++) {
#else
        for (size_t i = tid; i < total_count / thrd_number; i += thrd_number) {
#endif
            node *ptr;
#if uselocal == 0
            if (hash_freent == 4)
                ptr = (node *) ((epoch_wrapper<node> *) deallocator)->get();
            else
#endif
            if (hash_freent == 6) ptr = ((brown_hazard<node> *) deallocator)->allocate(tid);
            else ptr = (node *) std::malloc(sizeof(node));
            ptr->key = i;
            ptr->value = 1;
            uint64_t old;
            size_t idx = i % (list_volume / align_width) * align_width;
            do {
                old = bucket[idx].load();
            } while (!bucket[idx].compare_exchange_strong(old, (uint64_t) ptr));
            if (hash_freent == 2 || hash_freent == 4 || hash_freent >= 5) { // mshp maintains caches inside each hp.
                deallocator->free(old);
#if uselocal == 1
                if (hash_freent == 4) {
                    deallocator->load(tid, std::ref(bucket[idx]));
                }
#endif
            } else {
                oldqueue.push(old);
                if (oldqueue.size() >= queue_limit) {
                    while (true) {
                        uint64_t oldest = oldqueue.front();
                        oldqueue.pop();
                        if (!deallocator->free(oldest)) {
                            hitting++;
                            oldqueue.push(oldest);
                        } else {
                            break;
                        }
                    }
                }
            }
            total++;
            //if (tid % worker_gran == 0 && total % 100000 == 0) std::cout << "w" << tid << i << std::endl;
        }
    }
    runtime[tid] = tracer.getRunTime();
    operations[tid] = total;
    conflict[tid] = hitting;
}

int main(int argc, char **argv) {
    if (argc >= 7) {
        align_width = std::atol(argv[1]);
        list_volume = std::atol(argv[2]);
        thrd_number = std::atol(argv[3]);
        total_count = std::atol(argv[4]);
        queue_limit = std::atol(argv[5]);
        hash_freent = std::atol(argv[6]);
        worker_gran = thrd_number / 2;
    }
    if (argc >= 8) {
        worker_gran = std::atol(argv[7]);
    }
    std::cout << align_width << " " << list_volume << " " << thrd_number << "(" << worker_gran << ") " << total_count
              << " " << queue_limit << " " << hash_freent << std::endl;
    std::atomic<uint64_t> *bucket = new std::atomic<uint64_t>[list_volume];
    runtime = new long[thrd_number];
    operations = new uint64_t[thrd_number];
    conflict = new uint64_t[thrd_number];
    Tracer tracer;
    tracer.startTime();
    std::vector<std::thread> workers;
    switch (hash_freent) {
        case 1: {
            deallocator = new hash_hazard(worker_gran);
            break;
        }
        case 2: {
            deallocator = new mshazard_pointer(thrd_number);
            break;
        }
        case 3: {
            deallocator = new adaptive_hazard(worker_gran);
            break;
        }
        case 4 : {
            deallocator = new epoch_wrapper<node>(thrd_number);
            break;
        }
        case 5: {
            deallocator = new batch_hazard;
            break;
        }
        case 6: {
            deallocator = new brown_hazard<node>(thrd_number);
            break;
        }
        default: {
            deallocator = new memory_hazard;
            break;
        }
    }
    init(bucket);
    //print(bucket);
    Timer timer;
    timer.start();
    size_t t = 0;
    for (; t < worker_gran; t++) {
        deallocator->registerThread();
        workers.push_back(std::thread(reader, bucket, t));
    }
    for (; t < thrd_number; t++) {
        workers.push_back(std::thread(writer, bucket, t));
    }
    while (timer.elapsedSeconds() < 30) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);
    long readtime = 0;
    long writetime = 0;
    size_t readcount = 0;
    size_t writecount = 0;
    size_t freeconflict = 0;
    for (size_t t = 0; t < thrd_number; t++) {
        workers[t].join();
        if (t < worker_gran) {
            readtime += runtime[t];
            readcount += operations[t];
            std::cout << "\tReader" << t << ": " << operations[t] << "\t" << runtime[t] << std::endl;
        } else {
            writetime += runtime[t];
            writecount += operations[t];
            freeconflict += conflict[t];
            std::cout << "\tWriter" << t - worker_gran << ": " << operations[t] << "\t" << runtime[t] << std::endl;
        }
    }
    double readthp = (double) readcount * worker_gran / readtime;
    double writethp = (double) writecount * (thrd_number - worker_gran) / writetime;
    std::cout << "Total time: " << tracer.getRunTime() << " rthp: " << readthp << " wthp: " << writethp;
    std::cout << " wconflict: " << freeconflict << " weffective: " << writecount << " inform: " << deallocator->info()
              << std::endl;
    delete[] bucket;
    delete[] runtime;
    delete[] operations;
    delete[] conflict;
    delete deallocator;
    return 0;
}