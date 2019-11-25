//
// Created by iclab on 11/24/19.
//

#include <iostream>
#include <thread>
#include <queue>
#include "ihazard.h"
#include "memory_hazard.h"
#include "hash_hazard.h"
#include "tracer.h"

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
        for (size_t i = 0; i < total_count / thrd_number; i++) {
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
        node *ptr = (node *) std::malloc(sizeof(node));
        size_t idx = i % (list_volume / align_width) * align_width;
        ptr->key = idx;
        ptr->value = 1;
        bucket[idx].store((uint64_t) ptr);
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
    uint64_t total = 0, hitting = 0;
    std::queue<uint64_t> oldqueue;
    Tracer tracer;
    tracer.startTime();
    while (stopMeasure.load() == 0) {
        for (size_t i = 0; i < total_count / thrd_number; i++) {
            node *ptr = (node *) std::malloc(sizeof(node));
            ptr->key = i;
            ptr->value = 1;
            uint64_t old;
            size_t idx = i % (list_volume / align_width) * align_width;
            do {
                old = bucket[idx].load();
            } while (!bucket[idx].compare_exchange_strong(old, (uint64_t) ptr));
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
    init(bucket);
    //print(bucket);
    Tracer tracer;
    tracer.startTime();
    std::vector<std::thread> workers;
    if (hash_freent)
        deallocator = new hash_hazard(worker_gran);
    else
        deallocator = new memory_hazard;
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
    std::cout << " wconflict: " << freeconflict << " weffective: " << writecount << std::endl;
    delete[] bucket;
    delete[] runtime;
    delete[] operations;
    delete[] conflict;
    return 0;
}