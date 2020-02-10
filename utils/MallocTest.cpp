//
// Created by iclab on 2/10/20.
//

#include <pthread.h>
#include "tracer.h"

size_t total_count = (1 << 20);
size_t total_round = (1 << 4);
size_t thread_number = 20;

size_t *thread_time_malloc;
size_t *thread_time_free;

size_t ***workloads;

void *measureWorker(void *args) {
    int tid = *(int *) args;
    Tracer tracer;
    for (int r = 0; r < total_round; r++) {
        tracer.startTime();
        for (int i = 0; i < total_count; i++) {
            workloads[tid][i] = new size_t;
            *workloads[tid][i] = i;
        }
        thread_time_malloc[tid] += tracer.getRunTime();
        tracer.startTime();
        for (int i = 0; i < total_count; i++) {
            delete workloads[tid][i];
        }
        thread_time_free[tid] += tracer.getRunTime();
    }
}

void *ptest() {
    pthread_t *workers = new pthread_t[thread_number];
    workloads = new size_t **[thread_number];
    thread_time_malloc = new size_t[thread_number];
    thread_time_free = new size_t[thread_number];
    int *tids = new int[thread_number];

    for (int i = 0; i < thread_number; i++) {
        tids[i] = i;
        thread_time_malloc[i] = 0;
        thread_time_free[i] = 0;
        workloads[i] = new size_t *[total_count];
        pthread_create(&workers[i], nullptr, measureWorker, tids + i);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
        delete[] workloads[i];
    }
    delete[] tids;
    delete[] thread_time_malloc;
    delete[] thread_time_free;
    delete[] workloads;
    delete[] workers;
}

int main(int argc, char **argv) {
    if (argc > 3) {
        total_count = std::atol(argv[1]);
        total_round = std::atol(argv[2]);
        thread_number = std::atol(argv[3]);
    }
    ptest();
}