//
// Created by iclab on 1/23/20.
//

#include <iostream>
#include <pthread.h>
#include <stdio.h>
#include <cpuid.h>
#include <sys/time.h>
#include <stdlib.h>

#if linux
#if PMEM_NVMEMUL == 1

#include "pmalloc.h"

extern "C" void *pmalloc(size_t size);

#endif
#endif

#define MASK(msb, lsb) (~((~0) << (msb + 1)) & ((~0) << lsb))
#define EXTRACT(val, msb, lsb) ((MASK(msb, lsb) & val) >> lsb)
#define MODEL(eax) EXTRACT(eax, 7, 4)
#define EXTENDED_MODEL(eax) EXTRACT(eax, 19, 16)
#define MODEL_NUMBER(eax) ((EXTENDED_MODEL(eax) << 4) | MODEL(eax))
#define FAMILY(eax) EXTRACT(eax, 11, 8)
#define Extended_Family(eax) EXTRACT(eax, 27, 20)
#define Family_Number(eax) (FAMILY(eax) + Extended_Family(eax))

void get_family_model(int *family, int *model) {
    unsigned int eax, ebx, ecx, edx;
    int success = __get_cpuid(1, &eax, &ebx, &ecx, &edx);
    if (family != NULL) {
        *family = success ? Family_Number(eax) : 0;
    }

    if (model != NULL) {
        *model = success ? MODEL_NUMBER(eax) : 0;
    }
}

size_t total_element = (1llu << 20);
size_t run_iteration = (1llu < 10);
size_t gran_perround = (1llu << 4);
size_t thread_number = 4;
size_t numa_malloc = 1;

pthread_t *workers;
void ****ptrs;

void *ppmall(void *args) {
    int tid = *(int *) args;
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / run_iteration / thread_number); i++) {
#if PMEM_NVMEMUL == 1
            ptrs[tid][r][i] = pmalloc(gran_perround);
#else
            ptrs[tid][r][i] = malloc(gran_perround);
#endif
        }
    }
}

void *pmmall(void *args) {
    int tid = *(int *) args;
    ptrs[tid] = (void ***) malloc(sizeof(void **) * run_iteration);
    for (size_t r = 0; r < run_iteration; r++) {
        ptrs[tid][r] = (void **) malloc(sizeof(void *) * (total_element / run_iteration));
    }
}

void *ppfree(void *args) {
    int tid = *(int *) args;
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / run_iteration / thread_number); i++) {
#if PMEM_NVMEMUL == 1
            pfree(ptrs[tid][r][i], gran_perround);
#else
            free(ptrs[tid][r][i]);
#endif
        }
    }
}

void *pmfree(void *args) {
    int tid = *(int *) args;
    for (size_t r = 0; r < run_iteration; r++) {
        free(ptrs[tid][r]);
    }
    free(ptrs[tid]);
}

void multiWorkers() {
    ptrs = (void ****) malloc(sizeof(void ***) * thread_number);
    workers = (pthread_t *) malloc(sizeof(pthread_t) * thread_number);
    struct timeval begTime, endTime;

    int *tids = (int *) malloc(sizeof(int) * thread_number);
    gettimeofday(&begTime, NULL);
    for (int i = 0; i < thread_number; i++) {
        tids[i] = i;
        pthread_create(&workers[i], NULL, pmmall, tids + i);
#if linux
        if (numa_malloc and 0x1 == 0x1) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i % 8, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
#endif
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], NULL);
    }
    gettimeofday(&endTime, NULL);
    long duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    printf("malloc: %lld\n", duration);

    gettimeofday(&begTime, NULL);
    for (int i = 0; i < thread_number; i++) {
        tids[i] = i;
        pthread_create(&workers[i], NULL, ppmall, tids + i);
#if linux
        if (numa_malloc and 0x1 == 0x1) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i % 8, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
#endif
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], NULL);
    }
    gettimeofday(&endTime, NULL);
    duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    printf("pmalloc: %lld\n", duration);

    gettimeofday(&begTime, NULL);
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], NULL, ppfree, tids + i);
#if linux
        if (numa_malloc and 0x1 == 0x1) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i % 8, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
#endif
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], NULL);
    }
    gettimeofday(&endTime, NULL);
    duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    printf("pfree: %lld\n", duration);

    gettimeofday(&begTime, NULL);
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], NULL, pmfree, tids + i);
#if linux
        if (numa_malloc and 0x1 == 0x1) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i % 8, &cpuset);
            int rc = pthread_setaffinity_np(threads[i].native_handle(), sizeof(cpu_set_t), &cpuset);
            if (rc != 0) {
                std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
            }
        }
#endif
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], NULL);
    }
    gettimeofday(&endTime, NULL);
    duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    printf("free: %lld\n", duration);

    free(tids);
    free(workers);
    free(ptrs);
}

int main(int argc, char **argv) {
    if (argc > 1) {
        thread_number = atol(argv[1]);
        total_element = atol(argv[2]);
        run_iteration = atol(argv[3]);
        numa_malloc = atol(argv[4]);
    }
    struct timeval begin;
    gettimeofday(&begin, NULL);
    int family, model;
    get_family_model(&family, &model);
    printf("%d:%d\t%llu:%llu\n", family, model, begin.tv_sec, begin.tv_usec);
    printf("%llu\t%llu\t%llu\t%llu\n", thread_number, total_element, run_iteration, gran_perround);
    multiWorkers();
    /*void *ptr = pmalloc(1024);
    pfree(ptr, 1024);*/
    return 0;
}