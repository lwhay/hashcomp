//
// Created by iclab on 1/23/20.
//

#include <pthread.h>
#include <stdio.h>
#include <cpuid.h>
#include <sys/time.h>
#include <stdlib.h>
#include "pmalloc.h"


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
    if (family != nullptr) {
        *family = success ? Family_Number(eax) : 0;
    }

    if (model != nullptr) {
        *model = success ? MODEL_NUMBER(eax) : 0;
    }
}

size_t total_element = (1llu << 20);
size_t run_iteration = (1llu < 10);
size_t gran_perround = (1llu << 4);
size_t thread_number = 4;

pthread_t *workers;
void ***ptrs;

void *ppmall(void *args) {
    ptrs = new void **[run_iteration];
    for (size_t r = 0; r < run_iteration; r++) {
        ptrs[r] = new void *[total_element / run_iteration];
        for (size_t i = 0; i < (total_element / thread_number); i++) {
            ptrs[r][i] = pmalloc(gran_perround);
        }
    }
}

void *ppfree(void *args) {
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / thread_number); i++) {
            pfree(ptrs[r][i], gran_perround);
        }
        delete[] ptrs[r];
    }
    delete[] ptrs;
}

void multiWorkers() {
    timeval begTime, endTime;
    gettimeofday(&begTime, nullptr);
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, ppmall, nullptr);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
    }
    gettimeofday(&endTime, nullptr);
    long duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    printf("pmalloc: %ll", duration);
    gettimeofday(&begTime, nullptr);
    for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, ppfree, nullptr);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
    }
    duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    printf("pfree: %ll", duration);
}

int main(int argc, char **argv) {
    if (argc > 1) {
        thread_number = atol(argv[1]);
        total_element = atol(argv[2]);
        run_iteration = atol(argv[3]);
    }
    int family, model;
    get_family_model(&family, &model);
    printf("%d:%d\n", family, model);
    multiWorkers();
    /*void *ptr = pmalloc(1024);
    pfree(ptr, 1024);*/
    return 0;
}