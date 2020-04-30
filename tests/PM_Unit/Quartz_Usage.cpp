//
// Created by iclab on 1/23/20.
//

#include <cassert>
#include <atomic>
#include <pthread.h>
#include <stdio.h>
#include <cpuid.h>
#include <sys/time.h>
#include <stdlib.h>
#include "tracer.h"

#if linux
#if PMEM_NVMEMUL == 1

#include "pmalloc.h"

extern "C" void *pmalloc(size_t size);

#endif

#include <numa.h>

#define nmalloc numa_alloc_local

#define nfree numa_free
#else
#define nmalloc malloc
#define nfree(p, n) free(p)
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
size_t numa_malloc = 3;   // bit0: 0/1 non-numa/nua; bit1: non-local/numa-local

std::atomic<uint64_t> total_time{0};

std::atomic<uint64_t> total_count{0};

std::atomic<double> total_summation(.0f);

bool first_round = true;

#define ENFORCE_READING 1

#define OPERATION_TYPE 0 // 0: char; 1: ushort; 2: uint; 3: ulong

#define ROUND_ROBIN 2 // 0: 1-1; 1: 1-2, 2-3, ..., n-1; 2: 0:n, 1:n-1,...

#define INIT_SEPARATE 1

pthread_t *workers;
void ****ptrs;

void *ppmall(void *args) {
    Tracer tracer;
    tracer.startTime();
    int tid = *(int *) args;
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / run_iteration / thread_number); i++) {
#if PMEM_NVMEMUL == 1
            ptrs[tid][r][i] = pmalloc(gran_perround);
#else
            if (numa_malloc & 0x2 == 0x2)
                ptrs[tid][r][i] = nmalloc(gran_perround);
            else
                ptrs[tid][r][i] = malloc(gran_perround);
#endif
        }
    }
    total_time.fetch_add(tracer.getRunTime());
    total_count.fetch_add(run_iteration * total_element / run_iteration / thread_number);
}

void *pmmall(void *args) {
    Tracer tracer;
    tracer.startTime();
    int tid = *(int *) args;
    if (numa_malloc & 0x2 == 0x2)
        ptrs[tid] = (void ***) nmalloc(sizeof(void **) * run_iteration);
    else
        ptrs[tid] = (void ***) malloc(sizeof(void **) * run_iteration);
    for (size_t r = 0; r < run_iteration; r++) {
        if (numa_malloc & 0x2 == 0x2)
            ptrs[tid][r] = (void **) nmalloc(sizeof(void *) * (total_element / run_iteration));
        else
            ptrs[tid][r] = (void **) malloc(sizeof(void *) * (total_element / run_iteration));
    }
    total_time.fetch_add(tracer.getRunTime());
    total_count.fetch_add(run_iteration);
}

void *pmwriter(void *args) {
    Tracer tracer;
    tracer.startTime();
    int tid = *(int *) args;
    double total = .0f;
    bool init = first_round;
    int idx = tid;
#if ROUND_ROBIN == 1
    idx = (idx + 1) % thread_number;
#elif ROUND_ROBIN == 2
    idx = thread_number - 1 - idx;
#endif
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / run_iteration / thread_number); i++) {
            for (size_t j = 0; j < gran_perround; j++) {
#if OPERATION_TYPE == 0
                if (init)
                    *(((char *) ptrs[idx][r][i]) + j) = 0;
                else
                    *(((char *) ptrs[idx][r][i]) + ((j * 1009) % gran_perround)) += 1;
#elif OPERATION_TYPE == 1
                if (init)
                    *(((unsigned short *) ptrs[idx][r][i]) + (j * 2) % (gran_perround / 2)) = 0;
                else
                    *(((unsigned short *) ptrs[idx][r][i]) + ((j * 1009) % (gran_perround / 2))) += 1;
#elif OPERATION_TYPE == 2
                if (init)
                    *(((uint32_t *) ptrs[idx][r][i]) + (j * 4) % (gran_perround / 4)) = 0;
                else
                    *(((uint32_t *) ptrs[idx][r][i]) + ((j * 1009) % (gran_perround / 4))) += 1;
#elif OPERATION_TYPE == 3
                if (init)
                    *(((uint64_t *) ptrs[idx][r][i]) + (j * 8) % (gran_perround / 8)) = 0;
                else
                    *(((uint64_t *) ptrs[idx][r][i]) + ((j * 1009) % (gran_perround / 8))) += 1;
#endif
            }
        }
    }
    total_time.fetch_add(tracer.getRunTime());
    total_count.fetch_add(run_iteration * total_element * gran_perround / run_iteration / thread_number);
}

void *pmreader(void *args) {
    Tracer tracer;
    tracer.startTime();
    int tid = *(int *) args;
    double total = .0f;
    int idx = tid;
#if ROUND_ROBIN == 1
    idx = (idx + 1) % thread_number;
#elif ROUND_ROBIN == 2
    idx = thread_number - 1 - idx;
#endif
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / run_iteration / thread_number); i++) {
            for (size_t j = 0; j < gran_perround; j++) {
#if OPERATION_TYPE == 0
                total += *(((char *) ptrs[idx][r][i]) + ((j * 1009) % gran_perround));
#elif OPERATION_TYPE == 1
                total += *(((unsigned short *) ptrs[idx][r][i]) + ((j * 1009) % (gran_perround / 2)));
#elif OPERATION_TYPE == 2
                total += *(((uint32_t *) ptrs[idx][r][i]) + ((j * 1009) % (gran_perround / 4)));
#elif OPERATION_TYPE == 3
                total += *(((uint64_t *) ptrs[idx][r][i]) + ((j * 1009) % (gran_perround / 8)));
#endif
            }
        }
    }
#if ENFORCE_READING == 1
    double oldtotal;
    double newtotal;
    do {
        oldtotal = total_summation.load();
        newtotal = oldtotal + total;
    } while (!total_summation.compare_exchange_strong(oldtotal, newtotal));
#endif
    total_time.fetch_add(tracer.getRunTime());
    total_count.fetch_add(run_iteration * total_element * gran_perround / run_iteration / thread_number);
}

void *ppfree(void *args) {
    Tracer tracer;
    tracer.startTime();
    int tid = *(int *) args;
    for (size_t r = 0; r < run_iteration; r++) {
        for (size_t i = 0; i < (total_element / run_iteration / thread_number); i++) {
#if PMEM_NVMEMUL == 1
            pfree(ptrs[tid][r][i], gran_perround);
#else
            if (numa_malloc & 0x2 == 0x2)
                nfree(ptrs[tid][r][i], gran_perround);
            else
                free(ptrs[tid][r][i]);
#endif
        }
    }
    total_time.fetch_add(tracer.getRunTime());
    total_count.fetch_add(run_iteration * total_element / run_iteration / thread_number);
}

void *pmfree(void *args) {
    Tracer tracer;
    tracer.startTime();
    int tid = *(int *) args;
    for (size_t r = 0; r < run_iteration; r++) {
        if (numa_malloc & 0x2 == 0x2)
            nfree(ptrs[tid][r], sizeof(void *) * (total_element / run_iteration));
        else
            free(ptrs[tid][r]);
    }
    if (numa_malloc & 0x2 == 0x2)
        nfree(ptrs[tid], sizeof(void **) * run_iteration);
    else
        free(ptrs[tid]);
    total_time.fetch_add(tracer.getRunTime());
    total_count.fetch_add(run_iteration);
}

void srunner(void *func(void *), const char *fname, int r = -1) {
    int *tids = (int *) malloc(sizeof(int) * thread_number);
    struct timeval begTime, endTime;
    long duration;
    total_count.store(0);
    total_time.store(0);
    gettimeofday(&begTime, NULL);
    for (int i = 0; i < thread_number; i++) {
        tids[i] = i;
        func(tids + i);
#if linux
        if (numa_malloc & 0x1 == 0x1) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
#endif
    }
    gettimeofday(&endTime, NULL);
    duration = (endTime.tv_sec - begTime.tv_sec) * 1000000 + endTime.tv_usec - begTime.tv_usec;
    if (r == -1)
        printf("%s: %lld %f\n", fname, duration, (double) total_count.load() / total_time.load());
    else
        printf("%s%d: %lld %f mops %f\n", fname, r, duration,
               (double) total_count.load() * 1000000 / total_time.load() / (1llu << 30), total_summation.load());

    free(tids);
}

void prunner(void *func(void *), const char *fname, int r = -1) {
    int *tids = (int *) malloc(sizeof(int) * thread_number);
    struct timeval begTime, endTime;
    long duration;
    total_count.store(0);
    total_time.store(0);
    gettimeofday(&begTime, NULL);
    for (int i = 0; i < thread_number; i++) {
        tids[i] = i;
        pthread_create(&workers[i], NULL, func, tids + i);
#if linux
        if (numa_malloc & 0x1 == 0x1) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(i, &cpuset);
        int rc = pthread_setaffinity_np(workers[i], sizeof(cpu_set_t), &cpuset);
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
    if (r == -1)
        printf("%s: %lld %f\n", fname, duration, (double) total_count.load() * thread_number / total_time.load());
    else
        printf("%s%d: %lld %f mops %f\n", fname, r, duration,
               (double) total_count.load() * thread_number * 1000000 / total_time.load() / (1llu << 30),
               total_summation.load());

    free(tids);
}

void multiWorkers() {
    ptrs = (void ****) malloc(sizeof(void ***) * thread_number);
    workers = (pthread_t *) malloc(sizeof(pthread_t) * thread_number);

    for (int r = 0; r < 1; r++) {
#if INIT_SEPARATE == 1
        srunner(pmmall, "pmmall");
#else
        prunner(pmmall, "pmmall");
#endif
    }

    for (int r = 0; r < 1; r++) {
#if INIT_SEPARATE == 1
        srunner(ppmall, "ppmall");
#else
        prunner(ppmall, "ppmall");
#endif
    }

    for (int r = 0; r < 3; r++) {
#if INIT_SEPARATE == 1
        srunner(pmwriter, "pmwriter", r);
#else
        prunner(pmwriter, "pmwriter", r);
#endif
    }

    for (int r = 0; r < 3; r++) {
        prunner(pmreader, "pmreader", r);
    }

    first_round = false;
    for (int r = 0; r < 3; r++) {
        prunner(pmwriter, "pmwriter", r);
    }

    for (int r = 0; r < 3; r++) {
        prunner(pmreader, "pmreader", r);
    }

    for (int r = 0; r < 1; r++) {
        prunner(ppfree, "ppfree");
    }

    for (int r = 0; r < 1; r++) {
        prunner(pmfree, "pmfree");
    }

    free(workers);
    free(ptrs);
}

int main(int argc, char **argv) {
    if (argc > 5) {
        thread_number = atol(argv[1]);
        total_element = atol(argv[2]);
        run_iteration = atol(argv[3]);
        gran_perround = atol(argv[4]);
        numa_malloc = atol(argv[5]);
    }
    assert(gran_perround > 7);
    printf("%llu\t%llu\t%llu\t%llu\t%x\n", thread_number, total_element, run_iteration, gran_perround, numa_malloc);
    printf("numa thread: %x, numa malloc: %x\n", numa_malloc & 0x1, numa_malloc & 0x2);
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