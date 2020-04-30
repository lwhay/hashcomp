// Based on numatest.cpp by James Brock
// http://stackoverflow.com/questions/7259363/measuring-numa-non-uniform-memory-access-no-observable-asymmetry-why
//
// Changes by Andreas Kloeckner, 10/2012:
// - Rewritten in C + OpenMP
// - Added contention tests

#define _GNU_SOURCE

#include <numa.h>
#include <sched.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include <omp.h>
#include <assert.h>
#include "timing.h"

#define READ_OPERATION 1

void pin_to_core(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

void print_bitmask(const struct bitmask *bm) {
    for (size_t i = 0; i < bm->size; ++i)
        printf("%d", numa_bitmask_isbitset(bm, i));
}

template<typename T>
double measure_access(T *x, int tid, size_t array_size, size_t ntrips, size_t operations, double *out = nullptr) {
    timestamp_type t1;
    get_timestamp(&t1);
    size_t step = array_size / operations;
    int offset = tid * 31;
#if READ_OPERATION == 1
    *out = .0f;
    //char one;
#endif
    for (size_t i = 0; i < ntrips; ++i)
        for (size_t j = 0; j < array_size; j += step) {
#if READ_OPERATION == 1
            *out += *(((T *) x) + ((j * 1009 + offset) % array_size));
#else
            *(((T *) x) + ((j * 1009 + offset) % array_size)) += 1;
#endif
        }
#if READ_OPERATION
    //*out = one;
#endif

    timestamp_type t2;
    get_timestamp(&t2);

    return timestamp_diff_in_seconds(t1, t2);
}

template<typename T>
double verify_read(T *x, size_t array_size, size_t ntrips, size_t operations) {
    double ret = .0f;
    size_t step = array_size / operations;

    for (size_t i = 0; i < ntrips; ++i)
        for (size_t j = 0; j < array_size; j += step) {
            ret += *(((T *) x) + ((j * 1009) % array_size));
        }

    return ret;
}

template<typename T>
void run(size_t operations, size_t ntrips) {
    int num_cpus = numa_num_task_cpus();
    printf("num cpus: %d\n", num_cpus);

    printf("numa available: %d\n", numa_available());
    numa_set_localalloc();

    struct bitmask *bm = numa_bitmask_alloc(num_cpus);
    for (int i = 0; i <= numa_max_node(); ++i) {
        numa_node_to_cpus(i, bm);
        printf("numa node %d ", i);
        print_bitmask(bm);
        printf(" - %g GiB\n", numa_node_size(i, 0) / (1024. * 1024 * 1024.));
    }
    numa_bitmask_free(bm);

    puts("");

    T *xs[num_cpus];
    double outs[num_cpus];
    T *x;
    const size_t cache_line_size = 64;
    // Here, the storage type of array_size significantly impacts the total performance on g++
    const size_t array_size = 100 * 1000 * 1000;
    printf("array_size: %llu, operations: %llu, ntrips: %llu\n", array_size, operations, ntrips);

#pragma omp parallel
    {
        assert(omp_get_num_threads() == num_cpus);
        int tid = omp_get_thread_num();

        pin_to_core(tid);
        if (tid == 0) {
            x = (T *) numa_alloc_local(array_size * sizeof(T));
            memset(x, 0xf, array_size * sizeof(T));
        }

        xs[tid] = (T *) numa_alloc_local(array_size * sizeof(T));
        memset(xs[tid], tid, array_size * sizeof(T));

        // {{{ single access
#pragma omp barrier
        for (size_t i = 0; i < num_cpus; ++i) {
            if (tid == i) {
                double t = measure_access(x, tid, array_size, ntrips, operations, &outs[tid]);
                printf("sequential core %d -> core 0 : BW %g MB/s %g\n", i,
                       operations * ntrips * cache_line_size / t / 1e6, outs[tid]);
            }
#pragma omp barrier
        }
        // }}}

        // {{{ everybody contends for one

        {
            if (tid == 0) puts("");

#pragma omp barrier
            double t = measure_access(x, tid, array_size, ntrips, operations, &outs[tid]);
#pragma omp barrier
            for (size_t i = 0; i < num_cpus; ++i) {
                if (tid == i)
                    printf("all-contention core %d -> core 0 : BW %g MB/s %g\n", tid,
                           operations * ntrips * cache_line_size / t / 1e6, outs[tid]);
#pragma omp barrier
            }
        }

        // }}}

        // {{{ everybody localization without contending

        {
            if (tid == 0) puts("");

#pragma omp barrier
            double t = measure_access(xs[tid], tid, array_size, ntrips, operations, &outs[tid]);
#pragma omp barrier
            for (size_t i = 0; i < num_cpus; ++i) {
                if (tid == i)
                    printf("all-localization core %d -> core %d : BW %g MB/s %g\n", tid, tid,
                           operations * ntrips * cache_line_size / t / 1e6, outs[tid]);
#pragma omp barrier
            }
        }

        // }}}

        // {{{ zero and someone else contending

        if (tid == 0) puts("");

#pragma omp barrier
        for (size_t i = 1; i < num_cpus; ++i) {
            double t;
            if (tid == i || tid == 0)
                t = measure_access(x, tid, array_size, ntrips, operations, &outs[tid]);

#pragma omp barrier
            if (tid == 0) {
                printf("two-contention core %d -> core 0 : BW %g MB/s %g\n", tid,
                       operations * ntrips * cache_line_size / t / 1e6, outs[tid]);
            }
#pragma omp barrier
            if (tid == i) {
                printf("two-contention core %d -> core 0 : BW %g MB/s %g\n\n", tid,
                       operations * ntrips * cache_line_size / t / 1e6, outs[tid]);
            }
#pragma omp barrier
        }
    }

    printf("%g\n", verify_read(x, array_size, ntrips, operations));
    numa_free(x, array_size);
}

int main(int argc, const char **argv) {
    size_t operations = 100 * 1000 * 1000;
    size_t ntrips = 2;

    if (argc > 2) {
        operations = std::atol(argv[1]);
        ntrips = std::atol(argv[2]);
    }

    printf("------------------------------------ char ------------------------------------\n");
    run<char>(operations, ntrips);

    printf("------------------------------------ int -------------------------------------\n");
    run<int>(operations, ntrips);

    printf("------------------------------------ long ------------------------------------\n");
    run<long>(operations, ntrips);

    return 0;
}