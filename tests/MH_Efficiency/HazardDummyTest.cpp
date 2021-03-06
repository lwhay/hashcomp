//
// Created by iclab on 11/24/19.
//

#include <algorithm>
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
#include "brown_reclaim.h"
#include "faster_epoch.h"
#include "opthazard_pointer.h"
#include "tracer.h"

#ifdef linux

#include <numa.h>

#define ENABLE_NUMA 1
#endif

#define high_intensive 2 // 0: duplicated; 1: skipping; 2: thread_local

#define permutate_keys 1

#define read_factor (1 << 0)

#define brown_new_once 1
#define brown_use_pool 0

#if brown_new_once == 1
#define alloc allocator_new
#elif brown_new_once == 0
#define alloc allocator_once
#else
#define alloc allocator_bump
#endif

#if brown_use_pool == 0
#define pool pool_perthread_and_shared
#else
#define pool pool_none
#endif

class node {
public:
    uint64_t key;
    uint64_t value;
public:
    node() : key(-1), value(-1) {}

    ~node() { value = -1; }
};

typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_hazardptr<>> brown6;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_ebr_token<>> brown7;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_ebr_tree<>> brown8;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_ebr_tree_q<>> brown9;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_debra<>> brown10;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_debraplus<>> brown11;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_debracap<>> brown12;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_none<>> brown13;
typedef brown_reclaim<node, alloc<node>, pool<>, reclaimer_batchhp<>> brown14;

size_t hash_freent = 6;

size_t align_width = (1 << 6);

size_t list_volume = (1 << 20);

size_t thrd_number = (1 << 3);

size_t total_count = (1 << 20);

size_t queue_limit = (1 << 16);

atomic<int> stopMeasure(0);

uint64_t timer_limit = 30;

size_t worker_gran = thrd_number / 2;

ihazard<node> *deallocator;

long *runtime;

uint64_t *loads;

uint64_t *operations;

uint64_t *conflict;

int detail_print = 0;

void reader(std::atomic<uint64_t> *bucket, size_t tid) {
    deallocator->initThread(tid);
    uint64_t total = 0;
    size_t exception = 0;
    Tracer tracer;
    tracer.startTime();
    while (stopMeasure.load() == 0) {
#if high_intensive == 1
        for (size_t i = 0; i < total_count; i++) {
#elif high_intensive == 0
        for (size_t i = (tid * align_width); i < total_count; i += (thrd_number * align_width)) {
#else
        size_t start = tid * total_count / thrd_number;
        size_t end = (tid + 1) * total_count / thrd_number;
        for (size_t i = start; i < end; i += thrd_number) {
#endif
            size_t idx = loads[i] * align_width % (list_volume);
            assert(idx >= 0 && idx < list_volume);
            node *ptr = (node *) deallocator->load(tid, std::ref(bucket[idx]));
            //assert(ptr->value == 1);
            if (detail_print && ptr->value != 1) {
                exception++;
                /*std::cout << i << "*" << loads[i] << " " << loads[i] * align_width % (list_volume) << " " << ptr->key
                          << " " << ptr->value << std::endl;*/
            }
            unsigned count = 0;
            for (; count < read_factor; count++) count += ptr->value;
            total += count / read_factor;
            deallocator->read(tid);
            //std::cout << "r" << tid << i << std::endl;
        }
    }
    if (detail_print) std::cout << "Tid: " << tid << " exception: " << exception << std::endl;
    runtime[tid] = tracer.getRunTime();
    operations[tid] = total;
}

void init(std::atomic<uint64_t> *bucket) {
    Tracer tracer;
    tracer.startTime();
    deallocator->initThread(thrd_number);
    for (size_t i = 0; i < list_volume; i++) {
#if defined(linux) && ENABLE_NUMA == 1
        if (i % (list_volume / thrd_number) == 0) {
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(i / (list_volume / thrd_number), &cpuset);
            pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        }
#endif
        node *ptr;
#if uselocal == 0
        if (hash_freent == 4)
            ptr = (node *) ((epoch_wrapper<node> *) deallocator)->get();
        else
#endif
        if (hash_freent >= 5 && hash_freent <= 14) ptr = (node *) deallocator->allocate(0); // 5: ignored cache.
        else ptr = (node *) std::malloc(sizeof(node));
        size_t idx = i;
        assert(idx >= 0 && idx < list_volume);
        ptr->key = idx;
        ptr->value = 1;
        bucket[idx].store((uint64_t) ptr);
#if uselocal == 1
        if (hash_freent == 4) {
            deallocator->load(0, std::ref(bucket[idx]));
        }
#endif
    }
    std::cout << "init complete " << tracer.getRunTime() << std::endl;
}

void deinit(std::atomic<uint64_t> *bucket) {
    for (size_t i = 0; i < list_volume; i++) {
        size_t idx = i;
        assert(idx >= 0 && idx < list_volume);
        if (hash_freent >= 5 && hash_freent <= 14) deallocator->free(bucket[idx].load());
        else std::free((void *) bucket[idx].load());
    }
}

void print(std::atomic<uint64_t> *bucket) {
    for (size_t i = 0; i < 64; i++) {
        size_t idx = i * align_width % list_volume;
        node *ptr = (node *) bucket[idx].load();
        std::cout << ptr << " " << ptr->key << " " << ptr->value << std::endl;
    }
}

void writer(std::atomic<uint64_t> *bucket, size_t tid) {
    if (hash_freent >= 5 && hash_freent <= 15) deallocator->initThread(tid);
    else if (hash_freent == 2) ftid = tid;
    uint64_t total = 0, hitting = 0, exception = 0;
    std::queue<uint64_t> oldqueue;
    Tracer tracer;
    tracer.startTime();
    while (stopMeasure.load() == 0) {
#if high_intensive == 1
        for (size_t i = 0; i < total_count; i++) {
#elif high_intensive == 0
        for (size_t i = (tid * align_width); i < total_count; i += (thrd_number * align_width)) {
#else
        size_t start = tid * total_count / thrd_number;
        size_t end = (tid + 1) * total_count / thrd_number;
        for (size_t i = start; i < end; i += align_width) {
#endif
            node *ptr;
#if uselocal == 0
            if (hash_freent == 4)
                ptr = (node *) ((epoch_wrapper<node> *) deallocator)->get();
            else
#endif
            if (hash_freent >= 5 && hash_freent <= 14) ptr = (node *) deallocator->allocate(tid); // useless tid in 6-12
                //else if (hash_freent == 14) ptr = ((faster_epoch<node> *) deallocator)->allocate();
            else ptr = (node *) std::malloc(sizeof(node));
            ptr->key = loads[i];
            ptr->value = 1;
            uint64_t old;
            size_t idx = loads[i] * align_width % (list_volume);
            assert(idx >= 0 && idx < list_volume);
            do {
                old = bucket[idx].load();
            } while (!bucket[idx].compare_exchange_strong(old, (uint64_t) ptr, std::memory_order_relaxed));
            node *oldptr = (node *) old;
            if (hash_freent == 2 || hash_freent == 4 ||
                hash_freent >= 5 && hash_freent <= 16) { // mshp etc maintains caches inside each hp.
                //assert(oldptr->value == 1);
                if (detail_print && oldptr->value != 1) {
                    exception++;
                    /*std::cout << i << " " << loads[i] << " " << loads[i] * align_width % (list_volume) << " "
                              << oldptr->key << " " << oldptr->value << std::endl;*/
                }
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
    if (detail_print) std::cout << "Tid: " << tid << " exception: " << exception << std::endl;
    runtime[tid] = tracer.getRunTime();
    operations[tid] = total;
    conflict[tid] = hitting;
}

#if defined(linux) && ENABLE_NUMA == 1

void print_bitmask(const struct bitmask *bm) {
    for (size_t i = 0; i < bm->size; ++i)
        printf("%d", numa_bitmask_isbitset(bm, i));
}

#endif

int main(int argc, char **argv) {
    if (argc >= 8) {
        align_width = std::atol(argv[1]);
        list_volume = std::atol(argv[2]);
        thrd_number = std::atol(argv[3]);
        total_count = std::atol(argv[4]);
        queue_limit = std::atol(argv[5]);
        timer_limit = std::atol(argv[6]);
        hash_freent = std::atol(argv[7]);
        worker_gran = thrd_number / 2;
    }
    if (argc >= 9) {
        worker_gran = std::atol(argv[8]);
        detail_print = std::atoi(argv[9]);
    }
    std::cout << align_width << " " << list_volume << " " << thrd_number << "(" << worker_gran << ") " << total_count
              << " " << queue_limit << " " << timer_limit << " " << hash_freent << std::endl;
    std::atomic<uint64_t> *bucket = new std::atomic<uint64_t>[list_volume];
    loads = new uint64_t[total_count];
    for (uint64_t i = 0; i < total_count; i++) loads[i] = i;
#if permutate_keys == 1
    std::random_shuffle(loads, loads + total_count);
#endif
    runtime = new long[thrd_number];
    operations = new uint64_t[thrd_number];
    conflict = new uint64_t[thrd_number];
    Tracer tracer;
    tracer.startTime();
    std::vector<std::thread> workers;
    switch (hash_freent) {
        case 1: {
            deallocator = new hash_hazard<node>(worker_gran);
            break;
        }
        case 2: {
            deallocator = new mshazard_pointer<node>(thrd_number);
            break;
        }
        case 3: {
            deallocator = new adaptive_hazard<node>(worker_gran);
            break;
        }
        case 4 : {
            deallocator = new epoch_wrapper<node>(thrd_number);
            break;
        }
        case 5: {
            deallocator = new batch_hazard<node>(thrd_number);
            break;
        }
        case 6: {
            deallocator = new brown6(thrd_number);
            break;
        }
        case 7: {
            deallocator = new brown7(thrd_number);
            break;
        }
        case 8: {
            deallocator = new brown8(thrd_number);
            break;
        }
        case 9: {
            deallocator = new brown9(thrd_number);
            break;
        }
        case 10: {
            deallocator = new brown10(thrd_number);
            break;
        }
        case 11: {
            deallocator = new brown11(thrd_number);
            break;
        }
        case 12: {
            deallocator = new brown12(thrd_number);
            break;
        }
        case 13: {
            deallocator = new brown13(thrd_number);
            break;
        }
        case 14: {
            deallocator = new brown14(thrd_number);
            break;
        }
        case 15: {
            deallocator = new faster_epoch<node>(thrd_number);
            break;
        }
        case 16: {
            deallocator = new opt_hazard<node>(thrd_number);
            break;
        }
        default: {
            deallocator = new memory_hazard<node>(thrd_number);
            break;
        }
    }
#if defined(linux) && ENABLE_NUMA == 1
    int num_cpus = numa_num_task_cpus();
    printf("num cpus: %d\n", num_cpus);
    printf("numa available: %d\n", numa_available());
    struct bitmask *bm = numa_bitmask_alloc(num_cpus);
    for (int i = 0; i <= numa_max_node(); ++i) {
        numa_node_to_cpus(i, bm);
        printf("numa node %d ", i);
        print_bitmask(bm);
        printf(" - %g GiB\n", numa_node_size(i, 0) / (1024. * 1024 * 1024.));
    }
    numa_bitmask_free(bm);
    numa_set_localalloc();
#endif
    init(bucket);
    //print(bucket);
    Timer timer;
    timer.start();
    size_t t = 0;
    for (; t < worker_gran; t++) {
        deallocator->registerThread();
        workers.push_back(std::thread(reader, bucket, t));
#if defined(linux) && ENABLE_NUMA == 1
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t, &cpuset);
        int rc = pthread_setaffinity_np(workers[t].native_handle(), sizeof(cpu_set_t), &cpuset);
#endif
    }
    for (; t < thrd_number; t++) {
        if (hash_freent == 6) deallocator->registerThread();
        workers.push_back(std::thread(writer, bucket, t));
#if defined(linux) && ENABLE_NUMA == 1
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t, &cpuset);
        int rc = pthread_setaffinity_np(workers[t].native_handle(), sizeof(cpu_set_t), &cpuset);
#endif
    }
    while (timer.elapsedSeconds() < timer_limit) {
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
            if (detail_print == 1)
                std::cout << "\tReader" << t << ": " << operations[t] << "\t" << runtime[t] << std::endl;
        } else {
            writetime += runtime[t];
            writecount += operations[t];
            freeconflict += conflict[t];
            if (detail_print == 1)
                std::cout << "\tWriter" << t - worker_gran << ": " << operations[t] << "\t" << runtime[t] << std::endl;
        }
    }
    double readthp = (double) readcount * worker_gran / readtime;
    double writethp = (double) writecount * (thrd_number - worker_gran) / writetime;
    std::cout << "Total time: " << tracer.getRunTime() << " rthp: " << readthp << " wthp: " << writethp;
    std::cout << " wconflict: " << freeconflict << " weffective: " << writecount << " inform: " << deallocator->info()
              << std::endl;
    deinit(bucket);
    delete[] bucket;
    delete[] runtime;
    delete[] operations;
    delete[] conflict;
    delete[] loads;
    delete deallocator;
    return 0;
}