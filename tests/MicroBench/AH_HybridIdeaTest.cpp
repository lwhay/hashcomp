//
// Created by iclab on 6/24/20.
//

#include <iostream>
#include <sstream>
#include <chrono>
#include <stdio.h>
#include <stdlib.h>
#include <unordered_set>
#include "tracer.h"
#include "concurrent_hash_map.h"
#include "trick_concurrent_hash_map.h"
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

#define DEFAULT_THREAD_NUM (8)
#define DEFAULT_KEYS_COUNT (1 << 20)
#define DEFAULT_KEYS_RANGE (1 << 2)

#define MAX_SOCKET 4
#define MAX_CORES 128
#define MAX_CORES_PER_SOCKET (MAX_CORES / MAX_SOCKET)

#define DEFAULT_STR_LENGTH 256
//#define DEFAULT_KEY_LENGTH 8

#define INPLACE            0

#define COUNT_HASH         1

#define TRICK_MAP          5 // 0-15: ihazard inheritances, 16: origin awlmap

#ifdef linux

#include <numa.h>

void pin_to_core(size_t core) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core, &cpuset);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
}

size_t cpus_per_socket = 8;

int coreToSocket[MAX_CORES];
int socketToCore[MAX_SOCKET][MAX_CORES_PER_SOCKET];

size_t count_of_socket = 1;

#endif

#define BIG_CONSTANT(x) (x##LLU)

uint64_t MurmurHash64A(const void *key, int len, uint64_t seed) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^(len * m);

    const uint64_t *data = (const uint64_t *) key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *) data;

    switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

char buf[64];

struct MyHash {
    std::size_t operator()(const uint64_t &i) const noexcept {
        return MurmurHash64A((char *) &i, 8, 234324324231llu);
    }
};

//#define MyHash std::hash<uint64_t>

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

typedef trick::DataNode<uint64_t, uint64_t> node;

typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_hazardptr<>, node> brown6;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_ebr_token<>, node> brown7;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_ebr_tree<>, node> brown8;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_ebr_tree_q<>, node> brown9;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_debra<>, node> brown10;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_debraplus<>, node> brown11;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_debracap<>, node> brown12;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_none<>, node> brown13;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_batchhp<>, node> brown14;

#if TRICK_MAP == 0
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, memory_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 1
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, hash_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 2
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, mshazard_pointer<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 3
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, adaptive_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 4
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, epoch_wrapper<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 5
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, batch_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 6
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown6> maptype;
#elif TRICK_MAP == 7
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown7> maptype;
#elif TRICK_MAP == 8
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown8> maptype;
#elif TRICK_MAP == 9
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown9> maptype;
#elif TRICK_MAP == 10
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown10> maptype;
#elif TRICK_MAP == 11
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown11> maptype;
#elif TRICK_MAP == 12
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown12> maptype;
#elif TRICK_MAP == 13
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown13> maptype;
#elif TRICK_MAP == 14
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, brown14> maptype;
#elif TRICK_MAP == 15
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, faster_epoch<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 16
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, opt_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 17
typedef ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>> maptype;
#endif

maptype *store[MAX_SOCKET];

uint64_t **loads;

long total_time;

uint64_t exists = 0;

uint64_t read_success = 0, modify_success = 0;

uint64_t read_failure = 0, modify_failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

uint64_t timer_range = default_timer_range;

int numaScheme = 0x7f;     // 0th, 1st, 2nd numa-based initialization, insert, measure will be numa-based

int thread_number = DEFAULT_THREAD_NUM;

size_t key_range = DEFAULT_KEYS_RANGE;

double distribution_skew = 0.0;

int mapping = 0x0f;        //f: 3210: Whether the 0th, 1st, 2nd, 3rd numa-based sockets are activiated.

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int erasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - erasePercentage);

struct target {
    int tid;
    int core;
    int socket;
};

pthread_t *workers;

struct target *parms;

void *insertWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    if ((numaScheme & 0x2) != 0) pin_to_core(work->core);
    int inserted = 0;
    int c = work->socket;
    unordered_set<uint64_t> set;
    size_t load_count = total_count / cpus_per_socket;
    output[work->tid] << work->tid << " " << work->core << " " << work->socket << " "
                      << ((work->tid % cpus_per_socket) * load_count) << " "
                      << (((work->tid % cpus_per_socket) + 1) * load_count) << " ";
    for (size_t i = (work->tid % cpus_per_socket) * load_count;
         i < ((work->tid % cpus_per_socket) + 1) * load_count; i++) {
        store[c]->Insert(loads[c][i], loads[c][i]);
        set.insert(loads[c][i]);
        inserted++;
    }
    output[work->tid] << inserted << " " << tracer.getRunTime() << " " << set.size() << endl;
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    if ((numaScheme & 0x4) != 0) pin_to_core(work->core);
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    int evenRound = 0;
    uint64_t inserts = 0;
    uint64_t erased = 0;
    maptype *localstore = store[work->socket];
    uint64_t *localloads = loads[work->socket];
    output[work->tid] << work->tid << " " << work->core << " " << work->socket << " "
                      << ((work->tid % cpus_per_socket) * (total_count / cpus_per_socket)) << " "
                      << ((work->tid % cpus_per_socket + 1) * (total_count / cpus_per_socket)) << " ";
    try {
        while (stopMeasure.load(memory_order_relaxed) == 0) {
#if INPUT_METHOD == 0
            for (int i = 0; i < total_count; i++) {
#elif INPUT_METHOD == 1
            for (int i = work->tid % cpus_per_socket; i < total_count; i += cpus_per_socket) {
#else
            for (int i = (work->tid % cpus_per_socket) * (total_count / cpus_per_socket);
                 i < (work->tid % cpus_per_socket + 1) * (total_count / cpus_per_socket); i++) {
#endif
                if (updatePercentage > 0 && i % (totalPercentage / updatePercentage) == 0) {
#if INPLACE == 1
                    bool ret = localstore->InplaceUpdate(localloads[i], localloads[i]/*new Value(loads[i])*/);
#else
                    bool ret = localstore->Insert(localloads[i], localloads[i]/*new Value(loads[i])*/);
#endif
                    if (ret) mhit++;
                    else mfail++;
                } else if (erasePercentage > 0 && (i + 1) % (totalPercentage / erasePercentage) == 0) {
                    bool ret;
                    if (evenRound % 2 == 0) {
                        uint64_t key = thread_number * inserts++ + work->tid + (evenRound / 2 + 1) * key_range;
                        ret = localstore->Insert(key, key);
                    } else {
                        uint64_t key = thread_number * erased++ + work->tid + (evenRound / 2 + 1) * key_range;
                        ret = localstore->Delete(key);
                    }
                    if (ret) mhit++;
                    else mfail++;
                } else {
                    uint64_t value;
                    bool ret = localstore->Find(localloads[i], value);
                    if (ret && value == localloads[i]) rhit++;
                    else rfail++;
                }
            }
            if (evenRound++ % 2 == 0) erased = 0;
            else inserts = 0;
        }
    } catch (exception e) {
        cout << work->tid << ":" << erased << endl;
    }

    long elipsed = tracer.getRunTime();
    output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << endl;
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&read_success, rhit);
    __sync_fetch_and_add(&read_failure, rfail);
    __sync_fetch_and_add(&modify_success, mhit);
    __sync_fetch_and_add(&modify_failure, mfail);
}

void prepare() {
    cout << "prepare " << thread_number << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    int current_socket = -1;
    int current_core = cpus_per_socket;
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        if (current_core == cpus_per_socket) {
            do {
                current_socket++;
            } while (socketToCore[current_socket][0] < 0);
            current_core = 0;
        }
        parms[i].socket = current_socket;
        parms[i].core = socketToCore[current_socket][current_core++];
        //cout << parms[i].tid << " " << parms[i].core << " " << parms[i].socket << endl;
    }
}

void finish() {
    cout << "finish" << endl;
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void multiWorkers(bool init = true) {
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    for (int i = 0; i < numa_num_task_cpus(); i++) CPU_SET(i, &cpu_set);
    for (int i = 0; i < thread_number; i++) sched_setaffinity(i, sizeof(cpu_set_t), &cpu_set);
    output = new stringstream[thread_number];
    Tracer tracer;
    tracer.startTime();
    if (init) {
        for (int i = 0; i < thread_number; i++) {
            pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
        }
        for (int i = 0; i < thread_number; i++) {
            pthread_join(workers[i], nullptr);
            string outstr = output[i].str();
            cout << outstr;
        }
        cout << "Insert " << exists << " " << tracer.getRunTime() << endl;
    } else {
        Timer timer;
        timer.start();
        for (int i = 0; i < thread_number; i++) {
            pthread_create(&workers[i], nullptr, measureWorker, &parms[i]);
        }
        while (timer.elapsedSeconds() < timer_range) {
            sleep(1);
        }
        stopMeasure.store(1, memory_order_relaxed);
        for (int i = 0; i < thread_number; i++) {
            pthread_join(workers[i], nullptr);
            string outstr = output[i].str();
            cout << outstr;
        }
        cout << "Gathering ..." << endl;
    }
}

int main(int argc, char **argv) {
    if (argc > 7) {
        numaScheme = std::atoi(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        skew = std::atof(argv[5]);
        updatePercentage = std::atoi(argv[6]);
        erasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - erasePercentage;
    }
    if (argc > 8)
        mapping = std::atoi(argv[8]);
    count_of_socket = numa_max_node() + 1; // 2; //
    cout << count_of_socket << endl;
    cpus_per_socket = numa_num_task_cpus() / count_of_socket;
    struct bitmask *bm = numa_bitmask_alloc(numa_num_task_cpus());
    /*int pseudomapping[2][8] = {{1, 1, 1, 1, 0, 0, 0, 0},
                               {0, 0, 0, 0, 1, 1, 1, 1}};*/
    size_t mask = 0;
    thread_number = 0;
    for (int i = 0; i < MAX_CORES; i++) coreToSocket[i] = -1;
    for (int i = 0; i < MAX_SOCKET; i++) for (int j = 0; j < MAX_CORES_PER_SOCKET; j++) socketToCore[i][j] = -1;
    for (int i = 0; i < count_of_socket; i++) {
        mask |= (1 << i);
        numa_node_to_cpus(i, bm);
        if ((mapping & (1 << i)) != 0) {
            for (int j = 0, idx = 0; j < bm->size /*8*/; j++) {
                if (1 == /*pseudomapping[i][j]*/numa_bitmask_isbitset(bm, j)) {
                    socketToCore[i][idx++] = j;
                    coreToSocket[j] = i;
                }
            }
            thread_number += cpus_per_socket;
        }
    }
    size_t oldm = mapping;
    mapping &= mask;
    cout << count_of_socket << " " << cpus_per_socket << " " << mapping << " " << oldm << " " << mask << " " << bm->size
         << endl;
    /*for (int i = 0; i < MAX_CORES; i++) {
        cout << coreToSocket[i] << " ";
    }
    cout << endl;*/
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << skew << " u:e:r = " << updatePercentage << ":" << erasePercentage << ":"
         << readPercentage << " numa_input: " << numaScheme << endl;
    loads = new uint64_t *[MAX_SOCKET];

    for (int i = 0; i < MAX_SOCKET; i++)
        if (socketToCore[i][0] >= 0) {
            if ((numaScheme & 0x1) != 0) pin_to_core(socketToCore[i][0]);
            loads[i] = (uint64_t *) calloc(total_count, sizeof(uint64_t));
            store[i] = new maptype(1ull << 16, 20, thread_number + 1);
            RandomGenerator<uint64_t>::generate(loads[i], key_range, total_count, skew);
        } else loads[i] = nullptr;

    prepare();
    bitset<128> actived{0};
    for (int i = 0; i < thread_number; i++) {
        actived.set(parms[i].core);
    }
    cout << actived << endl;
    cout << "simple" << endl;

    multiWorkers(true);

    for (int i = 0; i < MAX_SOCKET; i++)
        if (socketToCore[i][0] >= 0) {
            if ((numaScheme & 0x1) != 0) pin_to_core(socketToCore[i][0]);
#if INPUT_SHUFFLE == 1
            std::random_shuffle(loads[i], loads[i] + (total_count / cpus_per_socket) * cpus_per_socket);
#endif
        }

    cout << "measuring" << endl;
    multiWorkers(false);
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << " hash size: " << endl;
    free(loads);
    finish();
    //delete mhash;
    return 0;
}