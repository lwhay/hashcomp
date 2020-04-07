//
// Created by iclab on 10/12/19.
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

#define DEFAULT_STR_LENGTH 256
//#define DEFAULT_KEY_LENGTH 8

#define INPLACE            0

#define COUNT_HASH         1

#define TRICK_MAP          5 // 0-16: ihazard inheritances, 16: origin awlmap

struct Value {
    uint64_t value;
public:
    Value(uint64_t v) : value(v) {}

    ~Value() {}

    uint64_t get() { return value; }
};


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
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, faster_epoch<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 15
typedef trick::ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>, opt_hazard<trick::TreeNode, trick::DataNode<uint64_t, uint64_t>>> maptype;
#elif TRICK_MAP == 16
typedef ConcurrentHashMap<uint64_t, /*Value **/uint64_t, MyHash, std::equal_to<>> maptype;
#endif

maptype *store;

uint64_t *loads;

long total_time;

uint64_t exists = 0;

uint64_t read_success = 0, modify_success = 0;

uint64_t read_failure = 0, modify_failure = 0;

uint64_t total_count = DEFAULT_KEYS_COUNT;

uint64_t timer_range = default_timer_range;

int thread_number = DEFAULT_THREAD_NUM;

size_t key_range = DEFAULT_KEYS_RANGE;

double distribution_skew = 0.0;

int root_capacity = (1 << 16);

stringstream *output;

atomic<int> stopMeasure(0);

int updatePercentage = 10;

int ereasePercentage = 0;

int totalPercentage = 100;

int readPercentage = (totalPercentage - updatePercentage - ereasePercentage);

struct target {
    int tid;
    uint64_t *insert;
    maptype *store;
};

pthread_t *workers;

struct target *parms;

#define HASH_VERIFY 0

#if HASH_VERIFY == 1

#include <algorithm>
#include <unordered_map>

#endif

void simpleInsert() {
#if TRICK_MAP >= 6 && TRICK_MAP <= 13
    store->initThread(thread_number);
#endif
    Tracer tracer;
    tracer.startTime();
    int inserted = 0;
    unordered_set<uint64_t> set;
#if HASH_VERIFY == 1
    unordered_map <uint64_t, uint64_t> spemap;
    unordered_map <uint64_t, uint64_t> stdmap;
    std::hash<uint64_t> stdhash;
    MyHash spehash;
#endif
    for (int i = 0; i < total_count; i++) {
        store->Insert(loads[i], loads[i]/*new Value(loads[i])*/);
        set.insert(loads[i]);
        inserted++;
#if HASH_VERIFY == 1
        uint64_t stdhk = stdhash(loads[i]);
        uint64_t spehk = spehash(loads[i]);
        if (spemap.find(spehk) == spemap.end()) spemap.insert(std::make_pair(spehk, 0));
        if (stdmap.find(stdhk) == stdmap.end()) stdmap.insert(std::make_pair(stdhk, 0));
        spemap.find(spehk)->second++;
        stdmap.find(stdhk)->second++;
#endif
    }
#if HASH_VERIFY == 1
    std::vector<std::pair<uint64_t, uint64_t >> stdvector;
    for (auto &e: stdmap) stdvector.push_back(e);
    sort(stdvector.begin(), stdvector.end(),
         [=](pair<uint64_t, uint64_t> &a, pair<uint64_t, uint64_t> &b) { return b.second < a.second; });

    std::vector<std::pair<uint64_t, uint64_t >> spevector;
    for (auto &e: spemap) spevector.push_back(e);
    sort(spevector.begin(), spevector.end(),
         [=](pair<uint64_t, uint64_t> &a, pair<uint64_t, uint64_t> &b) { return b.second < a.second; });
    cout << "\t" << stdmap.size() << " vs " << spemap.size() << endl;
    for (int i = 0; i < 100; i++)
        if (i < stdvector.size() && i < spevector.size())
            cout << "\t" << stdvector[i].first << ":" << stdvector[i].second << "\t" << spevector[i].first << ":"
                 << spevector[i].second << endl;
#endif
    cout << inserted << " " << tracer.getRunTime() << " " << set.size() << endl;
}

void *insertWorker(void *args) {
    struct target *work = (struct target *) args;
    uint64_t inserted = 0;
    for (int i = work->tid * total_count / thread_number; i < (work->tid + 1) * total_count / thread_number; i++) {
        store->Insert(loads[i], loads[i]);
        inserted++;
    }
    __sync_fetch_and_add(&exists, inserted);
}

void *measureWorker(void *args) {
    Tracer tracer;
    tracer.startTime();
    struct target *work = (struct target *) args;
    uint64_t mhit = 0, rhit = 0;
    uint64_t mfail = 0, rfail = 0;
    int evenRound = 0;
    uint64_t ereased = 0, inserts = 0;
#if TRICK_MAP != 16
    store->initThread(work->tid);
#endif
#if HASH_VERIFY == 1
    std::unordered_map<uint64_t, uint64_t> map;
    std::unordered_set<uint64_t> set;
    MyHash hasher;
#endif
    try {
        while (stopMeasure.load(memory_order_relaxed) == 0) {
#if INPUT_METHOD == 0
            for (int i = 0; i < total_count; i++) {
#elif INPUT_METHOD == 1
            for (int i = work->tid; i < total_count; i += thread_number) {
#else
            for (int i = work->tid * total_count / thread_number;
                 i < (work->tid + 1) * total_count / thread_number; i++) {
#endif
                if (updatePercentage > 0 && i % (totalPercentage / updatePercentage) == 0) {
#if INPLACE
                    bool ret = store->InplaceUpdate(loads[i], loads[i]);
#else
                    bool ret = store->Insert(loads[i], loads[i]/*new Value(loads[i])*/);
#endif
                    if (ret)
                        mhit++;
                    else
                        mfail++;
                } else if (ereasePercentage > 0 && (i + 1) % (totalPercentage / ereasePercentage) == 0) {
                    bool ret;
                    if (evenRound % 2 == 0) {
                        uint64_t key = thread_number * inserts++ + work->tid + (evenRound / 2 + 1) * key_range;
#if HASH_VERIFY == 1
                        set.insert(key);
                        uint64_t hk = hasher(key) % root_capacity;
                        if (map.find(hk) == map.end()) map.insert(std::make_pair(hk, 0));
                        map.find(hk)->second++;
#endif
                        ret = store->Insert(key, key);
                    } else {
                        uint64_t key = thread_number * ereased++ + work->tid + (evenRound / 2 + 1) * key_range;
                        ret = store->Delete(key);
                    }
                    if (ret) mhit++;
                    else mfail++;
                } else {
                    uint64_t value;
                    bool ret = store->Find(loads[i], value);
                    if (ret && value == loads[i])
                        rhit++;
                    else
                        rfail++;
                }
            }
            if (evenRound++ % 2 == 0) ereased = 0;
            else inserts = 0;
        }
    } catch (exception e) {
        cout << work->tid << endl;
    }

    long elipsed = tracer.getRunTime();

#if HASH_VERIFY == 1
    std::vector<std::pair<uint64_t, uint64_t >> vec;
    for (auto &e: map) vec.push_back(e);
    sort(vec.begin(), vec.end(),
         [=](pair<uint64_t, uint64_t> &a, pair<uint64_t, uint64_t> &b) { return b.second < a.second; });
    if (vec.size() > 0)
        output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << " " << set.size() << " "
                          << map.size() << " " << vec[0].first << " " << vec[0].second << endl;
    else
        output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << " " << set.size() << " "
                          << map.size() << endl;
#else
    output[work->tid] << work->tid << " " << elipsed << " " << mhit << " " << rhit << endl;
#endif
    __sync_fetch_and_add(&total_time, elipsed);
    __sync_fetch_and_add(&read_success, rhit);
    __sync_fetch_and_add(&read_failure, rfail);
    __sync_fetch_and_add(&modify_success, mhit);
    __sync_fetch_and_add(&modify_failure, mfail);
}

void prepare() {
    cout << "prepare" << endl;
    workers = new pthread_t[thread_number];
    parms = new struct target[thread_number];
    output = new stringstream[thread_number];
    for (int i = 0; i < thread_number; i++) {
        parms[i].tid = i;
        parms[i].store = store;
        parms[i].insert = (uint64_t *) calloc(total_count / thread_number, sizeof(uint64_t *));
        char buf[DEFAULT_STR_LENGTH];
        for (int j = 0; j < total_count / thread_number; j++) {
            std::sprintf(buf, "%d", i + j * thread_number);
            parms[i].insert[j] = j;
        }
    }
}

void finish() {
    cout << "finish" << endl;
    for (int i = 0; i < thread_number; i++) {
        delete[] parms[i].insert;
    }
    delete[] parms;
    delete[] workers;
    delete[] output;
}

void multiWorkers() {
    output = new stringstream[thread_number];
    Tracer tracer;
    tracer.startTime();
    /*for (int i = 0; i < thread_number; i++) {
        pthread_create(&workers[i], nullptr, insertWorker, &parms[i]);
    }
    for (int i = 0; i < thread_number; i++) {
        pthread_join(workers[i], nullptr);
    }*/
    cout << "Insert " << exists << " " << tracer.getRunTime() << endl;
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

int main(int argc, char **argv) {
    if (argc > 7) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        timer_range = std::atol(argv[4]);
        distribution_skew = std::stof(argv[5]);
        updatePercentage = std::atoi(argv[6]);
        ereasePercentage = std::atoi(argv[7]);
        readPercentage = totalPercentage - updatePercentage - ereasePercentage;
    }
    if (argc > 8)
        root_capacity = std::atoi(argv[8]);
    store = new maptype(root_capacity, 20, thread_number + 1);
    cout << " threads: " << thread_number << " range: " << key_range << " count: " << total_count << " timer: "
         << timer_range << " skew: " << distribution_skew << " u:e:r = " << updatePercentage << ":" << ereasePercentage
         << ":" << readPercentage << endl;
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
    prepare();
    cout << "simple" << endl;
    simpleInsert();
    cout << "multiinsert" << endl;
    multiWorkers();
    cout << "read operations: " << read_success << " read failure: " << read_failure << " modify operations: "
         << modify_success << " modify failure: " << modify_failure << " throughput: "
         << (double) (read_success + read_failure + modify_success + modify_failure) * thread_number / total_time
         << endl;
    free(loads);
    finish();
    delete store;
    return 0;
}