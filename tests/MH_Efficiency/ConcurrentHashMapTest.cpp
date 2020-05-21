//
// Created by Michael on 4/1/20.
//

#include <vector>
#include <thread>
#include <cassert>
#include "batch_hazard.h"
#include "brown_reclaim.h"
#include "hash_hazard.h"
#include "tracer.h"
#include "concurrent_hash_map.h"
#include "trick_concurrent_hash_map.h"

uint64_t thread_number = 4;
uint64_t total_count = 1llu << 10;

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

typedef batch_hazard<trick::TreeNode, node> batch;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_hazardptr<>, node> brown6;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_ebr_token<>, node> brown7;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_ebr_tree<>, node> brown8;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_ebr_tree_q<>, node> brown9;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_debra<>, node> brown10;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_debraplus<>, node> brown11;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_debracap<>, node> brown12;
typedef brown_reclaim<trick::TreeNode, alloc<node>, pool<>, reclaimer_none<>, node> brown13;

void multiAWLTest() {
    typedef ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>> maptype;
    maptype map(total_count / 2, 10, thread_number);
    Tracer tracer;
    tracer.startTime();
    std::cout << "AWLMultiInsert: " << typeid(maptype).name() << std::endl;
    std::vector<std::thread> threads;
    int i = 0;
    for (; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            for (uint64_t i = tid; i < total_count; i++) {
                map.Insert(i, i);
                if (tid == 0 && i % (total_count / 4) == 0) std::cout << i << std::endl;
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "AWLMultiInsert: " << tracer.getRunTime() << std::endl;
    for (int i = 0; i < total_count; i++) {
        uint64_t value;
        bool ret = map.Find(i, value);
        assert(ret && value == i);
    }
    std::cout << "Verified: " << tracer.getRunTime() << std::endl;
}

template<typename maptype>
void multiTrickTest() {
    // Look at line-48, we have a bug in brown reclaimer here.
    maptype map(total_count / 2, 10, thread_number);
    Tracer tracer;
    tracer.startTime();
    std::cout << "TrickMultiInsert: " << typeid(maptype).name() << std::endl;
    std::vector<std::thread> threads;
    int i = 0;
    for (; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            map.initThread(tid);
            for (uint64_t i = tid; i < total_count; i++) {
                map.Insert(i, i);
                if (tid == 0 && i % (total_count / 4) == 0) std::cout << i << std::endl;
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "TrickMultiInsert: " << tracer.getRunTime() << std::endl;
    for (int i = 0; i < total_count; i++) {
        uint64_t value;
        bool ret = map.Find(i, value);
        assert(ret && value == i);
    }
    std::cout << "Verified: " << tracer.getRunTime() << std::endl;
}

void brownTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, brown11> maptype;
    // Look at line-48, we have a bug in brown reclaimer here.
    maptype map(4, 2, thread_number);
    uint64_t v;
    Tracer tracer;
    tracer.startTime();
    for (uint64_t i = 0; i < 64; i++) {
        map.Insert(i, i);
        //std::cout << i << ":" << total_count << std::endl;
    }
    std::cout << "Brown11 simple Insert: " << tracer.getRunTime() << std::endl;
    for (uint64_t i = 0; i < 64; i++) {
        //std::cout << i << ":" << total_count << std::endl;
        bool ret = map.Find(i, v);
        if (ret) assert(i == v);
    }
    std::cout << "Find: " << tracer.getRunTime() << std::endl;
}

void deleteTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, batch> maptype;
    maptype map(4, 1, thread_number);
    map.initThread();
    /*for (uint64_t i = 0; i < 64; i++) map.Insert(i, i);
    map.Printer();*/
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            map.initThread(tid);
            for (uint64_t r = 0; r < (1llu << 20); r++) {
                for (uint64_t i = 0; i < 4; i++) {
                    uint64_t k = (uint64_t) -1 - (/*r * 4 +*/ i);
                    //printf(cm[tid], k);
                    map.Insert(k, k);
                    //std::cout << k << std::endl;
                }
                //if (tid == 0) map.Printer();
                for (uint64_t i = 0; i < 4; i++) {
                    uint64_t k = (uint64_t) -1 - (/*r * 4 +*/ i);
                    map.Delete(k);
                }
                for (uint64_t i = 0; i < 4; i += thread_number) {
                    uint64_t v;
                    uint64_t k = (uint64_t) -1 - (/*r * 4 +*/ i);
                    bool ret = map.Find(k, v);
                    //if (ret) printf(cm[tid], k);//std::cout << tid << ":" << k << std::endl;
                }
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    map.Printer();
    printf(cm[0], total_count);
}

template<typename reclaimer>
void SimpleTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, reclaimer> maptype;
    maptype map(total_count / 2, 10, thread_number);
    map.initThread();
    uint64_t v;
    Tracer tracer;
    tracer.startTime();
    for (uint64_t i = 0; i < total_count; i++) {
        map.Insert(i, i);
        //std::cout << i << ":" << total_count << std::endl;
    }
    std::cout << "Simple Insert: " << tracer.getRunTime() << " " << typeid(reclaimer).name() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) {
        //std::cout << i << ":" << total_count << std::endl;
        bool ret = map.Find(i, v);
        if (ret) assert(i == v);
    }
    std::cout << "Find: " << tracer.getRunTime() << std::endl;
}

template<typename reclaimer>
void MultiWriteTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, reclaimer> maptype;
    maptype map(total_count / 2, 10, thread_number);
    Tracer tracer;
    map.initThread(thread_number);
    tracer.startTime();
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            map.initThread(tid);
            for (uint64_t i = tid; i < total_count; i += thread_number) {
                map.Insert(i, i);
                if (tid == 0 && i % (total_count / 4) == 0) std::cout << i << std::endl;
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "MultiInsert: " << tracer.getRunTime() << std::endl;
}

template<typename reclaimer>
void MultiReadTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, reclaimer> maptype;
    maptype map(total_count / 2, 10, thread_number);
    Tracer tracer;
    tracer.startTime();
    map.initThread(thread_number);
    for (uint64_t i = 0; i < total_count; i++) map.Insert(i, i);
    std::cout << "MultiInsert: " << tracer.getRunTime() << std::endl;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            map.initThread(tid);
            uint64_t v;
            for (uint64_t i = tid; i < total_count; i += thread_number) {
                map.Find(i, v);
                assert(i == v);
                if (tid == 0 && i % (total_count / 4) == 0) {
                    if (i != v) std::cout << "\t" << i << ":" << v << std::endl;
                    std::cout << i << std::endl;
                }
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "MultiFind: " << tracer.getRunTime() << std::endl;
}

template<typename reclaimer>
void MultiRWTest() {
    typedef trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, reclaimer> maptype;
    maptype map(total_count / 2, 10, thread_number);
    Tracer tracer;
    tracer.startTime();
    std::cout << "MultiInsert: " << tracer.getRunTime() << std::endl;
    std::vector<std::thread> threads;
    int i = 0;
    for (; i < thread_number / 2; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            map.initThread(tid);
            for (uint64_t i = tid; i < total_count; i++) {
                map.Insert(i, i);
                if (tid == 0 && i % (total_count / 4) == 0) std::cout << i << std::endl;
            }
        }, std::ref(map), i));
    }
    for (; i < thread_number; i++) {
        threads.push_back(std::thread([](maptype &map, uint64_t tid) {
            map.initThread(tid);
            uint64_t v;
            for (uint64_t i = tid; i < total_count; i++) {
                bool ret = map.Find(i, v);
                if (ret) assert(i == v);
                if (tid == 0 && i % (total_count / 4) == 0) {
                    if (i != v) std::cout << "\t" << i << ":" << v << std::endl;
                    std::cout << i << std::endl;
                }
            }
        }, std::ref(map), i));
    }
    for (int i = 0; i < thread_number; i++) {
        threads[i].join();
    }
    std::cout << "MultiFind: " << tracer.getRunTime() << std::endl;
}

template<typename reclaimer>
void test() {
    std::cout << "-----------SimpleTest--------------" << typeid(reclaimer).name() << std::endl;
    SimpleTest<reclaimer>();
    std::cout << "-----------MultiReadTest-----------" << typeid(reclaimer).name() << std::endl;
    MultiReadTest<reclaimer>();
    std::cout << "-----------MultiWriteTest----------" << typeid(reclaimer).name() << std::endl;
    MultiWriteTest<reclaimer>();
    std::cout << "-----------MultiRWTest-------------" << typeid(reclaimer).name() << std::endl;
    MultiRWTest<reclaimer>();
}

int main(int argc, char **argv) {
    if (argc > 2) {
        thread_number = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
    }
    std::cout << thread_number << " " << total_count << std::endl;
    deleteTest();
    brownTest();
    multiAWLTest();
    multiTrickTest<trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>, brown7>>();
    multiTrickTest<trick::ConcurrentHashMap<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>>>();
    test<batch>();
    test<brown6>();
    test<brown7>();
    test<brown8>();
    test<brown9>();
    test<brown10>();
    test<brown11>();
    test<brown12>();
    test<brown13>();
    return 0;
}