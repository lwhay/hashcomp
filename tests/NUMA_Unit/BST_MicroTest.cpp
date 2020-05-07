//
// Created by Michael on 5/7/20.
//

#include <atomic>
#include <functional>
#include <iostream>
#include <signal.h>
#include <thread>
#include <vector>

#include "pool_none.h"
#include "allocator_new.h"
#include "pool_perthread_and_shared.h"
#include "record_manager.h"
#include "reclaimer_hazardptr.h"
#include "reclaimer_ebr_token.h"
#include "reclaimer_ebr_tree.h"
#include "reclaimer_ebr_tree_q.h"
#include "reclaimer_debra.h"
#include "reclaimer_debraplus.h"
#include "reclaimer_debracap.h"
#include "reclaimer_none.h"
#include "bst_impl.h"
//#include "chromatic_impl.h"
#include "node.h"
#include "tracer.h"

#ifdef linux

#include <numa.h>

#define ENABLE_NUMA 1
#endif

#define KEY_RANGE 16

#define RESHUFFLE 1

uint64_t thread_number = 4;
uint64_t total_count = (1llu << 20);
uint64_t timer_limit = 30;
int update_ratio = 10;
int reclaimer_type = 1;
bool high_intensive = false;
double dist_factor = .0f;
constexpr uint64_t NEUTRLIZE_SIGNAL = SIGQUIT;
int operation_type = 5; // 0: read; 1: update; 2: scan; 3: r/u; 4: s/u; 5: e/i

uint64_t *loads;
std::atomic<uint64_t> total_ops{0};
std::atomic<uint64_t> total_time{0};
std::atomic<uint64_t> total_hit{0};

typedef bst_ns::Node<uint64_t, uint64_t> NodeType;
typedef allocator_new<NodeType> Allocator;
typedef pool_perthread_and_shared<NodeType, Allocator> Pool;

#if defined(linux) && ENABLE_NUMA == 1
void print_bitmask(const struct bitmask *bm) {
    for (size_t i = 0; i < bm->size; ++i)
        printf("%d", numa_bitmask_isbitset(bm, i));
}
#endif

template<class reclaimer>
void multiTest() {
    //typedef typename reclaimer::template rebind2<NodeType, Pool> Reclaimer;
    typedef record_manager<reclaimer, Allocator, Pool, NodeType> RManager;
    typedef bst_ns::bst<uint64_t, uint64_t, std::less<uint64_t>, RManager> BinaryTree;
    BinaryTree *bt = new BinaryTree(-1, -1, thread_number, NEUTRLIZE_SIGNAL);
    std::vector<std::thread> workers;
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
    Tracer tracer;
    tracer.startTime();
    for (size_t t = 0; t < thread_number; ++t) {
        workers.push_back(std::thread([](BinaryTree *bt, size_t tid) {
            bt->initThread(tid);
            size_t start = tid * total_count / thread_number;
            size_t end = (tid + 1) * total_count / thread_number;
            for (size_t i = start; i < end; ++i) bt->insert(tid, loads[i], loads[i]);
        }, bt, t));
#if defined(linux) && ENABLE_NUMA == 1
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t, &cpuset);
        int rc = pthread_setaffinity_np(workers[t].native_handle(), sizeof(cpu_set_t), &cpuset);
#endif
    }
    for (size_t t = 0; t < thread_number; ++t) {
        workers[t].join();
    }
    std::cout << "Insert: " << tracer.getRunTime() << std::endl;
#if RESHUFFLE == 1
    std::random_shuffle(loads, loads + total_count);
#endif
    workers.clear();
    std::atomic<uint64_t> indicator{0};
    tracer.startTime();
    Timer timer;
    timer.start();

    for (size_t t = 0; t < thread_number; ++t) {
        workers.push_back(std::thread([](BinaryTree *bt, size_t tid, std::atomic<uint64_t> &indicator) {
            bt->initThread(tid);
            Tracer tracer;
            tracer.startTime();
            size_t start = tid * total_count / thread_number;
            size_t end = (tid + 1) * total_count / thread_number;
            while (indicator.load() == 0) {
                uint64_t counter = 0, hit = 0;
                size_t even = 0;
                for (size_t i = start; i < end; ++i) {
                    switch (operation_type) {
                        case 0: {
                            std::pair<uint64_t, bool> ret = bt->find(tid, loads[i]);
                            if (!ret.second || ret.first == loads[i]) hit++;
                            break;
                        }
                        case 1: {
                            bt->insert(tid, loads[i], loads[i]);
                            break;
                        }
                        case 2: {
                            uint64_t keys[KEY_RANGE], values[KEY_RANGE];
                            int rcnt = bt->rangeQuery(tid, loads[i], loads[i] + KEY_RANGE, keys, values);
                            for (int j = 0; j < rcnt; ++j) if (keys[j] == loads[i] + j) hit++;
                            break;
                        }
                        case 3: {
                            if ((i - start + tid) % (100 / update_ratio) == 0) bt->insert(tid, loads[i], loads[i]);
                            else {
                                std::pair<uint64_t, bool> ret = bt->find(tid, loads[i]);
                                if (!ret.second || ret.first == loads[i]) hit++;
                            }
                            break;
                        }
                        case 4: {
                            if ((i - start + tid) % (100 / update_ratio) == 0) bt->insert(tid, loads[i], loads[i]);
                            else {
                                uint64_t keys[KEY_RANGE], values[KEY_RANGE];
                                int rcnt = bt->rangeQuery(tid, loads[i], loads[i] + KEY_RANGE, keys, values);
                                for (int j = 0; j < rcnt; ++j) if (keys[j] == loads[i] + j) hit++;
                            }
                            break;
                        }
                        case 5: {
                            if (even % 2 == 0) bt->erase(tid, loads[i] + even);
                            else bt->insert(tid, loads[i] + even + 1, loads[i] + even + 1);
                            break;
                        }
                    }
                    counter++;
                }
                even++;
                total_ops.fetch_add(counter);
                total_hit.fetch_add(hit);
            }
            total_time.fetch_add(tracer.getRunTime());
        }, bt, t, std::ref(indicator)));
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

    indicator.store(1);

    for (size_t t = 0; t < thread_number; t++) {
        workers[t].join();
    }
    std::cout << "Operations: " << tracer.fetchTime() << " opers: " << total_ops.load() << " ops: "
              << ((double) total_ops.load() * thread_number / total_time.load()) << " hit: " << total_hit.load()
              << std::endl;

    delete bt;
}

int main(int argc, char **argv) {
    if (argc > 6) {
        thread_number = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        timer_limit = std::atol(argv[3]);
        reclaimer_type = std::atoi(argv[4]);
        operation_type = std::atoi(argv[5]);
        update_ratio = std::atoi(argv[6]);
    }

    loads = (uint64_t *) std::calloc(total_count, sizeof(uint64_t));
    for (int i = 0; i < total_count; i++) loads[i] = i;
    std::random_shuffle(loads, loads + total_count);

    switch (reclaimer_type) {
        case 0:
            multiTest<reclaimer_hazardptr<NodeType, Pool>>();
            break;
        case 1:
            multiTest<reclaimer_ebr_token<NodeType, Pool>>();
            break;
        case 2:
            multiTest<reclaimer_ebr_tree<NodeType, Pool>>();
            break;
        case 3:
            multiTest<reclaimer_ebr_tree_q<NodeType, Pool>>();
            break;
        case 4:
            multiTest<reclaimer_debra<NodeType, Pool>>();
            break;
        case 5:
            multiTest<reclaimer_debraplus<NodeType, Pool>>();
            break;
        case 6:
            multiTest<reclaimer_debracap<NodeType, Pool>>();
            break;
        default:
            multiTest<reclaimer_none<NodeType, Pool>>();
            break;
    }
    return 0;
}