//
// Created by Michael on 3/30/20.
//

#include <atomic>
#include <functional>
#include <iostream>
#include <signal.h>
#include <thread>
#include <vector>

#include "reclaimer_debra.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "bst_impl.h"
//#include "chromatic_impl.h"
#include "record_manager.h"
#include "node.h"
#include "tracer.h"

uint64_t thread_number = 4;
uint64_t total_count = (1llu << 20);
uint64_t timer_limit = 30;
constexpr uint64_t NEUTRLIZE_SIGNAL = SIGQUIT;

typedef bst_ns::Node<uint64_t, uint64_t> NodeType;
typedef reclaimer_debra<NodeType, pool_perthread_and_shared<NodeType, allocator_new<NodeType>>> Reclaimer;
typedef allocator_new<NodeType> Allocator;
typedef pool_perthread_and_shared<NodeType, allocator_new<NodeType>> Pool;
typedef record_manager<Reclaimer, Allocator, Pool, NodeType> RManager;
typedef bst_ns::bst<uint64_t, uint64_t, std::less<uint64_t>, RManager> BinaryTree;

void DummyTest() {
    unsigned char testin[8] = {0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0xff};
    for (int i = 0; i < 64; i++) {
        if (testin[i / 8] & 1llu << (i % 8)) std::cout << "1"; else std::cout << "0";
        if (i % 8 == 3) std::cout << "-";
        if (i % 8 == 7) std::cout << " ";
    }
    std::cout << std::endl;
    uint64_t testul = *(uint64_t *) testin;
    for (int i = 0; i < 64; i++) {
        if (testul & 1llu << i) std::cout << "1"; else std::cout << "0";
        if (i % 8 == 3) std::cout << "-";
        if (i % 8 == 7) std::cout << " ";
    }
    std::cout << std::endl;
}

void SimpleTest() {
    Tracer tracer;
    tracer.startTime();
    RManager mgr(thread_number, NEUTRLIZE_SIGNAL);
    BinaryTree *tree = new BinaryTree(-1, -1, thread_number, NEUTRLIZE_SIGNAL);
    tree->initThread(0);
    std::cout << "Init: " << tracer.getRunTime() << std::endl;

    for (uint64_t r = 0; r < timer_limit; r++) {
        for (uint64_t i = 0; i < total_count; i++) {
            tree->insert(0, i, i);
            if ((i + 1) % (1llu << 20) == 0) std::cout << "\t" << i << ": " << tree->size() << std::endl;
        }
        std::cout << "Insert" << r << ": " << tracer.getRunTime() << std::endl;

        for (uint64_t i = 0; i < total_count; i++) {
            tree->erase(0, i);
            if ((i + 1) % (1llu << 20) == 0) std::cout << "\t" << i << ": " << tree->size() << std::endl;
        }
        std::cout << "Erase" << r << ": " << tracer.getRunTime() << std::endl;
    }
    delete tree;
}

void MultiTest() {
    Tracer tracer;
    tracer.startTime();
    RManager mgr(thread_number, NEUTRLIZE_SIGNAL);
    BinaryTree *tree = new BinaryTree(-1, -1, thread_number, NEUTRLIZE_SIGNAL);
    std::cout << "Init: " << tracer.getRunTime() << std::endl;
    std::atomic<uint64_t> indicator{0};
    std::vector<std::thread> threads;
    Timer timer;
    timer.start();

    for (size_t t = 0; t < thread_number; t++) {
        threads.push_back(std::thread([](BinaryTree *tree, size_t tid, std::atomic<uint64_t> &indicator) {
            tree->initThread(tid);
            Tracer tracer;
            tracer.startTime();
            uint64_t r = 0;
            while (indicator.load() == 0) {
                for (uint64_t i = tid; i < total_count; i += thread_number) {
                    tree->insert(tid, i, i);
                    //if ((i + 1) % (1llu << 20) == 0) std::cout << "\t" << i << ": " << tree->size() << std::endl;
                }
                if (tid == 0) std::cout << "Insert" << r << ": " << tracer.getRunTime() << std::endl;

                for (uint64_t i = tid; i < total_count; i += thread_number) {
                    tree->erase(tid, i);
                    //if ((i + 1) % (1llu << 20) == 0) std::cout << "\t" << i << ": " << tree->size() << std::endl;
                }
                if (tid == 0) std::cout << "Erase" << r++ << ": " << tracer.getRunTime() << std::endl;
            }
        }, tree, t, std::ref(indicator)));
    }

    while (timer.elapsedSeconds() < timer_limit) {
        sleep(1);
    }

    indicator.store(1);

    for (size_t t = 0; t < thread_number; t++) {
        threads[t].join();
    }
    std::cout << "Operations: " << tracer.getRunTime() << std::endl;

    delete tree;
}

int main(int argc, char **argv) {
    if (argc > 3) {
        thread_number = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        timer_limit = std::atol(argv[3]);
    }
    //DummyTest();
    //SimpleTest();
    MultiTest();
    return 0;
}