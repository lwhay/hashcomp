#include <iostream>
#include <sstream>
#include <thread>
#include <unistd.h>
#include "AtomicStack.h"
#include "GlobalWriteEM.h"
#include "LocalWriteEM.h"
#include "test_suite.h"

using namespace peloton;
using namespace index;

void BasicTest() {
    PrintTestName("BasicTest");

    AtomicStack<uint64_t> as{};

    for (uint64_t i = 0; i < 100; i++) {
        as.Push(i);
    }

    for (uint64_t i = 0; i < 100; i++) {
        uint64_t top;
        uint64_t expected = 99 - i;
        bool ret = as.Pop(top);

        assert(ret == true);

        assert(top == expected);
    }

    return;
}

void ThreadTest(uint64_t thread_num, uint64_t op_num) {
    PrintTestName("ThreadTest");

    AtomicStack<uint64_t> as{};

    // This counts the number of push operation we have performed
    std::atomic<uint64_t> counter;
    counter.store(0);

    auto push_func = [&as, &counter, thread_num, op_num](uint64_t id) {
        for (uint64_t i = id; i < thread_num * op_num; i += thread_num) {
            as.Push(i);

            // Increase the counter atomically
            counter.fetch_add(1);
        }
    };

    // We use this to count what we have fetched from the stack
    std::atomic<uint64_t> sum;
    sum.store(0);
    counter.store(0);

    auto pop_func = [&as, &sum, &counter, thread_num, op_num](uint64_t id) {
        for (uint64_t i = 0; i < op_num; i++) {
            uint64_t data;
            bool ret = as.Pop(data);

            // The operation must success
            assert(ret == true);

            // Atomically adding the poped value onto the atomic
            sum.fetch_add(data);
        }
    };

    // Let threads start
    StartThreads(thread_num, push_func);

    // We must have performed exactly this number of operations
    assert(counter.load() == thread_num * op_num);

    StartThreads(thread_num, pop_func);

    uint64_t expected = (op_num * thread_num) * (op_num * thread_num - 1) / 2;

    dbg_printf("Sum = %lu; Expected = %lu\n", sum.load(), expected);
    assert(sum.load() == expected);

    return;
}

void MixedTest(uint64_t thread_num, uint64_t op_num) {
    PrintTestName("MixedTest");

    // Make sure thread number is an even number otherwise exit
    if ((thread_num % 2) != 0) {
        dbg_printf("MixedTest requires thread_num being an even number!\n");

        return;
    }

    AtomicStack<uint64_t> as{};

    // This counts the number of push operation we have performed
    std::atomic<uint64_t> counter;
    // We use this to count what we have fetched from the stack
    std::atomic<uint64_t> sum;

    sum.store(0);
    counter.store(0);

    auto func = [&as, &counter, &sum, thread_num, op_num](uint64_t id) {
        // For id = 0, 2, 4, 6, 8 keep popping until op_num
        // operations have succeeded
        if ((id % 2) == 0) {
            for (uint64_t i = 0; i < op_num; i++) {
                while (1) {
                    bool ret;
                    uint64_t data;

                    ret = as.Pop(data);
                    if (ret == true) {
                        sum.fetch_add(data);
                        break;
                    }

                }
            }
        } else {
            // id = 1, 3, 5, 7, 9, ..
            // but we actually make them 0, 1, 2, 3, 4
            // such that the numbers pushed into are consecutive
            id = (id - 1) >> 1;

            // This is the actual number of threads doing Push()
            uint64_t delta = thread_num >> 1;

            for (uint64_t i = id; i < delta * op_num; i += delta) {
                as.Push(i);

                // Increase the counter atomically
                counter.fetch_add(1);
            }
        }
    };

    // Let threads start
    StartThreads(thread_num, func);

    // Make the following computation easier
    thread_num >>= 1;

    // We must have performed exactly this number of operations
    assert(counter.load() == (thread_num * op_num));

    uint64_t expected = (op_num * thread_num) * (op_num * thread_num - 1) / 2;

    dbg_printf("Sum = %lu; Expected = %lu\n", sum.load(), expected);
    assert(sum.load() == expected);

    return;
}

void LocalWriteEMTest() {
    PrintTestName("LocalWriteEMTest");
    struct dummy {
        long a, b;
    };
    LocalWriteEM<dummy> *em = new LocalWriteEM<dummy>(1);
    em->SetGCInterval(1);
    em->StartGCThread();
    std::atomic<bool> signal{false};
    dummy *dn;
    std::thread worker = std::thread([](LocalWriteEM<dummy> *em, dummy *&dn, std::atomic<bool> &signal) {
        bool status = true;
        int idx = 1;
        dn = new dummy;
        em->AnnounceEnter(0);
        signal.store(status);
        status != status;
        dn->a = 1;
        dn->b = 1;
        sleep(1);
        std::stringstream mark;
        if (dn != nullptr) mark << dn->a << ":" << dn->b;
        else mark << "null";
        std::cout << "Creator" << idx++ << " " << mark.str() << std::endl;
        sleep(2);
        for (int i = 0; i < 3; i++) {
            sleep(1);
            if (dn != nullptr) mark << dn->a << ":" << dn->b << "-" << status << "-";
            else mark << "null";
            std::cout << "Creator" << idx++ << " " << mark.str() << std::endl;
            signal.store(status);
            status != status;
        }
        //em->AddGarbageNode(dn);
        em->FreeAllGarbage();
    }, em, std::ref(dn), std::ref(signal));
    std::thread reader = std::thread([](LocalWriteEM<dummy> *em, dummy *&dn, std::atomic<bool> &signal) {
        bool status = true;
        em->AnnounceEnter(0);
        while (signal.load() != status);
        status != status;
        std::stringstream mark;
        if (dn != nullptr) mark << dn->a << ":" << dn->b;
        else mark << "null";
        std::cout << "Reclaimer1 " << mark.str() << std::endl;
        sleep(2);
        if (dn != nullptr) mark << dn->a << ":" << dn->b;
        else mark << "null";
        std::cout << "Reclaimer2 " << mark.str() << std::endl;
        em->AddGarbageNode(dn);
        em->FreeAllGarbage();
        dummy *nd[100];
        for (int i = 0; i < 100; i++) nd[i] = new dummy{i, i};
        for (int i = 0; i < 100; i++) delete nd[i];
        for (int i = 0; i < 3; i++) {
            while (signal.load() != status);
            status != status;
            em->FreeAllGarbage();
            sleep(1);
            /*if (dn != nullptr) mark << dn->a << ":" << dn->b << "-" << status << "-";
            else mark << "null";
            std::cout << "Reclaimer " << mark.str() << std::endl;*/
        }
        if (dn != nullptr) mark << dn->a << ":" << dn->b;
        else mark << "null";
        std::cout << "Reclaimer3 " << mark.str() << std::endl;
    }, em, std::ref(dn), std::ref(signal));
    worker.join();
    reader.join();
    delete em;
}

void GlobalWriteEMTest() {
    PrintTestName("GlobalWriteEMTest");
    struct dummy {
        long a, b;
    };
    GlobalWriteEM<dummy> *em = new GlobalWriteEM<dummy>();
    em->StartGCThread();
    dummy *dn = nullptr;
    std::atomic<bool> signal{false};
    std::thread worker = std::thread([](GlobalWriteEM<dummy> *em, dummy *&dn, std::atomic<bool> &signal) {
        void *ep = em->JoinEpoch();
        std::cout << ep << ":" << em->GetEpochCreated() << std::endl;
        dn = new dummy/*(dummy *) ep*/;
        dn->a = 1;
        dn->b = 1;
        signal.store(true);
        sleep(1);
        em->LeaveEpoch(ep);
        em->FreeGarbageType(dn);
        dn = nullptr;
    }, em, std::ref(dn), std::ref(signal));
    std::thread reader = std::thread([](GlobalWriteEM<dummy> *em, dummy *&dn, std::atomic<bool> &signal) {
        while (!signal.load());
        if (dn != nullptr) std::cout << dn->a << ":" << dn->b << std::endl;
        sleep(2);
        if (dn != nullptr) std::cout << dn->a << ":" << dn->b << std::endl;
    }, em, std::ref(dn), std::ref(signal));
    reader.join();
    worker.join();
    delete em;
}

int main() {
    GlobalWriteEMTest();

    LocalWriteEMTest();

    BasicTest();
    // Many threads and small number of data
    ThreadTest(1024, 10);
    // Many data and smaller number of threads
    ThreadTest(4, 2000000);

    // Half push half pop, pop loops until it succeeds
    MixedTest(32, 100000);

    return 0;
}
