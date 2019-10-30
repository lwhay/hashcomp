//
// Created by iclab on 10/7/19.
//

#include <atomic>
#include <iostream>
#include <thread>
#include <time.h>
#include "faster.h"
#include "gtest/gtest.h"
#include "cvkvcontext.h"

using namespace std;
using namespace FASTER::api;
using namespace FASTER::io;
using namespace FASTER::core;

#define PRELOAD false

atomic<uint64_t> tick;
atomic<uint64_t> tickMulti;
constexpr uint64_t rounds = (1 << 20);
constexpr uint64_t bfsize = 256;
constexpr uint64_t insize = 64;
constexpr uint64_t update_number = 4;
constexpr uint64_t read_number = 4;
stringstream output[read_number];

TEST(ContextVerify, ConcurrentRU) {
    typedef QueueIoHandler handler_t;
    typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;
    using store_t = FasterKv<Key, Value, disk_t>;
    size_t init_size = next_power_of_two(512 / 2);
    store_t store{init_size, 17179869184, "storage"};
    Value loads[bfsize];
    uint64_t values[bfsize];
    for (uint64_t i = 0; i < bfsize; i++) {
        values[i] = bfsize - i % bfsize;
        loads[i].reset((uint8_t *) (values + i), 8);
    }

    auto updater = [](Value *loads, store_t *store, atomic<uint64_t> *tick) {
        for (int i = 0; i < rounds; i++) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            UpsertContext context(0, 8);
            context.reset(loads[i % bfsize].get());
            uint64_t *value = (uint64_t *) (loads[i % bfsize].get());
            ASSERT_EQ(*value, 256 - i % 256);
            Status stat = store->Upsert(context, callback, 1);
            tick->fetch_add(1);
        }
    };

    auto reader = [](store_t *store, atomic<uint64_t> *tick) {
        uint64_t oldtick = 0;
        for (int i = 0; i < (1 << 6); i++) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            ReadContext rc(0);
            while (tick->load() - oldtick < (1 << 14) && tick->load() < rounds) {
                this_thread::yield();
            }
            oldtick = tick->load();
            Status stat = store->Read(rc, callback, 1);

            // Nice print
            if (i % 8 == 0) cout << endl;
            cout.width(7);
            cout << tick->load();
            cout;
            cout << ":";
            cout.width(2);
            cout << i;
            cout;
            cout << "<->";
            cout.width(3);
            cout << *((uint64_t *) rc.output_bytes);
            cout;
            cout << " ";
        }
        cout << endl;
    };

    std::thread updateThread(updater, loads, &store, &tick);
    std::thread readThread(reader, &store, &tick);
    updateThread.join();
    readThread.join();
}

TEST(ContextVerify, NanoSleep) {
    timespec timespec;
    timespec.tv_sec = 0;
    timespec.tv_nsec = 1;
    timeval begin, end;
    gettimeofday(&begin, nullptr);
    for (int i = 0; i < 1000; i++) nanosleep(&timespec, nullptr);
    gettimeofday(&end, nullptr);
    cout << (end.tv_sec - begin.tv_sec) * 1000000 + end.tv_usec - begin.tv_usec << endl;
    gettimeofday(&begin, nullptr);
    for (int i = 0; i < 1000; i++) usleep(1000);
    gettimeofday(&end, nullptr);
    cout << (end.tv_sec - begin.tv_sec) * 1000000 + end.tv_usec - begin.tv_usec << endl;
}

TEST(ContextVerify, ConcurrentRUMultiBytes) {
    typedef QueueIoHandler handler_t;
    typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;
    using store_t = FasterKv<Key, Value, disk_t>;
    size_t init_size = next_power_of_two(512 / 2);
    store_t store{init_size, 17179869184, "storage"};
    Value loads[bfsize];
    atomic_bool begin(false);
    uint8_t **values = new uint8_t *[bfsize];
    for (uint64_t i = 0; i < bfsize; i++) {
        values[i] = new uint8_t[insize];
        for (int j = 0; j < insize / sizeof(uint64_t); j++) {
            *(uint64_t *) (values[i] + j * insize / sizeof(uint64_t)) = bfsize - i % bfsize;
        }
        loads[i].reset(values[i], insize);
    }

    auto updater = [](Value *loads, store_t *store, atomic<uint64_t> *tick, atomic_bool *begin) {
        while (!begin->load()) this_thread::yield();
        uint8_t buffer[insize];
        for (int i = 0; i < rounds / update_number; i++) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            UpsertContext context(0, insize);
#if PRELOAD
            context.reset(loads[i % bfsize].get());
#else
            for (int j = 0; j < insize / sizeof(uint64_t); j++) {
                *(uint64_t *) (buffer + j * insize / sizeof(uint64_t)) = i;
            }
            /*for (int j = 0; j < insize / sizeof(uint64_t); j++) {
                ASSERT_EQ(*(uint64_t *) (buffer + j * insize / sizeof(uint64_t)), *((uint64_t *) buffer));
            }*/
            context.reset(buffer);
#endif
            uint64_t *value = (uint64_t *) (loads[i % bfsize].get());
            ASSERT_EQ(*value, 256 - i % 256);
            Status stat = store->Upsert(context, callback, 1);
            tick->fetch_add(1);
            //usleep(5);
        }
    };

    auto reader = [](store_t *store, atomic<uint64_t> *tick, atomic_bool *begin, int *tid) {
        uint64_t oldtick = 0;
        uint64_t i = 0;
        begin->store(true);
        output[*tid] << *tid << ":" << i << "<->" << tick->load() << endl;
        while (tick->load() < rounds) {
            auto callback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            ReadContext rc(0);
            Status stat = store->Read(rc, callback, 1);
            if (tick->load() - oldtick < (1 << 14)) continue;
            oldtick = tick->load();
            // Nice print
            if (i % 8 == 0 && i != 0) output[*tid] << endl;
            output[*tid].width(7);
            output[*tid] << tick->load();
            output[*tid];
            output[*tid] << ":";
            output[*tid].width(2);
            output[*tid] << i++;
            output[*tid];
            output[*tid] << "<->";
            output[*tid].width(6);
            output[*tid] << *((uint64_t *) rc.output_bytes);
            output[*tid];
            output[*tid] << " ";
            for (int j = 0; j < insize / sizeof(uint64_t); j++) {
                ASSERT_EQ(*(uint64_t *) (rc.output_bytes + j * insize / sizeof(uint64_t)),
                          *((uint64_t *) rc.output_bytes));
            }
        }
        output[*tid] << endl << i << "<->" << tick->load() << endl;
    };

    std::thread updateThreads[update_number];
    std::thread readThreads[read_number];
    int tids[read_number];
    for (int i = 0; i < update_number; i++)
        updateThreads[i] = std::thread(updater, loads, &store, &tickMulti, &begin);
    for (int i = 0; i < read_number; i++) {
        tids[i] = i;
        readThreads[i] = std::thread(reader, &store, &tickMulti, &begin, tids + i);
    }
    for (int i = 0; i < update_number; i++)
        updateThreads[i].join();
    for (int i = 0; i < read_number; i++) {
        readThreads[i].join();
        string outstr = output[i].str();
        cout << outstr;
    }
    delete[] values;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}