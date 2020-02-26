//
// Created by iclab on 2/25/20.
//

#include <cstring>
#include <deque>
#include <vector>
#include "gtest/gtest.h"
#include "kvcontext.h"
#include "faster.h"

#define KEY_RANGE 1

#define OPERATION 10000000

using namespace FASTER::api;

typedef QueueIoHandler handler_t;
typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

using store_t = FasterKv<Key, Value, disk_t>;
size_t init_size = next_power_of_two(1000000 / 2);
store_t *store;

TEST(FASTERTest, uint64Test) {
    auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
    };
    for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
        for (uint64_t i = 0; i < KEY_RANGE; i++) {
            UpsertContext upsertContext{i, i};
            Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
        }
    UpsertContext upsertContext{0, 0};
    Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
}

TEST(FASTERTest, uint64MultiWriters) {
    std::vector<std::thread> writers;
    for (int i = 0; i < 4; i++) {
        writers.push_back(std::thread([]() {
            auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (int i = 0; i < KEY_RANGE; i++) {
                    UpsertContext upsertContext{i, i + 1000000};
                    Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
                }
        }));
    }
    for (int i = 0; i < 4; i++) {
        writers[i].join();
    }
}

TEST(FASTERTest, uint64MultiReaders) {
    std::vector<std::thread> readers;
    std::vector<std::thread> writers;
    for (int i = 0; i < 4; i++) {
        writers.push_back(std::thread([]() {
            auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (int i = 0; i < KEY_RANGE; i++) {
                    UpsertContext upsertContext{i, i + 1000000};
                    Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
                }
        }));
    }
    for (int i = 0; i < 4; i++) {
        readers.push_back(std::thread([]() {
            auto readCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (int i = 0; i < KEY_RANGE; i++) {
                    ReadContext readContext{i};
                    Status uStat = store->Read(readContext, readCallback, 1);
                    ASSERT_EQ(uStat, Status::Ok);
                    Value value;
                    readContext.Put(value);
                    ASSERT_EQ(value.Get(), 1000000 + i);
                }
        }));
    }
    for (int i = 0; i < 4; i++) {
        readers[i].join();
    }
    for (int i = 0; i < 4; i++) {
        writers[i].join();
    }
}

TEST(FASTERTest, uint64MultiDeleters) {
    std::vector<std::thread> deleters;
    for (int i = 0; i < 4; i++) {
        deleters.push_back(std::thread([](int tid) {
            auto deleteCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<DeleteContext> context{ctxt};
            };
            for (int i = tid; i < KEY_RANGE; i += 4) {
                DeleteContext deleteContext{i};
                Status uStat = store->Delete(deleteContext, deleteCallback, 1);
                ASSERT_EQ(uStat, Status::Ok);
            }
        }, i));
    }
    for (int i = 0; i < 4; i++) {
        deleters[i].join();
    }
}

TEST(FASTERTest, uint64MultiReReaders) {
    std::vector<std::thread> readers;
    for (int i = 0; i < 4; i++) {
        readers.push_back(std::thread([]() {
            auto readCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (int i = 0; i < KEY_RANGE; i++) {
                    ReadContext readContext{i + 1000000};
                    Status uStat = store->Read(readContext, readCallback, 1);
                    ASSERT_EQ(uStat, Status::NotFound);
                }
        }));
    }
    for (int i = 0; i < 4; i++) {
        readers[i].join();
    }
}

int main(int argc, char **argv) {
    store = new store_t(init_size, 17179869184, "storage");
    ::testing::InitGoogleTest(&argc, argv);
    bool ret = RUN_ALL_TESTS();
    delete store;
    return ret;
}