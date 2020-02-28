//
// Created by iclab on 2/26/20.
//

#include <cstring>
#include <deque>
#include <vector>
#include "gtest/gtest.h"
#include "stringcontext.h"
#include "faster.h"

#define KV_LENGTH 16

#define KEY_RANGE 4

#define OPERATION 100

using namespace FASTER::api;

typedef QueueIoHandler handler_t;
typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

using store_t = FasterKv<Key, Value, disk_t>;
size_t init_size = next_power_of_two(OPERATION);
store_t *store;

TEST(FASTERTest, incrementalTest) {
    auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
    };
    char key[KV_LENGTH];
    char ret[255];
    char *val = new char[255];
    std::memset(key, 0, KV_LENGTH);
    std::sprintf(key, "key%llu", 100000000);
    std::memset(val, 0, KV_LENGTH);
    std::memcpy(val, "val", 3);

    for (uint64_t i = 0; i < 200; i++) {
        char inc[2];
        std::memset(inc, 0, 2);
        std::sprintf(inc, "%llu", 1);
        val = std::strcat(val, inc);
        UpsertContext upsertContext{Key((uint8_t *) key, KV_LENGTH), Value((uint8_t *) val, std::strlen(val))};
        Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
        ASSERT_EQ(uStat, Status::Ok);

        auto readCallback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };

        ReadContext readContext{Key((uint8_t *) key, KV_LENGTH)};
        uStat = store->Read(readContext, readCallback, 1);
        ASSERT_EQ(uStat, Status::Ok);
        Value value;
        readContext.Put(value);
        std::memset(ret, 0, 255);
        std::memcpy(ret, value.get(), value.length());
        ASSERT_STREQ(ret, val);
    }
}

TEST(FASTERTest, singleTest) {
    auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<UpsertContext> context{ctxt};
    };
    char key[KV_LENGTH];
    char val[KV_LENGTH];
    char ret[KV_LENGTH];
    for (uint64_t i = 0; i < OPERATION; i++) {
        std::memset(key, 0, KV_LENGTH);
        std::sprintf(key, "key%llu", i);
        std::memset(val, 0, KV_LENGTH);
        std::sprintf(val, "val%llu", i);
        UpsertContext upsertContext{Key((uint8_t *) key, KV_LENGTH), Value((uint8_t *) val, KV_LENGTH)};
        Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
    }

    auto readCallback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<ReadContext> context{ctxt};
    };
    for (int i = 0; i < OPERATION; i++) {
        std::memset(key, 0, KV_LENGTH);
        std::sprintf(key, "key%llu", i);
        ReadContext readContext{Key((uint8_t *) key, KV_LENGTH)};
        Status uStat = store->Read(readContext, readCallback, 1);
        ASSERT_EQ(uStat, Status::Ok);
        std::memset(val, 0, KV_LENGTH);
        std::sprintf(val, "val%llu", i);
        Value value;
        readContext.Put(value);
        std::memset(ret, 0, KV_LENGTH);
        std::memcpy(ret, value.get(), value.length());
        ASSERT_STREQ(ret, val);
    }

    auto deleteCallback = [](IAsyncContext *ctxt, Status result) {
        CallbackContext<DeleteContext> context{ctxt};
    };
    for (int i = 0; i < OPERATION; i++) {
        std::memset(key, 0, KV_LENGTH);
        std::sprintf(key, "key%llu", i);
        DeleteContext deleteContext{Key((uint8_t *) key, KV_LENGTH)};
        Status uStat = store->Delete(deleteContext, deleteCallback, 1);
        ASSERT_EQ(uStat, Status::Ok);
    }

    for (int i = 0; i < OPERATION; i++) {
        std::memset(key, 0, KV_LENGTH);
        std::sprintf(key, "key%llu", i);
        ReadContext readContext{Key((uint8_t *) key, KV_LENGTH)};
        Status uStat = store->Read(readContext, readCallback, 1);
        ASSERT_EQ(uStat, Status::NotFound);
    }
}

TEST(FASTERTest, uint64MultiWriters) {
    std::vector<std::thread> writers;
    for (int i = 0; i < 4; i++) {
        writers.push_back(std::thread([]() {
            auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            char key[KV_LENGTH];
            char val[KV_LENGTH];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (uint32_t i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + OPERATION);
                    std::memset(val, 0, KV_LENGTH);
                    std::sprintf(val, "val%llu", i + OPERATION);
                    UpsertContext upsertContext{Key((uint8_t *) key, KV_LENGTH), Value((uint8_t *) val, KV_LENGTH)};
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
            char key[KV_LENGTH];
            char val[KV_LENGTH];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (uint32_t i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + OPERATION);
                    std::memset(val, 0, KV_LENGTH);
                    std::sprintf(val, "val%llu", i + OPERATION);
                    UpsertContext upsertContext{Key((uint8_t *) key, KV_LENGTH), Value((uint8_t *) val, KV_LENGTH)};
                    Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
                }
        }));
    }
    for (int i = 0; i < 4; i++) {
        readers.push_back(std::thread([]() {
            auto readCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            char key[KV_LENGTH];
            char val[KV_LENGTH];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (int i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + OPERATION);
                    std::memset(val, 0, KV_LENGTH);
                    std::sprintf(val, "val%llu", i + OPERATION);
                    ReadContext readContext{Key((uint8_t *) key, KV_LENGTH)};
                    Status uStat = store->Read(readContext, readCallback, 1);
                    ASSERT_EQ(uStat, Status::Ok);
                    Value value;
                    readContext.Put(value);
                    ASSERT_STREQ((char *) value.get(), val);
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
            auto readCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            auto deleteCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<DeleteContext> context{ctxt};
            };
            char key[KV_LENGTH];
            for (int i = tid; i < KEY_RANGE; i += 4) {
                if (i >= KEY_RANGE) break;
                std::memset(key, 0, KV_LENGTH);
                std::sprintf(key, "key%llu", i + OPERATION);
                ReadContext readContext{Key((uint8_t *) key, KV_LENGTH)};
                Status uStat = store->Read(readContext, readCallback, 1);
                Value value;
                readContext.Put(value);
                //std::cout << key << " " << Utility::retStatus(uStat) << std::endl;
                ASSERT_EQ(uStat, Status::Ok);

                DeleteContext deleteContext{Key((uint8_t *) key, KV_LENGTH)};
                uStat = store->Delete(deleteContext, deleteCallback, 1);
                //std::cout << key << " " << static_cast<uint32_t>(uStat) << std::endl;
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
            char key[KV_LENGTH];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (uint32_t i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + OPERATION);
                    ReadContext readContext{Key((uint8_t *) key, KV_LENGTH)};
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