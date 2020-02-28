//
// Created by iclab on 2/26/20.
//

#include <cstring>
#include <vector>
#include "gtest/gtest.h"
#include "stringcontext.h"
#include "faster.h"

#define KV_LENGTH 16

#define KEY_RANGE 1

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
        readContext.set(value);
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
        readContext.set(value);
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
                    readContext.set(value);
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
                readContext.set(value);
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

#define OPERATION 10000000
#define KEY_RANGE 100000
#define THD_COUNT 4
std::atomic<int> completed[2 * THD_COUNT];

TEST(FASTERTest, uint64MultiIncrementWriters) {
    std::vector<std::thread> readers;
    std::vector<std::thread> writers;
    std::vector<std::thread> rewriters;
    for (int i = 0; i < THD_COUNT; i++) {
        writers.push_back(std::thread([]() {
            auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            char key[KV_LENGTH];
            char val[255];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (uint32_t i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + 2 * OPERATION);
                    std::memset(val, 0, 255);
                    std::sprintf(val, "val%llu", i + 2 * OPERATION);
                    UpsertContext upsertContext{Key((uint8_t *) key, std::strlen(key)),
                                                Value((uint8_t *) val, std::strlen(val))};
                    Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
                    ASSERT_EQ(uStat, Status::Ok);
                }
        }));
    }
    for (int i = 0; i < THD_COUNT; i++) {
        writers[i].join();
    }
    std::cout << "\t" << "Testing begins" << std::endl;
    for (int i = 0; i < THD_COUNT; i++) {
        completed[i].store(0);
        readers.push_back(std::thread([](int tid) {
            auto readCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<ReadContext> context{ctxt};
            };
            char key[KV_LENGTH];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (uint32_t i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + 2 * OPERATION);
                    ReadContext readContext{Key((uint8_t *) key, std::strlen(key))};
                    Status uStat = store->Read(readContext, readCallback, 1);
                    ASSERT_EQ(uStat, Status::Ok);
                }
            completed[tid].store(1);
        }, i));
    }
    for (int i = 0; i < THD_COUNT; i++) {
        completed[THD_COUNT + i].store(0);
        rewriters.push_back(std::thread([](int tid) {
            auto upsertCallback = [](IAsyncContext *ctxt, Status result) {
                CallbackContext<UpsertContext> context{ctxt};
            };
            char key[KV_LENGTH];
            char val[255];
            for (uint64_t r = 0; r < OPERATION / KEY_RANGE; r++)
                for (uint32_t i = 0; i < KEY_RANGE; i++) {
                    std::memset(key, 0, KV_LENGTH);
                    std::sprintf(key, "key%llu", i + 2 * OPERATION);
                    std::memset(val, 0, 255);
                    std::sprintf(val, "valll%llu", (uint64_t) i + 2 * OPERATION * r);
                    UpsertContext upsertContext{Key((uint8_t *) key, std::strlen(key)),
                                                Value((uint8_t *) val, std::strlen(val))};
                    Status uStat = store->Upsert(upsertContext, upsertCallback, 1);
                    ASSERT_EQ(uStat, Status::Ok);
                }
            completed[tid].store(1);
        }, THD_COUNT + i));
    }
    uint64_t status = 0;
    for (int i = 0; i < THD_COUNT; i++) {
        status |= 1llu << i;
        status |= 1llu << (i + THD_COUNT);
    }
    int round = 0;
    while (status != 0) {
        for (int i = 0; i < THD_COUNT; i++) {
            if (completed[i].load() == 1) {
                status &= ((uint64_t) -1 xor (1llu << i));
                std::cout << "\t\t" << "Reader-" << i << " exists" << " " << status << std::endl;
            }
            if (completed[THD_COUNT + i].load() == 1) {
                status &= ((uint64_t) -1 xor (1llu << (THD_COUNT + i)));
                std::cout << "\t\t" << "Writer-" << i << " exists" << " " << status << std::endl;
            }
        }
        std::cout << "\t" << "Round " << round++ << " " << status << std::endl;
        std::this_thread::sleep_for(1000ms);
    }
    for (int i = 0; i < THD_COUNT; i++) {
        readers[i].join();
    }
    for (int i = 0; i < THD_COUNT; i++) {
        rewriters[i].join();
    }
}

int main(int argc, char **argv) {
    store = new store_t(init_size, 17179869184, "storage");
    ::testing::InitGoogleTest(&argc, argv);
    bool ret = RUN_ALL_TESTS();
    delete store;
    return ret;
}