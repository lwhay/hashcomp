//
// Created by iclab on 5/22/20.
//

#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include "gtest/gtest.h"

#include "faster.h"
#include "null_disk.h"
#include "basickvtypes.h"
#include "serializablecontext.h"

using namespace FASTER::api;
using namespace FASTER::io;
using namespace FASTER::core;

TEST(SerializableFaster, KeyLengthTest) {
#ifdef _WIN32
    typedef hreadPoolIoHandler handler_t;
#else
    typedef QueueIoHandler handler_t;
#endif
    typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

    using store_t = FasterKv<Key, Value, disk_t>;

    LightEpoch epoch;

    disk_t disk("test.log", epoch);

    PersistentMemoryMalloc<disk_t> hlog(256 * 1048576, epoch, disk, disk.log(), 0.9, false);

    Key key((uint8_t *) "Hello world", 11);

    typedef Record<Key, Value> record_t;

    Key *pk = (Key *) std::malloc(sizeof(Key) + 11);//new Key((uint8_t *) "Hello world", 11);
    new(pk) Key{(uint8_t *) "Hello world", 11};
    Value *pv = (Value *) std::malloc(sizeof(Value) + 100);
    new(pv) Value{
            (uint8_t *) "from Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom China",
            100};

    size_t rl = sizeof(RecordInfo);
    size_t kh = sizeof(*pk);
    size_t kl = alignof(*pk);
    size_t vh = sizeof(*pv);
    size_t vl = alignof(*pv);
    size_t ks = pk->size();
    size_t vs = pv->size();
    std::cout << hlog.head_address.load().offset() << std::endl;
    std::cout << hlog.GetTailAddress().offset() << std::endl;
    uint32_t record_size = record_t::size(*pk, pv->size());
    uint32_t page;
    Address retval = hlog.Allocate(record_size, page);
    while (retval < hlog.read_only_address.load()) {
        //Refresh();
        // Don't overrun the hlog's tail offset.
        bool page_closed = (retval == Address::kInvalidAddress);
        while (page_closed) {
            page_closed = !hlog.NewPage(page);
            //Refresh();
        }
        retval = hlog.Allocate(record_size, page);
    }
    record_t *record = reinterpret_cast<record_t *>(hlog.Get(retval));
    new(record) record_t{RecordInfo{0, true, false, false, retval}, *pk};

    std::cout << sizeof(*pk) << std::endl;
    std::cout << hlog.head_address.load().offset() << std::endl;
    std::cout << hlog.GetTailAddress().offset() << std::endl;

    assert(sizeof(*pk) == 16);
}

TEST(SerializableFaster, UpsertTest) {
    constexpr uint64_t limit = 10000000000llu;
    constexpr uint64_t range = 100000llu;
#ifdef _WIN32
    typedef hreadPoolIoHandler handler_t;
#else
    typedef QueueIoHandler handler_t;
#endif
    typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

    using store_t = FasterKv<Key, Value, disk_t>;

    store_t store(next_power_of_two(10000000 / 2), 17179869184, "storage");

    for (uint64_t i = 0; i < limit; i += range) {
        char key[255];
        char value[255];
        std::memset(key, 0, 255);
        std::sprintf(key, "%llu", i);
        std::memset(value, 0, 255);
        std::sprintf(value, "%llu", i);

        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{Key((uint8_t *) key, std::strlen(key)), Value((uint8_t *) value, std::strlen(value))};
        Status stat = store.Upsert(context, callback, 1);
        assert(stat == Status::Ok);
        //std::cout << i << std::endl;
    }
    for (uint64_t i = 0; i < limit; i += range) {
        char key[255];
        char value[255];
        std::memset(key, 0, 255);
        std::sprintf(key, "%llu", i);
        std::sprintf(value, "%llu", i);

        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
        ReadContext context{Key((uint8_t *) key, std::strlen(key))};
        Status stat = store.Read(context, callback, 1);
        assert(stat == Status::Ok);
        char *output = (char *) context.output_bytes;
        assert(std::strncmp(value, output, context.output_length) == 0);
        //std::cout << i << std::endl;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}