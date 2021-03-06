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
    constexpr uint64_t ssize = 255;
#ifdef _WIN32
    typedef hreadPoolIoHandler handler_t;
#else
    typedef QueueIoHandler handler_t;
#endif
    typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

    using store_t = FasterKv<Key, Value, disk_t>;

    store_t store(next_power_of_two(10000000 / 2), 17179869184, "storage");

#if LIGHT_CT_COPY == 1
    char dk[ssize];
    std::memset(dk, 0, ssize);
    std::sprintf(dk, "%llu", 0);
    char dv[ssize];
    std::memset(dv, 0, ssize);
    std::sprintf(dv, "%llu", 0);
    UpsertContext uc{Key((uint8_t *) dk, std::strlen(dk)), Value((uint8_t *) dv, std::strlen(dv))};
#endif
    for (uint64_t i = 0; i < limit; i += range) {
        char key[ssize];
        char value[ssize];
        std::memset(key, 0, ssize);
        std::sprintf(key, "%llu", i);
        std::memset(value, 0, ssize);
        std::sprintf(value, "%llu", i);
        size_t len = std::strlen(value);
        for (; len < ssize - std::strlen(key); len += std::strlen(key)) {
            std::sprintf(value + len, "%llu", i);
        }

        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
#if LIGHT_CT_COPY == 1
        Key k((uint8_t *) key, std::strlen(key));
        Value v((uint8_t *) value, std::strlen(value));
        uc.init(k, v);
#else
        UpsertContext uc{Key((uint8_t *) key, std::strlen(key)), Value((uint8_t *) value, std::strlen(value))};
#endif
        Status stat = store.Upsert(uc, callback, 1);
        assert(stat == Status::Ok);
        //std::cout << i << std::endl;
    }
#if LIGHT_CT_COPY == 1
    char dummy[ssize];
    std::memset(dummy, 0, ssize);
    std::sprintf(dummy, "%llu", 0);
    ReadContext rc{Key((uint8_t *) dummy, std::strlen(dummy))};
#endif
    for (uint64_t i = 0; i < limit; i += range) {
        char key[ssize];
        char value[ssize];
        std::memset(key, 0, ssize);
        std::sprintf(key, "%llu", i);
        std::sprintf(value, "%llu", i);
        size_t len = std::strlen(value);
        for (; len < ssize - std::strlen(key); len += std::strlen(key)) {
            std::sprintf(value + len, "%llu", i);
        }

        auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<ReadContext> context{ctxt};
        };
#if LIGHT_CT_COPY == 1
        Key k((uint8_t *) key, std::strlen(key));
        rc.init(k);
#else
        ReadContext rc{Key((uint8_t *) key, std::strlen(key))};
#endif
        Status stat = store.Read(rc, callback, 1);
        assert(stat == Status::Ok);
        char *output = (char *) rc.output_bytes;
        assert(std::strncmp(value, output, rc.output_length) == 0);
        //std::cout << i << ": " << context.output_length << std::endl;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}