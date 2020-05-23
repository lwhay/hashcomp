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

    typedef Record<Key, Value> record_t;

    Key key{(uint8_t *) "Hello world", 11};
    Value value{
            (uint8_t *) "from Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom Chinafrom China",
            100};

    size_t rl = sizeof(RecordInfo);
    size_t kh = sizeof(key);
    size_t kl = alignof(key);
    size_t vh = sizeof(value);
    size_t vl = alignof(value);
    size_t ks = key.size();
    size_t vs = value.size();
    std::cout << hlog.head_address.load().offset() << std::endl;
    std::cout << hlog.GetTailAddress().offset() << std::endl;
    uint32_t record_size = record_t::size(key, value.size());
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
    //new(record) record_t{RecordInfo{0, true, false, false, retval}, key};

    std::cout << sizeof(key) << std::endl;
    std::cout << hlog.head_address.load().offset() << std::endl;
    std::cout << hlog.GetTailAddress().offset() << std::endl;
    assert(sizeof(key) == 16);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}