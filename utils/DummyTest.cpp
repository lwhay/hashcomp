//
// Created by iclab on 10/7/19.
//

#include <atomic>
#include <bitset>
#include <cassert>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>
#include "tracer.h"

using namespace std;

uint64_t *loads;

uint64_t key_range = (1llu << 20);

uint64_t total_count = (1llu << 20);

uint64_t thread_number = 4;

char *switcher = "111111111";

double distribution_skew = .0f;

struct record {
    std::atomic<uint64_t> header1;
    uint64_t key;
    uint64_t value;
    uint8_t unaligned;
    bool ub;
};

thread_local double value;

std::atomic<uint64_t> total_time{0};

std::atomic<uint64_t> total_tick{0};

atomic<int> stopMeasure(0);

uint64_t timer_range = 30;

void RecordTest() {
    std::vector<std::thread> workers;
    record *records = new record[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[i].header1.store(loads[i]);
        records[i].key = loads[i];
        records[i].value = loads[i];
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record *records, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    value += records[loads[i] % total_count].value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, records, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "Record Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    delete[] records;
}

void RecordPtrTest() {
    std::vector<std::thread> workers;
    record **records = new record *[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[i] = new record;
        records[i]->header1.store(loads[i]);
        records[i]->key = loads[i];
        records[i]->value = loads[i];
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    value += records[loads[i] % total_count]->value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, records, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "RecordPtr Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) delete records[i];
    delete[] records;
}

void RecordScanTest() {
    std::vector<std::thread> workers;
    record **records = new record *[total_count];
    std::memset(records, 0, sizeof(record *) * total_count);
    for (uint64_t i = 0; i < total_count; i++) {
        uint64_t idx = loads[i] % total_count;
        records[idx] = new record;
        records[idx]->header1.store(loads[i]);
        records[idx]->key = loads[i];
        records[idx]->value = loads[i];
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    value += records[loads[i] % total_count]->value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, records, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "RecordScan Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) {
        uint64_t idx = loads[i] % total_count;
        if (records[idx] == nullptr) continue;
        delete records[idx];
        records[idx] = nullptr;
    }
    delete[] records;
}

#define BIG_CONSTANT(x) (x##LLU)

uint64_t MurmurHash64A(const void *key, int len, uint64_t seed) {
    const uint64_t m = BIG_CONSTANT(0xc6a4a7935bd1e995);
    const int r = 47;

    uint64_t h = seed ^(len * m);

    const uint64_t *data = (const uint64_t *) key;
    const uint64_t *end = data + (len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *) data;

    switch (len & 7) {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
        case 6:
            h ^= uint64_t(data2[5]) << 40;
        case 5:
            h ^= uint64_t(data2[4]) << 32;
        case 4:
            h ^= uint64_t(data2[3]) << 24;
        case 3:
            h ^= uint64_t(data2[2]) << 16;
        case 2:
            h ^= uint64_t(data2[1]) << 8;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

void RecordHashTest() {
    std::vector<std::thread> workers;
    record **records = new record *[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[i] = new record;
        records[i]->header1.store(loads[i]);
        records[i]->key = loads[i];
        records[i]->value = loads[i];
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    uint64_t hash =
                            MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                    uint64_t mark = records[hash]->header1.load();
                    value += records[hash]->value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, records, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "RecordHash Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) delete records[i];
    delete[] records;
}

constexpr uint64_t block_size = (1llu << 20);

uint64_t block_remaining = 0;

std::vector<uint64_t> blocks;

void RecordBlockHashTest() {
    std::vector<std::thread> workers;
    record **records = new record *[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        if (block_remaining <= sizeof(record)) {
            blocks.push_back((uint64_t) std::malloc(block_size));
            block_remaining = block_size;
        }
        records[i] = (record *) (blocks.back() + block_size - block_remaining);
        records[i]->header1.store(loads[i]);
        records[i]->key = loads[i];
        records[i]->value = loads[i];
        block_remaining -= sizeof(record);
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    uint64_t hash =
                            MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                    uint64_t mark = records[hash]->header1.load();
                    value += records[hash]->value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, records, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "RecordBlockHash Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < blocks.size(); i++) std::free((void *) blocks[i]);
    delete[] records;
}

#ifdef linux

#include <numa.h>

void RecordNumaBlockHashTest() {
    block_remaining = 0;
    blocks.clear();
    std::vector<std::thread> workers;
    record **records = new record *[total_count];
    unsigned num_cpus = std::thread::hardware_concurrency();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (size_t i = 1; i < num_cpus; i++) CPU_SET(i, &cpuset);
    std::cout << num_cpus << "\t" << *(cpuset.__bits) << std::endl;
    for (uint64_t i = 0; i < total_count; i++) {
        if (block_remaining <= sizeof(record)) {
            blocks.push_back((uint64_t) std::malloc(block_size));
            block_remaining = block_size;
        }
        records[i] = (record *) (blocks.back() + block_size - block_remaining);
        records[i]->header1.store(loads[i]);
        records[i]->key = loads[i];
        records[i]->value = loads[i];
        block_remaining -= sizeof(record);
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            Tracer tracer;
            tracer.startTime();
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    uint64_t hash =
                            MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                    uint64_t mark = records[hash]->header1.load();
                    value += records[hash]->value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, records, t));
        // Create a cpu_set_t object representing a set of CPUs. Clear it and mark
        // only CPU i as set.
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t, &cpuset);
        int rc = pthread_setaffinity_np(workers[t].native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "RecordNumaBlockHash Tpt: " << (double) total_tick.load() * thread_number / total_time.load()
              << std::endl;
    for (uint64_t i = 0; i < blocks.size(); i++) std::free((void *) blocks[i]);
    delete[] records;
}

#endif

class Address {
public:
    friend class PageOffset;

    /// An invalid address, used when you need to initialize an address but you don't have a valid
    /// value for it yet. NOTE: set to 1, not 0, to distinguish an invalid hash bucket entry
    /// (initialized to all zeros) from a valid hash bucket entry that points to an invalid address.
    static constexpr uint64_t kInvalidAddress = 1;

    /// A logical address is 8 bytes.
    /// --of which 48 bits are used for the address. (The remaining 16 bits are used by the hash
    /// table, for control bits and the tag.)
    static constexpr uint64_t kAddressBits = 48;
    static constexpr uint64_t kMaxAddress = ((uint64_t) 1 << kAddressBits) - 1;
    /// --of which 25 bits are used for offsets into a page, of size 2^25 = 32 MB.
    static constexpr uint64_t kOffsetBits = 25;
    static constexpr uint32_t kMaxOffset = ((uint32_t) 1 << kOffsetBits) - 1;
    /// --and the remaining 23 bits are used for the page index, allowing for approximately 8 million
    /// pages.
    static uint64_t x;
    static constexpr uint64_t kHBits = 8;
    static constexpr uint64_t kPageBits = kAddressBits - kOffsetBits - kHBits;
    static constexpr uint32_t kMaxPage = ((uint32_t) 1 << kPageBits) - 1;

    /// Default constructor.
    Address() : control_{0} {
    }

    Address(uint32_t page, uint32_t offset) : reserved_{0}, h_{0}, page_{page}, offset_{offset} {
    }

    Address(uint32_t page, uint32_t offset, uint32_t h) : reserved_{0}, h_{h}, page_{page}, offset_{offset} {
    }

    /// Copy constructor.
    Address(const Address &other) : control_{other.control_} {
    }

    Address(uint64_t control) : control_{control} {
        assert(reserved_ == 0);
    }

    inline Address &operator=(const Address &other) {
        control_ = other.control_;
        return *this;
    }

    inline Address &operator+=(uint64_t delta) {
        //assert(delta < UINT32_MAX);
        control_ += delta;
        return *this;
    }

    inline Address operator-(const Address &other) {
        return control_ - other.control_;
    }

    /// Comparison operators.
    inline bool operator<(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ < other.control_;
    }

    inline bool operator<=(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ <= other.control_;
    }

    inline bool operator>(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ > other.control_;
    }

    inline bool operator>=(const Address &other) const {
        assert(reserved_ == 0);
        assert(other.reserved_ == 0);
        return control_ >= other.control_;
    }

    inline bool operator==(const Address &other) const {
        return control_ == other.control_;
    }

    inline bool operator!=(const Address &other) const {
        return control_ != other.control_;
    }

    /// Accessors.
    inline uint32_t page() const {
        return static_cast<uint32_t>(page_);
    }

    inline uint32_t offset() const {
        return static_cast<uint32_t>(offset_);
    }

    inline uint32_t h() const {
        return static_cast<uint32_t>(h_);
    }

    inline uint64_t control() const {
        return control_;
    }

private:
    union {
        struct {
            uint64_t offset_ : kOffsetBits;         // 25 bits
            uint64_t page_ : kPageBits;  // 15 bits
            uint64_t h_:kHBits; //8 bit
            uint64_t reserved_ : 64 - kAddressBits; // 16 bits
        };
        uint64_t control_;
    };
};

class AtomicAddress {
public:
    AtomicAddress(const Address &address) : control_{address.control()} {
    }

    /// Atomic access.
    inline Address load() const {
        return Address{control_.load()};
    }

    inline void store(Address value) {
        control_.store(value.control());
    }

    inline bool compare_exchange_strong(Address &expected, Address desired) {
        uint64_t expected_control = expected.control();
        bool result = control_.compare_exchange_strong(expected_control, desired.control());
        expected = Address{expected_control};
        return result;
    }

    /// Accessors.
    inline uint32_t page() const {
        return load().page();
    }

    inline uint32_t offset() const {
        return load().offset();
    }

    inline uint64_t control() const {
        return load().control();
    }

private:
    /// Atomic access to the address.
    std::atomic<uint64_t> control_;
};

#define USE_ATOMIC_ADDRESS 0

constexpr uint64_t page_size = (1llu << 25);

uint64_t page_remaining = 0;

std::vector<uint64_t> pages;

#define SHUFFLE 1

void RecordPageHashTest() {
    std::vector<std::thread> workers;
    Address *addresses = new Address[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        if (page_remaining <= sizeof(record)) {
            pages.push_back((uint64_t) std::malloc(page_size));
            page_remaining = page_size;
        }
        uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
        addresses[hash] = Address(pages.size() - 1, page_size - page_remaining);
        record *ptr = (record *) (pages[addresses[hash].page()] + addresses[hash].offset());
        ptr->header1.store(loads[i]);
        ptr->key = loads[i];
        ptr->value = loads[i];
        page_remaining -= sizeof(record);
    }
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
#if SHUFFLE == 1
    for (uint64_t t = 0; t < thread_number; t++)
        std::random_shuffle(loads + t * total_count / thread_number, loads + (t + 1) * total_count / thread_number);
#endif
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](Address *addresses, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            Tracer tracer;
            tracer.startTime();
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
                for (uint64_t i = start; i < start + card; i++) {
                    uint64_t hash =
                            MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
#if USE_ATOMIC_ADDRESS == 1
                    Address address = AtomicAddress(addresses[hash]).load();
#else
                    Address address = addresses[hash];
#endif
                    record *ptr = (record *) (pages[address.page()] + address.offset());
                    ptr->header1.load();
                    value += ptr->value;
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
        }, addresses, t));
    }
    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();

    std::cout << "RecordPageHash Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < pages.size(); i++) std::free((void *) pages[i]);
    pages.clear();
    delete[] addresses;
}

std::vector<uint64_t> *heap;
uint64_t *heap_remaining;

#define USE_SEPARATE 1 // It means that pages are thread-local;

#if USE_SEPARATE == 1
#define FULL_ISOLATE 1 // It means that addresses are thread-local;
#else
#define FULL_ISOLATE 0
#endif

#ifdef linux
#define NUMA_ISOLATE 1 // It means that threads are numa-based
#else
#define NUMA_ISOLATE 0
#endif

#if USE_SEPARATE == 1
std::vector<uint64_t> *localloads;
#endif

void RecordPageLocalTest() {
    std::vector<std::thread> workers;
#if NUMA_ISOLATE == 1
    unsigned num_cpus = std::thread::hardware_concurrency();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (size_t i = 1; i < num_cpus; i++) CPU_SET(i, &cpuset);
    std::cout << num_cpus << "\t" << *(cpuset.__bits) << std::endl;
#endif
#if FULL_ISOLATE == 1
    std::vector<Address> *localaddress = new std::vector<Address>[thread_number];
    for (uint64_t i = 0; i < thread_number; i++) //localaddress[i].reserve(total_count / thread_number);
        for (uint64_t j = 0; j < total_count / thread_number; j++)
            localaddress[i].push_back(Address(0, 0));
#else
    Address *addresses = new Address[total_count];
#endif
    std::cout << "begin2" << std::endl;
    heap = new std::vector<uint64_t>[thread_number];
    heap_remaining = new uint64_t[thread_number];
    for (uint64_t t = 0; t < thread_number; t++) {
        heap_remaining[t] = 0;
#if FULL_ISOLATE == 1
        workers.push_back(std::thread([](std::vector<Address> &addresses, uint64_t tid) {
#else
            workers.push_back(std::thread([](Address *addresses, uint64_t tid) {
#endif
            for (uint64_t i = 0; i < total_count; i++) {
                if (heap_remaining[tid] <= sizeof(record)) {
                    heap[tid].push_back((uint64_t) std::malloc(page_size));
                    heap_remaining[tid] = page_size;
                }
#if FULL_ISOLATE == 1
                uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323);
                if (hash % thread_number != tid) continue;
                hash = hash % (total_count / thread_number);
                addresses[hash] = Address(heap[tid].size() - 1, page_size - heap_remaining[tid]);
                record *ptr = (record *) (heap[tid][addresses[hash].page()] + addresses[hash].offset());
#else
                uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                if (hash % thread_number != tid) continue;
                addresses[hash] = Address(heap[tid].size() - 1, page_size - heap_remaining[tid]);
                record *ptr = (record *) (heap[tid][addresses[hash].page()] + addresses[hash].offset());
#endif
                ptr->header1.store(loads[i]);
                ptr->key = loads[i];
                ptr->value = loads[i];
                heap_remaining[tid] -= sizeof(record);
            }
#if FULL_ISOLATE == 1
        }, std::ref(localaddress[t]), t));
#else
        }, addresses, t));
#endif
#if NUMA_ISOLATE
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t, &cpuset);
        int rc = pthread_setaffinity_np(workers[t].native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
#endif
    }
    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    workers.clear();
    total_time.store(0);
    total_tick.store(0);
    stopMeasure.store(0);
    std::cout << "begin1" << std::endl;
#if USE_SEPARATE == 1
    localloads = new std::vector<uint64_t>[thread_number];
    for (uint64_t i = 0; i < total_count; i++) {
#if FULL_ISOLATE == 1
        uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323);
#else
        uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
#endif
        uint64_t tid = hash % thread_number;
        localloads[tid].push_back(loads[i]);
    }
    for (uint64_t t = 0; t < thread_number; t++) {
        std::cout << t << ":" << localloads[t].size() << "\t";
        if ((t + 1) % 8 == 0) std::cout << std::endl;
#if SHUFFLE == 1
        std::random_shuffle(localloads[t].begin(), localloads[t].end());
#endif
    }
#endif
    std::cout << std::endl << "begin" << std::endl;
    Timer timer;
    timer.start();
    for (uint64_t t = 0; t < thread_number; t++) {
#if FULL_ISOLATE == 1
        workers.push_back(std::thread([](std::vector<Address> &addresses, uint64_t tid) {
#else
            workers.push_back(std::thread([](Address *addresses, uint64_t tid) {
#endif
            uint64_t card = total_count / thread_number;
            Tracer tracer;
            tracer.startTime();
            uint64_t tick = 0;
            while (stopMeasure.load() == 0) {
#if USE_SEPARATE == 1
                for (uint64_t i = 0; i < localloads[tid].size(); i++) {
#if FULL_ISOLATE
                    uint64_t hash = MurmurHash64A((void *) &localloads[tid][i], sizeof(uint64_t), 0x234233242324323) %
                                    (total_count / thread_number);
#else
                    uint64_t hash = MurmurHash64A((void *) &localloads[tid][i], sizeof(uint64_t), 0x234233242324323) %
                                    total_count;
#endif
#else
                    for (uint64_t i = 0; i < total_count; i++) {
                        uint64_t hash =
                                MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                        if (hash % thread_number != tid) continue;
#endif
#if USE_ATOMIC_ADDRESS == 1
                    Address address = AtomicAddress(addresses[hash]).load();
#else
                    Address address = addresses[hash];
#endif
                    record *ptr = (record *) (heap[tid][address.page()] + address.offset());
                    ptr->header1.load();
                    value += ptr->value;
                    /*if (tid == 0)
                        std::cout << "\t" << tid << " " << tick << " " << address.page() << " " << address.offset()
                                  << " " << ptr->key << " " << hash << " " << hash % thread_number << std::endl;*/
                    tick++;
                }
            }
            total_time.fetch_add(tracer.getRunTime());
            total_tick.fetch_add(tick);
#if FULL_ISOLATE == 1
        }, std::ref(localaddress[t]), t));
#else
        }, addresses, t));
#endif
#if NUMA_ISOLATE
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(t, &cpuset);
        int rc = pthread_setaffinity_np(workers[t].native_handle(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }
#endif
    }

    while (timer.elapsedSeconds() < timer_range) {
        sleep(1);
    }
    stopMeasure.store(1, memory_order_relaxed);

    for (uint64_t t = 0; t < thread_number; t++)
        workers[t].join();

    std::cout << "RecordPageLocal Tpt: " << (double) total_tick.load() * thread_number / total_time.load() << std::endl;
    for (uint64_t t = 0; t < thread_number; t++) {
        for (uint64_t i = 0; i < pages.size(); i++)
            std::free((void *) heap[t][i]);
    }
    delete[] heap;
    delete[] heap_remaining;
#if FULL_ISOLATE == 1
    delete[] localaddress;
#else
    delete[] addresses;
#endif
}

int main(int argc, char **argv) {
    if (argc > 5) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        distribution_skew = std::atof(argv[4]);
        switcher = argv[5];
    }
    std::cout << thread_number << " " << key_range << " " << total_count << " " << distribution_skew << " " << switcher
              << std::endl;
    loads = new uint64_t[total_count];
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);

    if (std::strlen(switcher) > 0 && switcher[0] == '1') RecordTest();
    if (std::strlen(switcher) > 1 && switcher[1] == '1') RecordPtrTest();
    if (std::strlen(switcher) > 2 && switcher[2] == '1') RecordScanTest();
    if (std::strlen(switcher) > 3 && switcher[3] == '1') RecordHashTest();
    if (std::strlen(switcher) > 4 && switcher[4] == '1') RecordBlockHashTest();
#ifdef linux
    if (std::strlen(switcher) > 5 && switcher[5] == '1') RecordNumaBlockHashTest();
#endif
    if (std::strlen(switcher) > 6 && switcher[6] == '1') RecordPageHashTest();
    if (std::strlen(switcher) > 7 && switcher[7] == '1') RecordPageLocalTest();
    delete[] loads;

    if (std::strlen(switcher) > 8 && switcher[8] == '1') {
        std::bitset<128> bs(0);
        for (int i = 0; i < 128; i++) if (i % 2 == 0) bs.set(i);
        for (int i = 0; i < 128; i++) if (bs.test(i)) cout << "1"; else cout << "0";
        cout << endl;
        cout << bs.to_string() << endl;
        atomic<long long> tick(0);
        cout << tick.load() << endl;
        tick.fetch_add(1);
        cout << tick.load() << endl;
        u_char a = 0x3;
        u_char b = 0x6;
        cout << "0x3 and 0x0: " << (a and 0x0) << endl;
        cout << "0x3 and 0x6: " << (a and b) << endl;
        cout << "0x3 & 0x6: " << (a & b) << endl;
        cout << "0x3 or 0x6: " << (a or b) << endl;
        cout << "0x3 | 0x6: " << (a | b) << endl;
        cout << "0x3 xor 0x6: " << (a xor b) << endl;
        cout << "~0x6: " << (0xff & (~b)) << endl;
        u_char c = a;
        c &= b;
        cout << "0x3 &= 0x6: " << (0xff & c) << endl;
        c = a;
        c |= b;
        cout << "0x3 |= 0x6: " << (0xff & c) << endl;
        c = (a and b);
        cout << "0x3 and= 0x6: " << (0xff & c) << endl;
        c = (a or b);
        cout << "0x3 and= 0x6: " << (0xff & c) << endl;
    }
}