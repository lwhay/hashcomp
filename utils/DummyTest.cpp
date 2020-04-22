//
// Created by iclab on 10/7/19.
//

#include <cassert>
#include <atomic>
#include <bitset>
#include <iostream>
#include <thread>
#include <vector>
#include <tracer.h>

using namespace std;

uint64_t *loads;

uint64_t key_range = (1llu << 30);

uint64_t total_count = (1llu << 20);

uint64_t thread_number = 4;

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

void RecordTest() {
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
    record *records = new record[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[i].header1.store(loads[i]);
        records[i].key = loads[i];
        records[i].value = loads[i];
    }
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record *records, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                value += records[loads[i]].value;
            }
        }, records, t));
    }

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "Record Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    delete[] records;
}

void RecordPtrTest() {
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
    record **records = new record *[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[i] = new record;
        records[i]->header1.store(loads[i]);
        records[i]->key = loads[i];
        records[i]->value = loads[i];
    }
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                value += records[loads[i]]->value;
            }
        }, records, t));
    }

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "RecordPtr Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) delete records[i];
    delete[] records;
}

void RecordScanTest() {
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
    record **records = new record *[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[loads[i]] = new record;
        records[loads[i]]->header1.store(loads[i]);
        records[loads[i]]->key = loads[i];
        records[loads[i]]->value = loads[i];
    }
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                value += records[loads[i]]->value;
            }
        }, records, t));
    }

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "RecordScan Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) delete records[i];
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
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
    record **records = new record *[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        records[i] = new record;
        records[i]->header1.store(loads[i]);
        records[i]->key = loads[i];
        records[i]->value = loads[i];
    }
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                uint64_t mark = records[hash]->header1.load();
                value += records[hash]->value;
            }
        }, records, t));
    }

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "RecordHash Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) delete records[i];
    delete[] records;
}

constexpr uint64_t block_size = (1llu << 20);

uint64_t block_remaining = 0;

std::vector<uint64_t> blocks;

void RecordBlockHashTest() {
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
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
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                uint64_t mark = records[hash]->header1.load();
                value += records[hash]->value;
            }
        }, records, t));
    }

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "RecordBlockHash Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < blocks.size(); i++) std::free((void *) blocks[i]);
    delete[] records;
}

#ifdef linux

#include <numa.h>

void RecordNumaBlockHashTest() {
    block_remaining = 0;
    blocks.clear();
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
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
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](record **records, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
                uint64_t mark = records[hash]->header1.load();
                value += records[hash]->value;
            }
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

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "RecordNumaBlockHash Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
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

void RecordPageHashTest() {
    loads = new uint64_t[total_count];
    std::vector<std::thread> workers;
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution_skew);
    Address *addresses = new Address[total_count];
    for (uint64_t i = 0; i < total_count; i++) {
        if (page_remaining <= sizeof(record)) {
            pages.push_back((uint64_t) std::malloc(page_size));
            page_remaining = page_size;
        }
        addresses[i] = Address(pages.size() - 1, page_remaining);
        record *ptr = (record *) (pages[addresses[i].page()] + addresses[i].offset());
        ptr->header1.store(loads[i]);
        ptr->key = loads[i];
        ptr->value = loads[i];
        page_remaining -= sizeof(record);
    }
    Tracer tracer;
    tracer.startTime();
    for (uint64_t t = 0; t < thread_number; t++) {
        workers.push_back(std::thread([](Address *addresses, uint64_t tid) {
            uint64_t card = total_count / thread_number;
            uint64_t start = tid * card;
            for (uint64_t i = start; i < start + card; i++) {
                uint64_t hash = MurmurHash64A((void *) &loads[i], sizeof(uint64_t), 0x234233242324323) % total_count;
#if USE_ATOMIC_ADDRESS == 1
                Address address = AtomicAddress(addresses[hash]).load();
#else
                Address address = addresses[hash];
#endif
                record *ptr = (record *) (pages[address.page()] + address.offset());
                ptr->header1.load();
                value += ptr->value;
            }
        }, addresses, t));
    }

    for (uint64_t t = 0; t < thread_number; t++) workers[t].join();
    total_time.fetch_add(tracer.getRunTime());

    std::cout << "RecordPageHash Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < pages.size(); i++) std::free((void *) pages[i]);
    pages.clear();
    delete[] addresses;
}

int main(int argc, char **argv) {
    if (argc > 4) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        distribution_skew = std::atof(argv[4]);
    }
    RecordTest();
    RecordPtrTest();
    RecordScanTest();
    RecordHashTest();
    RecordBlockHashTest();
#ifdef linux
    RecordNumaBlockHashTest();
#endif
    RecordPageHashTest();

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