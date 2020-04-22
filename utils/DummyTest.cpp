//
// Created by iclab on 10/7/19.
//

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

    std::cout << "Recordptr Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
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
        records[loads[i]].header1.store(loads[i]);
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

    std::cout << "Recordseq Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
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

    std::cout << "Recordseq Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
    for (uint64_t i = 0; i < total_count; i++) delete records[i];
    delete[] records;
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