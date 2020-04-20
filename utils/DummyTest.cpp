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
    uint64_t header1;
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
        records[i].header1 = 0xff;
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

    std::cout << "Tpt: " << (double) total_count * thread_number / total_time.load() << std::endl;
}

int main(int argc, char **argv) {
    if (argc > 4) {
        thread_number = std::atol(argv[1]);
        key_range = std::atol(argv[2]);
        total_count = std::atol(argv[3]);
        distribution_skew = std::atof(argv[4]);
    }
    RecordTest();

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