//
// Created by Michael on 2019-10-15.
//

#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include "generator.h"
#include "GK.h"
#include "tracer.h"

using namespace std;

uint64_t total_count = 10000000;

int usingzipf = 1;

int reverseorder = 0;

bool comp(uint32_t a, uint32_t b) {
    return a < b;
}

void discreteGen() {
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::default_random_engine generator(seed);
    std::discrete_distribution<int> loaded_die{0, 1, 1, 1, 1, 1, 3};
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < 100; i++) {
        cout << loaded_die(generator) << endl;
    }
    cout << tracer.getRunTime() << endl;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        total_count = std::atol(argv[1]);
        usingzipf = std::atoi(argv[2]);
        reverseorder = std::atoi(argv[3]);
    }
    Tracer tracer;
    tracer.startTime();
    std::mt19937 mt;
    vector<uint32_t> v;
    if (usingzipf != 0) {
        zipf_distribution<uint32_t> gen(1000000000, 1.0);
        for (int i = 0; i < total_count; i++) {
            v.push_back(gen(mt));
        }
    } else {
        std::uniform_int_distribution<uint32_t> gen(0, 1000000000);
        for (int i = 0; i < total_count; i++) {
            v.push_back(gen(mt));
        }
    }
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    GK<uint32_t> gk(0.001, 1000000000);
    for (int i = 0; i < total_count; i++) {
        if (reverseorder != 0)
            gk.feed(v[total_count - i - 1]);
        else
            gk.feed(v[i]);
    }
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    gk.finalize();
    std::sort(v.begin(), v.end(), comp);
    cout << tracer.getRunTime() << endl;
    for (double d = 0.1; d <= 1.0; d += 0.1) {
        cout << "\t" << d << " " << gk.query_for_value(d) << " " << v[v.size() * d] << endl;
    }
    return 0;
}