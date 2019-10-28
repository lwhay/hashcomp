//
// Created by Michael on 2019-10-15.
//

#include <chrono>
#include <iostream>
#include "generator.h"
#include "GK.h"
#include "tracer.h"

using namespace std;

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
    Tracer tracer;
    tracer.startTime();
    zipf_distribution<uint32_t> gen(10000000, 1.0);
    std::mt19937 mt;
    vector<uint32_t> v;
    for (int i = 0; i < 10000000; i++) {
        v.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << endl;
    tracer.startTime();
    GK<uint32_t> gk(0.001, 10000);
    for (int i = 0; i < 10000000; i++) {
        gk.feed(v[i]);
        cout << v[i]<<endl;
    }
    cout << tracer.getRunTime() << endl;
    return 0;
}