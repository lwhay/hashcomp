//
// Created by Michael on 10/26/19.
//

#include <iostream>
#include <vector>
#include "tracer.h"
#include "generator.h"
#include "SpaceSaving.h"

using namespace std;

int main(int argc, char **argv) {
    zipf_distribution<uint64_t> gen((1LLU << 32), 1.5);
    std::mt19937 mt;
    vector<uint64_t> keys;
    SS<uint64_t> ss(1000);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < (1 << 28); i++) {
        keys.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << endl;
    tracer.getRunTime();
    for (int i = 0; i < (1 << 28); i++) {
        ss.put(keys[i]);
    }
    cout << tracer.getRunTime() << ":" << ss.getCounterNumber() << endl;
}