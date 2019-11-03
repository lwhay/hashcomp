//
// Created by Michael on 11/2/19.
//

#include <iostream>
#include <vector>
#include "tracer.h"
#include "generator.h"
#include "../../src/heavyhitter/LazySpaceSaving.h"
#include "../../src/heavyhitter/GeneralLazySS.h"

using namespace std;

uint64_t total_round = (1 << 24);

int main(int argc, char **argv) {
    if (argc > 1) {
        total_round = std::atol(argv[1]);
    }
    zipf_distribution<uint64_t> gen((1LLU << 32), 1.5);
    std::mt19937 mt;
    vector<uint64_t> keys;
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        keys.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << " with " << keys.size() << endl;
    for (int i = 0; i < 10; i++) {
        LazySpaceSaving lss(0.00001);
        tracer.startTime();
        for (int i = 0; i < total_round; i++) {
            lss.put(keys[i]);
        }
        cout << "LazySS: " << tracer.getRunTime() << ":" << lss.size() << endl;

        GeneralLazySS<uint32_t> glss(0.00001);
        tracer.startTime();
        for (int i = 0; i < total_round; i++) {
            glss.put(keys[i]);
        }
        cout << "GLSS: " << tracer.getRunTime() << ":" << lss.size() << endl;
    }
}