//
// Created by Michael on 11/2/19.
//

#include <iostream>
#include <vector>
#include "tracer.h"
#include "generator.h"
#include "../../src/heavyhitter/LazySpaceSaving.h"
#include "../../src/heavyhitter/GeneralLazySS.h"

#define PERMUTATION false

#if PERMUTATION

#include <unordered_map>

#endif

using namespace std;

uint64_t watch = (1 << 24);

template<typename IT>
void funcCall() {
    uint64_t max = ((uint64_t) std::numeric_limits<IT>::max() <= (1LLU << 48))
                   ? (uint64_t) std::numeric_limits<IT>::max() : (1LLU << 48);
    zipf_distribution<IT> gen(max, 1.5);
    std::mt19937 mt;
    vector<IT> keys;
    Tracer tracer;
    tracer.startTime();
#if PERMUTATION
    unordered_map<IT, IT> keymap;
    for (int i = 0; i < 10000; i++) {
        IT key = (IT) rand();
        if (key != (IT) 0) keymap.insert(make_pair((IT) i, key));
    }
#endif
    for (int i = 0; i < watch; i++) {
#if PERMUTATION
        IT key = gen(mt);
        if (keymap.find(key) != keymap.end()) keys.push_back(keymap.find(key)->second);
        else keys.push_back(key);
#else
        keys.push_back(gen(mt));
#endif
    }
    cout << tracer.getRunTime() << " with " << keys.size() << endl;
    for (int i = 0; i < 10; i++) {
        LazySpaceSaving lss(0.0001);
        tracer.startTime();
        for (int i = 0; i < watch; i++) {
            lss.put(keys[i]);
        }
        cout << "LazySS: " << tracer.getRunTime() << ":" << lss.size() << endl;

        GeneralLazySS<IT> glss(0.0001);
        tracer.startTime();
        for (int i = 0; i < watch; i++) {
            glss.put(keys[i]);
        }
        cout << "GLSS: " << tracer.getRunTime() << ":" << lss.size() << endl;
    }
}

int main(int argc, char **argv) {
    if (argc > 2) {
        watch = std::atol(argv[1]);
        int type = std::atoi(argv[2]);
        cout << "command watch=" << watch << " type=" << type << endl;
        switch (type) {
            case 0: {
                funcCall<uint8_t>();
                break;
            }
            case 1: {
                funcCall<uint16_t>();
                break;
            }
            case 2: {
                funcCall<uint32_t>();
                break;
            }
            case 3: {
                funcCall<uint64_t>();
                break;
            }
            default:
                cout << "type 0: uint8_t, 1: uint16_t, 2: uint32_t, 3: uint64_t" << endl;
                return -1;
        }
    }
}