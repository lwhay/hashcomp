//
// Created by iclab on 7/13/21.
//

#include <unordered_map>
#include <iostream>
#include <vector>
#include "tracer.h"

size_t max_count = 10000;

float DATA_SKEW = 0.9f;

double cutoff_f = 0;

using namespace std;

int main(int argc, char **argv) {
    if (argc > 1) {
        max_count = std::atol(argv[1]);
        cutoff_f = std::atof(argv[2]);
        DATA_SKEW = std::atof(argv[3]);
    }
    unordered_map<uint64_t, vector<uint64_t>> keyOffset;
    unordered_map<uint64_t, uint64_t> curKeyCnt;
    vector<uint64_t> gap;
    unordered_map<uint64_t, uint64_t> keyFreq;

    uint64_t *_keys = (uint64_t *) calloc(max_count, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint64_t>::generate(_keys, (1llu << 32), max_count, DATA_SKEW);
    for (size_t i = 0; i < max_count; i++) {
        if (keyOffset.find(_keys[i]) == keyOffset.end()) keyOffset.insert(make_pair(_keys[i], vector<uint64_t>()));
        if (curKeyCnt.find(_keys[i]) == curKeyCnt.end()) curKeyCnt.insert(make_pair(_keys[i], 0));
        if (keyFreq.find(_keys[i]) == keyFreq.end()) keyFreq.insert(make_pair(_keys[i], 0));

        if (keyOffset.find(_keys[i])->second.size() > 0)
            gap.push_back(i - keyOffset.find(_keys[i])->second.at(curKeyCnt.find(_keys[i])->second - 1));
        else gap.push_back(0);
        curKeyCnt.find(_keys[i])->second++;
        keyOffset.find(_keys[i])->second.push_back(i);
        keyFreq.find(_keys[i])->second++;
        // cout << _keys[i] << endl;
    }

    double avg_g = .0, avg_f = .0, count = 0;
    for (size_t i = 0; i < max_count; i++) {
        if (gap.at(i) > cutoff_f) {
            avg_g += gap.at(i);
            count++;
        }
    }
    for (size_t i = 0; i < max_count; i++) {
        if (gap.at(i) > cutoff_f) {
            avg_f += count / (1 + keyFreq.find(_keys[i])->second);
        }
    }
    avg_g /= count;
    avg_f /= count;
    // cout << avg_g << "*" << avg_f << endl;
    double sum_sup = .0, sum_sub_g = .0, sum_sub_f = .0;
    for (size_t i = 0; i < max_count; i++) {
        if (gap.at(i) > cutoff_f) {
            uint64_t f = count / (1 + keyFreq.find(_keys[i])->second);
            cout << _keys[i] << ":" << gap.at(i) << "," << f << endl;
            sum_sup += (gap.at(i) - avg_g) * (f - avg_f);
            sum_sub_g += (gap.at(i) - avg_g) * (gap.at(i) - avg_g);
            sum_sub_f += (f - avg_f) * (f - avg_f);
        }
    }
    double pf = sum_sup / (sqrt(sum_sub_g) * sqrt(sum_sub_f));
    cout << pf << endl;

    return 0;
}