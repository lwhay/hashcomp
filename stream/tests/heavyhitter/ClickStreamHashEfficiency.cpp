//
// Created by Michael on 11/4/2019.
//

#include <iostream>
#include <ostream>
#include <vector>
#include "FrequentStatistics.h"
#include "StringUtil.h"
#include "tracer.h"
#include "../../src/heavyhitter/LazySpaceSaving.h"
#include "../../src/heavyhitter/GeneralLazySS.h"
#include "../../../src/mhash/util/City.h"

using namespace std;

string path("../../res/realKVdata/clickkeys.dat");

template<typename type>
void print(pair<type, uint64_t> p) { cout << p.first << "\t" << p.second << endl; }

int main(int argc, char **argv) {
    int count = 100000000;
    int field = 3;
    if (argc > 1) path = argv[1];
    if (argc > 2) field = std::atoi(argv[2]);
    if (argc > 3) count = std::atoi(argv[3]);
    ifstream input;
    unordered_map<uint64_t, int> counters;
    unordered_map<uint32_t, int> counters32;
    vector<uint64_t> ids;
    vector<uint32_t> id32s;
    input.open(path);
    char line[4096];
    while (input.getline(line, 4096)) {
        vector<string> fields = split(line, "\t");
        uint64_t hash;
        switch (field) {
            case 1: {
                hash = CityHash64(fields[0].c_str(), std::strlen(fields[0].c_str()));
                break;
            }
            case 2: {
                hash = CityHash64(fields[1].c_str(), std::strlen(fields[1].c_str()));
                break;
            }
            case 3: {
                std::memset(line, 0, 4096);
                std::strcpy(line, fields[0].c_str());
                std::strcpy(line + fields[0].size(), fields[1].c_str());
                hash = CityHash64(line, std::strlen(line));
                //cout << fields[0] << " " << fields[1] << " " << line << " " << hash << endl;
                break;
            }
            default:
                exit(-1);
        }
        //cout << fields[1] << " " << hash << endl;
        ids.push_back(hash);
        if (counters.find(hash) == counters.end()) counters.insert(make_pair(hash, 0));
        counters.find(hash)->second++;
        id32s.push_back(hash % (1LLU << 31));
        if (counters32.find(hash % (1LLU << 31)) == counters32.end())
            counters32.insert(make_pair(hash % (1LLU << 31), 0));
        counters32.find(hash % (1LLU << 31))->second++;
        //cout << "\t" << (hash % (1LLU << 31)) << endl;
        if (count % 1000000 == 0) cout << ".";
        if (count-- == 0) break;
        if (count % 50000000 == 0) cout << endl;
    }
    cout << endl;
    input.close();

    Tracer tracer;
    LazySpaceSaving lss(0.00001);
    tracer.startTime();
    for (uint32_t key : id32s) {
        lss.put(key, 1);
    }
    cout << "LazySS: " << tracer.getRunTime() << " size: " << counters32.size();

    Counter *output = lss.output(true);
    size_t size = (counters32.size() < 100) ? counters32.size() : 100;
    cout << " volume: " << size << " total: " << id32s.size() << endl;
    for (int i = 1; i < size; i++) {
        std::cout << output[i].getItem() << ":" << output[i].getCount() << ":" << output[i].getDelta() << ":"
                  << output[i].getCount() - output[i].getDelta() << ":" << counters32.find(output[i].getItem())->second
                  << std::endl;
    }

    GeneralLazySS<uint64_t> glss(0.00001);
    tracer.startTime();
    for (uint64_t key : ids) {
        glss.put(key, 1);
    }
    cout << "GLSS: " << tracer.getRunTime() << " size: " << counters.size();

    Item<uint64_t> *items = glss.output(true);
    size = (counters.size() < 100) ? counters.size() : 100;
    cout << " volume: " << size << " total: " << ids.size() << endl;
    for (int i = 1; i < size; i++) {
        std::cout << items[i].getItem() << ":" << items[i].getCount() << ":" << items[i].getDelta() << ":"
                  << items[i].getCount() - items[i].getDelta() << ":" << counters.find(items[i].getItem())->second
                  << std::endl;
    }
    return 0;
}