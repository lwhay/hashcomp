//
// Created by Michael on 11/2/19.
//

#include <iostream>
#include <vector>
#include <thread>
#include "tracer.h"
#include "generator.h"
#include "../../src/heavyhitter/GeneralLazySS.h"
#include "../../src/heavyhitter/LazySpaceSaving.h"

using namespace std;

uint64_t degree = 4;

uint64_t watch = (1 << 16);

double fPhi = 0.001;

int main(int argc, char **argv) {
    if (argc > 2) {
        degree = std::atol(argv[1]);
        watch = std::atoi(argv[2]);
        fPhi = std::atof(argv[3]);
    }
    cout << "Efficiency of parallel GLSS: total: " << watch << " degree: " << degree << endl;

    vector<thread> threads;
    vector<GeneralLazySS<uint64_t> *> glss;
    vector<vector<uint64_t> *> loads;
    Tracer tracer;
    vector<long> runtime;

    tracer.startTime();
    for (int t = 0; t < degree;) {
        glss.push_back(new GeneralLazySS<uint64_t>(fPhi));
        loads.push_back(new vector<uint64_t>());
        threads.push_back(thread([](vector<uint64_t> *load) {
            zipf_distribution<uint64_t> gen((1llu << 63), 0.99);
            mt19937 mt(time(0));
            for (int i = 0; i < watch; i++) load->push_back(gen(mt));
        }, loads[t++]));
    }
    for (auto &t: threads) t.join();
    runtime.push_back(tracer.getRunTime());
    cout << "Generated" << endl;
    threads.clear();

    tracer.startTime();
    LazySpaceSaving lss(0.00001);
    tracer.startTime();
    for (int t = 0; t < degree; t++) for (auto v : *loads[t]) lss.put(v % (1llu << 31 - 1));
    cout << "LazySS put: " << tracer.getRunTime() << ": " << lss.size() << endl;
    tracer.startTime();
    size_t miss = 0, found = 0, total = 0;
    Counter *counter = nullptr;
    for (int t = 0; t < degree; t++)
        for (auto v: *loads[t]) {
            total++;
            counter = lss.find(v % (1llu << 31 - 1));
            if (nullptr == counter) miss++;
            else found += counter->getCount();
        }
    cout << "LazySS find: " << tracer.getRunTime() << ": " << miss << " " << found << " " << total << endl;
    tracer.startTime();
    for (int t = 0; t < degree; t++) {
        threads.push_back(thread([](vector<uint64_t> *load, GeneralLazySS<uint64_t> *ss) {
            for (auto v : *load) ss->put(v);
        }, loads[t], glss[t]));
    }
    for (auto &t: threads) t.join();
    runtime.push_back(tracer.getRunTime());
    cout << "Inserted" << endl;
    tracer.startTime();
#ifndef NDEBUG
    /*for (int i = 0; i < glss[0]->volume(); i++) {
        for (int t = 0; t < degree; t++)
            cout << i << "\t" << glss[t]->output(true)[i].getItem() << "(" << glss[t]->output(true)[i].getCount() << ")"
                 << "\t";
        cout << endl;
    }*/
#endif
    for (int t = 1; t < degree; t++) {
        glss[0]->merge(*glss[t]);
        cout << "\t" << glss[0]->output(false)[1].getItem() << " " << glss[0]->output(false)[1].getCount() << endl;
    }
    runtime.push_back(tracer.getRunTime());
    cout << "Merged: " << endl;
    for (auto t:runtime) cout << t << "\t";
    miss = 0, found = 0, total = 0;
    tracer.startTime();
    for (int t = 0; t < degree; t++) {
        for (auto v: *loads[t]) {
            Item<uint64_t> *ip = glss[0]->find(v);
            if (ip == nullptr) miss++;
            else found += ip->getCount();
            total++;
        }
    }
    cout << "\nGeneral find: " << tracer.getRunTime() << ": " << miss << " " << found << " " << total << endl;
    cout << endl;
}