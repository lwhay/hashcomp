//
// Created by iclab on 10/19/19.
//

#include <boost/bind.hpp>
#include <chrono>
#include <iostream>
#include <unordered_map>
#include <algorithm>
#include <functional>
#include <random>
#include "generator.h"
#include "tracer.h"

uint32_t range = 1000000000;

double percentage = 0.5;

double zipffactor = 1.0;

double samplingrate = 1.0;

using namespace std;

bool second = true;

uint32_t total = -1;

uint32_t satisfied = 0;

uint32_t ticks = 0;

bool comp(pair<uint32_t, uint32_t> a, pair<uint32_t, uint32_t> b) {
    if (second)
        return a.second > b.second;
    else
        return a.first > b.first;
}

void offer(unordered_map<uint32_t, uint32_t> &freqmap, uint32_t k) {
    if (freqmap.find(k) == freqmap.end()) {
        freqmap.insert(make_pair<>(k, 0));
    }
    freqmap.find(k)->second++;
}

void tick(unordered_map<uint32_t, uint32_t> &freqmap, pair<uint32_t, uint32_t> p) {
    if (freqmap.find(p.second) == freqmap.end()) {
        freqmap.insert(make_pair<>(p.second, 0));
    }
    freqmap.find(p.second)->second++;
}

ofstream fout;

bool print(std::pair<uint32_t, uint32_t> p) {
    fout << p.first << "\t" << p.second << endl;
}

int main(int argc, char **argv) {
    if (argc > 1) {
        range = std::atoi(argv[1]);
        percentage = std::atof(argv[2]);
        zipffactor = std::atof(argv[3]);
    }
    std::random_device rd;  //Will be used to obtain a seed for the random number engine
    std::mt19937 prob(rd()); //Standard mersenne_twister_engine seeded with rd()
    std::uniform_real_distribution<> dis(0.0, 1.0);

    Tracer tracer;
    tracer.startTime();
    zipf_distribution<uint32_t> gen((1LLU << 32), zipffactor);
    std::mt19937 mt;
    vector<uint32_t> v;
    for (int i = 0; i < range; i++) {
        if (dis(prob) < samplingrate)
            v.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << "<->" << v.size() << endl;

    total = v.size();

    total *= percentage;

    tracer.startTime();
    unordered_map<uint32_t, uint32_t> freqmap;
    for_each(v.begin(), v.end(), boost::bind(offer, ref(freqmap), _1));
    cout << tracer.getRunTime() << "<->" << freqmap.size() << endl;

    second = true;
    tracer.startTime();
    std::vector<std::pair<uint32_t, uint32_t>> elems(freqmap.begin(), freqmap.end());
    std::sort(elems.begin(), elems.end(), comp);
    cout << tracer.getRunTime() << "<->" << elems.size() << endl;

    fout.open("./keyfreq.log");
    for_each(elems.begin(), elems.end(), print);
    fout.close();

    tracer.startTime();
    freqmap.clear();
    for_each(elems.begin(), elems.end(), boost::bind(tick, ref(freqmap), _1));
    cout << tracer.getRunTime() << "<->" << freqmap.size() << endl;

    second = false;
    tracer.startTime();
    elems.clear();
    elems.assign(freqmap.begin(), freqmap.end());
    std::sort(elems.begin(), elems.end(), comp);
    cout << tracer.getRunTime() << "<->" << elems.size() << endl;

    fout.open("./freqcount.log");
    for_each(elems.begin(), elems.end(), print);
    fout.close();

    for_each(elems.begin(), elems.end(), [](pair<uint32_t, uint32_t> &p) {
        if ((satisfied += (p.first * p.second)) < total) ticks += p.second;
    });

    cout << ticks << "<->" << satisfied << "<->" << total << "<->" << (total / percentage) << endl;

    return 0;
}