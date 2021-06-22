//
// Created by iclab on 6/22/21.
//
#include <cassert>
#include <iostream>
#include <unordered_map>
#include "ReplacementAlgorithm.h"
#include "tracer.h"

#define MAX_COUNT 100000000
#define HIT_COUNT (MAX_COUNT / 100)
#define DATA_SKEW 0.99f

std::vector<uint64_t> keys;

void generate(size_t num = MAX_COUNT) {
    keys.clear();
    zipf_distribution<uint64_t> gen((1LLU << 32), DATA_SKEW);
    std::mt19937 mt;
    Tracer tracer;
    tracer.startTime();
    std::unordered_map<uint64_t, int> freq;
    for (int i = 0; i < num; i++) {
        uint64_t key = gen(mt);
        keys.push_back(key);
        if (freq.end() == freq.find(key)) freq.insert(make_pair(key, 0));
        freq.find(key)->second++;
    }
    cout << tracer.getRunTime() << " with " << keys.size() << " " << freq.size() << endl;
}

void genmerge() {
    cout << (std::numeric_limits<uint64_t>::max()) << endl;
    std::vector<GeneralReplacement<uint64_t> *> grs;
    Tracer tracer;
    for (int i = 0; i < 10; i++) {
        grs.push_back(new GeneralReplacement<uint64_t>(HIT_COUNT));
        tracer.startTime();
        for (int j = 0; j < MAX_COUNT / 10; j++) {
            grs.at(i)->put(keys[i * MAX_COUNT / 10 + j]);
        }
        cout << i << " push: " << tracer.getRunTime() << ":" << grs.at(i)->volume() << ":" << HIT_COUNT << endl;
        assert(grs.at(i)->volume() == HIT_COUNT + 1);
        tracer.startTime();
        Item<uint64_t> *partial = grs.at(i)->prepare(i);
        cout << i << " merge: " << tracer.getRunTime() << endl;
        /*for (int i = 0; i < 10; i++) {
            cout << partial[i].getItem() << ":" << partial[i].getCount() << "->";
        }
        cout << endl;*/
    }
    tracer.startTime();
    Item<uint64_t> *merged = grs.at(0)->merge(grs);
    cout << "total merge:" << tracer.getRunTime() << endl;
    /*for (int i = 0; i < 10; i++) {
        cout << merged[i].getItem() << ":" << merged[i].getCount() << "->";
    }
    cout << endl;*/
}

void dummy() {
    uint64_t ar[4][13] = {{1, 1, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10},
                          {1, 1, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10},
                          {1, 1, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10},
                          {1, 1, 1, 2, 3, 3, 4, 5, 6, 7, 8, 9, 10}};

    GeneralReplacement<uint64_t> gr[] = {GeneralReplacement<uint64_t>(5),
                                         GeneralReplacement<uint64_t>(5),
                                         GeneralReplacement<uint64_t>(5),
                                         GeneralReplacement<uint64_t>(5)};

    std::vector<GeneralReplacement<uint64_t> *> grs;
    for (int i = 0; i < 4; i++) {
        for (int j = 0; j < 13; j++) {
            gr[i].put(ar[i][j]);
        }
        for (int j = 0; j < 13; j++) {
            gr[i].find(ar[i][j]);
        }
        Item<uint64_t> *sorted = gr[i].prepare(i);
        for (int j = 1; j <= gr[i].volume(); j++) {
            cout << sorted[j].getItem() << ":" << sorted[j].getCount() << "->";
        }
        cout << endl;

        for (int j = 0; j < 13; j++) {
            gr[i].find(ar[i][j]);
        }
        grs.push_back(&gr[i]);
    }
    Item<uint64_t> *merged = grs[0]->merge(grs);
    for (int i = 0; i < grs[0]->volume(); i++) {
        cout << merged[i].getItem() << ":" << merged[i].getCount() << "->";
    }
    cout << endl;
}

int main(int argc, char **argv) {
    dummy();
    generate();
    genmerge();
}