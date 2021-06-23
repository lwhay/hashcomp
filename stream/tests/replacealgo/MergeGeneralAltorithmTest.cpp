//
// Created by iclab on 6/22/21.
//
#include <cassert>
#include <thread>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include "ReplacementAlgorithm.h"
#include "tracer.h"

int total_threads_num = 10;
size_t max_count = 100000000;
size_t hit_count = (max_count / 100);
float data_skew = 0.99f;

uint64_t *keys;
stringstream *output;

void generate() {
    keys = (uint64_t *) calloc(max_count, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint64_t>::generate(keys, (1llu << 32), max_count, data_skew);
    cout << tracer.getRunTime() << " with " << max_count << endl;
}

void genmerge() {
    cout << (std::numeric_limits<uint64_t>::max()) << endl;
    std::vector<GeneralReplacement<uint64_t> *> grs;
    Tracer tracer;
    for (int i = 0; i < 10; i++) {
        grs.push_back(new GeneralReplacement<uint64_t>(hit_count));
    }
    vector<thread> workers;
    tracer.startTime();
    for (int i = 0; i < 10; i++) {
        workers.push_back(thread([](GeneralReplacement<uint64_t> *gr, int tid) {
            Tracer tracer;
            tracer.startTime();
            size_t start = tid * max_count / total_threads_num;
            for (int j = 0; j < max_count / total_threads_num; j++) {
                gr->put(keys[start + j]);
            }
            Item<uint64_t> *partial = gr->prepare(tid);
            output[tid] << "\t" << tid << " " << tracer.getRunTime() << " " << gr->volume() << " ";
            for (int i = 0; i < 3; i++) {
                output[tid] << partial[i].getItem() << ":" << partial[i].getCount() << "->";
            }
            output[tid] << "->...->";
            for (int i = 0; i < 3; i++) {
                output[tid] << partial[gr->volume() - i].getItem() << ":" << partial[gr->volume() - i].getCount()
                            << "->";
            }
            output[tid] << endl;
        }, grs.at(i), i));
    }
    for (int i = 0; i < 10; i++) {
        workers[i].join();
        string outstr = output[i].str();
        cout << outstr;
    }
    cout << "push: " << tracer.getRunTime() << ":" << hit_count << endl;
    tracer.startTime();
    Item<uint64_t> *merged = grs.at(0)->merge(grs);
    cout << "total merge:" << tracer.getRunTime() << " " << endl;
    tracer.startTime();
    GeneralReplacement<uint64_t> gr(grs.at(0)->volume() - 1);
    for (int i = 0; i < grs.at(0)->volume(); i++) gr.put(merged[i].getItem());
    cout << "rebuild:" << tracer.getRunTime() << " ";
    for (int i = 0; i < 3; i++) {
        cout << merged[i].getItem() << ":" << merged[i].getCount() << "->";
    }
    cout << "->...->";
    for (int i = 3; i > 0; i--) {
        cout << merged[grs.at(0)->volume() - i].getItem() << ":" << merged[grs.at(0)->volume() - i].getCount() << "->";
    }
    cout << endl;
    for (int i = 0; i < grs.at(0)->volume(); i++) assert(gr.find(merged[i].getItem()) != nullptr);
    //assert(grs.at(0)->find(merged[i].getItem()) != nullptr);
    cerr << gr.volume() << ":" << grs.at(0)->volume() << endl;
    assert(gr.volume() == grs.at(0)->volume());
    for (auto gr: grs) delete gr;
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
    if (argc > 1) {
        total_threads_num = std::atoi(argv[1]);
        max_count = std::atol(argv[2]);
        hit_count = std::atoi(argv[3]);
        data_skew = std::atof(argv[4]);
    }
    output = new stringstream[total_threads_num];
    dummy();
    generate();
    genmerge();
    delete[] output;
}