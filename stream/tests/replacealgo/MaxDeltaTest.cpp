//
// Created by iclab on 6/27/21.
//
#include <iostream>
#include "ReplacementAlgorithm.h"
#include "tracer.h"

int total_threads_num = 10;
size_t max_count = 1000000000;
size_t hit_count = (max_count / 100);
float data_skew = 0.99f;

uint64_t *keys;
stringstream *output;

using namespace std;

void generate() {
    keys = (uint64_t *) calloc(max_count, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint64_t>::generate(keys, (1llu << 32), max_count, data_skew);
    cout << tracer.getRunTime() << " with " << max_count << endl;
}

int main(int argc, char **argv) {
    if (argc > 2) {
        max_count = std::atol(argv[1]);
        hit_count = std::atol(argv[2]);
    }
    GeneralReplacement<uint64_t> gr(hit_count);
    generate();
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) gr.put(keys[i]);
    cout << tracer.getRunTime() << endl;
    int maxCounter = 0, minCounter = 10000000, maxDelta = 0, minDelta = 100000000;
    Item<uint64_t> *root = gr.output(false);
    for (int i = 0; i < gr.volume() + 1; i++) {
        if ((root + i)->getCount() > maxCounter) maxCounter = (root + i)->getCount();
        if ((root + i)->getCount() < minCounter) minCounter = (root + i)->getCount();
        if ((root + i)->getDelta() > maxDelta) maxDelta = (root + i)->getDelta();
        if ((root + i)->getDelta() < minDelta) minDelta = (root + i)->getDelta();
    }
    cout << gr.size() << ":" << maxCounter << ":" << minCounter << ":" << maxDelta << ":" << minDelta << endl;
    priority_queue<uint64_t, vector<uint64_t>, less<uint64_t>> topCounter, topDelta;
    tracer.startTime();
    for (int i = 0; i < gr.volume() + 1; i++) {
        topCounter.push((root + i)->getCount());
        topDelta.push((root + i)->getDelta());
    }
    cout << tracer.getRunTime() << endl;
    for (int i = 0; i < 1000; i++) {
        if (i >= topCounter.size()) break;
        else {
            cout << topCounter.top() << " ";
            if ((i + 1) % 32 == 0) cout << endl;
            topCounter.pop();
        }
    }
    cout << "-----------------------------" << endl;
    for (int i = 0; i < 1000; i++) {
        if (i >= topDelta.size()) break;
        else {
            cout << topDelta.top() << " ";
            if ((i + 1) % 32 == 0) cout << endl;
            topDelta.pop();
        }
    }
}