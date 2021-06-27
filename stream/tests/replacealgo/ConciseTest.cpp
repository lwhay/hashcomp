//
// Created by iclab on 6/26/21.
//

#include <iostream>
#include "ReplacementAlgorithm.h"
#include "ConciseLFUAlgorithm.h"
#include "tracer.h"

int total_threads_num = 10;
size_t max_count = 25;//000;
size_t hit_count = 6; //(max_count / 10);
float data_skew = 0.99f;

uint32_t *keys;
stringstream *output;

using namespace std;

void generate() {
    keys = (uint32_t *) calloc(max_count, sizeof(uint64_t));
    Tracer tracer;
    tracer.startTime();
    RandomGenerator<uint32_t>::generate(keys, (1llu << 32), max_count, data_skew);
    cout << tracer.getRunTime() << " with " << max_count << endl;
}

void fullGeneralTest() {
    GeneralReplacement<uint64_t> gr(hit_count);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) {
        gr.put(keys[i]);
        if (i % 1000 == 0) cout << ".";
    }
    cout << endl;
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


void fullConciseTest() {
    ConciseLFUAlgorithm gr(hit_count);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) {
        cout << i << ":" << keys[i] << "&" << endl;
        if (i == 20) {
            uint32_t aa = keys[i];
            aa = 1;
        }
        gr.put(keys[i]);
        gr.print();
    }
    cout << tracer.getRunTime() << endl;
    int maxCounter = 0, minCounter = 10000000, maxDelta = 0, minDelta = 100000000;
    FreqItem *root = gr.output(false);
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

void naiveTest() {
    std::cout << 0xfffffllu << " " << 0xfffff00000llu << " " << 0xfffff0000000000llu << std::endl;
    uint64_t x = -1, y = 0;
    x &= 0xfffffllu;
    y |= 0xfffffllu;
    std::cout << x << " " << y << std::endl;

    GeneralReplacement<uint64_t> gr(10);
    for (int i = 0; i < 100; i++) {
        std::cout << i << "&";
        uint64_t r = gr.put(i);
        std::cout << r << "&";
        gr.print();
    }
    std::cout << "--------------------------------------------------------------------------------------" << std::endl;
    ConciseLFUAlgorithm glfu(10);
    for (int i = 0; i < 100; i++) {
        std::cout << i << "&";
        uint32_t r = glfu.put(i);
        std::cout << r << "&";
        glfu.print();
    }
}

void dumpTest() {
    GeneralReplacement<uint64_t> gr(10);
    for (int i = 0; i < 11; i++) {
        std::cout << i << "&";
        uint32_t r = gr.put(i);
        std::cout << r << "&";
        gr.print();
    }
    for (int i = 0; i < 100; i++) {
        std::cout << i << "&";
        if (i == 2)
            int gg = 0;
        uint32_t r = gr.put(i);
        std::cout << r << "&";
        gr.print();
    }
    std::cout << "--------------------------------------------------------------------------------------" << std::endl;
    ConciseLFUAlgorithm glfu(10);
    for (int i = 0; i < 11; i++) {
        std::cout << i << "&";
        if (i == 10)
            int gg = 0;
        uint32_t r = glfu.put(i);
        std::cout << r << "&";
        glfu.print();
    }
    for (int i = 0; i < 100; i++) {
        std::cout << i << "&";
        if (i == 1)
            int gg = 0;
        uint32_t r = glfu.put(i);
        std::cout << r << "&";
        glfu.print();
    }
}

int main(int argc, char **argv) {
    // naiveTest();
    // dumpTest();
    generate();
    fullGeneralTest();
    fullConciseTest();
    return 0;
}