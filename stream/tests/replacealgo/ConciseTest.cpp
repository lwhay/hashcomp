//
// Created by iclab on 6/26/21.
//

#include <iostream>
#include <thread>
#include <vector>
#include <sstream>
#include "ReplacementAlgorithm.h"
#include "ConciseLFUAlgorithm.h"
#include "ConciseLRUAlgorithm.h"
#include "ConciseARCAlgorithm.h"
#include "tracer.h"

int total_threads_num = 10;
size_t max_count = 1000000000;
size_t hit_count = (max_count / 10);
float data_skew = 0.99f;

uint32_t *keys;
stringstream *output;

using namespace std;

void generate() {
    keys = (uint32_t *) calloc(max_count, sizeof(uint32_t));
    Tracer tracer;
    tracer.startTime();
    cout << "begin" << endl;
    RandomGenerator<uint32_t>::generate(keys, (1llu << 32), max_count, data_skew);
    cout << tracer.getRunTime() << " with " << max_count << endl;
}

void fullGeneralTest() {
    GeneralReplacement<uint32_t> gr(hit_count);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) {
        //cout << i << ":" << keys[i] << "&" << endl;
        if (i == 20)
            int bb = 0;
        gr.put(keys[i]);
        //gr.print();
        // if (i % 1000 == 0) cout << ".";
    }
    cout << endl;
    cout << tracer.getRunTime() << endl;
    int maxCounter = 0, minCounter = 10000000, maxDelta = 0, minDelta = 100000000;
    Item<uint32_t> *root = gr.output(false);
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
    cout << "time: " << tracer.getRunTime() << endl;
    for (int i = 0; i < 1000; i++) {
        if (i >= topCounter.size()) break;
        else {
            cout << topCounter.top() << " ";
            if ((i + 1) % 32 == 0) cout << endl;
            topCounter.pop();
        }
    }
    cout << endl << "-----------------------------" << endl;
    for (int i = 0; i < 1000; i++) {
        if (i >= topDelta.size()) break;
        else {
            cout << topDelta.top() << " ";
            if ((i + 1) % 32 == 0) cout << endl;
            topDelta.pop();
        }
    }
    size_t miss = 0, hit = 0;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) {
        if (gr.find(keys[i]) != nullptr) hit++;
        else miss++;
    }
    cout << endl << "find: " << tracer.getRunTime() << " miss: " << miss << " hit: " << hit << endl;
}

void verificationTest() {
    GeneralReplacement<uint32_t> gr(hit_count);
    for (int i = 0; i < max_count; i++) {
        if (i > 18532 && i < 18536) cout << i << ":" << keys[i] << endl;
        if (i == 18534)
            int aa = 0;
        gr.put(keys[i]);
        if (i > 18532 && i < 18536) gr.print();
    }
    cout << "=================================================================================================" << endl;
    ConciseLFUAlgorithm cgr(hit_count);
    for (int i = 0; i < max_count; i++) {
        /*if (i > 18532 && i < 18536)*/ cout << i << ":" << keys[i] << endl;
        if (i == 71388)
            int aa = 0;
        cgr.put(keys[i]);
        /*if (i > 18532 && i < 18536)*/ cgr.print();
    }
}

void fullConciseTest() {
    ConciseLFUAlgorithm gr(hit_count);
    Tracer tracer;
    tracer.startTime();
    size_t miss = 0, hit = 0;
    for (int i = 0; i < max_count; i++) {
        //cout << i << ":" << keys[i] << "&" << endl;
        if (i == 20) {
            uint32_t aa = keys[i];
            aa = 1;
        }
        if (gr.find(keys[i]) != nullptr) hit++; else miss++;
        gr.put(keys[i]);
        //gr.print();
    }
    cout << "time: " << tracer.getRunTime() << " c: " << gr.volume() << " miss: " << miss << " hit: " << hit << endl;
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
    cout << "time: " << tracer.getRunTime() << endl;
    for (int i = 0; i < 1000; i++) {
        if (i >= topCounter.size()) break;
        else {
            cout << topCounter.top() << " ";
            if ((i + 1) % 32 == 0) cout << endl;
            topCounter.pop();
        }
    }
    cout << endl << "-----------------------------" << endl;
    for (int i = 0; i < 1000; i++) {
        if (i >= topDelta.size()) break;
        else {
            cout << topDelta.top() << " ";
            if ((i + 1) % 32 == 0) cout << endl;
            topDelta.pop();
        }
    }
    miss = 0, hit = 0;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) {
        if (gr.find(keys[i]) != nullptr) hit++;
        else miss++;
    }
    cout << endl << "find: " << tracer.getRunTime() << " miss: " << miss << " hit: " << hit << endl;
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

void parallelConciseTest() {
    output = new stringstream[total_threads_num];
    vector<thread> workers;
    std::vector<ConciseLFUAlgorithm *> grs;
    Tracer tracer;
    tracer.getRunTime();
    for (int i = 0; i < total_threads_num; i++) {
        grs.push_back(new ConciseLFUAlgorithm(hit_count));
    }
    for (int i = 0; i < total_threads_num; i++) {
        workers.push_back(thread([](ConciseLFUAlgorithm *gr, int tid) {
            Tracer tracer;
            tracer.startTime();
            size_t begin = max_count / (total_threads_num) * tid, end = max_count / (total_threads_num) * (tid + 1);
            for (size_t i = begin; i < end; i++) {
                //if (i > 9600) cout << i << ":" << keys[i] << endl;
                if (i == 9602)
                    int aa = 0;
                gr->put(keys[i]);
            }
            PartItem *partial = gr->prepare(tid);
            output[tid] << "\t" << tid << " " << tracer.getRunTime() << " " << gr->volume() << " ";
            for (int i = 0; i < 3; i++) {
                output[tid] << partial[i].getItem() << ":" << partial[i].getCount() << "->";
            }
            output[tid] << "->...->";
            for (int i = 0; i < 3; i++) {
                output[tid] << partial[gr->volume() - i].getItem() << ":" << partial[gr->volume() - i].getCount()
                            << "->";
            }
        }, grs.at(i), i));
    }
    for (int i = 0; i < total_threads_num; i++) {
        workers.at(i).join();
        string outstr = output[i].str();
        cout << outstr;
        cout << endl;
    }
    cout << "push: " << tracer.getRunTime() << ":" << hit_count << endl;
    tracer.startTime();
    PartItem *merged = grs.at(0)->merge(grs);
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
    delete[] output;
}

void naiveConciseARCTest() {
    ConciseARCAlgorithm clru(10);
    for (int i = 0; i < 100; i++) {
        cout << i << " ";
        if ((i + 1) % 50 == 0) cout << endl;
        clru.add(i);
    }
}

void naiveConciseLRUTest() {
    ConciseLRUAlgorithm clru(10);
    for (int i = 0; i < 100; i++) {
        clru.add(i);
        clru.print();
    }
}

void conciseLRUTest() {
    ConciseLRUAlgorithm clru(hit_count);
    Tracer tracer;
    tracer.startTime();
    size_t miss = 0, hit = 0;
    for (int i = 0; i < max_count; i++) {
        //cout << i << ":" << keys[i] << endl;
        if (clru.contains(keys[i])) hit++; else miss++;
        clru.add(keys[i]);
    }
    cout << "time: " << tracer.getRunTime() << " c: " << clru.getSize() << " miss " << miss << " hit: " << hit << endl;
}

void conciseARCTest() {
    ConciseARCAlgorithm r(hit_count);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < max_count; i++) {
        //cout << i << ":" << keys[i] << endl;
        r.add(keys[i]);
    }
    cout << "time: " << tracer.getRunTime() << " miss: " << r.getTotalMiss() << " all: " << r.getTotalRequest() << endl;
}

int main(int argc, char **argv) {
    naiveConciseARCTest();
    if (argc == 1) {
        naiveTest();
        dumpTest();
        naiveConciseLRUTest();
        return 0;
    }
    if (argc != 5) {
        cout << "command: type:0/verification,1/parallel, total_count, capacity, thread_number/1(parallel)" << endl;
        exit(-1);
    }
    int type = std::atoi(argv[1]);
    max_count = std::atol(argv[2]);
    hit_count = std::atol(argv[3]);
    total_threads_num = std::atoi(argv[4]);
    switch (type) {
        case 0:
            generate();
            fullGeneralTest();
            cout << endl << endl << "===========================================================" << endl << endl;
            fullConciseTest();
            break;
        case 1:
            cerr << "merge still issues a bug" << endl;
            cout << "parallel total: " << max_count << " c: " << hit_count << " trd: " << total_threads_num << endl;
            generate();
            parallelConciseTest();
            break;
        case 2:
            cout << "conciselru test: " << max_count << " c: " << hit_count << " dum: " << total_threads_num << endl;
            generate();
            conciseLRUTest();
        case 3:
            cout << "concisearc test: " << max_count << " c: " << hit_count << " dum: " << total_threads_num << endl;
            generate();
            conciseARCTest();
        case 4:
            cout << "verification test: " << max_count << " c: " << hit_count << " dum: " << total_threads_num << endl;
            generate();
            verificationTest();
            break;
        default:
            cout << "command: type:0/verification,1/parallel, total_count, capacity, thread_number/1(parallel)" << endl;
    }
    return 0;
}