//
// Created by Michael on 10/26/19.
//

#include <iostream>
#include <vector>
#include "tracer.h"
#include "generator.h"
#include "../src/heavyhitter/GroupFrequent.h"
#include "../src/heavyhitter/LazySpaceSaving.h"
#include "../src/heavyhitter/SpaceSaving.h"
#include "../src/cfrequent/Frequency.h"
#include "../src/heavyhitter/Frequent.h"
#include "../src/cfrequent/LossyCount.h"

using namespace std;

uint64_t total_round = (1 << 28);

int main(int argc, char **argv) {
    if (argc > 1) {
        total_round = std::atol(argv[1]);
    }
    zipf_distribution<uint64_t> gen((1LLU << 32), 0.99);
    std::mt19937 mt;
    vector<uint64_t> keys;
    SS<uint64_t> ss(1000);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        keys.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << " with " << keys.size() << endl;
    tracer.getRunTime();
    /*for (int i = 0; i < total_round; i++) {
        ss.put(keys[i]);
    }
    exit(0);*/
    cout << "Original SS:" << tracer.getRunTime() << ":" << ss.getCounterNumber() << endl;
    freq_type *ft = Freq_Init(0.00001);
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        Freq_Update(ft, keys[i]);
    }
    cout << "CFrequency: " << tracer.getRunTime() << ":" << Freq_Size(ft) << endl;
    /*Freq_Output(ft, 0);*/
    Freq_Destroy(ft);
    LCL_type *lcl = LCL_Init(0.00001);
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        LCL_Update(lcl, keys[i], 1);
    }
    cout << "CLSLazy: " << tracer.getRunTime() << ":" << LCL_Size(lcl) << endl;
    /*LCL_Output(lcl);
    LCL_ShowHash(lcl);
    LCL_ShowHeap(lcl);*/
    LCL_Destroy(lcl);
    GroupFrequent gf(0.00001);
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        gf.put(keys[i]);
    }
    cout << "GroupFrequent: " << tracer.getRunTime() << ":" << gf.size() << endl;
    LazySpaceSaving lss(0.00001);
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        lss.put(keys[i]);
    }
    cout << "LazySS: " << tracer.getRunTime() << ":" << lss.size() << endl;
    Frequent frequent(1000);
    for (int i = 0; i < total_round; i++) {
        frequent.add(keys[i]);
    }
    cout << "Cplusplus Frequency: " << tracer.getRunTime() << ":" << frequent.size() << endl;
#ifndef NDEBUG
    Node *head = frequent.header();
    while (head->getNext() != nullptr) {
        cout << head->getNext()->getIndex() << ":" << head->getNext()->getFreq() << endl;
        head = head->getNext();
    }
#endif
}