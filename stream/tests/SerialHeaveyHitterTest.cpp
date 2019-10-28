//
// Created by Michael on 10/26/19.
//

#include <iostream>
#include <vector>
#include "tracer.h"
#include "generator.h"
#include "SpaceSaving.h"
#include "Frequency.h"
#include "Frequent.h"

using namespace std;

int main(int argc, char **argv) {
    zipf_distribution<uint64_t> gen((1LLU << 32), 1.5);
    std::mt19937 mt;
    vector<uint64_t> keys;
    SS<uint64_t> ss(1000);
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < (1 << 28); i++) {
        keys.push_back(gen(mt));
    }
    cout << tracer.getRunTime() << endl;
    tracer.getRunTime();
    for (int i = 0; i < (1 << 28); i++) {
        ss.put(keys[i]);
    }
    cout << "Original SS:" << tracer.getRunTime() << ":" << ss.getCounterNumber() << endl;
    freq_type *ft = Freq_Init(0.01);
    tracer.startTime();
    for (int i = 0; i < (1 << 28); i++) {
        Freq_Update(ft, i);
    }
    cout << "Original Frequency: " << tracer.getRunTime() << ":" << Freq_Size(ft) << endl;
    Frequent frequent(1000);
    for (int i = 0; i < (1 << 28); i++) {
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