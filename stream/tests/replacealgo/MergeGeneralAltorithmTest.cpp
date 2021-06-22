//
// Created by iclab on 6/22/21.
//
#include <cassert>
#include <iostream>
#include <unordered_map>
#include "ReplacementAlgorithm.h"
#include "tracer.h"

int main(int argc, char **argv) {
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