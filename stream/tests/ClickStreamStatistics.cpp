//
// Created by Michael on 10/25/2019.
//

#include <iostream>
#include <ostream>
#include <fstream>
#include <vector>
#include "FrequentStatistics.h"
#include "StringUtil.h"

using namespace std;

char *path = "../../res/realKVdata/clickstream.tsv";

int main(int argc, char **argv) {
    int pos = 1;
    if (argc > 1) path = argv[1];
    if (argc > 2) pos = std::atoi(argv[2]);
    ifstream input;
    input.open(path);
    FCCount<const char *> fcCount;
    char line[256];
    while (input.getline(line, 256)) {
        vector<string> fields = split(line, "\t");
        cout << line << endl;
        cout << fields.size() << "\t";
        for (int i = 0; i < fields.size(); i++) cout << fields[i] << "\t";
        fcCount.offer(fields[pos].c_str());
        cout << endl;
    }
    cout << fcCount.volume();
    unordered_map<const char*, uint64_t>::iterator iter = fcCount.get().begin();
    while (iter != fcCount.get().end()) {
        cout << iter++->first << endl;
    }
    input.close();
}