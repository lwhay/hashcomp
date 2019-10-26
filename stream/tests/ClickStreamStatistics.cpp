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

string path("../../res/realKVdata/clickstream.tsv");

template<typename type>
void print(pair<type, uint64_t> p) { cout << p.first << "\t" << p.second << endl; }

int main(int argc, char **argv) {
    int pos = 1;
    if (argc > 1) path = argv[1];
    if (argc > 2) pos = std::atoi(argv[2]);
    ifstream input;
    input.open(path);
    FCCount<string> fcCount;
    char line[4096];
    while (input.getline(line, 4096)) {
        vector<string> fields = split(line, "\t");
        fcCount.offer(fields[pos]);
    }
    fcCount.wind();
    for_each(fcCount.get().begin(), fcCount.get().end(), print<string>);
    cout << "***************************" << fcCount.volume() << " ***************************" << endl;
    for_each(fcCount.final().begin(), fcCount.final().end(), print<uint64_t>);
    input.close();
}