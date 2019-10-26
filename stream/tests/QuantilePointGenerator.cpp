//
// Created by Michael on 10/26/2019.
//

#include <fstream>
#include <iostream>
#include <vector>
#include "QuantileStatistics.h"
#include "StringUtil.h"

using namespace std;

int main(int argc, char **argv) {
    ifstream input(argv[1]);
    FCQuantile<uint64_t, uint64_t> qt;
    char line[4096];
    while (input.getline(line, 4096)) {
        vector<string> fields = split(line, "\t");
        if (fields.size() < 2 || !isIntegeric(fields[0].c_str()) || !isIntegeric(fields[1].c_str())) continue;
        qt.add(make_pair(std::atol(fields[0].c_str()), std::atol(fields[1].c_str())));
    }
    for (double c = 10000; c <= 60000; c += 10000) cout << c << ": " << qt.getAccCount(c / qt.getKeyCount()) << endl;
    for (double r = 0.1; r <= 1.0; r += 0.1) cout << r << ": " << qt.getKeyCount(r) << endl;
    input.close();
    return 0;
}