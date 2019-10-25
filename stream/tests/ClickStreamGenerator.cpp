//
// Created by Michael on 10/25/2019.
//

#include <iostream>
#include <ostream>
#include <vector>
#include <unordered_map>
#include <boost/bind.hpp>
#include "tracer.h"
#include "FrequentStatistics.h"
#include "StringUtil.h"

using namespace std;

string path("../../res/realKVdata/clickstream.tsv");

string result("../../res/realKVdata/clickkeys.dat");

void print(string key, ofstream &output, unordered_map<string, string> &loads) {
    output << key << "\t" << loads.find(key)->second << endl;
}

void generateReadLoad(int argc, char **argv) {
    int pos = 1;
    if (argc > 1) path = argv[1];
    if (argc > 2) pos = std::atoi(argv[2]);
    ifstream input;
    input.open(path);
    vector<string> clickedkeys;
    unordered_map<string, string> loads;
    char line[256];
    while (input.getline(line, 256)) {
        vector<string> fields = split(line, "\t");
        int freq;
        if (fields[3].length() >= 1 && '9' >= fields[3].at(0) && fields[3].at(0) >= '0') {
            freq = std::atoi(fields[3].c_str());
        } else {
            cout << fields[3] << endl;
            continue;
        }
        if (loads.find(fields[pos]) == loads.end()) loads.insert(make_pair(fields[pos], line));
        for (int i = 0; i < freq; i++) { clickedkeys.push_back(fields[pos]); }
    }
    cout << clickedkeys.size() << endl;
    Tracer tracer;
    tracer.startTime();
    auto seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::shuffle(clickedkeys.begin(), clickedkeys.end(), std::default_random_engine(seed));
    ofstream output(result);
    for_each(clickedkeys.begin(), clickedkeys.end(), boost::bind(print, _1, std::ref(output), std::ref(loads)));
    output.flush();
    output.close();
    cout << tracer.getRunTime() << endl;
}

int main(int argc, char **argv) {
    generateReadLoad(argc, argv);
}