//
// Created by iclab on 6/17/21.
//
#include <cmath>
#include <unordered_set>
#include <cassert>
#include <iostream>
#include <ostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include "FrequentStatistics.h"
#include "StringUtil.h"

using namespace std;

string path("../../res/load.dat");

#define MAX_WINDOW_SIZE 11

template<typename type>
void print(pair<type, uint64_t> p) {
    cout << p.first << "\t" << p.second << endl;
}

template<typename type>
void genstat(FCCount<type> count, char *name) {
    char keypath[255];
    std::memset(keypath, 0, 255);
    std::strncpy(keypath, name, std::strlen(name));
    std::strncpy(keypath + std::strlen(name), ".sta", 4);
    ofstream outf = ofstream(keypath);
    streambuf *coutf = cout.rdbuf();
    cout.rdbuf(outf.rdbuf());
    for_each(count.get().begin(), count.get().end(), print<type>);
    cout.rdbuf(coutf);
    cout << "***************************" << count.volume() << " ***************************" << endl;

    std::memset(keypath, 0, 255);
    std::strncpy(keypath, name, std::strlen(name));
    std::strncpy(keypath + std::strlen(name), ".fre", 4);
    outf = ofstream(keypath);
    cout.rdbuf(outf.rdbuf());
    for_each(count.final().begin(), count.final().end(), print<uint64_t>);
    cout.rdbuf(coutf);
}

int main(int argc, char **argv) {
    int pos = 2;
    if (argc > 1) path = argv[1];
    if (argc > 2) pos = std::atoi(argv[2]);
    ifstream input;
    input.open(path);
    uint64_t wdSize[MAX_WINDOW_SIZE], lineCount = 0;
    memset(wdSize, 0, MAX_WINDOW_SIZE * sizeof(uint64_t));
    unordered_set<string> wdCount[MAX_WINDOW_SIZE];
    FCCount<string> fcCount;
    FCCount<int> klCount;
    FCCount<int> vlCount;
    FCCount<int> opCount;
    FCCount<int> ttlCount;
    char line[4096];
    while (input.getline(line, 4096)) {
        vector<string> fields = split(line, "\t");
        if (fields.size() < 7) continue;
        fields[pos].erase(
                remove_if(fields[pos].begin(), fields[pos].end(), [](unsigned char x) { return std::isspace(x); }),
                fields[pos].end());
        // cout << hex << fields[pos].find(" ") << endl;
        assert(fields[pos].find(" ") == (std::size_t) -1);
        // cout << fields[pos] << endl;
        for (int i = 0; i < MAX_WINDOW_SIZE; i++) {
            if (lineCount % (int) (pow(2, i)) == 0) {
                wdSize[i] += wdCount[i].size();
                wdCount[i].clear();
            }
            wdCount[i].insert(fields[pos]);
        }
        fcCount.offer(fields[pos]);
        klCount.offer(stoi(fields[3].substr(4)));
        vlCount.offer(stoi(fields[4].substr(4)));
        opCount.offer(stoi(fields[5].substr(2)));
        ttlCount.offer(stoi(fields[6].substr(3)));
        lineCount++;
    }
    fcCount.wind();
    klCount.wind();
    vlCount.wind();
    opCount.wind();
    ttlCount.wind();
    genstat<string>(fcCount, "fc");
    genstat<int>(klCount, "kl");
    genstat<int>(vlCount, "vl");
    genstat<int>(opCount, "op");
    genstat<int>(ttlCount, "ttl");
    for (int i = 0; i < MAX_WINDOW_SIZE; i++)
        cout << (pow(2, i)) << ":" << (double) wdSize[i] / lineCount << ":" << wdSize[i] << ":" << lineCount << endl;
    input.close();
}