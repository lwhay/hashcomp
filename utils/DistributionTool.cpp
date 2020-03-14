//
// Created by lwh on 19-6-16.
//

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
#include <algorithm>
#include <map>
#include <iterator>
#include "tracer.h"

using namespace std;
using namespace ycsb;

vector<pair<string, uint64_t>> orderedCounters(YCSB_operator com, char *inpath, size_t limit = (1 << 32)) {
    unordered_map<string, uint64_t> counters;
    string type;
    string content;
    YCSBLoader loader(inpath, limit);
    std::vector<YCSB_request *> requests = loader.load();
    vector<std::pair<string, uint64_t>> tmp;
    size_t cur = 0;
    for (auto &e: requests) {
        if (counters.find(requests[cur]->getKey()) == counters.end())
            counters.insert(std::make_pair(requests[cur]->getKey(), 0));
        counters.find(requests[cur]->getKey())->second++;
        cur++;
    }
    for (auto &e: counters) {
        tmp.push_back(e);
    }
    sort(tmp.begin(), tmp.end(),
         [=](pair<string, uint64_t> &a, pair<string, uint64_t> &b) { return b.second < a.second; });
    vector<pair<string, uint64_t>>::iterator iter = tmp.begin();
    while (iter++ != tmp.end()) {
        if (iter - tmp.begin() == limit)
            break;
        cout << iter->first << " " << iter->second << endl;
    }
    /*map<int, string> tmp;
    transform(counters.begin(), counters.end(), std::inserter(tmp, tmp.begin()),
              [](std::pair<string, int> a) { return std::pair<int, string>(a.second, a.first); });

    map<int, string>::iterator iter = tmp.begin();
    while (++iter != tmp.end()) {
        cout << (*iter).first << " " << (*iter).second << endl;
    }*/
    cout << counters.size() << "<->" << tmp.size() << endl;
    return tmp;
}

void dumpData(char *inpath, size_t limit) {
    YCSBLoader loader(inpath, limit);
    std::vector<YCSB_request(*)> requests = loader.load();
    FILE *fp = fopen(existingFilePath, "wb+");
    uint64_t *array = new uint64_t[loader.size()];
    size_t cur = 0;
    for (auto &e : requests) {
        array[cur] = std::atol(requests[cur++]->getKey());
    }
    fwrite(array, sizeof(uint64_t), loader.size(), fp);
}

int main(int argc, char **argv) {
    if (std::atoi(argv[1]) != 7)
        orderedCounters(static_cast<YCSB_operator>(std::atoi(argv[1])), argv[2], std::atol(argv[3]));
    else
        dumpData(argv[2], std::atol(argv[3]));
    return 0;
}