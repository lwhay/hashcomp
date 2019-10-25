//
// Created by Michael on 10/25/2019.
//

#ifndef HASHCOMP_FREQUENTSTATISTICS_H
#define HASHCOMP_FREQUENTSTATISTICS_H

#include <algorithm>
#include <boost/bind.hpp>
#include <vector>
#include <unordered_map>

#define SORTORDER false

bool secondOrder;

template<typename keytype>
class FCCount {
private:
    std::unordered_map<keytype, uint64_t> freqmap;
    std::unordered_map<uint64_t, uint64_t> countmap;
    std::vector<std::pair<keytype, uint64_t>> sortedfreqents;
    std::vector<std::pair<uint64_t, uint64_t>> sortedcounts;

    template<typename firsttype>
    static bool comp(std::pair<firsttype, uint64_t> a, std::pair<firsttype, uint64_t> b) {
        if (secondOrder)
            return a.second > b.second;
        else
            return a.first > b.first;
    }

    static void tick(std::pair<keytype, uint64_t> p, std::unordered_map<uint64_t, uint64_t> &countmap) {
        if (countmap.find(p.second) == countmap.end()) {
            countmap.insert(std::make_pair<>(p.second, 0));
        }
        countmap.find(p.second)->second++;
    }

public:

    FCCount(bool ordertype = true) {
        secondOrder = ordertype;
    }

    uint64_t volume() { return freqmap.size(); }

    std::vector<std::pair<keytype, uint64_t>> &get() { return sortedfreqents; }

    std::vector<std::pair<uint64_t, uint64_t>> &final() { return sortedcounts; }

    void offer(keytype k) {
        if (freqmap.find(k) == freqmap.end()) {
            freqmap.insert(std::make_pair<>(k, 0));
        }
        freqmap.find(k)->second++;
    }

    void wind() {
        sortedfreqents.assign(freqmap.begin(), freqmap.end());
        secondOrder = true;
        std::sort(sortedfreqents.begin(), sortedfreqents.end(), comp<keytype>);
        for_each(sortedfreqents.begin(), sortedfreqents.end(), boost::bind(tick, _1, std::ref(countmap)));
        sortedcounts.assign(countmap.begin(), countmap.end());
        secondOrder = false;
        std::sort(sortedcounts.begin(), sortedcounts.end(), comp<uint64_t>);
    }
};

#endif //HASHCOMP_FREQUENTSTATISTICS_H
