//
// Created by Michael on 10/25/2019.
//

#ifndef HASHCOMP_FREQUENTSTATISTICS_H
#define HASHCOMP_FREQUENTSTATISTICS_H

#include <unordered_map>

template<typename keytype>
class FCCount {
private:
    bool secondOrder;
    std::unordered_map<keytype, uint64_t> freqmap;

    bool comp(std::pair<keytype, uint64_t> a, std::pair<keytype, uint64_t> b) {
        if (secondOrder)
            return a.second > b.second;
        else
            return a.first > b.first;
    }

public:

    FCCount(bool secondOrder = true) : secondOrder(secondOrder) {}

    uint64_t volume() { return freqmap.size(); }

    std::unordered_map<keytype, uint64_t> &get() { return freqmap; }

    void offer(keytype k) {
        if (freqmap.find(k) == freqmap.end()) {
            freqmap.insert(std::make_pair<>(k, 0));
        }
        freqmap.find(k)->second++;
    }

    void tick(std::pair<uint64_t, uint64_t> p) {
        if (freqmap.find(p.second) == freqmap.end()) {
            freqmap.insert(std::make_pair<>(p.second, 0));
        }
        freqmap.find(p.second)->second++;
    }
};

#endif //HASHCOMP_FREQUENTSTATISTICS_H
