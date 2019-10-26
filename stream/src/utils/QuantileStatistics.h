//
// Created by Michael on 10/26/2019.
//

#ifndef HASHCOMP_QUANTILESTATISTICS_H
#define HASHCOMP_QUANTILESTATISTICS_H

#include <vector>

template<typename freqtype, typename counttype>
class FCQuantile {
private:
    std::vector<std::pair<freqtype, counttype>> freqCounts;
    uint64_t accCount = 0;
    uint64_t keyCount = 0;

public:
    void add(std::pair<freqtype, counttype> p) {
        freqCounts.push_back(p);
        keyCount += p.second;
        accCount += (p.first * p.second);
    }

    uint64_t getAccCount() { return accCount; }

    uint64_t getKeyCount() { return keyCount; }

    uint64_t getAccCount(double perc) {
        uint64_t expect = (uint64_t) (keyCount * perc);
        uint64_t acount = 0;
        uint64_t kcount = 0;
        for (int i = 0; i < freqCounts.size(); i++) {
            if (kcount > expect) {
                break;
            }
            kcount += freqCounts.at(i).second;
            acount += (freqCounts.at(i).first * freqCounts.at(i).second);
        }
        return acount;
    }

    uint64_t getKeyCount(double perc) {
        uint64_t expect = (uint64_t) (accCount * perc);
        uint64_t acount = 0;
        uint64_t kcount = 0;
        for (int i = 0; i < freqCounts.size(); i++) {
            if (acount > expect) {
                break;
            }
            kcount += freqCounts.at(i).second;
            acount += (freqCounts.at(i).first * freqCounts.at(i).second);
        }
        return kcount;
    }
};

#endif //HASHCOMP_QUANTILESTATISTICS_H
