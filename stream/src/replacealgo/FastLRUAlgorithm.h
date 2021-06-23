//
// Created by Michael on 6/23/21.
//

#ifndef STREAM_FASTLRUALGORITHM_H
#define STREAM_FASTLRUALGORITHM_H

#include <list>

template<typename IT>
class FastLRUAlgorithm {
    std::list<int> cache;
    std::unordered_map<int, bool> currPages;
    std::unordered_map<int, std::list<int>::iterator> itLocs;
    size_t cacheSize, numHits, numRequests, size;
    float hitRatio;
public:
    FastLRUAlgorithm(int cacheSize) {
        this->cacheSize = cacheSize;
        numHits = 0;
        numRequests = 0;
        size = 0;
    }

    ~FastLRUAlgorithm() {}

    void setCacheSize(int cacheSize) {
        this->cacheSize = cacheSize;
    }

    size_t getTotalMiss() { return (numRequests - numHits); }

    size_t getTotalRequest() { return numRequests; }

    int getSize() { return size; }

    void add(IT page) {
        numRequests++;

        if (contains(page))
            moveToBack(page);
        else {
            if (cache.size() < cacheSize) {
                itLocs[page] = cache.insert(cache.end(), page);
                currPages[page] = true;
                size++;
            } else
                replacePage(page);
        }
    }

    bool contains(IT page) {
        if (currPages[page]) {
            numHits++;
            return true;
        }

        return false;
    }

    float getHitRatio() {
        calculateHitRatio();

        return hitRatio;
    }

    void calculateHitRatio() {
        hitRatio = (float) numHits / (float) numRequests * 100;
    }

    void moveToBack(IT page) {
        cache.erase(itLocs[page]);
        itLocs[page] = cache.insert(cache.end(), page);
    }

    void replacePage(IT page) {
        IT oldPage = cache.front();

        //lower memory use, higher runtime
        //currPages.erase(oldPage);

        //higher memory use, lower runtime
        removePage(oldPage);


        currPages[page] = true;

        cache.pop_front();

        itLocs[page] = cache.insert(cache.end(), page);
    }

    void removePage(IT page) {
        currPages[page] = false;
        size--;
    }
};

#endif //STREAM_FASTLRUALGORITHM_H
