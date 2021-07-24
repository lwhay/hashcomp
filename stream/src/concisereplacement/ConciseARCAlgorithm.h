//
// Created by iclab on 6/28/21.
//

#ifndef HASHCOMP_CONCISEARCALGORITHM_H
#define HASHCOMP_CONCISEARCALGORITHM_H

#include "ConciseLRUAlgorithm.h"

class ConciseARCAlgorithm {
private:
    enum Adjustment {
        MIN, MAX
    };
    ConciseLRUAlgorithm *t1, *t2, *b1, *b2;
    float hitRatio;
    size_t numHits, numRequests, numMisses, cacheSize, p;


    bool checkCaseOne(uint32_t page) {
        bool retVal = false;
        if (t1->contains(page)) {
            t2->add(page);
            t1->removePage(page);
            retVal = true;
            numHits++;
        } else if (t2->contains(page)) {
            t2->moveToBack(page);
            retVal = true;
            numHits++;
        }

        return retVal;
    }

    bool checkCaseTwo(uint32_t page) {
        bool retVal = false;

        if (b1->contains(page)) {
            adjust(page, MIN);
            replace(page);
            t2->add(page);
            b1->removePage(page);
            retVal = true;
            numMisses++;
        }

        return retVal;
    }

    bool checkCaseThree(uint32_t page) {
        bool retVal = false;

        if (b2->contains(page)) {
            adjust(page, MAX);
            replace(page);
            t2->add(page);
            b2->removePage(page);
            retVal = true;
            numMisses++;
        }

        return retVal;
    }

    bool checkCaseFour(uint32_t page) {
        if (t1->getSize() + b1->getSize() == cacheSize) {
            if (t1->getSize() < cacheSize) {
                b1->removePage(page);
                replace(page);
            } else
                t1->removePage(page);
        } else {
            size_t size = t1->getSize() + t2->getSize() + b1->getSize() + b2->getSize();

            if (size >= cacheSize) {
                if (size == 2 * cacheSize)
                    b2->removePage(page);

                replace(page);
            }
        }

        t1->add(page);
        numMisses++;

        return true;
    }

    void adjust(uint32_t page, Adjustment adjType) {
        size_t adj;

        if (adjType == MIN) {
            if (b1->getSize() >= b2->getSize())
                adj = 1;
            else {
                if (b1->getSize() > 0)
                    adj = b2->getSize() / b1->getSize();
                else
                    adj = cacheSize;
            }

            p = std::min((p + adj), cacheSize);
        } else {
            if (b2->getSize() >= b1->getSize())
                adj = 1;
            else {
                if (b2->getSize() > 0)
                    adj = b1->getSize() / b2->getSize();
                else
                    adj = 0;
            }

            p = (size_t) std::max((float) (p - adj), (float) 0);
        }
    }

    void replace(uint32_t page) {
        if (t1->getSize() > 0 && (t1->getSize() > p || (b2->contains(page) && t1->getSize() == p))) {
            t1->removePage(page);
            b1->add(page);
        } else {
            t2->removePage(page);
            b2->add(page);
        }
    }

public:
    ConciseARCAlgorithm(size_t cacheSize) {
        p = 0;
        numHits = 0;
        numRequests = 0;
        numMisses = 0;
        hitRatio = 0;
        this->cacheSize = cacheSize;

        t1 = new ConciseLRUAlgorithm(cacheSize);
        t2 = new ConciseLRUAlgorithm(cacheSize);

        b1 = new ConciseLRUAlgorithm(cacheSize);
        b2 = new ConciseLRUAlgorithm(cacheSize);

    }

    ~ConciseARCAlgorithm() {}

    size_t getTotalMiss() { return (numRequests - numHits); }

    size_t getTotalRequest() { return numRequests; }

    bool add(uint32_t page) {
        numRequests++;

        if (checkCaseOne(page))
            return true;

        if (checkCaseTwo(page))
            return true;

        if (checkCaseThree(page))
            return true;

        if (checkCaseFour(page))
            return false;

        printf("There is a problem!\n");
    }

    bool contains(uint32_t page) {
        return t1->contains(page) || t2->contains(page) || b1->contains(page) || b2->contains(page);
    }

    float getHitRatio() {
        calculateHitRatio();

        return hitRatio;
    }

    void calculateHitRatio() {
        hitRatio = (float) numHits / (float) numRequests * 100;
    }
};

#endif //HASHCOMP_CONCISEARCALGORITHM_H
