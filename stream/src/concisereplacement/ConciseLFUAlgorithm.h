//
// Created by iclab on 6/26/21.
//

#ifndef HASHCOMP_CONCISELRU_H
#define HASHCOMP_CONCISELRU_H

#include <algorithm>
#include <functional>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <queue>
#include <vector>

#define verifyRefresh 0

#define NEXT_BITS 0xfffffllu
#define NEXT_MASK 0xfffffffffff00000llu
#define LEFT_BITS 0xfffff00000llu
#define LEFT_MASK 0xffffff00000fffffllu
#define RIGHT_BITS 0x0fffff0000000000llu
#define RIGHT_MASK 0xf00000ffffffffffllu

#define DELTA_BITS 0xfff
#define DELTA_MASK 0xfffff000
#define COUNT_BITS 0xfffff000
#define COUNT_MASK 0xfff

class FreqItem {
private:
    uint32_t item;
    uint32_t deltaCount;
    uint64_t points; // 0-19: *next, 20-39: *left, 40:59 *right;
public:
    void setitem(uint32_t ihash) { this->item = ihash; }

    uint32_t getItem() { return item; }

    void setCount(int count) {
        deltaCount &= COUNT_MASK;
        if (count < 0x100000) deltaCount |= (count << 12); else deltaCount |= 0xfffff000;
    }

    void chgCount(int value) {
        uint32_t oldCount = (deltaCount >> 12);
        deltaCount &= COUNT_MASK;
        if (value + oldCount < 0x100000) deltaCount |= ((value + oldCount) << 12); else deltaCount |= COUNT_BITS;
    }

    int getCount() { return (deltaCount >> 12); }

    void setDelta(int delta) {
        deltaCount &= DELTA_MASK;
        if (delta < 0x1000) deltaCount |= delta; else deltaCount |= DELTA_BITS;
    }

    int getDelta() { return (deltaCount & DELTA_BITS); }

    void setLeft(int offset) {
        points &= LEFT_MASK;
        points |= ((uint64_t) offset << 20);
    }

    int getLeft() { return ((points & LEFT_BITS) >> 20); }

    void setRight(int offset) {
        points &= RIGHT_MASK;
        points |= ((uint64_t) offset << 40);
    }

    int getRight() { return ((points & RIGHT_BITS) >> 40); }

    void setNext(int offset) {
        points &= NEXT_MASK;
        points |= offset;
    }

    int getNext() {
        uint64_t copy = points;
        uint32_t off = points & NEXT_BITS;
        return (points & NEXT_BITS);
    }

    static bool freqComp(FreqItem &a, FreqItem &b) {
        return a.getCount() > b.getCount();
    }

    static bool freqPrec(FreqItem &a, FreqItem &b) {
        return a.getItem() < b.getItem();
    }

    bool operator==(FreqItem &target) const {
        return item == target.getItem();
    }
};

//#define PART_BITS 0xff000000
#define PART_MASK 0x00ffffff
#define SUM_BITS 0x00ffffff
#define SUM_MASK 0xff000000

class PartItem {
private:
    uint32_t item;
    uint32_t partCount;
public:
    void setitem(uint32_t item) { this->item = item; }

    uint32_t getItem() { return item; }

    void setCount(int count) {
        partCount &= SUM_MASK;
        if (count < 0x1000000) partCount |= count; else partCount |= SUM_BITS;
    }

    void chgCount(int value) {
        int sum = (partCount & SUM_BITS) + value;
        partCount &= SUM_MASK;
        if (sum < 0x1000000) partCount |= sum; else partCount |= SUM_BITS;
    }

    int getCount() { return partCount & SUM_BITS; }

    void setPart(int part) {
        assert(part < 0x100);
        partCount &= PART_MASK;
        partCount |= (part << 24);
    }

    int getPart() { return (partCount >> 24); }

    static bool partComp(PartItem &a, PartItem &b) {
        return a.getCount() > b.getCount();
    }

    static bool partPrec(PartItem &a, PartItem &b) {
        return a.getItem() < b.getItem();
    }

    bool operator==(PartItem &target) const {
        return item == target.getItem();
    }
};

class ConciseLFUAlgorithm {
protected:
    constexpr static uint32_t GLSS_HASHMULT = 3;
    constexpr static uint32_t GLSS_NULLITEM = std::numeric_limits<uint32_t>::max();
    constexpr static uint32_t GLSS_MOD = std::numeric_limits<uint32_t>::max() / 2;
    constexpr static int GLSS_HL = 8 * sizeof(uint32_t) - 1;
    int n;
    uint32_t hashsize;
    int _size;
    FreqItem *root;
    FreqItem *counters;
    PartItem *merged;
    uint32_t *hashtable;

protected:
    inline uint32_t hk(uint32_t hash) { return ((hash % (hashsize - 1)) + 1); }

    inline void Heapify(uint32_t ptr) {
        FreqItem tmp;
        FreqItem *cpt, *minchild;
        uint32_t mc;

        while (1) {
            if ((ptr << 1) + 1 > _size) break;

            cpt = &counters[ptr];
            mc = (ptr << 1) + ((counters[ptr << 1].getCount() < counters[(ptr << 1) + 1].getCount()) ? 0 : 1);
            minchild = &counters[mc];

            if (cpt->getCount() < minchild->getCount()) break;
            tmp = *cpt;
            *cpt = *minchild;
            *minchild = tmp;
            uint32_t hcpt = hk(cpt->getItem()), hmin = hk(minchild->getItem());

            if (hcpt == hmin) {
                minchild->setNext(cpt->getNext());
                cpt->setNext(tmp.getNext());
            } else {
                // cpt (in hmin) and minchild (in hcpt) should be found in hashtable when they are not GLSS_NULLITEM
                FreqItem *cur = counters + hashtable[hmin], *prev = nullptr;
                uint32_t oldhcpt = hashtable[hcpt];
                // assert(hk(minchild->getItem()) == hmin);
                int count = 0;
                while (cur != counters) {
                    if (cur == cpt) {
                        if (prev == nullptr) {
                            if (cpt->getItem() != GLSS_NULLITEM) {
                                oldhcpt = hashtable[hcpt];
                                hashtable[hcpt] = (cpt - counters);
                            }
                        } else {
                            prev->setNext(cpt - counters);
                        }
                        break;
                    }
                    prev = cur;
                    cur = (counters + cur->getNext());
                    if (count++ > 20) {
                        std::cerr << "error" << 204 << std::endl;
                        exit(-1);
                    }
                }
                // assert(cur != counters);
                FreqItem *tt = counters + oldhcpt;
                if (cpt->getItem() == GLSS_NULLITEM) {
                    uint32_t tt = hashtable[hmin];
                    // assert(hashtable[hmin] == ptr);
                    hashtable[hmin] = (minchild - counters);
                } else {
                    cur = counters + oldhcpt, prev = nullptr;
                    count = 0;
                    while (cur != counters) {
                        if (cur == minchild) {
                            if (prev == nullptr) {
                                hashtable[hmin] = (minchild - counters);
                            } else {
                                prev->setNext(minchild - counters);
                            }
                            break;
                        }
                        prev = cur;
                        cur = (counters + cur->getNext());
                        if (count++ > 20) {
                            std::cerr << "error" << 229 << std::endl;
                            exit(-1);
                        }
                    }
                    //assert(cur != counters);
                }
            }
            ptr = mc;
        }
        for (int i = 0; i < hashsize; i++) assert(hashtable[i] != 1 || root->getNext() != 1);
    }

public:
    ConciseLFUAlgorithm(int K) {
        if (K >= ((1 << 20) - 1)) {
            std::cerr << "Concise HT can not support capacity larger than 1M and k number half billion" << std::endl;
            exit(-1);
        }
        int k = K;
        _size = (1 + k) | 1;
        hashsize = GLSS_HASHMULT * _size;
        hashtable = new uint32_t[hashsize];
        std::memset(hashtable, 0, sizeof(uint32_t) * hashsize);
        counters = (FreqItem *) new FreqItem[1 + _size];
        std::memset(counters, 0, sizeof(FreqItem) * (1 + _size));
        merged = nullptr;
        n = 0;

        for (int i = 0; i <= _size; i++) {
            counters[i].setitem(GLSS_NULLITEM);
        }

        root = &counters[1];
    }

    void reset() {
        std::memset(hashtable, 0, sizeof(uint32_t) * hashsize);
        std::memset(counters, 0, sizeof(FreqItem) * (1 + _size));
        if (merged != nullptr) std::memset(merged, 0, sizeof(PartItem) * (1 + _size));
        n = 0;

        for (int i = 0; i <= _size; i++) {
            counters[i].setitem(GLSS_NULLITEM);
        }
        root = &counters[1];
    }

    ~ConciseLFUAlgorithm() {
        delete[] hashtable;
        delete[] counters;
        if (merged != nullptr) delete[] merged;
    }

    int range() {
        return hashsize;
    }

    int volume() {
        return _size;
    }

    int size() {
        return sizeof(ConciseLFUAlgorithm) + hashsize * sizeof(uint32_t) + _size * sizeof(FreqItem);
    }

    inline uint32_t put(uint32_t item, int value = 1) {
        uint32_t hashval;
        FreqItem *hashptr;

        uint32_t ret = item;
        n += value;
        counters->setitem(0);
        hashval = hk(item);
        hashptr = counters + hashtable[hashval];
        // assert(hashtable[hashval] != 1); // permit here
        while (hashptr != counters) {
            if (hashptr->getItem() == item) {
                hashptr->chgCount(value);
                Heapify(hashptr - counters);
                return ret;
            } else hashptr = (counters + hashptr->getNext());
        }
        assert(hashtable[hashval] != 1);
        uint32_t roothash = hk(root->getItem());
        FreqItem *cur = counters + hashtable[roothash], *prev = nullptr;
        while (cur != counters) {
            if (cur == root) {
                if (prev == nullptr) {
                    hashtable[roothash] = root->getNext();
                } else {
                    prev->setNext(root->getNext());
                }
                break;
            }
            prev = cur;
            cur = (counters + cur->getNext());
        }
        assert(hashtable[hashval] != 1);
        FreqItem *tt = counters + hashtable[hashval];
        root->setNext(hashtable[hashval]);
        uint32_t hk1 = hk((counters + hashtable[hashval])->getItem()), hk2 = hk(counters[1].getItem());
        assert(hashtable[hashval] != 1 || (counters + hashtable[hashval])->getNext() != 1);
        hashtable[hashval] = (root - counters);

        ret = root->getItem();
        root->setitem(item);
        root->setDelta(root->getCount());
        root->setCount(value + root->getDelta());
#if PRINT_TRACE
        std::cout << ret << " " << item << std::endl;
        print();
#endif
        Heapify(1);
#if PRINT_TRACE
        print();
#endif
        return ret;
    }

    FreqItem *find(uint32_t item) {
        FreqItem *hashptr = counters + hashtable[hk(item)];
        while (hashptr) {
            if (hashptr->getItem() == item) break;
            else hashptr = (counters + hashptr->getNext());
        }
        return hashptr;
    }

    void print() {
        for (int i = 0; i <= _size; i++)
            std::cout << "\033[34m" << counters[i].getItem() << "\033[0m" << ":"
                      << "\033[33m" << hk(counters[i].getItem()) << "\033[0m" << ":"
                      << "\033[31m" << counters[i].getCount() << "\033[0m" << ":"
                      << "\033[32m" << counters[i].getDelta() << "\033[0m" << ":"
                      << "\033[31m" << counters[i].getNext() << "\033[0m" << ":"
                      << "\033[31m" << i << "\033[0m" << "->";
        std::cout << std::endl;
    }

    FreqItem *output(bool sorting = false) {
        if (sorting) std::sort(counters + 1, counters + _size, FreqItem::freqComp);
        return counters;
    }

    PartItem *prepare(int idx) {
        /*std::memcpy(merged, counters, sizeof(Item < IT > ) * (_size + 1));
        std::sort(merged + 1, merged + _size + 1, Item<IT>::comp);
        for (size_t i = 1; i <= _size; i++)
            merged[i].setDelta(idx);*/
        return merged;
    }

    PartItem *fetch() { return merged + 1; }

    static bool comp(PartItem *a, PartItem *b) { return a->getCount() > b->getCount(); }

    PartItem *merge(std::vector<ConciseLFUAlgorithm *> lss, bool overwrite = true) {
        std::memset(merged + _size + 1, 0, sizeof(PartItem) * _size);
        PartItem *final = merged + _size + 1;
        std::priority_queue<PartItem *, std::vector<PartItem *>, std::function<bool(PartItem *, PartItem *)>> pq(comp);
        size_t indicators[lss.size()];
        std::memset(indicators, 0, sizeof(size_t) * lss.size());
        for (int i = 0; i < lss.size(); i++) {
            pq.push(&lss.at(i)->fetch()[0]); // issue without idx
            indicators[i]++;
        }
        int capacity = 0, size = this->_size;
        while (capacity < this->_size) {
            PartItem *top = pq.top();
            FreqItem *org = lss.at(top->getPart())->find(top->getItem());
            pq.pop();
            if (org == nullptr) continue; // for substream whose items have been used up.
            if (org->getDelta() != -1) {
                final[capacity].setitem(top->getItem());
                final[capacity].setCount(top->getCount());
                for (int i = 0; i < lss.size(); i++) {
                    ConciseLFUAlgorithm *who = lss.at(i);
                    uint32_t value = top->getItem();
                    FreqItem *cur = who->find(value);
                    if (i != top->getPart() && cur != nullptr) {
                        final[capacity].chgCount(cur->getCount());
                        cur->setDelta(-1);
                    }
                }
                capacity++;
            }

            size_t idx = top->getPart();
            pq.push(&lss.at(idx)->fetch()[indicators[idx]]); // issue without idx
            indicators[idx]++;
        }
        return final;
    }

    /*Item <IT> *merge(GeneralReplacement &lss, bool overwrite = true) {
        assert(lss.volume() == _size);
        std::memcpy(merged, counters, sizeof(Item < IT > ) * (_size + 1));
        std::memset(merged + _size + 1, 0, sizeof(Item < IT > ) * _size);
        int idx = 1;
        for (; idx < _size + 1; idx++) {
            Item <IT> *target = lss.find(merged[idx].getItem());
            if (target) {
                merged[idx].setCount(merged[idx].getCount() + target->getCount());
            }
        }
        Item <IT> *targets = lss.output(true);
        // this->refresh(); // So far, I don't know why the refreshment is not necessary here.
        for (int i = 1; i < _size + 1; i++) {
            if (!this->find(targets[i].getItem())) {
                merged[idx].setitem(targets[i].getItem());
                merged[idx].setCount(targets[i].getCount());
                merged[idx].setDelta(targets[i].getDelta());
                idx++;
            }
        }
        std::sort(merged + 1, merged + idx, Item<IT>::comp);
        if (overwrite) {
            std::memcpy(counters, merged, sizeof(Item < IT > ) * (_size + 1));
            return counters;
        } else return merged;
    }

    Item <IT> *merge1(GeneralReplacement &lss, bool overwrite = false) {
        assert(lss.volume() == _size);
        Item <IT> *merged = counters;
        if (!overwrite) merged = new Item<IT>[_size];
        std::sort(counters + 1, counters + _size, Item<IT>::comp);
        for (int i = 0; i < _size; i++) {
            Item <IT> *cptr = lss.find(counters[i].getItem());
            if (cptr != nullptr) {
                merged[i].setitem(counters[i].getItem());
                merged[i].setCount(counters[i].getCount() + cptr->getCount());
                merged[i].setDelta(counters[i].getDelta() + cptr->getDelta());
            } else {
                merged[i].setitem(counters[i].getItem());
                merged[i].setCount(counters[i].getCount());
                merged[i].setDelta(counters[i].getDelta());
            }
        }
        return merged;
    }*/
};

#endif //HASHCOMP_CONCISELRU_H
