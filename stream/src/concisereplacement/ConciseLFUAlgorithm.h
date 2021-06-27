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
    void setitem(uint32_t item) { this->item = item; }

    uint32_t getItem() { return item; }

    void setCount(int count) {
        deltaCount &= COUNT_MASK;
        if (count < 0x100000) deltaCount |= (count << 12); else deltaCount |= 0xfffff000;
    }

    void chgCount(int value) {
        uint32_t oldCount = (deltaCount >> 12);
        deltaCount &= COUNT_MASK;
        if (value + oldCount < 0x100000) deltaCount |= ((value + oldCount) << 12); else deltaCount |= 0xfffff000;
    }

    int getCount() { return (deltaCount >> 12); }

    void setDelta(int delta) {
        deltaCount &= DELTA_MASK;
        if (deltaCount < 0x1000) deltaCount |= delta; else deltaCount |= 0xfff;
    }

    int getDelta() { return delta; }

    void setLeft(int offset) {
        points &= (LEFT_MASK);
        points |= ((uint64_t) offset << 20);
    }

    int getLeft() { return ((points & LEFT_BITS) >> 20); }

    void setRight(int offset) {
        points &= (RIGHT_MASK);
        points |= ((uint64_t) offset << 40);
    }

    int getRight() { return ((points & RIGHT_BITS) >> 40); }

    void setNext(int offset) {
        points &= (NEXT_MASK);
        points |= offset;
    }

    int getNext() { return (points & NEXT_BITS); }

    static bool freqComp(FreqItem<IT> &a, FreqItem<IT> &b) {
        return a.getCount() > b.getCount();
    }

    static bool freqPrec(FreqItem<IT> &a, FreqItem<IT> &b) {
        return a.getItem() < b.getItem();
    }

    bool operator==(FreqItem<IT> &target) const {
        return item == target.getItem();
    }
};

template<typename IT>
class PartItem {
private:
    IT item;
    int count;
    int part;
public:
    void setitem(IT item) { this->item = item; }

    IT getItem() { return item; }

    void setCount(int count) { this->count = count; }

    void chgCount(int value) { this->count += value; }

    int getCount() { return count; }

    void setPart(int part) { this->part = part; }

    int getPart() { return part; }

    static bool partComp(PartItem<IT> &a, PartItem<IT> &b) {
        return a.getCount() > b.getCount();
    }

    static bool partPrec(PartItem<IT> &a, PartItem<IT> &b) {
        return a.getItem() < b.getItem();
    }

    bool operator==(PartItem<IT> &target) const {
        return item == target.getItem();
    }
};

template<typename IT>
class GeneralReplacement {
protected:
    constexpr static uint32_t GLSS_HASHMULT = 3;
    constexpr static IT GLSS_NULLITEM = std::numeric_limits<IT>::max();
    constexpr static IT GLSS_MOD = std::numeric_limits<IT>::max() / 2;
    constexpr static int GLSS_HL = 8 * sizeof(IT) - 1;
    int n;
    IT hasha, hashb, hashsize;
    int _size;
    int *root;
    FreqItem<IT> *counters;
    FreqItem<IT> *merged;
    int *hashtable;

    inline IT hash(IT a, IT b, IT x) {
        IT result;
        IT lresult;
        result = (a * x) + b;
        result = ((result >> GLSS_HL) + result) & GLSS_MOD;
        lresult = result;

        return (lresult);
    }

protected:
    inline void Heapify(IT ptr) {
        Item <IT> tmp;
        Item <IT> *cpt, *minchild;
        IT mc;
        while (1) {
            if ((ptr << 1) + 1 > _size) break;

            cpt = &counters[ptr];
            mc = (ptr << 1) + ((counters[ptr << 1].getCount() < counters[(ptr << 1) + 1].getCount()) ? 0 : 1);
            minchild = &counters[mc];

            if (cpt->getCount() < minchild->getCount()) break;
            tmp = *cpt;
            *cpt = *minchild;
            *minchild = tmp;

            if (cpt->getHash() == minchild->getHash()) {
                minchild->setPrev(cpt->getPrev());
                cpt->setPrev(tmp.getPrev());
                minchild->setNext(cpt->getNext());
                cpt->setNext(tmp.getNext());
            } else {
                if (!cpt->getPrev()) {
                    if (cpt->getItem() != GLSS_NULLITEM) hashtable[cpt->getHash()] = cpt;
                } else cpt->getPrev()->setNext(cpt);

                if (cpt->getNext()) cpt->getNext()->setPrev(cpt);

                if (!minchild->getPrev()) hashtable[minchild->getHash()] = minchild;
                else minchild->getPrev()->setNext(minchild);

                if (minchild->getNext()) minchild->getNext()->setPrev(minchild);
            }
            ptr = mc;
        }
    }

public:
    GeneralReplacement(int K) {
        int k = K;
        _size = (1 + k) | 1;
        hashsize = GLSS_HASHMULT * _size;
        hashtable = (Item < IT > **)
        new Item <IT> *[hashsize];
        std::memset(hashtable, 0, sizeof(Item < IT > *) * hashsize);
        counters = (Item < IT > *)
        new Item<IT>[1 + _size];
        merged = new Item<IT>[2 * _size + 1];
        std::memset(counters, 0, sizeof(Item < IT > ) * (1 + _size));

        /*srand(time(NULL));
        hasha = (IT) lrand48();
        hashb = (IT) lrand48();*/
        hasha = (IT) (151261303 % std::numeric_limits<IT>::max());
        hashb = (IT) (6722461 % std::numeric_limits<IT>::max());
        n = 0;

        for (int i = 0; i <= _size; i++) {
            counters[i].setNext(nullptr);
            counters[i].setPrev(nullptr);
            counters[i].setitem(GLSS_NULLITEM);
        }

        root = &counters[1];
    }

    void reset() {
        std::memset(hashtable, 0, sizeof(Item < IT > *) * hashsize);
        std::memset(counters, 0, sizeof(Item < IT > ) * (1 + _size));
        n = 0;

        for (int i = 0; i <= _size; i++) {
            counters[i].setNext(nullptr);
            counters[i].setPrev(nullptr);
            counters[i].setitem(GLSS_NULLITEM);
        }
        root = &counters[1];
    }

    ~GeneralReplacement() {
        // std::cout << "clean" << std::endl;
        delete[] hashtable;
        delete[] counters;
        delete[] merged;
    }

    int range() {
        return hashsize;
    }

    int volume() {
        return _size;
    }

    int size() {
        return sizeof(GeneralReplacement) + hashsize * sizeof(IT) + _size * sizeof(Item < IT > );
    }

    inline IT put(IT item, int value = 1) {
        IT hashval;
        Item <IT> *hashptr;

        IT ret = item;
        n += value;
        counters->setitem(0);
        hashval = hash(hasha, hashb, item) % hashsize;
        hashptr = hashtable[hashval];

        while (hashptr) {
            if (hashptr->getItem() == item) {
                hashptr->chgCount(value);
                Heapify(hashptr - counters);
                return ret;
            } else hashptr = hashptr->getNext();
        }

        if (!root->getPrev()) hashtable[root->getHash()] = root->getNext();
        else root->getPrev()->setNext(root->getNext());

        if (root->getNext()) root->getNext()->setPrev(root->getPrev());

        hashptr = hashtable[hashval];
        root->setNext(hashptr);
        if (hashptr) hashptr->setPrev(root);
        hashtable[hashval] = root;

        root->setPrev(nullptr);
        ret = root->getItem();
        root->setitem(item);
        root->setHash(hashval);
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

    Item <IT> *find(IT item) {
        IT hashval = hash(hasha, hashb, item) % hashsize;
        Item <IT> *hashptr = hashtable[hashval];
        while (hashptr) {
            if (hashptr->getItem() == item) break;
            else hashptr = hashptr->getNext();
        }
        return hashptr;
    }

    void print() {
        for (int i = 0; i <= _size; i++)
            std::cout << "\033[34m" << (((counters[i].getItem() + 1) & 0x7fffffff) - 1) << "\033[0m" << ":"
                      << "\033[33m" << hash(hasha, hashb, counters[i].getItem()) % hashsize << "\033[0m" << ":"
                      << "\033[31m" << counters[i].getCount() << "\033[0m" << ":"
                      << "\033[32m" << counters[i].getDelta() << "\033[0m" << "->";
        std::cout << std::endl;
    }

    Item <IT> *output(bool sorting = false) {
        if (sorting) std::sort(counters + 1, counters + _size, Item<IT>::comp);
        return counters;
    }

    Item <IT> *prepare(int idx) {
        std::memcpy(merged, counters, sizeof(Item < IT > ) * (_size + 1));
        std::sort(merged + 1, merged + _size + 1, Item<IT>::comp);
        for (size_t i = 1; i <= _size; i++)
            merged[i].setDelta(idx);
        return merged;
    }

    /*void finish() {
        std::memcpy(counters + 1, merged + _size + 1, sizeof(Item<IT>) * (_size));
    }*/

    Item <IT> *fetch() { return merged + 1; }

    void refresh() {
#if verifyRefresh
        for (int i = 1; i < _size + 1; i++) {
            if (!find(counters[i].getItem()))
                std::cout << "***\t\t" << i << " " << counters[i].getItem() << " " << counters[i].getCount() << "<->"
                          << std::endl;
            else if (counters[i].getItem() == 72183 || i % 16384 == 0)
                std::cout << "---\t\t" << i << " " << counters[i].getItem() << " " << counters[i].getCount() << "<->"
                          << find(merged[i].getItem())->getCount() << std::endl;
        }
#endif
        std::memcpy(merged, counters, sizeof(Item < IT > ) * (_size + 1));
        //std::sort(merged + 1, merged + _size + 1, Item<IT>::comp);
        std::memset(counters, 0, sizeof(Item < IT > ) * (_size + 1));
        std::memset(hashtable, 0, sizeof(Item < IT > *) * hashsize);
        n = 0;
        for (int i = 0; i <= _size; i++) {
            counters[i].setitem(GLSS_NULLITEM);
        }
        root = &counters[1];
        for (int i = 1; i < _size + 1; i++) put(merged[i].getItem(), merged[i].getCount());
        for (int i = 1; i < _size + 1; i++) {
            Item <IT> *ptr = find(merged[i].getItem());
            if (ptr) ptr->setDelta(merged[i].getDelta());
        }
#if verifyRefresh
        for (int i = 1; i < _size + 1; i++) {
            if (!find(merged[i].getItem()))
                std::cout << "***\t\t" << i << " " << merged[i].getItem() << " " << merged[i].getCount() << "<->"
                          << std::endl;
            else if (merged[i].getItem() == 72183 || i % 16384 == 0)
                std::cout << "---\t\t" << i << " " << merged[i].getItem() << " " << merged[i].getCount() << "<->"
                          << find(merged[i].getItem())->getCount() << std::endl;
        }
#endif
    }

    static bool comp(Item <IT> *a, Item <IT> *b) { return a->getCount() > b->getCount(); }

    Item <IT> *merge(std::vector<GeneralReplacement<IT> *> lss, bool overwrite = true) {
        std::memset(merged + _size + 1, 0, sizeof(Item < IT > ) * _size);
        Item <IT> *final = merged + _size + 1;
        std::priority_queue<Item < IT> *, std::vector<Item < IT> *>, std::function<bool(Item < IT > *, Item < IT > *)>>
        pq(comp);
        size_t indicators[lss.size()];
        std::memset(indicators, 0, sizeof(size_t) * lss.size());
        for (int i = 0; i < lss.size(); i++) {
            pq.push(&lss.at(i)->fetch()[0]); // issue without idx
            indicators[i]++;
        }
        int capacity = 0, size = this->_size;
        while (capacity < this->_size) {
            Item <IT> *top = pq.top();
            Item <IT> *org = lss.at(top->getDelta())->find(top->getItem());
            pq.pop();
            if (org == nullptr) continue; // for substream whose items have been used up.
            if (org->getDelta() != -1) {
                final[capacity].setitem(top->getItem());
                final[capacity].setCount(top->getCount());
                for (int i = 0; i < lss.size(); i++) {
                    GeneralReplacement<IT> *who = lss.at(i);
                    IT value = top->getItem();
                    Item <IT> *cur = who->find(value);
                    if (i != top->getDelta() && cur != nullptr) {
                        final[capacity].chgCount(cur->getCount());
                        cur->setDelta(-1);
                    }
                }
                capacity++;
            }

            size_t idx = top->getDelta();
            pq.push(&lss.at(idx)->fetch()[indicators[idx]]); // issue without idx
            indicators[idx]++;
        }
        return final;
    }

    Item <IT> *merge(GeneralReplacement &lss, bool overwrite = true) {
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
    }
};

#endif //HASHCOMP_CONCISELRU_H
