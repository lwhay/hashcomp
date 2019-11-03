//
// Created by iclab on 10/29/19.
//

#ifndef HASHCOMP_GENERALLAZYSPACESAVING_H
#define HASHCOMP_GENERALLAZYSPACESAVING_H

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>

template<typename IT>
class Item {
private:
    IT item;
    int hash;
    int count;
    int delta;
    Item *prev, *next;
public:
    void setitem(IT item) { this->item = item; }

    IT getItem() { return item; }

    void setHash(int hash) { this->hash = hash; }

    int getHash() { return hash; }

    void setCount(int count) { this->count = count; }

    void chgCount(int value) { this->count += value; }

    int getCount() { return count; }

    void setDelta(int delta) { this->delta = delta; }

    int getDelta() { return delta; }

    void setPrev(Item *counter) { prev = counter; }

    Item *getPrev() { return prev; }

    void setNext(Item *counter) { next = counter; }

    Item *getNext() { return next; }

    static bool comp(Item<IT> &a, Item<IT> &b) {
        return a.getCount() > b.getCount();
    }
};

template<typename IT>
class GeneralLazySS {
    constexpr static uint32_t GLSS_HASHMULT = 3;
    constexpr static uint32_t GLSS_NULLITEM = 0x7FFFFFFF;
    constexpr static int GLSS_MOD = 2147483647;
    constexpr static int GLSS_HL = 31;
private:
    int n;
    int hasha, hashb, hashsize;
    int _size;
    Item<IT> *root;
    Item<IT> *counters;
    Item<IT> **hashtable;

    inline long hash31(int64_t a, int64_t b, int64_t x) {
        int64_t result;
        long lresult;
        result = (a * x) + b;
        result = ((result >> GLSS_HL) + result) & GLSS_MOD;
        lresult = (long) result;

        return (lresult);
    }

private:
    void Heapify(int ptr) {
        Item<IT> tmp;
        Item<IT> *cpt, *minchild;
        int mc;
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
    GeneralLazySS(double fPhi) {
        int k = 1 + (int) 1.0 / fPhi;
        _size = (1 + k) | 1;
        hashsize = GLSS_HASHMULT * _size;
        hashtable = (Item<IT> **) new Item<IT> *[hashsize];
        std::memset(hashtable, 0, sizeof(Item<IT> *) * hashsize);
        counters = (Item<IT> *) new Item<IT>[1 + _size];
        std::memset(counters, 0, sizeof(Item<IT>) * (1 + _size));

        hasha = 151261303;
        hashb = 6722461;
        n = 0;

        for (int i = 0; i <= _size; i++) {
            counters[i].setNext(nullptr);
            counters[i].setPrev(nullptr);
            counters[i].setitem(GLSS_NULLITEM);
        }

        root = &counters[1];
    }

    ~GeneralLazySS() {
        delete[] hashtable;
        delete[] counters;
    }

    int range() {
        return hashsize;
    }

    int volume() {
        return _size;
    }

    int size() {
        return sizeof(GeneralLazySS) + hashsize * sizeof(int) + _size * sizeof(Item<IT>);
    }

    void put(int item, int value = 1) {
        int hashval;
        Item<IT> *hashptr;

        n += value;
        counters->setitem(0);
        hashval = (int) hash31(hasha, hashb, item) % hashsize;
        hashptr = hashtable[hashval];

        while (hashptr) {
            if (hashptr->getItem() == item) {
                hashptr->chgCount(value);
                Heapify(hashptr - counters);
                return;
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
        root->setitem(item);
        root->setHash(hashval);
        root->setDelta(root->getCount());
        root->setCount(value + root->getDelta());
        Heapify(1);
    }

    Item<IT> *find(int item) {
        int hashval = (int) hash31(hasha, hashb, item) % hashsize;
        Item<IT> *hashptr = hashtable[hashval];
        while (hashptr) {
            if (hashptr->getItem() == item) break;
            else hashptr = hashptr->getNext();
        }
        return hashptr;
    }

    Item<IT> *output(bool sorting = false) {
        if (sorting) std::sort(counters + 1, counters + _size, Item<IT>::comp);
        return counters;
    }

    Item<IT> *merge(GeneralLazySS &lss, bool overwrite = false) {
        assert(lss.volume() == _size);
        Item<IT> *merged = counters;
        if (!overwrite) merged = new Item<IT>[_size];
        std::sort(counters + 1, counters + _size, Item<IT>::comp);
        for (int i = 0; i < _size; i++) {
            Item<IT> *cptr = lss.find(counters[i].getItem());
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

#endif //HASHCOMP_GENERALLAZYSPACESAVING_H
