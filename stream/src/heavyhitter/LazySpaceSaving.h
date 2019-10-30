//
// Created by iclab on 10/29/19.
//

#ifndef HASHCOMP_LAZYSPACESAVING_H
#define HASHCOMP_LAZYSPACESAVING_H

#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>

constexpr uint32_t LCL_HASHMULT = 3;
constexpr uint32_t LCL_NULLITEM = 0x7ffffffff;

class Counter {
private:
    uint32_t item;
    int hash;
    int count;
    int delta;
    Counter *prev, *next;
public:
    void setitem(uint32_t item) { this->item = item; }

    uint32_t getItem() { return item; }

    void setHash(int hash) { this->hash = hash; }

    int getHash() { return hash; }

    void setCount(int count) { this->count = count; }

    void chgCount(int value) { this->count += value; }

    int getCount() { return count; }

    void setDelta(int delta) { this->delta = delta; }

    int getDelta() { return delta; }

    void setPrev(Counter *counter) { prev = counter; }

    Counter *getPrev() { return prev; }

    void setNext(Counter *counter) { next = counter; }

    Counter *getNext() { return next; }
};

/*constexpr int MOD = 2147483647;

constexpr int HL = 31;*/

class LazySpaceSaving {
private:
    int n;
    int hasha, hashb, hashsize;
    int _size;
    Counter *root;
    Counter *counters;
    Counter **hashtable;

    inline long hash31(int64_t a, int64_t b, int64_t x) {
        int64_t result;
        long lresult;
        result = (a * x) + b;
        result = ((result >> HL) + result) & MOD;
        lresult = (long) result;

        return (lresult);
    }

private:
    void Heapify(int ptr) {
        Counter tmp;
        Counter *cpt, *minchild;
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
                    if (cpt->getItem() != LCL_NULLITEM) hashtable[cpt->getHash()] = cpt;
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
    LazySpaceSaving(double fPhi) {
        int k = 1 + (int) 1.0 / fPhi;
        _size = (1 + k) | 1;
        hashsize = LCL_HASHMULT * _size;
        hashtable = (Counter **) new Counter *[hashsize];
        std::memset(hashtable, 0, sizeof(Counter *) * hashsize);
        counters = (Counter *) new Counter[_size];

        hasha = 151261303;
        hashb = 6722461;
        n = 0;

        for (int i = 0; i <= _size; i++) {
            counters[i].setNext(nullptr);
            counters[i].setPrev(nullptr);
            counters[i].setitem(LCL_NULLITEM);
        }

        root = &counters[1];
    }

    ~LazySpaceSaving() {
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
        return sizeof(LazySpaceSaving) + hashsize * sizeof(int) + _size * sizeof(Counter);
    }

    void put(int item, int value = 1) {
        int hashval;
        Counter *hashptr;

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

    Counter *find(int item) {
        int hashval = (int) hash31(hasha, hashb, item) % hashsize;
        Counter *hashptr = hashtable[hashval];
        while (hashptr) {
            if (hashptr->getItem() == item) break;
            else hashptr = hashptr->getNext();
        }
        return hashptr;
    }
};

#endif //HASHCOMP_LAZYSPACESAVING_H