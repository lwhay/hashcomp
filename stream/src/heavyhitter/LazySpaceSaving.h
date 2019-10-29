//
// Created by iclab on 10/29/19.
//

#ifndef HASHCOMP_LAZYSPACESAVING_H
#define HASHCOMP_LAZYSPACESAVING_H

#include <cmath>
#include <cstdint>
#include <random>

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

    int getCount() { return count; }

    void setDelta(int delta) { this->delta = delta; }

    int getDelta() { return delta; }

    void setPrev(Counter *counter) { prev = counter; }

    Counter *getPrev() { return prev; }

    void setNext(Counter *counter) { next = counter; }

    Counter *getNext() { return next; }
};

class LazySpaceSaving {
private:
    int n;
    int hasha, hashb, hashsize;
    int size;
    Counter *root;
    Counter *counters;
    Counter **hashtable;
private:

public:

};

#endif //HASHCOMP_LAZYSPACESAVING_H
