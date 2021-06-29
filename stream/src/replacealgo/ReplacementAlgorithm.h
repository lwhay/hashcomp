//
// Created by iclab on 10/29/19.
//
#ifndef HASHCOMP_REPLACEMENTALGORITHM_H
#define HASHCOMP_REPLACEMENTALGORITHM_H

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

template<typename IT>
class Item {
private:
    IT item;
    IT hash;
    int count;
    int delta;
    Item *prev, *next, *left, *right;
public:
    void setitem(IT item) { this->item = item; }

    IT getItem() { return item; }

    void setHash(IT hash) { this->hash = hash; }

    IT getHash() { return hash; }

    void setCount(int count) { this->count = count; }

    void chgCount(int value) { this->count += value; }

    int getCount() { return count; }

    void setDelta(int delta) { this->delta = delta; }

    int getDelta() { return delta; }

    void setPrev(Item *counter) { prev = counter; }

    void setLeft(Item *counter) { left = counter; }

    Item *getPrev() { return prev; }

    Item *getLeft() { return left; }

    void setNext(Item *counter) { next = counter; }

    void setRight(Item *counter) { right = counter; }

    Item *getNext() { return next; }

    Item *getRight() { return right; }

    static bool comp(Item<IT> &a, Item<IT> &b) {
        return a.getCount() > b.getCount();
    }

    static bool prec(Item<IT> &a, Item<IT> &b) {
        return a.getItem() < b.getItem();
    }

    bool operator==(Item<IT> &target) const {
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
    Item<IT> *root;
    Item<IT> *counters;
    Item<IT> *merged;
    Item<IT> **hashtable;

    inline IT hash(IT a, IT b, IT x) {
        /*IT result;
        IT lresult;
        result = (a * x) + b;
        result = ((result >> GLSS_HL) + result) & GLSS_MOD;
        lresult = result;

        return (lresult);*/
        return (x % (hashsize - 1) + 1);
    }

protected:
    inline void Heapify(IT ptr) {
        Item<IT> tmp;
        Item<IT> *cpt, *minchild;
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
            /*int t = 0;
                for (int i = 0; i < hashsize; i++) if (hashtable[i] == root) t++;
                assert((t <= 1));*/
        }
        /*for (int i = 0; i < hashsize; i++)
            if (hashtable[i] == root) assert(root->getHash() == i);*/
    }

public:
    GeneralReplacement(int K) {
        int k = K;
        _size = (1 + k) | 1;
        hashsize = GLSS_HASHMULT * _size;
        hashtable = (Item<IT> **) new Item<IT> *[hashsize];
        std::memset(hashtable, 0, sizeof(Item<IT> *) * hashsize);
        counters = (Item<IT> *) new Item<IT>[1 + _size];
        merged = new Item<IT>[2 * _size + 1];
        std::memset(counters, 0, sizeof(Item<IT>) * (1 + _size));

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
        std::memset(hashtable, 0, sizeof(Item<IT> *) * hashsize);
        std::memset(counters, 0, sizeof(Item<IT>) * (1 + _size));
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
        return sizeof(GeneralReplacement) + hashsize * sizeof(IT) + _size * sizeof(Item<IT>);
    }

    inline IT put(IT item, int value = 1) {
        IT hashval;
        Item<IT> *hashptr;

        // assert((root - counters) == 1 && root->getNext() != root);
        IT ret = item;
        n += value;
        counters->setitem(0);
        hashval = hash(hasha, hashb, item) % hashsize;
        hashptr = hashtable[hashval];
        // assert(hashtable[hashval] != root); // might be
        while (hashptr) {
            if (hashptr->getItem() == item) {
                hashptr->chgCount(value);
                Heapify(hashptr - counters);
                return ret;
            } else hashptr = hashptr->getNext();
        }
        // assert(hashtable[hashval] != root); // might be
        // assert(root->getNext() != root);
        if (!root->getPrev()) hashtable[root->getHash()] = root->getNext();
        else root->getPrev()->setNext(root->getNext());

        if (root->getNext()) root->getNext()->setPrev(root->getPrev());

        // assert(hashtable[hashval] != root); // must not
        hashptr = hashtable[hashval];
        root->setNext(hashptr);
        /*assert(hashptr != root);
        for (int i = 1; i <= _size; i++) {
            if ((counters[i]).getNext() == &counters[i]) {
                std::cout << i << ":" << i << counters[i].getItem() << std::endl;
            }
            assert((counters[i]).getNext() != &counters[i]);
        }*/
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
        /*if (hashtable[hashval] == root)
            assert(root->getHash() == hashval);
        for (int i = 0; i < hashsize; i++)
            if (hashtable[i]) {
                assert(!(hashtable[i] == root && hashtable[i]->getNext() == root));
                //if (hashtable[i] == root) std::cout << hashtable[i]->getNext() << ":" << root << std::endl;
                assert(hashtable[i]->getNext() != hashtable[i]);
            }*/
        return ret;
    }

    Item<IT> *find(IT item) {
        IT hashval = hash(hasha, hashb, item) % hashsize;
        Item<IT> *hashptr = hashtable[hashval];
        while (hashptr) {
            if (hashptr->getItem() == item) break;
            else hashptr = hashptr->getNext();
        }
        return hashptr;
    }

    void print() {
        for (int i = 0; i <= _size; i++)
            std::cout << "\033[34m" << counters[i].getItem() << "\033[0m" << ":"
                      << "\033[33m" << hash(hasha, hashb, counters[i].getItem()) % hashsize << "\033[0m" << ":"
                      << "\033[31m" << counters[i].getCount() << "\033[0m" << ":"
                      << "\033[32m" << counters[i].getDelta() << "\033[0m" << "->";
        std::cout << std::endl;
    }

    Item<IT> *output(bool sorting = false) {
        if (sorting) std::sort(counters + 1, counters + _size, Item<IT>::comp);
        return counters;
    }

    Item<IT> *prepare(int idx) {
        std::memcpy(merged, counters, sizeof(Item<IT>) * (_size + 1));
        std::sort(merged + 1, merged + _size + 1, Item<IT>::comp);
        for (size_t i = 1; i <= _size; i++)
            merged[i].setDelta(idx);
        return merged;
    }

    /*void finish() {
        std::memcpy(counters + 1, merged + _size + 1, sizeof(Item<IT>) * (_size));
    }*/

    Item<IT> *fetch() { return merged + 1; }

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
        std::memcpy(merged, counters, sizeof(Item<IT>) * (_size + 1));
        //std::sort(merged + 1, merged + _size + 1, Item<IT>::comp);
        std::memset(counters, 0, sizeof(Item<IT>) * (_size + 1));
        std::memset(hashtable, 0, sizeof(Item<IT> *) * hashsize);
        n = 0;
        for (int i = 0; i <= _size; i++) {
            counters[i].setitem(GLSS_NULLITEM);
        }
        root = &counters[1];
        for (int i = 1; i < _size + 1; i++) put(merged[i].getItem(), merged[i].getCount());
        for (int i = 1; i < _size + 1; i++) {
            Item<IT> *ptr = find(merged[i].getItem());
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

    static bool comp(Item<IT> *a, Item<IT> *b) { return a->getCount() > b->getCount(); }

    Item<IT> *merge(std::vector<GeneralReplacement<IT> *> lss, bool overwrite = true) {
        std::memset(merged + _size + 1, 0, sizeof(Item<IT>) * _size);
        Item<IT> *final = merged + _size + 1;
        std::priority_queue<Item<IT> *, std::vector<Item<IT> *>, std::function<bool(Item<IT> *, Item<IT> *)>> pq(comp);
        size_t indicators[lss.size()];
        std::memset(indicators, 0, sizeof(size_t) * lss.size());
        for (int i = 0; i < lss.size(); i++) {
            pq.push(&lss.at(i)->fetch()[0]); // issue without idx
            indicators[i]++;
        }
        int capacity = 0, size = this->_size;
        while (capacity < this->_size) {
            Item<IT> *top = pq.top();
            Item<IT> *org = lss.at(top->getDelta())->find(top->getItem());
            pq.pop();
            if (org == nullptr) continue; // for substream whose items have been used up.
            if (org->getDelta() != -1) {
                final[capacity].setitem(top->getItem());
                final[capacity].setCount(top->getCount());
                for (int i = 0; i < lss.size(); i++) {
                    GeneralReplacement<IT> *who = lss.at(i);
                    IT value = top->getItem();
                    Item<IT> *cur = who->find(value);
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

    Item<IT> *merge(GeneralReplacement &lss, bool overwrite = true) {
        assert(lss.volume() == _size);
        std::memcpy(merged, counters, sizeof(Item<IT>) * (_size + 1));
        std::memset(merged + _size + 1, 0, sizeof(Item<IT>) * _size);
        int idx = 1;
        for (; idx < _size + 1; idx++) {
            Item<IT> *target = lss.find(merged[idx].getItem());
            if (target) {
                merged[idx].setCount(merged[idx].getCount() + target->getCount());
            }
        }
        Item<IT> *targets = lss.output(true);
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
            std::memcpy(counters, merged, sizeof(Item<IT>) * (_size + 1));
            return counters;
        } else return merged;
    }

    Item<IT> *merge1(GeneralReplacement &lss, bool overwrite = false) {
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

#endif //HASHCOMP_REPLACEMENTALGORITHM_H
