//
// Created by iclab on 6/18/21.
//

#ifndef HASHCOMP_LRUALGORITHM_H
#define HASHCOMP_LRUALGORITHM_H

#include "ReplacementAlgorithm.h"

/*class Parent {
protected:
    int id;
public:
    int getId() {return id;}
};

class Child: public Parent {
public:
    int getId() {return id;}
};*/

template<typename IT, int TYPE = 0>
class LRUAlgorithm : public GeneralReplacement<IT, TYPE> {
protected:
    Item<IT> *head, *tail;

public:
    LRUAlgorithm(int K) : GeneralReplacement<IT, TYPE>(K), head(nullptr), tail(nullptr) {}

    ~LRUAlgorithm() {}

    inline IT put(IT item, int value = 1) {
        IT hashval;
        Item<IT> *hashptr;

        IT ret = item;
        this->n += value;
        this->counters->setitem(0);
        hashval = this->hash(this->hasha, this->hashb, item) % this->hashsize;
        hashptr = this->hashtable[hashval];

        while (hashptr) {
            if (hashptr->getItem() == item) {
                hashptr->chgCount(value);
                this->Heapify(hashptr - this->counters);
                return ret;
            } else hashptr = hashptr->getNext();
        }

        this->counters[hashval].setFoll(head);
        head = &this->counters[hashval];
        if (nullptr != head->getFoll()) head->getFoll()->setPred(head);

        if (!this->root->getPrev()) this->hashtable[this->root->getHash()] = this->root->getNext();
        else this->root->getPrev()->setNext(this->root->getNext());

        if (this->root->getNext()) this->root->getNext()->setPrev(this->root->getPrev());

        hashptr = this->hashtable[hashval];
        this->root->setNext(hashptr);
        if (hashptr) hashptr->setPrev(this->root);
        this->hashtable[hashval] = this->root;

        this->root->setPrev(nullptr);
        ret = this->root->getItem();
        this->root->setitem(item);
        this->root->setHash(hashval);
        if (TYPE == 0) {
            this->root->setDelta(this->root->getCount());
            this->root->setCount(value + this->root->getDelta());
        }
        this->Heapify(1);
        return ret;
    }
};

#endif //HASHCOMP_LRUALGORITHM_H
