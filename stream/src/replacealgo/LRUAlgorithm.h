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

template<typename IT>
class LRUAlgorithm : public GeneralReplacement<IT> {
protected:
    Item<IT> *head, *tail;
    size_t count, capacity;

public:
    LRUAlgorithm(int K) : GeneralReplacement<IT>(K), head(&this->counters[1]), tail(&this->counters[1]), count(0),
                          capacity(0) {
        for (int i = 1; i <= this->_size; i++) {
            this->counters[i].setRight(&this->counters[i % this->_size + 1]);
            this->counters[i].setLeft((i == 1) ? &this->counters[this->_size] : &this->counters[i - 1]);
        }
    }

    ~LRUAlgorithm() {}

    void reset() {
        GeneralReplacement<IT>::reset();
        head = &this->counters[1];
        tail = &this->counters[1];
        // count = 0;  // accumulate it
        capacity = 0;
        for (int i = 1; i <= this->_size; i++) {
            this->counters[i].setRight(&this->counters[i % this->_size + 1]);
            this->counters[i].setLeft((i == 1) ? &this->counters[this->_size] : &this->counters[i - 1]);
        }
    }

    void verify() {
        for (int i = 0; i < this->hashsize; i++)
            assert(this->hashtable[i] == nullptr || this->hashtable[i]->getPrev() == nullptr);
    }

    inline IT put(IT item, int value = 1) {
        IT hashval;
        Item<IT> *hashptr;

        IT ret = item;
        this->n += value;
        this->counters->setitem(0);
        hashval = this->hash(this->hasha, this->hashb, item) % this->hashsize;
        hashptr = this->hashtable[hashval];

        IT change = std::numeric_limits<int>::min();
        while (hashptr) {
            count++;
            if (hashptr->getItem() == item) {
                if (head != hashptr) {
                    if (tail == hashptr) tail = tail->getRight();
                    // pick out hashptr
                    hashptr->getLeft()->setRight(hashptr->getRight());
                    hashptr->getRight()->setLeft(hashptr->getLeft());
                    // replace tail by hashptr
                    hashptr->setRight(head);
                    hashptr->setLeft(head->getLeft());
                    head->getLeft()->setRight(hashptr);
                    head->setLeft(hashptr);
                    // std::cout << "--------------------------------------" << std::endl;
                } else if (tail == hashptr) {
                    tail = tail->getRight();
                    head = tail;
                }
                return ret;
            } else hashptr = hashptr->getNext();
        }
        if (this->n > value && head == tail) {
            tail = tail->getRight();
            change = tail->getItem();
        }
        ret = tail->getItem();
        IT hashret = this->hash(this->hasha, this->hashb, ret) % this->hashsize;
        Item<IT> *cur = this->hashtable[hashret];
        bool asroot = true;
        if (change != std::numeric_limits<int>::min()) {
            while (cur != nullptr) {
                count++;
                if (cur == tail) {
                    this->capacity--;
                    if (asroot) {
                        if (tail->getNext() != nullptr) tail->getNext()->setPrev(nullptr);
                        this->hashtable[hashret] = tail->getNext();
                    } else {
                        if (tail->getNext() != nullptr) tail->getNext()->setPrev(tail->getPrev());
                        tail->getPrev()->setNext(tail->getNext());
                    }
                    break;
                } else asroot = false;
                cur = cur->getNext();
            }
        }

        if (this->hashtable[hashval] != nullptr) {
            head->setNext(this->hashtable[hashval]->getNext());
            this->hashtable[hashval]->setPrev(head);
        } else head->setNext(nullptr);
        head->setPrev(nullptr);
        this->hashtable[hashval] = head;
        head->setitem(item);
        head = head->getRight();
        capacity++;

        return ret;
    }

    size_t getCount() { return count; }

    void print() {
        Item<IT> *cur = head;
        using namespace std;
        char *normal = "\033[0m";
        int retrieved = 0;
        cout << (head - this->counters) << "-" << (tail - this->counters) << "&";
        do {
            cout << "\033[34m" << (((cur->getItem() + 1) & 0x7fffffff) - 1) << normal << ":"
                 << "\033[33m" << this->hash(this->hasha, this->hashb, cur->getItem()) % this->hashsize << normal << ":"
                 << "\033[31m" << cur->getCount() << normal << ":"
                 << "\033[32m" << cur->getDelta() << normal << "->";
            cur = cur->getRight();
            if (retrieved++ > this->_size) exit(-1);
        } while (cur != head);
        std::cout << std::endl;
    }

    bool contains(IT k) { return this->find(k) != nullptr; }

    void add(IT k) { this->put(k); }

    IT moveToBack(IT item) { removePage(item, false); }

    size_t getSize() { return this->capacity; }

    IT removePage(IT item, bool remove = true) {
        IT hashval;
        Item<IT> *hashptr;
        IT ret = (IT) -1;
        this->counters->setitem(0);
        hashval = this->hash(this->hasha, this->hashb, item) % this->hashsize;
        hashptr = this->hashtable[hashval];

        bool asroot = true;
        while (hashptr) {
            count++;
            if (hashptr->getItem() == item) {
                if (head != hashptr) {
                    // pick out hashptr
                    hashptr->getLeft()->setRight(hashptr->getRight());
                    hashptr->getRight()->setLeft(hashptr->getLeft());
                    // replace tail by hashptr
                    hashptr->setRight(head);
                    hashptr->setLeft(head->getLeft());
                    head->getLeft()->setRight(hashptr);
                    head->setLeft(hashptr);
                    // std::cout << "--------------------------------------" << std::endl;
                } else if (tail == hashptr) {
                    tail = tail->getRight();
                    head = tail;
                }
                if (remove) {
                    this->capacity--;
                    if (asroot) {
                        if (hashptr->getNext() != nullptr) hashptr->getNext()->setPrev(nullptr);
                        this->hashtable[hashval] = hashptr->getNext();
                    } else {
                        if (hashptr->getNext() != nullptr) hashptr->getNext()->setPrev(hashptr->getPrev());
                        hashptr->getPrev()->setNext(hashptr->getNext());
                    }
                }
                ret = item;
                break;
            } else hashptr = hashptr->getNext();
            asroot = false;
        }
        return ret;
    }
};

#endif //HASHCOMP_LRUALGORITHM_H
