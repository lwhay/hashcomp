//
// Created by iclab on 6/22/21.
//

#ifndef HASHCOMP_RANDOMALGORITHM_H
#define HASHCOMP_RANDOMALGORITHM_H

#include "ReplacementAlgorithm.h"
#include <random>
#include <unordered_set>

template<typename IT>
class RandomAlgorithm : public GeneralReplacement<IT> {
private:
    size_t count, rep;

    std::random_device rd;

    std::mt19937 gen;

    std::uniform_int_distribution<int> dis;
public:

    RandomAlgorithm(int K) : GeneralReplacement<IT>(K), count(0), rep(-1), gen(std::mt19937(rd())),
                             dis(std::uniform_int_distribution<int>(1, this->_size)) {}

    ~RandomAlgorithm() {}

    void reset() {
        GeneralReplacement<IT>::reset();
        // count = 0; // accumulate it
        rep = -1;
    }

    inline IT put(IT item, int value = 1) {
        IT hashval;
        Item<IT> *hashptr;

        IT ret = item;
        this->n += value;
        this->counters->setitem(0);
        hashval = this->hash(this->hasha, this->hashb, item) % this->hashsize;
        hashptr = this->hashtable[hashval];

        // std::cerr << dis(gen) << std::endl;
        while (hashptr != nullptr) {
            count++;
            if (hashptr->getItem() == item) {
                return ret;
            } else hashptr = hashptr->getNext();
        }

        rep = dis(gen);
        Item<IT> *replace = &this->counters[rep];
        IT hashrep = this->hash(this->hasha, this->hashb, replace->getItem()) % this->hashsize;
        ret = replace->getItem();
        Item<IT> *cur = this->hashtable[hashrep];
        while (cur != nullptr) {
            count++;
            if (cur == replace) {
                if (cur == this->hashtable[hashrep]) {
                    if (cur->getNext() != nullptr) cur->getNext()->setPrev(nullptr);
                    this->hashtable[hashrep] = cur->getNext();
                } else {
                    if (cur->getNext() != nullptr) cur->getNext()->setPrev(cur->getPrev());
                    if (cur->getPrev() != nullptr) cur->getPrev()->setNext(cur->getNext());
                }
                break;
            } else cur = cur->getNext();
        }

        replace->setitem(item);
        replace->setPrev(nullptr);
        if (this->hashtable[hashval] != nullptr) {
            replace->setNext(hashptr);
            if (this->hashtable[hashval]->getNext() != nullptr) this->hashtable[hashval]->getNext()->setPrev(replace);
        } else {
            replace->setNext(nullptr);
        }
        this->hashtable[hashval] = replace;
        return ret;
    }

    size_t getCount() { return count; }

    void print() {
        std::cout << rep << "&";
        for (int i = 0; i <= this->_size; i++)
            std::cout << "\033[34m" << (((this->counters[i].getItem() + 1) & 0x7fffffff) - 1) << "\033[0m" << ":"
                      << "\033[33m"
                      << this->hash(this->hasha, this->hashb, this->counters[i].getItem()) % this->hashsize
                      << "\033[0m" << ":"
                      << "\033[31m" << this->counters[i].getCount() << "\033[0m" << ":"
                      << "\033[32m" << this->counters[i].getDelta() << "\033[0m" << "->";
        std::cout << std::endl;
    }
};

#endif //HASHCOMP_RANDOMALGORITHM_H
