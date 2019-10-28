//
// Created by Michael on 2019-10-15.
//

#ifndef HASHCOMP_FREQUENT_H
#define HASHCOMP_FREQUENT_H

#include <map>
#include "Structure.h"

class Frequent {
private:
    Node *head;
    Node *tail;
    std::multimap<int, Node *> fasttbl;
    int bsize;
public:
    Frequent(int size) : head(new Node(-1, -1)), tail(new Node(-1, -1)), bsize(size) {
        head->setNext(tail);
        tail->setPrev(head);
    }

    void add(int v) {
        auto cur = fasttbl.find(v);
        if (cur != fasttbl.end()) {
            cur->second++;
        }
    }
};

#endif //HASHCOMP_FREQUENT_H
