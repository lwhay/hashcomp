/**
 * C++ implementation of lock-free chromatic tree using LLX/SCX and DEBRA(+).
 * 
 * Copyright (C) 2016 Trevor Brown
 * Contact (tabrown [at] cs [dot] toronto [dot edu]) with any questions or comments.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef NODE_H
#define NODE_H

#include <iostream>
#include <iomanip>
#include <atomic>
#include <set>
#include "scxrecord.h"

using namespace std;

template<class K, class V>
class Node {
public:
    int weight;
    V value;
    K key;
    atomic_uintptr_t scxRecord;
    atomic_bool marked; // might be able to combine this elegantly with scx record pointer... (maybe we can piggyback on the version number mechanism, using the same bit to indicate ver# OR marked)
    atomic_uintptr_t left;
    atomic_uintptr_t right;

    Node() {
        // left blank for efficiency with custom allocator
    }

    Node(const Node &node) {
        // left blank for efficiency with custom allocator
    }

    K getKey() { return key; }

    V getValue() { return value; }

    friend ostream &operator<<(ostream &os, const Node<K, V> &obj) {
        ios::fmtflags f(os.flags());
        os << "[key=" << obj.key
           << " weight=" << obj.weight
           << " marked=" << obj.marked.load(memory_order_relaxed);
        os << " scxRecord@0x" << hex << (long) (obj.scxRecord.load(memory_order_relaxed));
//        os.flags(f);
        os << " left@0x" << hex << (long) (obj.left.load(memory_order_relaxed));
//        os.flags(f);
        os << " right@0x" << hex << (long) (obj.right.load(memory_order_relaxed));
//        os.flags(f);
        os << "]" << "@0x" << hex << (long) (&obj);
        os.flags(f);
        return os;
    }

    // somewhat slow version that detects cycles in the tree
    void printTreeFile(ostream &os, set<Node<K, V> *> *seen) {
//        os<<"(["<<key<<","<</*(long)(*this)<<","<<*/marked<<","<<scxRecord->state<<"],"<<weight<<",";
        os << "([" << key << "," << marked.load(memory_order_relaxed) << "],"
           << ((SCXRecord <K, V> *) scxRecord.load(memory_order_relaxed))->state.load(memory_order_relaxed) << ",";
        Node<K, V> *__left = (Node<K, V> *) left.load(memory_order_relaxed);
        Node<K, V> *__right = (Node<K, V> *) right.load(memory_order_relaxed);
        if (__left == NULL) {
            os << "-";
        } else if (seen->find(__left) != seen->end()) {   // for finding cycles
            os << "!"; // cycle!                          // for finding cycles
        } else {
            seen->insert(__left);
            __left->printTreeFile(os, seen);
        }
        os << ",";
        if (__right == NULL) {
            os << "-";
        } else if (seen->find(__right) != seen->end()) {  // for finding cycles
            os << "!"; // cycle!                          // for finding cycles
        } else {
            seen->insert(__right);
            __right->printTreeFile(os, seen);
        }
        os << ")";
    }

    void printTreeFile(ostream &os) {
        set<Node<K, V> *> seen;
        printTreeFile(os, &seen);
    }

    // somewhat slow version that detects cycles in the tree
    void printTreeFileWeight(ostream &os, set<Node<K, V> *> *seen) {
//        os<<"(["<<key<<","<</*(long)(*this)<<","<<*/marked<<","<<scxRecord->state<<"],"<<weight<<",";
        os << "([" << key << "]," << weight << ",";
        Node<K, V> *__left = (Node<K, V> *) left.load(memory_order_relaxed);
        Node<K, V> *__right = (Node<K, V> *) right.load(memory_order_relaxed);
        if (__left == NULL) {
            os << "-";
        } else if (seen->find(__left) != seen->end()) {   // for finding cycles
            os << "!"; // cycle!                          // for finding cycles
        } else {
            seen->insert(__left);
            __left->printTreeFileWeight(os, seen);
        }
        os << ",";
        if (__right == NULL) {
            os << "-";
        } else if (seen->find(__right) != seen->end()) {  // for finding cycles
            os << "!"; // cycle!                          // for finding cycles
        } else {
            seen->insert(__right);
            __right->printTreeFileWeight(os, seen);
        }
        os << ")";
    }

    void printTreeFileWeight(ostream &os) {
        set<Node<K, V> *> seen;
        printTreeFileWeight(os, &seen);
    }

};

#endif /* NODE_H */

