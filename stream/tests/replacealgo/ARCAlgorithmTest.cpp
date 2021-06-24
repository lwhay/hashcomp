//
// Created by Michael on 6/23/21.
//
#include <iostream>
#include "LRUAlgorithm.h"
#include "ARCAlgorithm.h"
#include "DefaultARCAlgorithm.h"

void testLRUWithBack(bool speical, int step, bool remove = true) {
    LRUAlgorithm<uint64_t> lru(10);
    for (int i = 0; i < 100; i++) {
        int k = i;
        if ((i + 1) % (step + 1) == 0) k -= step;
        lru.add(k);
        if (speical && (i + 1) % (step + 1) == 0) {
            if (i > 11 && remove) assert(lru.getSize() == 11);
            lru.removePage(k, remove);
            if (i > 14 && remove) assert(lru.getSize() == 10);
            if (remove) assert(!lru.contains(k)); else assert(lru.contains(k));
        }
        std::cout << lru.getSize() << "&" << k << "&";
        lru.print();
    }
    std::cout << lru.size() << ":" << lru.getSize() << ":" << lru.getCount() << ":" << lru.getSize() << std::endl;
    for (int i = 0; i < 129; i++) std::cout << "=";
    std::cout << std::endl;
}

int main(int argc, char **argv) {
    testLRUWithBack(false, 11);
    testLRUWithBack(false, 10);
    testLRUWithBack(true, 11, true);
    testLRUWithBack(true, 10, true);
    testLRUWithBack(true, 11, false);
    testLRUWithBack(true, 10, false);

    DefaultARCAlgorithm<uint64_t> arc(100);
    for (int i = 0; i < 10000; i++) arc.arc_lookup(i);
    std::cout << arc.getMiss() << ":" << arc.getHit() << std::endl;

    ARCAlgorithm<uint64_t> ar(100);
    for (int i = 0; i < 10000; i++) {
        ar.add(i);
    }
    std::cout << ar.getTotalMiss() << ":" << ar.getTotalRequest() - ar.getTotalMiss() << std::endl;
    return 0;
}