//
// Created by Michael on 6/23/21.
//
#include <iostream>
#include "LRUAlgorithm.h"
#include "ARCAlgorithm.h"
#include "DefaultARCAlgorithm.h"

int main(int argc, char **argv) {
    LRUAlgorithm<uint64_t> lru(10);
    for (int i = 0; i < 100; i++) {
        int k = i;
        if ((i + 1) % 11 == 0) k -= 10;
        lru.add(k);
        //lru.removePage(i);
        std::cout << k << "&";
        lru.print();
    }
    std::cout << lru.size() << ":" << lru.getSize() << ":" << lru.getCount() << std::endl;

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