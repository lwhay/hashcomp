//
// Created by Michael on 6/23/21.
//
#include <unordered_set>
#include <iostream>
#include "LRUAlgorithm.h"
#include "ARCAlgorithm.h"
#include "DefaultARCAlgorithm.h"

int testlen = 32;

void testLRUWithBack(bool speical, int step, bool remove = true) {
    LRUAlgorithm<uint64_t> lru(10);
    std::unordered_set<uint64_t> hot;
    for (int i = 0; i < testlen; i++) {
        int k = i;
        if ((i + 1) % (step + 1) == 0) k -= step;
        uint64_t r = lru.add(k);
        hot.insert(k);
        if (k == 0)
            int gg = 0;
        if (r != k) hot.erase(r);
        int start = (i >= 11) ? (i - 11) : 0;
        for (int j = start; j <= i; j++)
            if (hot.find(j) != hot.end()) {
                if (!lru.contains(j)) {
                    std::cout << j << ":" << i << std::endl << lru.getSize() << ":";
                    lru.print();
                }
                assert(lru.contains(j));
            } else {
                //std::cout << j << ":" << i << std::endl;
                assert(!lru.contains(j));
            }
        if (speical && (i + 1) % (step + 1) == 0) {
            if (i > 11 && remove) assert(lru.getSize() == 11);
            lru.removePage(k, remove);
            if (remove) hot.erase(k);
            if (i > 14 && remove) assert(lru.getSize() == 10);
            if (remove) assert(!lru.contains(k)); else assert(lru.contains(k));
        }
        for (int j = start; j <= i; j++)
            if (hot.find(j) != hot.end())
                assert(lru.contains(j));
            else
                assert(!lru.contains(j));
        std::cout << lru.getSize() << "&" << k << "&";
        lru.print();
    }
    std::cout << lru.size() << ":" << lru.getSize() << ":" << lru.getCount() << ":" << lru.getSize() << std::endl;
    std::cout << speical << " " << step << " " << remove << " ";
    for (int i = 0; i < 129; i++) std::cout << "=";
    std::cout << std::endl;
}

int main(int argc, char **argv) {
    testLRUWithBack(false, 12);
    testLRUWithBack(false, 11);
    testLRUWithBack(false, 10);
    testLRUWithBack(true, 12, true);
    testLRUWithBack(true, 11, true);
    testLRUWithBack(true, 10, true);
    testLRUWithBack(true, 12, false);
    testLRUWithBack(true, 11, false);
    testLRUWithBack(true, 10, false);


    LRUAlgorithm<uint64_t> lru(2);
    lru.put(1);
    std::cout << 1 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(2);
    std::cout << 2 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(3);
    std::cout << 3 << "&" << lru.getSize() << "&";
    lru.print();
    lru.removePage(1, false);
    std::cout << 1 << "+" << lru.getSize() << "&";
    lru.print();
    lru.put(4);
    std::cout << 4 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(5);
    std::cout << 5 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(2);
    std::cout << 2 << "&" << lru.getSize() << "&";
    lru.print();
    lru.removePage(5, true);
    assert(!lru.contains(5));
    std::cout << 5 << "-" << lru.getSize() << "&";
    lru.print();
    lru.put(1);
    std::cout << 1 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(2);
    std::cout << 2 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(3);
    std::cout << 3 << "&" << lru.getSize() << "&";
    lru.print();
    lru.removePage(2, true);
    assert(!lru.contains(2));
    std::cout << 2 << "-" << lru.getSize() << "&";
    lru.print();
    lru.put(6);
    std::cout << 6 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(7);
    std::cout << 7 << "&" << lru.getSize() << "&";
    lru.print();
    lru.removePage(7, true);
    assert(!lru.contains(7));
    std::cout << 7 << "-" << lru.getSize() << "&";
    lru.print();
    lru.put(8);
    std::cout << 8 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(9);
    std::cout << 9 << "&" << lru.getSize() << "&";
    lru.print();
    lru.removePage(6, true);
    assert(!lru.contains(6));
    std::cout << 6 << "-" << lru.getSize() << "&";
    lru.print();
    lru.put(10);
    std::cout << 10 << "&" << lru.getSize() << "&";
    lru.print();
    lru.put(11);
    std::cout << 11 << "&" << lru.getSize() << "&";
    lru.print();

    for (int i = 0; i < 129; i++) std::cout << "=";
    std::cout << std::endl;

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