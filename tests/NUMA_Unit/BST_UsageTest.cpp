//
// Created by Michael on 3/30/20.
//

#include <iostream>
#include "reclaimer_debra.h"
#include "pool_none.h"
#include "allocator_new.h"
#include "bst_impl.h"
#include "chromatic_impl.h"
#include "node.h"

int main(int argc, char **argv) {
    typedef Node<uint64_t, uint64_t> node;
    typedef reclaimer_debra<node, pool_none<node, allocator_new<node>>> Reclaimer;
    typedef allocator_new<node> Allocator;
    typedef pool_none<node, allocator_new<node>> Pool;

    unsigned char testin[8] = {0x00, 0x00, 0x00, 0x77, 0x00, 0x00, 0x00, 0xff};
    for (int i = 0; i < 64; i++) {
        if (testin[i / 8] & 1llu << (i % 8)) std::cout << "1"; else std::cout << "0";
        if (i % 8 == 3) std::cout << "-";
        if (i % 8 == 7) std::cout << " ";
    }
    std::cout << std::endl;
    uint64_t testul = *(uint64_t *) testin;
    for (int i = 0; i < 64; i++) {
        if (testul & 1llu << i) std::cout << "1"; else std::cout << "0";
        if (i % 8 == 3) std::cout << "-";
        if (i % 8 == 7) std::cout << " ";
    }
    std::cout << std::endl;
    return 0;
}