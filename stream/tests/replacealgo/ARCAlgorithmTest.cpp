//
// Created by Michael on 6/23/21.
//
#include <iostream>
#include "ARCAlgorithm.h"

int main(int argc, char **argv) {
    ARCAlgorithm<uint64_t> arc(100);
    for (int i = 0; i < 10000; i++) arc.arc_lookup(i);
    std::cout << arc.getMiss() << ":" << arc.getHit() << std::endl;
    return 0;
}