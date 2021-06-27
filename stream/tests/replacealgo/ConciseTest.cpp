//
// Created by iclab on 6/26/21.
//

#include <iostream>
//#include "ConciseLFUAlgorithm.h"

int main(int argc, char **argv) {
    std::cout << 0xfffffllu << " " << 0xfffff00000llu << " " << 0xfffff0000000000llu << std::endl;
    uint64_t x = -1, y = 0;
    x &= 0xfffffllu;
    y |= 0xfffffllu;
    std::cout << x << " " << y << std::endl;
    return 0;
}