//
// Created by Michael on 5/12/20.
//

#include <cstdint>
#include <cstring>
#include <functional>
#include "tracer.h"

#include "libcuckoo/cuckoohash_map.hh"

size_t total_count = (1llu << 20);
size_t total_round = 10;
uint64_t *loads;

int main(int argc, char **argv) {
    if (argc > 2) {
        total_count = std::atol(argv[1]);
        total_round = std::atol(argv[2]);
    }

    typedef libcuckoo::cuckoohash_map<uint64_t, uint64_t, std::hash<uint64_t>, std::equal_to<uint64_t>,
            std::allocator<std::pair<const uint64_t, uint64_t>>, 8> cmap;

    cmap *store = new cmap(total_count);
    loads = new uint64_t[total_count];
    for (size_t i = 0; i < total_count; i++) loads[i] = i;

    for (int i = 0; i < total_count; i++) {
        store->insert(loads[i], loads[i]);
    }

    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < total_round; i++) {
        for (int j = 0; j < total_count; j++) {
            uint64_t value;
            store->find(loads[i], value);
            assert(loads[i] == value);
        }
    }

    std::cout << "total: " << total_count << " round: " << total_round << " time: " << tracer.getRunTime() << std::endl;

    delete store;
    delete[] loads;
    return 0;
}