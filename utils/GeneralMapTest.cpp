//
// Created by Michael on 4/23/20.
//

#include <iostream>
#include <unordered_map>
#include <sparsehash/dense_hash_map>
#include "tracer.h"

uint64_t key_range = (1llu << 20);

uint64_t total_count = (1llu << 20);

uint64_t total_round = 10;

double distribution = .0f;

uint64_t map_type = 2; // 0/1: unordered_map with same/random order; 2/3: dense_hash_map with random/random order

uint64_t *loads;

void stdmaptest(bool random) {
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution);
    std::unordered_map<uint64_t, uint64_t> map;
    uint64_t value = 0;
    Tracer tracer;
    tracer.startTime();
    for (uint64_t i = 0; i < total_count; i++) map.insert(std::pair<uint64_t, uint64_t>(loads[i], loads[i]));
    std::cout << "Std unorderedmap insert: " << (double) total_count / tracer.getRunTime() << std::endl;
    if (random) std::random_shuffle(loads, loads + total_count);
    std::cout << "Std unorderedmap shuffle: " << (double) total_count / tracer.getRunTime() << std::endl;
    for (int r = 0; r < 10; r++) for (uint64_t i = 0; i < total_count; i++) value = map.find(loads[i])->second;
    std::cout << "Std unorderedmap find: " << (double) total_count * total_round / tracer.getRunTime() << std::endl;
    for (int r = 0; r < 10; r++) for (uint64_t i = 0; i < total_count; i++) map.find(loads[i])->second = loads[i];
    std::cout << "Std unorderedmap update: " << (double) total_count * total_round / tracer.getRunTime() << std::endl;
}

void densemaptest(bool random) {
    loads = (uint64_t *) calloc(total_count, sizeof(uint64_t));
    RandomGenerator<uint64_t>::generate(loads, key_range, total_count, distribution);
    google::dense_hash_map<uint64_t, uint64_t> map;
    uint64_t value = 0;
    Tracer tracer;
    tracer.startTime();
    for (uint64_t i = 0; i < total_count; i++) map.insert(std::pair<uint64_t, uint64_t>(loads[i], loads[i]));
    std::cout << "Google densesemap insert: " << (double) total_count / tracer.getRunTime() << std::endl;
    if (random) std::random_shuffle(loads, loads + total_count);
    std::cout << "Google densesemap shuffle: " << (double) total_count / tracer.getRunTime() << std::endl;
    for (int r = 0; r < 10; r++) for (uint64_t i = 0; i < total_count; i++) value = map.find(loads[i])->second;
    std::cout << "Google densesemap find: " << (double) total_count * total_round / tracer.getRunTime() << std::endl;
    for (int r = 0; r < 10; r++) for (uint64_t i = 0; i < total_count; i++) map.find(loads[i])->second = loads[i];
    std::cout << "Google densesemap find: " << (double) total_count * total_round / tracer.getRunTime() << std::endl;
}

int main(int argc, char **argv) {
    if (argc > 5) {
        key_range = std::atol(argv[1]);
        total_count = std::atol(argv[2]);
        total_round = std::atol(argv[3]);
        distribution = std::atof(argv[4]);
        map_type = std::atol(argv[5]);
    }
    switch (map_type) {
        case 0:
            stdmaptest(false);
            break;
        case 1:
            stdmaptest(true);
            break;
        case 2:
            densemaptest(false);
            break;
        case 3:
            densemaptest(true);
            break;
        default:
            std::cout << "Type 0|1|2|3" << std::endl;
    }
}