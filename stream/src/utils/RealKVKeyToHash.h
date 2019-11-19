//
// Created by iclab on 11/18/19.
//

#ifndef HASHCOMP_REALKVKEYTOHASH_H
#define HASHCOMP_REALKVKEYTOHASH_H

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "StringUtil.h"
#include "../../../src/mhash/util/City.h"

class RealWorkload {
public:

    static uint64_t *ycsbKeyToUint64(size_t load_size) {
        std::ifstream input;
        input.open("../../res/realKVdata/load.dat");
        uint64_t *hashes = new uint64_t[load_size];
        char line[4096];
        size_t count = 0;
        while (input.getline(line, 4096)) {
            std::vector<std::string> fields = split(line, " ");
            int ret = fields[0].compare("INSERT");
            if (fields.size() < 4 || (fields[0].compare("INSERT") != 0 && fields[0].compare("READ") != 0 &&
                                      fields[0].compare("UPDATE") != 0))
                continue;
            std::memset(line, 0, 4096);
            std::strcpy(line, fields[2].substr(4, fields[2].length()).c_str());
            hashes[count] = CityHash64(line, std::strlen(line));
            if (count++ >= load_size) break;
        }
        input.close();
        return hashes;
    }

    static uint64_t *clickstreamKeyToUint64(size_t load_size) {
        std::ifstream input;
        input.open("../../res/realKVdata/clickkeys.dat");
        uint64_t *hashes = new uint64_t[load_size];
        char line[4096];
        size_t count = 0;
        while (input.getline(line, 4096)) {
            std::vector<std::string> fields = split(line, "\t");
            std::memset(line, 0, 4096);
            std::strcpy(line, fields[0].c_str());
            std::strcpy(line + fields[0].size(), fields[1].c_str());
            hashes[count] = CityHash64(line, std::strlen(line));
            if (count++ >= load_size) break;
        }
        input.close();
        return hashes;
    }
};

#endif //HASHCOMP_REALKVKEYTOHASH_H
