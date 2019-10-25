//
// Created by Michael on 10/25/2019.
//

#ifndef HASHCOMP_STRINGUTIL_H
#define HASHCOMP_STRINGUTIL_H

#include <vector>
#include <cstring>

std::vector<std::string> split(const std::string &str, const std::string &delim) {
    std::vector<std::string> res;
    if ("" == str) return res;
    char *strs = new char[str.length() + 1];
    std::strcpy(strs, str.c_str());

    char *d = new char[delim.length() + 1];
    std::strcpy(d, delim.c_str());

    char *p = std::strtok(strs, d);
    while (p) {
        std::string s = p;
        res.push_back(s);
        p = std::strtok(nullptr, d);
    }

    return res;
}

#endif //HASHCOMP_STRINGUTIL_H
