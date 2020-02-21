//
// Created by lwh on 19-7-12.
//
#ifndef HASHCOMP_YCSBHELPER_H
#define HASHCOMP_YCSBHELPER_H

#include <regex>
#include <stdio.h>
#include <stdlib.h>

using namespace std;

char *dummy_set[]{
        "abwerewrsdfrwereweewrewrrewrwerw",
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw",
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw",
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw",
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
};

char *update_set[]{
        "werewsdvcxewwrsdfxcvwerwesdfwerw",
        "werewsdvcxewwrsdfxcvwerwesdfwerw"
        "werewsdvcxewwrsdfxcvwerwesdfwerw"
        "werewsdvcxewwrsdfxcvwerwesdfwerw"
        "werewsdvcxewwrsdfxcvwerwesdfwerw",
        "werewsdvcxewwrsdfxcvwerwesdfwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw",
        "werewsdvcxewwrsdfxcvwerwesdfwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw",
        "werewsdvcxewwrsdfxcvwerwesdfwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
        "abwerewrsdfrwereweewrewrrewrwerw"
};

void loadYCSB(char *path, size_t total, char **sinput) {
    ifstream fin(path);
    string s;
    string regex_str("user\\d");
    regex pattern(regex_str, regex::icase);
    match_results<string::const_iterator> results;
    long found = 0;
    while (getline(fin, s)) {
        if (s.substr(0, 6) == "INSERT") {
            if (regex_search(s, results, pattern)) {
                size_t spos = results[0].second - s.begin();
                size_t epos = s.find(" ", spos);
                string os = s.substr(spos - 1, epos - spos + 1);
                sinput[found] = new char[os.size() + 1];
                memcpy(sinput[found++], os.c_str(), os.size());
                //input.push_back(os);
                //cout << found++ << " " << os << " " << os.size() << endl;
            }
            //cout << s << endl;
            /*size_t spos = s.find("user\\[0-9\\]");
            size_t epos = s.find(" ", spos);
            cout << s.substr(spos + 4, epos) << endl;*/
        }
        if (found == total) break;
    }
    cout << "Load " << found << endl;
}

struct threadConfig {
    int tid;
    int vscale;
};

#endif //HASHCOMP_YCSBHELPER_H
