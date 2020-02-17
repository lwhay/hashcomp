//
// Created by lwh on 19-7-9.
//

#include <iostream>
#include <sstream>
#include <iterator>
#include <vector>
#include <functional>
#include <unordered_set>
#include "tracer.h"
#include "universal_hash_table.h"
#include "lock_free_hash_table.h"
#include "concurrent_hash_table.h"
#include "basic_hash_table.h"
#include "atomic_shared_ptr.h"
#include "ycsbHelper.h"

using namespace std;

size_t total = 100;

int scale = 0;

int paral = 4;

char **sinput;

#define COUNT_HASH         0

#if COUNT_HASH == 1
//#define UNSAFE
#ifdef UNSAFE
neatlib::ConcurrentHashTable<char *, char *,
        std::hash<char *>, 4, 16,
        neatlib::unsafe::atomic_shared_ptr,
        neatlib::unsafe::shared_ptr> *mhash;
#else
neatlib::ConcurrentHashTable<char *, char *, std::hash<char *>, 4, 16> *mhash;
#endif
#elif COUNT_HASH == 2
neatlib::BasicHashTable<char *, char *, std::hash<char *>,
        std::equal_to<char *>, std::allocator<std::pair<const char *, char *>>, 4> *mhash;
#elif COUNT_HASH == 3
neatlib::UniversalHashTable<char *, char *, std::hash<char *>, 4, 16> *mhash;
#else
neatlib::LockFreeHashTable<char *, char *, std::hash<char *>, 4, 16> *mhash;
#endif

void simpleInsert() {
    for (int i = 0; i < 4; i++) {
        mhash->Insert(dummy[i], dummy[i]);
    }
    for (int i = 0; i < 4; i++) {
        cout << mhash->Get(dummy[i]).second << endl;
    }
}


void initYCSB(int vscale) {
    for (int i = 0; i < total; i++) {
        mhash->Insert(sinput[i], dummy[vscale]);
    }
}

void *initYCSB(void *args) {
    threadConfig *conf = static_cast<threadConfig *>(args);
    for (int i = conf->tid; i < total; i += paral) {
        if (!mhash->Insert(sinput[i], dummy[conf->vscale])) {
            cout << conf->tid << " " << i << endl;
        }
    }
}

void pinitYCSB(int vscale) {
    pthread_t threads[paral];
    threadConfig confs[paral];
    for (int i = 0; i < paral; i++) {
        confs[i].tid = i;
        confs[i].vscale = vscale;
        pthread_create(&threads[i], nullptr, initYCSB, &confs[i]);
    }
    for (int i = 0; i < paral; i++) {
        pthread_join(threads[i], nullptr);
    }
}

void verifyYCSB(int vscale) {
    if (vscale == scale) {
        size_t found = 0;
        size_t missed = 0;
        equal_to<char *> equalTo;
        for (int i = 0; i < total; i++) {
            //cout << i << endl;
            pair<char *, char *> kv = mhash->Get(sinput[i]);
            if (!equalTo(kv.first, sinput[i]) || !equalTo(kv.second, dummy[vscale])) {
                missed++;
            } else {
                found++;
            }
        }
        cout << "Found: " << found << " missed: " << missed << endl;
    } else {
        for (int i = 0; i < total; i++) {
            mhash->Update(sinput[i], dummy[vscale]);
        }
    }
}

void freeLoad() {
    for (int i = 0; i < total; i++) {
        delete[] sinput[i];
    }
    delete[] sinput;
}

int main(int argc, char **argv) {
    if (argc <= 1) {
        cout << "Parameter needed." << endl;
        exit(0);
    }
    switch (std::atoi(argv[1])) {
        case 0:
            break;
        case 1: {
            total = std::atol(argv[3]);
            scale = std::atoi(argv[4]);
            break;
        }
        case 2: {
            total = std::atol(argv[3]);
            scale = std::atoi(argv[4]);
            paral = std::atoi(argv[5]);
            break;
        }
        default:
            cout << "Parameter needed." << endl;
    }
#if COUNT_HASH == 1
#ifdef UNSAFE
    mhash = new neatlib::ConcurrentHashTable<char *, char *,
            std::hash<char *>, 4, 16,
            neatlib::unsafe::atomic_shared_ptr,
            neatlib::unsafe::shared_ptr>();
#else
    mhash = new neatlib::ConcurrentHashTable<char *, char *, std::hash<char *>, 4, 16>();
#endif
#elif COUNT_HASH == 2
    mhash = new neatlib::BasicHashTable<char *, char *, std::hash<char *>, std::equal_to<char *>, std::allocator<std::pair<const char *, char *>>, 4>();
#elif COUNT_HASH == 3
    mhash = new neatlib::UniversalHashTable<char *, char *, std::hash<char *>, 4, 16>();
#else
    mhash = new neatlib::LockFreeHashTable<char *, char *, std::hash<char *>, 4, 16>(paral);
#endif
    switch (std::atoi(argv[1])) {
        case 0:
            simpleInsert();
            break;
        case 1: {
            Tracer tracer;
            tracer.startTime();
            sinput = new char *[total];
            loadYCSB(argv[2], total, sinput);
            cout << "Load time: " << tracer.getRunTime() << endl;
            tracer.startTime();
            initYCSB(scale);
            cout << "Init time: " << tracer.getRunTime() << endl;
            tracer.startTime();
            verifyYCSB(scale);
            cout << "Search time: " << tracer.getRunTime() << endl;
            tracer.startTime();
            verifyYCSB(scale + 1);
            cout << "Update time: " << tracer.getRunTime() << endl;
            freeLoad();
            break;
        }
        case 2: {
            Tracer tracer;
            tracer.startTime();
            sinput = new char *[total];
            loadYCSB(argv[2], total, sinput);
            cout << "Load time: " << tracer.getRunTime() << endl;
            tracer.startTime();
            pinitYCSB(scale);
            cout << "Pinit time: " << tracer.getRunTime() << endl;
            tracer.startTime();
            verifyYCSB(scale);
            cout << "Search time: " << tracer.getRunTime() << endl;
            tracer.startTime();
            break;
        }
        default:
            cout << "Parameter needed." << endl;
    }
    return 0;
}