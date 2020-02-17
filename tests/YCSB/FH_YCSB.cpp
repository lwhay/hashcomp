//
// Created by lwh on 19-6-17.
//


#include <iostream>
#include <sstream>
#include <chrono>
#include <iterator>
#include <vector>
#include <functional>
#include <unordered_set>
#include "tracer.h"
#include "faster.h"
#include "kvcontext.h"
#include "ycsbHelper.h"

using namespace std;

size_t total = 100;

int scale = 0;

int paral = 4;

char **sinput;

#define DEFAULT_STORE_BASE 100000000LLU

using namespace FASTER::api;

#ifdef _WIN32
typedef hreadPoolIoHandler handler_t;
#else
typedef QueueIoHandler handler_t;
#endif
typedef FileSystemDisk<handler_t, 1073741824ull> disk_t;

using store_t = FasterKv<Key, Value, disk_t>;

size_t init_size = next_power_of_two(DEFAULT_STORE_BASE / 2);

store_t store{init_size, 17179869184, "storage"};

void simpleInsert() {
    for (int i = 0; i < 4; i++) {
        /*auto callback = [](IAsyncContext *ctxt, Status result) {
            CallbackContext<UpsertContext> context{ctxt};
        };
        UpsertContext context{dummy[i], dummy[i]};
        Status stat = store.Upsert(context, callback, 1);*/
    }
    for (int i = 0; i < 4; i++) {
        //cout << mhash->Get(dummy[i]).second << endl;
    }
}


void initYCSB(int vscale) {
    for (int i = 0; i < total; i++) {
        //mhash->Insert(sinput[i], dummy[vscale]);
    }
}

void *initYCSB(void *args) {
    threadConfig *conf = static_cast<threadConfig *>(args);
    for (int i = conf->tid; i < total; i += paral) {
        //mhash->Insert(sinput[i], dummy[conf->vscale]);
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
            /*pair<char *, char *> kv = mhash->Get(sinput[i]);
            if (!equalTo(kv.first, sinput[i]) || !equalTo(kv.second, dummy[vscale])) {
                missed++;
            } else {
                found++;
            }*/
        }
        cout << "Found: " << found << " missed: " << missed << endl;
    } else {
        for (int i = 0; i < total; i++) {
            //mhash->Update(sinput[i], dummy[vscale]);
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

#if COUNT_HASH == 1
#define UNSAFE
#ifdef UNSAFE
    mhash = new neatlib::ConcurrentHashTable<char *, char *,
            std::hash<char *>, 4, 16,
            neatlib::unsafe::atomic_shared_ptr,
            neatlib::unsafe::shared_ptr>();
#else
    mhash = new neatlib::ConcurrentHashTable<char *, char *, std::hash<char *>, 4, 16> ();
#endif
#elif COUNT_HASH == 2
    mhash = new neatlib::BasicHashTable<char *, char *, std::hash<char *>, std::equal_to<char *>, std::allocator<std::pair<const char *, char *>>, 4>();
#endif

    if (argc <= 1) {
        cout << "Parameter needed." << endl;
        exit(0);
    }
    switch (std::atoi(argv[1])) {
        case 0:
            simpleInsert();
            break;
        case 1: {
            Tracer tracer;
            tracer.startTime();
            total = std::atol(argv[3]);
            scale = std::atoi(argv[4]);
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
            total = std::atol(argv[3]);
            scale = std::atoi(argv[4]);
            paral = std::atoi(argv[5]);
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