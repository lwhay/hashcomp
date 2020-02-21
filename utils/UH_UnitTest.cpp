//
// Created by Michael on 2019-07-10.
//

#include <functional>
#include <iostream>
#include <stack>
#include <thread>
#include <unordered_set>
#include <boost/functional/hash/hash.hpp>
#include <boost/lockfree/stack.hpp>
#include "tracer.h"
#include "basic_hash_table.h"
#include "concurrent_hash_table.h"
#include "universal_hash_table.h"
#include "ycsbHelper.h"
#include "hash.h"
#include "hasher.h"

#if defined(__linux__)

#include <tr1/functional>
#include <cpuid.h>

#else
#define stdHasher boost::hash_detail::hash_value_unsigned<uint64_t>
#endif

using namespace std;
using namespace boost;
using namespace neatlib;

#define VARIANT_FIELD 1
#define CACHE_RESERVE (1 << 8)
#define INPLACE_NEW   0

uint64_t total = 10000000;
int pdegree = 4;
int simple = 0;
long total_runtime = 0;
long max_runtime = 0;
long min_runtime = std::numeric_limits<long>::max();

bool equalto(const char *left, const char *right) {
    return left == right;
}

int strCmp(const char *left, const char *right) {
    int ret = 0;
    size_t cursor = 0;
    while (left[cursor] != 0) {
        if (right[cursor] == 0) {
            return left[cursor] - right[cursor];
        }
        ret = left[cursor] - right[cursor];
        cursor++;
        if (ret != 0) {
            return ret;
        }
    }
    if (right[cursor++] != 0) {
        ret = right[cursor] - left[cursor];
    }
    return ret;
}

bool equalTo(const char *left, const char *right) {
    return strCmp(left, right) == 0;
}

void simpleOperationTests() {
    char *vleft = "Meet the new boss...";
    char *vmiddle = "Meet the new boss";
    char *vright = (char *) malloc(strlen(vleft) + 1);
    memcpy(vright, vleft, strlen(vleft));
    //char *vright = "Meet the new boss...";
    std::hash<char *> vhasher;
    std::equal_to<char *> vet;
    //vright[10] = 'E';
    cout << std::strlen(vleft) << " " << std::strlen(vright) << endl;
    cout << vet(vleft, vright) << endl;
    cout << equalTo(vleft, vright) << endl;
    cout << vhasher(vleft) << "<->" << (size_t) vleft << endl;

    mhasher<char *> myhasher;
    cout << myhasher.hash(vmiddle) << "<->" << vleft << endl;

    const char *cleft = "Meet the new boss...";
    const char *cmiddle = "Meet the new boss";
    const char *cright = "Meet the new boss...";
    std::hash<const char *> chasher;
    std::equal_to<const char *> cet;


    string sleft("Meet the new boss...");
    string smiddle("Meet the new boss");
    string sright("Meet the new boss...");
    const char *ccleft = sleft.c_str();
    const char *ccmiddle = smiddle.c_str();
    const char *ccright = sright.c_str();
    std::hash<const char *> cchasher;
    std::equal_to<const char *> ccet;

    cout << equalto(ccleft, ccright) << " " << equalTo(ccleft, ccright) << endl;

    cout << chasher(cleft) << " " << chasher(cright) << " " << chasher(cmiddle) << endl;
    cout << cchasher(cleft) << " " << cchasher(cright) << " " << cchasher(cmiddle) << endl;
    cout << chasher(ccleft) << " " << chasher(ccright) << " " << chasher(ccmiddle) << endl;
    cout << cchasher(ccleft) << " " << cchasher(ccright) << " " << cchasher(ccmiddle) << endl;
    cout << equalTo(cleft, ccleft) << " " << cet(cleft, cright) << " " << cet(cleft, cmiddle) << " " << true << endl;
    cout << equalTo(ccleft, ccright) << " " << ccet(ccleft, ccright) << " " << ccet(cleft, ccleft) << " "
         << ccet(ccleft, ccmiddle) << " " << equalTo(ccleft, ccmiddle) << " " << true << endl;
    cout << strcmp(cleft, ccleft) << " " << std::strcmp(cleft, ccleft) << " " << std::strcmp(cleft, cmiddle) << " "
         << std::strcmp(cmiddle, cleft) << " " << (int) '.' << endl;

    string left = "Meet the new boss...";
    string middle = "Meet the new boss";
    string right = "Meet the new boss...";
    std::hash<string> hasher;
    std::equal_to<string> et;
    cout << hasher(left.c_str()) << " " << hasher(right.c_str()) << " " << hasher(middle.c_str()) << endl;
    cout << hasher(left) << " " << hasher(right) << " " << hasher(middle) << endl;
    cout << et(left, right) << " " << et(left, middle) << " " << true << endl;

    // Notice here that uint64_t cannot generate random hash key.
    uint64_t ileft = 123456789LLU;
    uint64_t imiddle = 1234567890LLU;
    uint64_t iright = 123456789LLU;
    std::hash<uint64_t> ihasher;
    std::equal_to<uint64_t> iet;
    mhasher<uint64_t> imet;
    boost::hash<uint64_t> bhet;
    std::size_t seed = 40343;
    using bchash = boost::hash_detail::hash_base<uint64_t>;

#if defined(__linux__)
    typedef boost::hash_detail::basic_numbers<uint64_t>::type bet;
    std::tr1::hash<uint64_t> thasher;
    //bchash(seed, 123);
    cout << "thash: " << thasher(ileft) << " " << thasher(iright) << " " << thasher(imiddle) << endl;
    cout << "bhash: " << bet(ileft) << " " << bet(iright) << " " << bet(imiddle) << endl;
#else
    cout << "dhash: " << stdHasher(ileft) << " " << stdHasher(iright) << " " << stdHasher(imiddle) << endl;
#endif
    cout << "shash: " << ihasher(ileft) << " " << ihasher(iright) << " " << ihasher(imiddle) << endl;
    cout << "hhash: " << bhet(ileft) << " " << bhet(iright) << " " << bhet(imiddle) << endl;
    //cout << "bchash: " << bet(ileft) << " " << bet(iright) << " " << bet(imiddle) << endl;
    cout << "mhash: " << imet(ileft) << " " << imet(iright) << " " << imet(imiddle) << endl;
    cout << "equal: " << iet(ileft, iright) << " " << iet(ileft, imiddle) << " " << true << endl;

    UniversalHashTable<char *, char *, std::hash<char *>, 4, 16> uhash;
    for (int i = 0; i < 5; i++) {
        uhash.Insert(dummy_set[i], dummy_set[i]);
    }
    ConcurrentHashTable<char *, char *, std::hash<char *>, 4, 16> chash;
    for (int i = 0; i < 5; i++) {
        chash.Insert(dummy_set[i], dummy_set[i]);
    }
    BasicHashTable<char *, char *, std::hash<char *>, std::equal_to<char *>,
            std::allocator<std::pair<const char *, char *>>, 4> bhash;
    for (int i = 0; i < 5; i++) {
        bhash.Insert(dummy_set[i], dummy_set[i]);
    }
}

vector<uint64_t> input;

void mhasherTests(bool init = true) {
    Tracer tracer;
    tracer.startTime();
    unordered_set<uint64_t> umap;
    mhasher<uint64_t> hasher;
    if (init) {
        for (size_t i = 0; i < (total * 100); i++) {
            input.push_back(i);
        }
        cout << "\tKey gen: " << tracer.getRunTime() << " with " << input.size() << endl;
    }
    for (auto key: input) {
        //umap.insert(hasher.hash(key));
        uint64_t gk = hasher.hash(key);
        if (gk == -1) {
            umap.insert(gk);
        }
    }
    cout << "\tSet gen: " << tracer.getRunTime() << " with " << umap.size() << endl;
    if (init) {
        for (auto key: input) {
            uint64_t gk = hasher.hash(key);
            umap.insert(gk);
        }
        cout << "\tSet vrf: " << tracer.getRunTime() << " with " << umap.size() << endl;
#define HISTGRAM_BINS 1024
        size_t histgram[HISTGRAM_BINS];
        cout << "********** " << sizeof(histgram) << endl;
        std::memset(histgram, 0, sizeof(histgram));
        for (auto key: input) {
            size_t idx = hasher.hash(key) / (std::numeric_limits<size_t>::max() / HISTGRAM_BINS);
            histgram[idx]++;
        }
        for (int i = 0; i < HISTGRAM_BINS; i++) {
            cout << histgram[i] << "\t";
            if ((i + 1) % 16 == 0) {
                cout << endl;
            }
        }
        cout << "\tHst gen: " << tracer.getRunTime() << " with " << umap.size() << endl;
    }
}

void boostHasherTests() {
#if defined(__linux__)
    typedef boost::hash_detail::basic_numbers<uint64_t>::type stdHasher;
#endif
    unordered_set<uint64_t> umap;
    for (auto key: input) {
        //umap.insert(hasher.hash(key));
        uint64_t gk = stdHasher(key);
        if (gk == -1) {
            umap.insert(gk);
        }
    }
}

void multiHasherTests() {
    Tracer tracer;
    tracer.startTime();
    std::vector<std::thread> workers;
    for (int t = 0; t < pdegree; t++) {
        workers.push_back(std::thread(mhasherTests, false));
    }
    for (int t = 0; t < pdegree; t++) {
        workers[t].join();
    }
    cout << "Set gen: " << tracer.getRunTime() << " with " << (input.size() * pdegree) << endl;
}

/*template<typename S>
void stackWorker(S &s) {
    for (int i = 0; i < 1000000; i++) {
        s.push(i);
    }
    for (int i = 0; i < 1000000; i++) {
        //s.pop();
    }
}*/

void stdWorker(std::stack<int> *s) {
    for (int i = 0; i < total; i++) {
        s->push(i);
    }
    for (int i = 0; i < total; i++) {
        s->pop();
    }
}

void bstWorker(boost::lockfree::stack<int> *s, long *newtime, long *freetime) {
    Tracer tracer;
    tracer.startTime();
    for (int i = 0; i < total / pdegree; i++) {
        s->push(i);
    }
    *newtime = tracer.getRunTime();
    for (int i = 0; i < total / pdegree; i++) {
        int v = -1;
        s->pop(v);
    }
    *freetime = tracer.getRunTime();
}

void boostStackVSstdStack(bool usingStd) {
    std::vector<std::thread> workers;
    if (usingStd) {
        std::stack<int> stdStack;
        stdWorker(&stdStack);
    } else {
        long newtime[pdegree];
        long freetime[pdegree];
        boost::lockfree::stack<int> bstStack(128);
        for (int t = 0; t < pdegree; t++) {
            workers.push_back(std::thread(bstWorker, &bstStack, newtime + t, freetime + t));
        }
        for (int t = 0; t < pdegree; t++) {
            workers[t].join();
            cout << "\t" << t << " " << newtime[t] << " " << freetime[t] << endl;
        }
    }
}

void newWorker(bool inBatch, int tid, long *newtime, long *freetime) {
    typedef UniversalHashTable<uint64_t, uint64_t, std::hash<uint64_t>, 4, 16>::data_node datanode;
    datanode **loads = new datanode *[total];
    Tracer tracer;
    tracer.startTime();
    if (inBatch) {
        datanode *cache;
        size_t cursor = CACHE_RESERVE;
        std::hash<uint64_t> hasher;
        std::stack<datanode *> allocated;
        for (int i = 0; i < total; i++) {
            if (cursor == CACHE_RESERVE) {
                cache = (datanode *) malloc(sizeof(datanode) * CACHE_RESERVE);
                allocated.push(cache);
                cursor = 0;
            }
#if INPLACE_NEW
            loads[i] = new(cache + cursor)datanode(i, i);
#else
            loads[i] = &cache[cursor];
            loads[i]->reset(i, i);
            uint64_t hashv = loads[i]->hash();
            uint64_t key = loads[i]->get().first;
            uint64_t value = loads[i]->get().second;
#endif
            if (i == (pdegree - 1) && (pdegree - 1) == tid)
                cout << loads[i]->get().first << " " << loads[i]->hash() << " " << hasher(i) << endl;
            cursor++;
        }
        *newtime = tracer.getRunTime();
#if INPLACE_NEW
        for (int i = 0; i < total; i++) {
            // De-constructor can not really delete each object here.
            loads[i]->~datanode();
        }
#endif
        while (!allocated.empty()) {
            datanode *element = allocated.top();
            allocated.pop();
            free(element);
        }
        *freetime = tracer.getRunTime();
    } else {
        for (int i = 0; i < total; i++) {
            loads[i] = new datanode(i, i, sizeof(uint64_t));
        }
        *newtime = tracer.getRunTime();
        for (int i = 0; i < total; i++) {
            delete loads[i];
        }
        *freetime = tracer.getRunTime();
    }
    delete[] loads;
}

void concurrentDataAllocate(bool inBatch) {
    cout << "Allocator per " << sizeof(UniversalHashTable<uint64_t, uint64_t, std::hash<uint64_t>, 4, 16>::data_node)
         << endl;
    std::vector<std::thread> workers;
    long newtime[pdegree];
    long freetime[pdegree];
    for (int t = 0; t < pdegree; t++) {
        workers.push_back(std::thread(newWorker, inBatch, t, newtime + t, freetime + t));
    }
    for (int t = 0; t < pdegree; t++) {
        workers[t].join();
        cout << "\t" << t << " " << newtime[t] << " " << freetime[t] << endl;
    }
}

struct paramstruct {
    int tid;
#if VARIANT_FIELD
    UniversalHashTable<const char *, const char *, std::hash<char *>, 4, 16> *xh;
#else
    UniversalHashTable<uint64_t, uint64_t, std::hash<uint64_t>, 4, 16> *xh;
#endif
    long runtime = 0;
};

#if VARIANT_FIELD
UniversalHashTable<const char *, const char *, std::hash<char *>, 4, 16> *xh;
#else
UniversalHashTable<uint64_t, uint64_t, std::hash<uint64_t>, 4, 16> *xh;
#endif

void *worker(void *args) {
    paramstruct *param = static_cast<paramstruct *>(args);
    Tracer tracer;
    tracer.startTime();

    for (int i = param->tid; i < total; i += pdegree) {
#if VARIANT_FIELD
        const char *key = std::to_string(i).c_str();
        param->xh->Insert(key, key);
#else
        param->xh->Insert(i, i);
#endif
    }

    param->runtime = tracer.getRunTime();
}

void insert() {
    total_runtime = 0;
    pthread_t threads[pdegree];
    paramstruct params[pdegree];
#if VARIANT_FIELD
    xh = new UniversalHashTable<const char *, const char *, std::hash<char *>, 4, 16>();
#else
    xh = new UniversalHashTable<uint64_t, uint64_t, std::hash<uint64_t>, 4, 16>();
#endif

    for (int i = 0; i < pdegree; i++) {
        params[i].tid = i;
        params[i].xh = xh;
        pthread_create(&threads[i], nullptr, worker, &params[i]);
    }

    for (int i = 0; i < pdegree; i++) {
        pthread_join(threads[i], nullptr);
        cout << "\t" << i << " " << params[i].runtime << endl;
        total_runtime += params[i].runtime;
        if (params[i].runtime > max_runtime) {
            max_runtime = params[i].runtime;
        }
        if (params[i].runtime < min_runtime) {
            min_runtime = params[i].runtime;
        }
    }
}

int main(int argc, char **argv) {
    if (argc > 3) {
        total = std::atol(argv[1]);
        pdegree = std::atoi(argv[2]);
        simple = std::atoi(argv[3]);
    }
    cout << total << " " << pdegree << " " << simple << endl;
    Tracer tracer;
    tracer.startTime();
    if (simple) {
        simpleOperationTests();
        mhasherTests();
        multiHasherTests();
    } else {
        insert();
    }
    cout << "Ist: " << tracer.getRunTime() << endl;
    tracer.startTime();
    concurrentDataAllocate(false);
    cout << "new: " << tracer.getRunTime() << endl;
    tracer.startTime();
    concurrentDataAllocate(true);
    cout << "bew: " << tracer.getRunTime() << endl;
#if defined(__linux__)
    cout << "CPUs: " << sysconf(_SC_NPROCESSORS_ONLN) << endl;
    if (sysconf(_SC_NPROCESSORS_ONLN) > 8) {
        tracer.startTime();
        boostStackVSstdStack(true);
        cout << "Std: " << tracer.getRunTime() << endl;
        tracer.startTime();
        boostStackVSstdStack(false);
        cout << "Bst: " << tracer.getRunTime() << endl;
    }
#endif
    return 0;
}