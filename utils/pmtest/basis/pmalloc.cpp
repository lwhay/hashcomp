//
// Created by lwh on 2020/11/14.
//

#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/transaction.hpp>

#include <thread>
#include <string>
#include <unistd.h>

#include "tracer.h"

#define LAYOUT "concurrent_hash_map"

char *path = "/dev/shm/test/pmm"; //"/home/iclab/pmm"; //"/mnt/pmm"; // "/dev/nvram/pmm";

int concurrency = 8;

const int thread_items = (1 << 20);

namespace nvobj = pmem::obj;

class key_equal {
public:
    template<typename M, typename U>
    bool operator()(const M &l, const U r) const { return l == r; }
};

class string_hasher {
    static const size_t hash_multiplier = 11400714819323198485ULL;
public:
    using transparent_key_eqaul = key_equal;

    size_t operator()(const nvobj::string &str) const { return hash(str.c_str(), str.size()); }

private:
    size_t hash(const char *str, size_t size) const {
        size_t h = 0;
        for (size_t i = 0; i < size; ++i) {
            h = static_cast<size_t>(str[i] ^ (h * hash_multiplier));
        }
        return h;
    }
};

typedef nvobj::concurrent_hash_map<nvobj::string, nvobj::p<int>, string_hasher> pmmtype;
typedef nvobj::vector<nvobj::string> tlstype;

struct root {
    nvobj::persistent_ptr<pmmtype> cons;
    nvobj::persistent_ptr<tlstype> tls;
};

void dummy() {
    string_hasher hasher;
    size_t ac;
    for (uint64_t l = 0; l < (1llu << 30); l++) {
        if (l % 2 == 0) ac |= hasher(nvobj::string((char *) &l, sizeof(l)));
        else ac &= hasher(nvobj::string((char *) &l, sizeof(l)));
    }
}

void set(nvobj::pool<root> &pop, size_t concurrency = 8, size_t thread_items = 50) {
    do {
        std::cout << "TEST: " << __PRETTY_FUNCTION__ << std::endl;
    } while (0);
}


template<typename Function>
void parallel_exec(size_t concurrency, Function f) {
    std::vector<std::thread> threads;
    threads.reserve(concurrency);

    for (size_t i = 0; i < concurrency; ++i) {
        threads.emplace_back(f, i);
    }

    for (auto &t : threads) {
        t.join();
    }
}

void naive() {
    Tracer tracer;
    tracer.startTime();
    nvobj::pool<root> pop;
    std::cout << "pool: " << tracer.getRunTime() << std::endl;
    if (access(path, F_OK) != -1)
        pop = nvobj::pool<root>::open(path, LAYOUT);
    else
        pop = nvobj::pool<root>::create(path, LAYOUT, PMEMOBJ_MIN_POOL * (1 << 10), S_IWUSR | S_IRUSR);
    std::cout << "open: " << tracer.getRunTime() << std::endl;
    for (int i = 0; i < (1llu < 30); i++) set(pop, 8);
    std::cout << "setz: " << tracer.getRunTime() << std::endl;

    pmem::obj::transaction::run(pop, [&] {
        pop.root()->cons = nvobj::make_persistent<pmmtype>();
        pop.root()->tls = nvobj::make_persistent<tlstype>();
    });
    pop.root()->cons->runtime_initialize();

    nvobj::persistent_ptr<tlstype> &tls = pop.root()->tls;
    using accessor = pmmtype::accessor;
    using const_acc = pmmtype::const_accessor;

    tls->resize(concurrency);
    parallel_exec(concurrency, [&](size_t thread_id) {
        int begin = thread_id * thread_items;
        int end = begin + int(thread_items);
        auto &pstr = tls->at(thread_id);

        for (int i = begin; i < end; i++) {
            pstr = std::to_string(i);
            const pmmtype::key_type &val = pstr;
            bool result = pop.root()->cons->insert_or_assign(val, i);
            // UT_ASSERT(result);
            assert(result);
        }
        for (int i = begin; i < end; i++) {
            //test.check_item<accessor>(std::to_string(i), i);
            accessor acc;
            bool found = pop.root()->cons->find(acc, std::to_string(i));
            assert(found);
        }
        for (int i = begin; i < end; i++) {
            /* assign existing keys new values */
            pstr = std::to_string(i);
            const pmmtype::key_type &val = pstr;
            bool result = pop.root()->cons->insert_or_assign(val, i + 1);
            // UT_ASSERT(!result);
            assert(!result);
        }
        for (int i = begin; i < end; i++) {
            // test.check_item<const_acc>(std::to_string(i), i + 1);
            accessor acc;
            bool found = pop.root()->cons->find(acc, std::to_string(i));
            // assert(acc->first == i);
            assert(acc->second == i + 1);
        }
    });
    //test.check_consistency();
    // clear and recheck
    pop.root()->cons->clear();
    assert(pop.root()->cons->size() == 0);
    assert(std::distance(pop.root()->cons->begin(), pop.root()->cons->end()) == 0);
    tls->clear();
}

int main(int argc, char **argv) {
    if (argc < 3) {
        std::cout << "Command: path tnum" << std::endl;
        exit(0);
    }
    path = argv[1];
    concurrency = std::atoi(argv[2]);
    // dummy();
    naive();
    return 0;
}