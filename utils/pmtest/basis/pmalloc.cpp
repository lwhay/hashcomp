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

#define LAYOUT "concurrent_hash_map"

static const char *path = "/mnt/pmm"; // "/dev/nvram/pmm";

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

void naive() {
    nvobj::pool<root> pop;
    if (access(path, F_OK) != -1)
        pop = nvobj::pool<root>::open(path, LAYOUT);
    else
        pop = nvobj::pool<root>::create(path, LAYOUT, PMEMOBJ_MIN_POOL * (1 << 10), S_IWUSR | S_IRUSR);
    for (int i = 0; i < (1llu < 20); i++) set(pop, 8);
}

int main() {
    // dummy();
    naive();
    return 0;
}