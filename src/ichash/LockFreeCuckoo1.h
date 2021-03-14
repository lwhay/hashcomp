//
// Created by iclab on 3/7/21.
//

#ifndef HASHCOMP_LOCKFREECUCKOO_H
#define HASHCOMP_LOCKFREECUCKOO_H

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <atomic>
#include <thread>
#include <memory>
#include <cstring>
#include <mutex>

using namespace std;


class StrComparator {
public:
    int operator()(const char *a, const uint32_t a_len, const char *b, uint32_t b_len) const {
        if (a_len != b_len) return false;
        return std::memcmp(a, b, a_len);
    }
};


class Entry {
public:
    char key[24]{};
    char value[24]{};
    uint32_t k_len;
    uint32_t v_len;

    Entry(const char *k, uint32_t k_len, const char *v, uint32_t v_len) : k_len(k_len), v_len(v_len) {
        memcpy(key, k, k_len);
        memcpy(value, v, v_len);
    }
};


thread_local size_t kick_num_l;
thread_local size_t max_path_len_l;
thread_local size_t kick_path_len_accumulate_l;

class lockFreeCuckoo {
private:
    Entry *cursor;
    static const int FIRST = 0;
    static const int SECOND = 1;
    static const int NIL = -1;
    // static const char *NP = (char *) 0;


    //statistical info
    size_t kv_num;
    double occupancy;
    size_t kick_num;
    size_t max_path_len;
    size_t kick_path_len_accumulate;
    double avg_path_len;

    mutex statistic_mtx;

public:
    std::atomic<Entry *> *table1;
    std::atomic<Entry *> *table2;
    size_t t1Size;
    size_t t2Size;

    lockFreeCuckoo(size_t size1, size_t size2);

    ~lockFreeCuckoo();

    Entry *Search(const char *key, uint32_t key_len);

    bool Insert(const char *key, uint32_t key_len, const char *value, uint32_t value_len);

    bool Delete(const char *key, uint32_t key_len);

    bool Contains(const char *key, uint32_t key_len) const;

    bool moveToKey(const char *searchKey, uint32_t key_len);

    char *nextValueAtKey();

    size_t getSize();

    void ensureCapacity(uint32_t capacity);    // resize
    void printTable();

    void merge_info();

    void cal_info_and_show();

private:
    void init();

    int Find(const char *key, uint32_t key_len, Entry **ent1, Entry **ent2);

    bool Relocate(int tableNum, int pos);

    void helpRelocate(int table, int idx, bool initiator);

    void deleteDup(int idx1, Entry *ent1, int idx2, Entry *ent2);

    int hash1(const char *key, uint32_t k_len) const;

    int hash2(const char *key, uint32_t k_len) const;

private:
    // KeyComparator不需要大家实现，但是key的比较需要由compare_来完成
    StrComparator /*const*/ compare_;

};


//-----
// for template class, all in .h files.

// a 64-bits pointer only use low 48-bits
// we use high 16-bits to store the counter
inline int get_cnt(void *ptr) {
    unsigned long a = ((unsigned long) ptr & (0xffff000000000000));
    return a >> 48;
}


inline void inc_counter(Entry **ptr) {
    *ptr = (Entry *) ((unsigned long) *ptr + 0x0001000000000000);
}


inline void store_count(Entry **ptr, int cnt) {
    unsigned long new_cnt = (unsigned long) cnt << 48;
    *ptr = (Entry *) ((unsigned long) *ptr & 0x0000FFFFFFFFFFFF);
    *ptr = (Entry *) ((unsigned long) *ptr + new_cnt);
}

// the last 2-bits of Entrypointer is always zero.

inline Entry *extract_address(Entry *e) {
    e = (Entry *) ((unsigned long) e & (0x0000fffffffffffc));
    return e;
}

bool is_marked(void *ptr) {
    if ((unsigned long) ptr & 0x01)
        return true;
    else
        return false;
}

bool checkCounter(int ctr1, int ctr2, int ctrs1, int ctrs2) {
    if ((ctrs1 - ctr1) >= 2 && (ctrs2 - ctr1) >= 2)
        return true;
    return false;
}


lockFreeCuckoo::lockFreeCuckoo(size_t size1, size_t size2) : t1Size(size1), t2Size(size2) {
    kv_num = 0;
    occupancy = 0.0;
    kick_num = 0;
    max_path_len = 0;
    kick_path_len_accumulate = 0;
    avg_path_len = 0.0;

    table1 = new std::atomic<Entry *>[size1];
    table2 = new std::atomic<Entry *>[size2];
    cursor = NULL;
    init();
}


lockFreeCuckoo::~lockFreeCuckoo() {
    delete table1;
    delete table2;
}


void lockFreeCuckoo::init() {
    Entry *temp = NULL;
    for (int i = 0; i < t1Size; i++) {
        atomic_store(&table1[i], temp);
    }

    for (int i = 0; i < t2Size; i++) {
        atomic_store(&table2[i], temp);
    }
}


int lockFreeCuckoo::hash1(const char *keyIn, uint32_t k_len) const {

    const char *key = (const char *) keyIn;
    constexpr uint32_t kHashSeed = 7079;
    const uint64_t m = 0xc6a4a7935bd1e995ull;
    const size_t r = 47;
    uint64_t seed = kHashSeed;

    uint64_t h = seed ^(k_len * m);

    const auto *data = (const uint64_t *) key;
    const uint64_t *end = data + (k_len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const auto *data2 = (const unsigned char *) data;

    switch (k_len & 7ull) {
        case 7:
            h ^= uint64_t(data2[6]) << 48ull;
        case 6:
            h ^= uint64_t(data2[5]) << 40ull;
        case 5:
            h ^= uint64_t(data2[4]) << 32ull;
        case 4:
            h ^= uint64_t(data2[3]) << 24ull;
        case 3:
            h ^= uint64_t(data2[2]) << 16ull;
        case 2:
            h ^= uint64_t(data2[1]) << 8ull;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h % t1Size;
}


int lockFreeCuckoo::hash2(const char *keyIn, uint32_t k_len) const {

    const char *key = (const char *) keyIn;
    constexpr uint32_t kHashSeed = 16785407;
    const uint64_t m = 0xc6a4a7935bd1e995ull;
    const size_t r = 47;
    uint64_t seed = kHashSeed;

    uint64_t h = seed ^(k_len * m);

    size_t len = k_len;
    const auto *data = (const uint64_t *) key;
    const uint64_t *end = data + (k_len / 8);

    while (data != end) {
        uint64_t k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const auto *data2 = (const unsigned char *) data;

    switch (k_len & 7ull) {
        case 7:
            h ^= uint64_t(data2[6]) << 48ull;
        case 6:
            h ^= uint64_t(data2[5]) << 40ull;
        case 5:
            h ^= uint64_t(data2[4]) << 32ull;
        case 4:
            h ^= uint64_t(data2[3]) << 24ull;
        case 3:
            h ^= uint64_t(data2[2]) << 16ull;
        case 2:
            h ^= uint64_t(data2[1]) << 8ull;
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h % t2Size;
}


void lockFreeCuckoo::helpRelocate(int which, int idx, bool initiator) {
    Entry *srcEntry, *dstEntry, *tmpEntry;
    int dstIdx, nCnt, cnt1, cnt2, size[2];
    atomic<Entry *> *tbl[2];
    tbl[0] = table1;
    tbl[1] = table2;
    size[0] = t1Size;
    size[1] = t2Size;
    while (true) {
        srcEntry = atomic_load_explicit(&tbl[which][idx], memory_order_seq_cst);
        //Marks the Entry to logically swap it
        while (initiator && !(is_marked((void *) srcEntry))) {
            if (extract_address(srcEntry) == NULL)
                return;
            // mark the flag
            tmpEntry = (Entry *) ((unsigned long) srcEntry | 1);
            atomic_compare_exchange_strong(&(tbl[which][idx]), &srcEntry, tmpEntry);
            srcEntry = atomic_load_explicit(&tbl[which][idx], memory_order_seq_cst);
        }
        if (!(is_marked((void *) srcEntry)))
            return;

        char *key = extract_address(srcEntry)->key;
        uint32_t k_len = extract_address(srcEntry)->k_len;
        dstIdx = (which == FIRST ? hash2(key, k_len) : hash1(key, k_len));
        dstEntry = atomic_load_explicit(&tbl[1 - which][dstIdx], memory_order_seq_cst);
        if (extract_address(dstEntry) == NULL) {
            cnt1 = get_cnt((void *) srcEntry);
            cnt2 = get_cnt((void *) dstEntry);
            nCnt = cnt1 > cnt2 ? cnt1 + 1 : cnt2 + 1;
            if (srcEntry != atomic_load_explicit(&tbl[which][idx], memory_order_seq_cst))
                continue;

            Entry *tmpSrcEntry = srcEntry;
            // unmark the flag
            tmpSrcEntry = (Entry *) ((unsigned long) srcEntry & ~1);
            store_count(&tmpSrcEntry, nCnt);
            tmpEntry = NULL;
            store_count(&tmpEntry, cnt1 + 1);
            if (atomic_compare_exchange_strong(&(tbl[1 - which][dstIdx]), &dstEntry, tmpSrcEntry))
                atomic_compare_exchange_strong(&(tbl[which][idx]), &srcEntry, tmpEntry);
            return;
        }

        if (srcEntry == dstEntry) {
            tmpEntry = NULL;
            store_count(&tmpEntry, cnt1 + 1);
            atomic_compare_exchange_strong(&(tbl[which][idx]), &srcEntry, tmpEntry);
            return;
        }
        tmpEntry = (Entry *) ((unsigned long) srcEntry & (~1));
        store_count(&tmpEntry, cnt1 + 1);
        atomic_compare_exchange_strong(&(tbl[which][idx]), &srcEntry, tmpEntry);
        return;
    }
}


void lockFreeCuckoo::deleteDup(int idx1, Entry *ent1, int idx2, Entry *ent2) {
    Entry *tmp1, *tmp2;
    char *key1, *key2;
    int cnt;
    tmp1 = atomic_load(&table1[idx1]);
    tmp2 = atomic_load(&table2[idx2]);
    if ((ent1 != tmp1) && (ent2 != tmp2))
        return;
    key1 = extract_address(ent1)->key;
    uint32_t k1_len = extract_address(ent1)->k_len;
    key2 = extract_address(ent2)->key;
    uint32_t k2_len = extract_address(ent2)->k_len;
    if (compare_(key1, k1_len, key2, k2_len))
        return;
    tmp2 = NULL;
    cnt = get_cnt(ent2);
    store_count(&tmp2, cnt + 1);
    atomic_compare_exchange_strong(&(table2[idx2]), &ent2, tmp2);
}


Entry *lockFreeCuckoo::Search(const char *key, uint32_t key_len) {
    int h1 = hash1(key, key_len);
    int h2 = hash2(key, key_len);
    int cnt1, cnt2, ncnt1, ncnt2;
    while (true) {
        // 1st round
        // Looking in table 1
        Entry *ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        cnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->k_len, key, key_len))
            return ent1;
        // Looking in table 2
        Entry *ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        cnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->k_len, key, key_len))
            return ent2;

        // 2nd round
        ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        ncnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->k_len, key, key_len))
            return ent1;
        ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        ncnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->k_len, key, key_len))
            return ent2;

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return nullptr/*NIL*/;
    }
}

int lockFreeCuckoo::Find(const char *key, uint32_t key_len, Entry **ent1, Entry **ent2) {
    int h1 = hash1(key, key_len);
    int h2 = hash2(key, key_len);
    int result = NIL;
    int cnt1, cnt2, ncnt1, ncnt2;
    Entry *e;
    while (true) {
        // 1st round
        e = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        *ent1 = e;
        cnt1 = get_cnt(e);
        e = extract_address(e);
        // helping other concurrent thread if its not completely done with its job
        if (e != NULL) {
            if (is_marked((void *) e)) {
                helpRelocate(0, h1, false);
                continue;
            } else if (!compare_(e->key, e->k_len, key, key_len))
                result = FIRST;
        }

        e = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        *ent2 = e;
        cnt2 = get_cnt(e);
        e = extract_address(e);
        // helping other concurrent thread if its not completely done with its job
        if (e != NULL) {
            if (is_marked((void *) e)) {
                helpRelocate(1, h2, false);
                continue;
            }
            if (!compare_(e->key, e->k_len, key, key_len)) {
                if (result == FIRST) {
                    //printf("Find(): Delete_dup()\n");
                    deleteDup(h1, *ent1, h2, *ent2);
                } else
                    result = SECOND;
            }
        }
        if (result == FIRST || result == SECOND)
            return result;

        // 2nd round
        e = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        *ent1 = e;
        ncnt1 = get_cnt(e);
        e = extract_address(e);
        if (e != NULL) {
            if (is_marked((void *) e)) {
                helpRelocate(0, h1, false);
                printf("Find(): help_relocate()");
                continue;
            } else if (!compare_(e->key, e->k_len, key, key_len))
                result = FIRST;
        }

        e = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        *ent2 = e;
        ncnt2 = get_cnt(e);
        e = extract_address(e);
        if (e != NULL) {
            if (is_marked((void *) e)) {
                helpRelocate(1, h2, false);
                continue;
            }
            if (!compare_(e->key, e->k_len, key, key_len)) {
                if (result == FIRST) {
                    printf("Find(): Delete_dup()\n");
                    deleteDup(h1, *ent1, h2, *ent2);
                } else
                    result = SECOND;
            }
        }
        if (result == FIRST || result == SECOND)
            return result;

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return NIL;
    }
}


bool lockFreeCuckoo::Relocate(int which, int index) {
    ++kick_num_l;

    int threshold = 10000;
    int route[threshold]; // store cuckoo path
    int startLevel = 0, tblNum = which;
    char *key;
    int idx = index, preIdx = 0;
    Entry *curEntry = NULL;
    Entry *preEntry = NULL;
    atomic<Entry *> *tbl[2];
    tbl[0] = table1;
    tbl[1] = table2;

    //discovery cuckoo path
    path_discovery:
    bool found = false;
    int depth = startLevel;
    do {
        curEntry = atomic_load(&tbl[tblNum][idx]);
        while (is_marked((void *) curEntry)) {
            helpRelocate(tblNum, idx, false);
            curEntry = atomic_load(&tbl[tblNum][idx]);
        }

        Entry *preEntAddr, *curEntAddr;
        preEntAddr = extract_address(preEntry);
        curEntAddr = extract_address(curEntry);
        if (curEntAddr != NULL && preEntAddr != NULL) {
            if (preEntry == curEntry ||
                !compare_(preEntAddr->key, preEntAddr->k_len, curEntAddr->key, curEntAddr->v_len)) {
                if (tblNum == FIRST)
                    deleteDup(idx, curEntry, preIdx, preEntry);
                else
                    deleteDup(preIdx, preEntry, idx, curEntry);
            }
        }

        // not an empty slot, continue discovery
        if (curEntAddr != NULL) {
            route[depth] = idx;
            key = curEntAddr->key;
            uint32_t k_len = curEntAddr->k_len;
            preEntry = curEntry;
            preIdx = idx;
            tblNum = 1 - tblNum; // change to another table
            idx = (tblNum == FIRST ? hash1(key, k_len) : hash2(key, k_len));
        }
            // find an empty slot
        else {
            found = true;
        }
    } while (!found && ++depth < threshold);

    // insert key
    if (found) {
        Entry *srcEntry, *dstEntry;
        int dstIdx;

        tblNum = 1 - tblNum;
        for (int i = depth - 1; i >= 0; --i, tblNum = 1 - tblNum) {
            idx = route[i];
            srcEntry = atomic_load(&tbl[tblNum][idx]);
            if (is_marked((void *) srcEntry)) {
                helpRelocate(tblNum, idx, false);
                srcEntry = atomic_load(&tbl[tblNum][idx]);
            }

            Entry *srcEntryAddr = extract_address(srcEntry);
            if (srcEntryAddr == NULL) {
                continue;
            }
            key = srcEntryAddr->key;
            uint32_t k_len = srcEntryAddr->k_len;
            dstIdx = (tblNum == FIRST ? hash2(key, k_len) : hash1(key, k_len));
            dstEntry = atomic_load(&tbl[1 - tblNum][dstIdx]);
            if (extract_address(dstEntry) != NULL) {
                startLevel = i + 1;
                idx = dstIdx;
                tblNum = 1 - tblNum;
                goto path_discovery;
            }
            helpRelocate(tblNum, idx, true);
        }
    }
    if (depth > max_path_len_l) max_path_len_l = depth;
    kick_path_len_accumulate_l += depth;
    return found;
}


bool lockFreeCuckoo::Insert(const char *key, uint32_t key_len, const char *address, uint32_t value_len) {
    Entry *newEntry = new Entry(key, key_len, address, value_len);
    Entry *ent1 = NULL, *ent2 = NULL;
    // shared_ptr<Entry<KeyType>> spent1(ent1);
    // shared_ptr<Entry<KeyType>> spent2(ent2);
    int cnt = 0;
    int h1 = hash1(key, key_len);
    int h2 = hash2(key, key_len);

    while (true) {
        int result = Find(key, key_len, &ent1, &ent2);
        //updating existing content
        if (result == FIRST) {
            cnt = get_cnt(ent1);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
            if (casResult == true)
                return false;
            else
                continue;
        }
        if (result == SECOND) {
            cnt = get_cnt(ent2);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
            if (casResult == true)
                return false;
            else
                continue;
        }
        // avoiding double duplicate instance of key
        // always insert to table 1 first.
        if (extract_address(ent1) == NULL && extract_address(ent2) == NULL) {
            cnt = get_cnt(ent1);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
            if (casResult == true)
                return true;
            else
                continue;
        }

        if (extract_address(ent1) == NULL) {
            cnt = get_cnt(ent1);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
            if (casResult == true)
                return true;
            else
                continue;
        }

        if (extract_address(ent2) == NULL) {
            cnt = get_cnt(ent2);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
            if (casResult == true)
                return true;
            else
                continue;
        }

        bool relocateResult = Relocate(FIRST, h1);
        if (relocateResult == true) {
            continue;
        } else {
            //TODO: rehash
            printf("insert %d failed! need rehash or resize!\n", key);
            assert(false);
            return false;
        }
    }
}


bool lockFreeCuckoo::Delete(const char *key, uint32_t key_len) {
    Entry *ent1 = NULL, *ent2 = NULL;
    int cnt = 0;
    int h1 = hash1(key, key_len);
    int h2 = hash2(key, key_len);
    while (true) {
        int result = Find(key, key_len, &ent1, &ent2);
        if (result == FIRST) {
            Entry *tmp = NULL;
            cnt = get_cnt(ent1);
            store_count(&tmp, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, tmp);
            if (casResult == true)
                return true;
            else
                continue;
        } else if (result == SECOND) {
            if (table1[h1] != ent1) {
                continue;
            }
            Entry *tmp = NULL;
            cnt = get_cnt(ent2);
            store_count(&tmp, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, tmp);
            if (casResult == true)
                return true;
            else
                continue;
        } else {
            return false;
        }
    }
}


bool lockFreeCuckoo::Contains(const char *key, uint32_t key_len) const {
    int h1 = hash1(key, key_len);
    int h2 = hash2(key, key_len);
    int cnt1, cnt2, ncnt1, ncnt2;
    while (true) {
        // 1st round
        // Looking in table 1
        Entry *ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        cnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->k_len, key, key_len))
            return true;
        // Looking in table 2
        Entry *ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        cnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->k_len, key, key_len))
            return true;
        // 2nd round
        ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        ncnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->k_len, key, key_len))
            return true;
        ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        ncnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->k_len, key, key_len))
            return true;

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return false;
    }
}

void lockFreeCuckoo::printTable() {
    printf("******************hash_table 1*****************\n");
    Entry *e, *tmp = NULL;
    for (int i = 0; i < t1Size; i++) {
        if (table1[i] != NULL) {
            e = atomic_load_explicit(&table1[i], memory_order_relaxed);
            tmp = extract_address(e);
            if (tmp != NULL)
                printf("%d\t%016lx\t%d\t%s\n", i, (long) e, tmp->key, tmp->value);
            else
                printf("%d\t%016lx\n", i, (long) e);
        } else {
            printf("%d\tNULL\n", i);
        }
    }
    printf("****************hash_table 2*******************\n");
    for (int i = 0; i < t2Size; i++) {
        if (table2[i] != NULL) {
            e = atomic_load_explicit(&table2[i], memory_order_relaxed);
            tmp = extract_address(e);
            if (tmp != NULL)
                printf("%d\t%016lx\t%d\t%s\n", i, (long) e, tmp->key, tmp->value);
            else
                printf("%d\t%016lx\n", i, (long) e);
        } else {
            printf("%d\tNULL\n", i);
        }
    }
    printf("\n");
}


bool lockFreeCuckoo::moveToKey(const char *searchKey, uint32_t key_len) {
    int h1 = hash1(searchKey, key_len);
    int h2 = hash2(searchKey, key_len);
    int cnt1, cnt2, ncnt1, ncnt2;
    while (true) {
        // 1st round
        // Looking in table 1
        Entry *ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        cnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->k_len, searchKey, key_len)) {
            this->cursor = ent1;
            return true;
        }
        // Looking in table 2
        Entry *ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        cnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->k_len, searchKey, key_len)) {
            this->cursor = ent2;
            return true;
        }

        // 2nd round
        ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        ncnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->k_len, searchKey, key_len)) {
            this->cursor = ent1;
            return true;
        }
        ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        ncnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->k_len, searchKey, key_len)) {
            this->cursor = ent2;
            return true;
        }

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return false;
    }
}


char *lockFreeCuckoo::nextValueAtKey() {
    if (this->cursor == NULL)
        return new char(0);
    return this->cursor->value;
}


size_t lockFreeCuckoo::getSize() {
    return t1Size + t2Size;
}

// Only for hash index to reSize and rehash
// 默认2个表分别为capacity/2

void lockFreeCuckoo::ensureCapacity(uint32_t capacity) {
    std::atomic<Entry *> *oldTable1 = table1;
    std::atomic<Entry *> *oldTable2 = table2;
    size_t old1 = t1Size;
    size_t old2 = t2Size;
    table1 = new std::atomic<Entry *>[capacity / 2];
    table2 = new std::atomic<Entry *>[capacity / 2];
    t1Size = capacity / 2;
    t2Size = capacity / 2;
    cursor = NULL;
    Entry *temp = NULL;
    for (int i = 0; i < t1Size; i++) {
        atomic_store(&table1[i], temp);
    }

    for (int i = 0; i < t2Size; i++) {
        atomic_store(&table2[i], temp);
    }

    // insert old value.
    for (int i = 0; i < old1; i++) {
        temp = extract_address(atomic_load_explicit(&oldTable1[i], memory_order_relaxed));
        if (temp)
            Insert(temp->key, temp->k_len, temp->value, temp->v_len);

    }
    for (int i = 0; i < old2; i++) {
        temp = extract_address(atomic_load_explicit(&oldTable2[i], memory_order_relaxed));
        if (temp)
            Insert(temp->key, temp->k_len, temp->value, temp->v_len);
    }

    delete oldTable1;
    delete oldTable2;
}

void lockFreeCuckoo::merge_info() {
    statistic_mtx.lock();
    if (max_path_len_l > max_path_len) max_path_len = max_path_len_l;
    kick_num += kick_num_l;
    kick_path_len_accumulate += kick_path_len_accumulate_l;
    statistic_mtx.unlock();
}

void lockFreeCuckoo::cal_info_and_show() {
    statistic_mtx.lock();
    size_t tmp = 0;
    for (size_t i = 0; i < t1Size; i++) {
        if (extract_address(table1[i].load()) != nullptr) {
            ++tmp;
        }
    }
    for (size_t i = 0; i < t2Size; i++) {
        if (extract_address(table2[i].load()) != nullptr) {
            ++tmp;
        }
    }
    kv_num = tmp;
    occupancy = kv_num * 1.0 / (t1Size + t2Size);

    avg_path_len = kick_path_len_accumulate * 1.0 / kick_num;

    cout << "kv in table " << kv_num << endl;
    cout << "occupancy " << occupancy << endl;
    cout << "kick num " << kick_num << endl;
    cout << "max path length " << max_path_len << endl;
    cout << "average kick path length " << avg_path_len << endl;
    cout << "   ------------  " << endl;

    statistic_mtx.unlock();
}

#endif //HASHCOMP_LOCKFREECUCKOO_H
