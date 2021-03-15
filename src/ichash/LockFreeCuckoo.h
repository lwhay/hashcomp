//
// Created by iclab on 3/7/21.
//

#ifndef HASHCOMP_LOCKFREECUCKOO_H
#define HASHCOMP_LOCKFREECUCKOO_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <atomic>
#include <thread>
#include <memory>

using namespace std;

/*---------------------------------------------------------------------------*/

/*template<class KeyType>
class DefaultKeyComparator {
public:
// Compare a and b. Return a negative value if a is less than b, 0 if they
// are equal, and a positive value if a is greater than b
    // virtual int operator()(const KeyType &a, const KeyType &b) const = 0;
    int operator()(const KeyType &a, const KeyType &b) const {
        if (a < b)
            return -1;
        if (a == b)
            return 0;
        return 1;
    }
};*/

template<class KeyType>
class StrComparator {
public:
    int operator()(const KeyType &a, uint32_t kl_a, const KeyType &b, uint32_t kl_b) const {
        if (kl_a != kl_b) return false;
        return std::memcmp((char *) a, (char *) b, kl_a);
    }
};

template<class KeyType>
class Entry {
public:
    KeyType key;
    char *value;
    uint32_t klen;
    uint32_t vlen;

    Entry<KeyType>(const KeyType &k, uint32_t k_len, const char *v, uint32_t v_len);

    ~Entry<KeyType>();
};

template<class KeyType>
Entry<KeyType>::Entry(const KeyType &k, uint32_t k_len, const char *v, uint32_t v_len) : klen(k_len), vlen(v_len) {
    key = static_cast<KeyType>(std::malloc(klen));
    value = static_cast<char *>(std::malloc(vlen));
    std::memcpy(key, k, klen);
    std::memcpy(value, v, vlen);
    klen = k_len;
    vlen = v_len;
}

template<class KeyType>
Entry<KeyType>::~Entry() {
    std::free(key);
    std::free(value);
}

template<class KeyType>
class LockFreeCuckoo {
private:
    Entry<KeyType> *cursor;
    static const int FIRST = 0;
    static const int SECOND = 1;
    static const int NIL = -1;
    // static const char *NP = (char *) 0;

    std::atomic<Entry<KeyType> *> *table1;
    std::atomic<Entry<KeyType> *> *table2;
    size_t t1Size;
    size_t t2Size;

public:
    LockFreeCuckoo(size_t size1, size_t size2);

    ~LockFreeCuckoo();

    char *Search(KeyType key, uint32_t k_len);

    bool Insert(const KeyType &key, uint32_t k_len, const char *address, uint32_t v_len);

    bool Delete(const KeyType &key, uint32_t k_len);

    bool Contains(const KeyType &key, uint32_t k_len) const;

    bool moveToKey(const KeyType &searchKey, uint32_t k_len);

    char *nextValueAtKey();

    size_t getSize();

    void ensureCapacity(uint32_t capacity);    // resize

    void printTable();

private:
    void init();

    int Find(KeyType key, uint32_t k_len, Entry<KeyType> **ent1, Entry<KeyType> **ent2);

    bool Relocate(int tableNum, int pos);

    void helpRelocate(int table, int idx, bool initiator);

    void deleteDup(int idx1, Entry<KeyType> *ent1, int idx2, Entry<KeyType> *ent2);

    int hash1(const KeyType key, uint32_t k_len) const;

    int hash2(const KeyType key, uint32_t k_len) const;

private:
    // KeyComparator不需要大家实现，但是key的比较需要由compare_来完成
    StrComparator<KeyType> /*const*/ compare_;
};


//-----
// for template class, all in .h files.

// a 64-bits pointer only use low 48-bits
// we use high 16-bits to store the counter
inline int get_cnt(void *ptr) {
    unsigned long a = ((unsigned long) ptr & (0xffff000000000000));
    return a >> 48;
}

template<class KeyType>
inline void inc_counter(Entry<KeyType> **ptr) {
    *ptr = (Entry<KeyType> *) ((unsigned long) *ptr + 0x0001000000000000);
}

template<class KeyType>
inline void store_count(Entry<KeyType> **ptr, int cnt) {
    unsigned long new_cnt = (unsigned long) cnt << 48;
    *ptr = (Entry<KeyType> *) ((unsigned long) *ptr & 0x0000FFFFFFFFFFFF);
    *ptr = (Entry<KeyType> *) ((unsigned long) *ptr + new_cnt);
}

// the last 2-bits of Entrypointer is always zero.
template<class KeyType>
inline Entry<KeyType> *extract_address(Entry<KeyType> *e) {
    e = (Entry<KeyType> *) ((unsigned long) e & (0x0000fffffffffffc));
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

template<class KeyType>
LockFreeCuckoo<KeyType>::LockFreeCuckoo(size_t size1, size_t size2) : t1Size(size1), t2Size(size2) {
    table1 = new std::atomic<Entry<KeyType> *>[size1];
    table2 = new std::atomic<Entry<KeyType> *>[size2];
    cursor = NULL;
    init();
}

template<class KeyType>
LockFreeCuckoo<KeyType>::~LockFreeCuckoo() {
    cout << table1 << endl;
    delete table1;
    delete table2;
}

template<class KeyType>
void LockFreeCuckoo<KeyType>::init() {
    Entry<KeyType> *temp = NULL;
    for (int i = 0; i < t1Size; i++) {
        atomic_store(&table1[i], temp);
    }

    for (int i = 0; i < t2Size; i++) {
        atomic_store(&table2[i], temp);
    }
}

template<class KeyType>
int LockFreeCuckoo<KeyType>::hash1(const KeyType keyIn, uint32_t k_len) const {
    /*int num = 0x27d4eb2d; // a prime or an odd constant
    KeyType key = keyIn;
    key = (key ^ 61) ^ (key >> 16);
    key = key + (key << 3);
    key = key ^ (key >> 4);
    key = key * num;
    key = key ^ (key >> 15);
    return key % t1Size;*/
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

template<class KeyType>
int LockFreeCuckoo<KeyType>::hash2(const KeyType keyIn, uint32_t k_len) const {
    /*KeyType key = keyIn;
    key = ((key >> 16) ^ key) * 0x45d9f3b;
    key = ((key >> 16) ^ key) * 0x45d9f3b;
    key = (key >> 16) ^ key;
    return key % t2Size;*/
    const char *key = (const char *) keyIn;
    constexpr uint32_t kHashSeed = 16785407;
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

    return h % t2Size;
}

template<class KeyType>
void LockFreeCuckoo<KeyType>::helpRelocate(int which, int idx, bool initiator) {
    Entry<KeyType> *srcEntry, *dstEntry, *tmpEntry;
    int dstIdx, nCnt, cnt1, cnt2, size[2];
    atomic<Entry<KeyType> *> *tbl[2];
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
            tmpEntry = (Entry<KeyType> *) ((unsigned long) srcEntry | 1);
            atomic_compare_exchange_strong(&(tbl[which][idx]), &srcEntry, tmpEntry);
            srcEntry = atomic_load_explicit(&tbl[which][idx], memory_order_seq_cst);
        }
        if (!(is_marked((void *) srcEntry)))
            return;

        Entry<KeyType> *ent = extract_address(srcEntry);
        dstIdx = (which == FIRST ? hash2(ent->key, ent->klen) : hash1(ent->key, ent->klen));
        dstEntry = atomic_load_explicit(&tbl[1 - which][dstIdx], memory_order_seq_cst);
        if (extract_address(dstEntry) == NULL) {
            cnt1 = get_cnt((void *) srcEntry);
            cnt2 = get_cnt((void *) dstEntry);
            nCnt = cnt1 > cnt2 ? cnt1 + 1 : cnt2 + 1;
            if (srcEntry != atomic_load_explicit(&tbl[which][idx], memory_order_seq_cst))
                continue;

            Entry<KeyType> *tmpSrcEntry = srcEntry;
            // unmark the flag
            tmpSrcEntry = (Entry<KeyType> *) ((unsigned long) srcEntry & ~1);
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
        tmpEntry = (Entry<KeyType> *) ((unsigned long) srcEntry & (~1));
        store_count(&tmpEntry, cnt1 + 1);
        atomic_compare_exchange_strong(&(tbl[which][idx]), &srcEntry, tmpEntry);
        return;
    }
}

template<class KeyType>
void LockFreeCuckoo<KeyType>::deleteDup(int idx1, Entry<KeyType> *ent1, int idx2, Entry<KeyType> *ent2) {
    Entry<KeyType> *tmp1, *tmp2;
    KeyType key1, key2;
    int cnt;
    tmp1 = atomic_load(&table1[idx1]);
    tmp2 = atomic_load(&table2[idx2]);
    if ((ent1 != tmp1) && (ent2 != tmp2))
        return;
    Entry<KeyType> *e1 = extract_address(ent1);
    Entry<KeyType> *e2 = extract_address(ent2);
    if (compare_(e1->key, e1->klen, e2->key, e2->klen))
        return;
    tmp2 = NULL;
    cnt = get_cnt(ent2);
    store_count(&tmp2, cnt + 1);
    atomic_compare_exchange_strong(&(table2[idx2]), &ent2, tmp2);
}

template<class KeyType>
char *LockFreeCuckoo<KeyType>::Search(KeyType key, uint32_t k_len) {
    int h1 = hash1(key, k_len);
    int h2 = hash2(key, k_len);
    int cnt1, cnt2, ncnt1, ncnt2;
    while (true) {
        // 1st round
        // Looking in table 1
        Entry<KeyType> *ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        cnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->klen, key, k_len))
            return ent1->value;
        // Looking in table 2
        Entry<KeyType> *ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        cnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->klen, key, k_len))
            return ent2->value;

        // 2nd round
        ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        ncnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->klen, key, k_len))
            return ent1->value;
        ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        ncnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->klen, key, k_len))
            return ent2->value;

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return nullptr/*NIL*/;
    }
}

template<class KeyType>
int LockFreeCuckoo<KeyType>::Find(KeyType key, uint32_t k_len, Entry<KeyType> **ent1, Entry<KeyType> **ent2) {
    int h1 = hash1(key, k_len);
    int h2 = hash2(key, k_len);
    int result = NIL;
    int cnt1, cnt2, ncnt1, ncnt2;
    Entry<KeyType> *e;
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
            } else if (!compare_(e->key, e->klen, key, k_len))
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
            if (!compare_(e->key, e->klen, key, k_len)) {
                if (result == FIRST) {
                    // printf("Find(): Delete_dup() 1\n");
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
            } else if (!compare_(e->key, e->klen, key, k_len))
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
            if (!compare_(e->key, e->klen, key, k_len)) {
                if (result == FIRST) {
                    printf("Find(): Delete_dup() 2\n");
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

template<class KeyType>
bool LockFreeCuckoo<KeyType>::Relocate(int which, int index) {
    int threshold = /*t1Size + t2Size*/65536;
    int route[threshold]; // store cuckoo path
    int startLevel = 0, tblNum = which;
    KeyType key;
    int idx = index, preIdx = 0;
    Entry<KeyType> *curEntry = NULL;
    Entry<KeyType> *preEntry = NULL;
    atomic<Entry<KeyType> *> *tbl[2];
    tbl[0] = table1;
    tbl[1] = table2;

    //discovery cuckoo path
    path_discovery:
    bool found = false;
    int depth = startLevel;
    do {
        assert(tbl[tblNum] == table1 || tbl[tblNum] == table2);
        assert(tblNum == 0 || tblNum == 1);
        assert(idx >= 0 && idx < t1Size);
        curEntry = atomic_load(&tbl[tblNum][idx]);
        while (is_marked((void *) curEntry)) {
            helpRelocate(tblNum, idx, false);
            curEntry = atomic_load(&tbl[tblNum][idx]);
        }

        Entry<KeyType> *preEntAddr, *curEntAddr;
        preEntAddr = extract_address(preEntry);
        curEntAddr = extract_address(curEntry);
        if (curEntAddr != NULL && preEntAddr != NULL) {
            if (preEntry == curEntry ||
                !compare_(preEntAddr->key, preEntAddr->klen, curEntAddr->key, curEntAddr->klen)) {
                if (tblNum == FIRST)
                    deleteDup(idx, curEntry, preIdx, preEntry);
                else
                    deleteDup(preIdx, preEntry, idx, curEntry);
            }
        }
        // not an empty slot, continue discovery
        if (curEntAddr != NULL) {
            route[depth] = idx;
            preEntry = curEntry;
            preIdx = idx;
            tblNum = 1 - tblNum; // change to another table
            idx = (tblNum == FIRST ? hash1(curEntAddr->key, curEntAddr->klen) : hash2(curEntAddr->key,
                                                                                      curEntAddr->klen));
        }
            // find an empty slot
        else {
            found = true;
        }
    } while (!found && ++depth < threshold);

    // insert key
    if (found) {
        Entry<KeyType> *srcEntry, *dstEntry;
        int dstIdx;

        tblNum = 1 - tblNum;
        for (int i = depth - 1; i >= 0; --i, tblNum = 1 - tblNum) {
            idx = route[i];
            srcEntry = atomic_load(&tbl[tblNum][idx]);
            if (is_marked((void *) srcEntry)) {
                helpRelocate(tblNum, idx, false);
                srcEntry = atomic_load(&tbl[tblNum][idx]);
            }

            Entry<KeyType> *srcEntryAddr = extract_address(srcEntry);
            if (srcEntryAddr == NULL) {
                continue;
            }
            dstIdx = (tblNum == FIRST ? hash2(srcEntryAddr->key, srcEntryAddr->klen) : hash1(srcEntryAddr->key,
                                                                                             srcEntryAddr->klen));
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
    return found;
}

template<class KeyType>
bool LockFreeCuckoo<KeyType>::Insert(const KeyType &key, uint32_t klen, const char *address, uint32_t vlen) {
    Entry<KeyType> *newEntry = new Entry<KeyType>(key, klen, address, vlen);
    Entry<KeyType> *ent1 = NULL, *ent2 = NULL;
    // shared_ptr<Entry<KeyType>> spent1(ent1);
    // shared_ptr<Entry<KeyType>> spent2(ent2);
    int cnt = 0;
    int h1 = hash1(key, klen);
    int h2 = hash2(key, klen);

    // for (int i = 0; i < t1Size; i++) atomic_load(&table1[i]);
    while (true) {
        int result = Find(key, klen, &ent1, &ent2);
        //updating existing content
        if (result == FIRST) {
            cnt = get_cnt(ent1);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table1[h1]), &ent1, newEntry);
            if (casResult == true)
                return true;
            else
                continue;
        }
        if (result == SECOND) {
            cnt = get_cnt(ent2);
            store_count(&newEntry, cnt + 1);
            bool casResult = atomic_compare_exchange_strong(&(table2[h2]), &ent2, newEntry);
            if (casResult == true)
                return true;
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
            return false;
        }
    }
}


template<class KeyType>
bool LockFreeCuckoo<KeyType>::Delete(const KeyType &key, uint32_t klen) {
    Entry<KeyType> *ent1 = NULL, *ent2 = NULL;
    int cnt = 0;
    int h1 = hash1(key, klen);
    int h2 = hash2(key, klen);
    while (true) {
        int result = Find(key, klen, &ent1, &ent2);
        if (result == FIRST) {
            Entry<KeyType> *tmp = NULL;
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
            Entry<KeyType> *tmp = NULL;
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

template<class KeyType>
bool LockFreeCuckoo<KeyType>::Contains(const KeyType &key, uint32_t klen) const {
    int h1 = hash1(key, klen);
    int h2 = hash2(key, klen);
    int cnt1, cnt2, ncnt1, ncnt2;
    while (true) {
        // 1st round
        // Looking in table 1
        Entry<KeyType> *ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        cnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->klen, key, klen))
            return true;
        // Looking in table 2
        Entry<KeyType> *ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        cnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->klen, key, klen))
            return true;
        // 2nd round
        ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        ncnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->klen, key, klen))
            return true;
        ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        ncnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->klen, key, klen))
            return true;

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return false;
    }
}


template<class KeyType>
void LockFreeCuckoo<KeyType>::printTable() {
    printf("******************hash_table 1*****************\n");
    Entry<KeyType> *e, *tmp = NULL;
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

template<class KeyType>
bool LockFreeCuckoo<KeyType>::moveToKey(const KeyType &searchKey, uint32_t klen) {
    int h1 = hash1(searchKey, klen);
    int h2 = hash2(searchKey, klen);
    int cnt1, cnt2, ncnt1, ncnt2;
    while (true) {
        // 1st round
        // Looking in table 1
        Entry<KeyType> *ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        cnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->klen, searchKey, klen)) {
            this->cursor = ent1;
            return true;
        }
        // Looking in table 2
        Entry<KeyType> *ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        cnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->klen, searchKey, klen)) {
            this->cursor = ent2;
            return true;
        }

        // 2nd round
        ent1 = atomic_load_explicit(&table1[h1], memory_order_relaxed);
        ncnt1 = get_cnt(ent1);
        ent1 = extract_address(ent1);
        if (ent1 != NULL && !compare_(ent1->key, ent1->klen, searchKey, klen)) {
            this->cursor = ent1;
            return true;
        }
        ent2 = atomic_load_explicit(&table2[h2], memory_order_relaxed);
        ncnt2 = get_cnt(ent2);
        ent2 = extract_address(ent2);
        if (ent2 != NULL && !compare_(ent2->key, ent2->klen, searchKey, klen)) {
            this->cursor = ent2;
            return true;
        }

        if (checkCounter(cnt1, cnt2, ncnt1, ncnt2))
            continue;
        else
            return false;
    }
}

template<class KeyType>
char *LockFreeCuckoo<KeyType>::nextValueAtKey() {
    if (this->cursor == NULL)
        return new char(0);
    return this->cursor->value;
}

template<class KeyType>
size_t LockFreeCuckoo<KeyType>::getSize() {
    return t1Size + t2Size;
}

// Only for hash index to reSize and rehash
// 默认2个表分别为capacity/2
template<class KeyType>
void LockFreeCuckoo<KeyType>::ensureCapacity(uint32_t capacity) {
    std::atomic<Entry<KeyType> *> *oldTable1 = table1;
    std::atomic<Entry<KeyType> *> *oldTable2 = table2;
    size_t old1 = t1Size;
    size_t old2 = t2Size;
    table1 = new std::atomic<Entry<KeyType> *>[capacity / 2];
    table2 = new std::atomic<Entry<KeyType> *>[capacity / 2];
    t1Size = capacity / 2;
    t2Size = capacity / 2;
    cursor = NULL;
    Entry<KeyType> *temp = NULL;
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
            Insert(temp->key, temp->klen, temp->value, temp->vlen);

    }
    for (int i = 0; i < old2; i++) {
        temp = extract_address(atomic_load_explicit(&oldTable2[i], memory_order_relaxed));
        if (temp)
            Insert(temp->key, temp->klen, temp->value, temp->vlen);
    }

    delete oldTable1;
    delete oldTable2;
}

#endif //HASHCOMP_LOCKFREECUCKOO_H
