#ifndef CZL_CUCKOO_BUCKET_CONTAINER_HH
#define CZL_CUCKOO_BUCKET_CONTAINER_HH

#include <array>
#include <atomic>
#include <mutex>
#include <vector>

#include "hash_func.h"
#include "cuckoohash_config.hh"
#include "cuckoohash_config.hh"
#include "item.h"
#include "reclaimer_debra.h"
#include "entry_def.h"
#include "threadworkmanager.h"


namespace libcuckoo {

using Reclaimer = Reclaimer_debra;

struct str_equal_to {
    bool operator()(const char *first, size_t first_len, const char *second, size_t second_len) {
        if (first_len != second_len) return false;
        return !std::memcmp(first, second, first_len);
    }
};

struct str_hash {
    std::size_t operator()(const char *str, size_t n) const noexcept {
        return MurmurHash64A((void *) str, n);
    }
};

template<size_type SLOT_PER_BUCKET>
class Bucket_container {
public:
    Bucket_container(size_type hp, int cuckoo_thread_num) : rc(cuckoo_thread_num) {
        set_hashpower(hp);
        buckets_ = new Bucket[get_size()]();
    };

    ~Bucket_container() { destory_bc(); }

    void destory_bc() {
        bool still_have_item = false;
        for (size_type i = 0; i < get_size(); ++i) {
            for (size_type j = 0; j < SLOT_PER_BUCKET; j++) {
                if (buckets_[i].values_[j].load(std::memory_order_relaxed) != empty_entry) {
                    still_have_item = true;
                    break;
                }
            }
        }
        debug ASSERT(!still_have_item, "bucket still have item!");
        delete[]buckets_;
    }

    class Bucket {
    public:
        Bucket() {
            for (size_type i = 0; i < SLOT_PER_BUCKET; i++)
                values_[i].store((uint64_t) nullptr);
        };

        ~Bucket() = default;

        std::array<std::atomic<uint64_t>, SLOT_PER_BUCKET> values_;
    };

//        Bucket_container &operator[](size_type i) { return buckets_[i]; }
//
//        const Bucket_container &operator[](size_type i) const { return buckets_[i]; }

    void init_thread(int tid) {
        rc.initThread(tid);
    }

    inline size_type get_hashpower() const { return hashpower_; }

    inline void set_hashpower(size_type hp) { hashpower_ = hp; }

    inline size_type get_size() const { return size_type(1) << get_hashpower(); }

    inline size_type get_slot_num() const { return SLOT_PER_BUCKET * get_size(); }


    void startOp() { rc.startOp(cuckoo_tid); }

    void endOp() { rc.endOp(cuckoo_tid); }

    void deallocate(Item *ptr) { rc.deallocate(cuckoo_tid, ptr); }

    Item *allocate_item(char *key, size_t key_len, char *value, size_t value_len) {
        //Item * p = (Item * )malloc(key_len + value_len + 2 * sizeof(uint32_t));
        Item *p = (Item *) rc.allocate(cuckoo_tid, ITEM_LEN_ALLOC(key_len, value_len));
        debug ASSERT(p != nullptr, "malloc failure");
        p->key_len = key_len;
        p->value_len = value_len;
        memcpy(ITEM_KEY(p), key, key_len);
        memcpy(ITEM_VALUE(p), value, value_len);
        return p;
    }


    inline size_type read_from_bucket_slot(size_type ind, size_type slot) {
        return buckets_[ind].values_[slot].load();
    }

    //do insert: just input old_entry = empty_entry;
    //do erase: just input update_entry = empty_entry;
    //this function does not deallocate item
    bool try_update_entry(size_type ind, size_type slot, uint64_t old_entry, uint64_t update_entry) {
        Bucket &b = buckets_[ind];
        uint64_t old = b.values_[slot].load();
        if (old != old_entry) return false;
        if (b.values_[slot].compare_exchange_strong(old, update_entry)) {
            return true;
        } else {
            return false;
        }
    }

    bool unique_in_two_buckets(size_type i1, size_type slot_ind, size_type i2) {
        //uint64_t par_ptr =  buckets_.read_from_bucket_slot(i1,slot_ind);
        uint64_t check_entry = read_from_bucket_slot(i1, slot_ind);
        partial_t check_par = extract_partial(check_entry);
        Item *check_ptr = extract_ptr(check_entry);
        for (int i = 0; i < SLOT_PER_BUCKET; i++) {
            if (i == slot_ind) continue;
            //uint64_t com_par_ptr = buckets_.read_from_bucket_slot(i1,i);
            uint64_t com_entry = read_from_bucket_slot(i1, i);
            if (com_entry == (uint64_t) nullptr) continue;
            partial_t com_par = extract_partial(com_entry);
            Item *com_ptr = extract_ptr(com_entry);
            if (com_par != check_par) {
                continue;
            } else {
                if (str_equal_to()(ITEM_KEY(check_ptr), ITEM_KEY_LEN(check_ptr), ITEM_KEY(com_ptr),
                                   ITEM_KEY_LEN(com_ptr)))
                    return false;
            }
        }
        for (int i = 0; i < SLOT_PER_BUCKET; i++) {
            //uint64_t com_par_ptr = buckets_.read_from_bucket_slot(i2,i);
            uint64_t com_entry = read_from_bucket_slot(i2, i);
            if (com_entry == (uint64_t) nullptr) continue;
            partial_t com_par = extract_partial(com_entry);
            Item *com_ptr = extract_ptr(com_entry);
            if (com_par != check_par) {
                continue;
            } else {
                if (str_equal_to()(ITEM_KEY(check_ptr), ITEM_KEY_LEN(check_ptr), ITEM_KEY(com_ptr),
                                   ITEM_KEY_LEN(com_ptr)))
                    return false;
            }
        }
        return true;
    }

    void dump_two_buckets(size_type b1, size_type b2) {
        cout << "ERROR!!! dump duplicate keys" << endl;
        if (b1 > b2) swap(b1, b2);
        cout << "bucket 1 : " << b1 << endl;
        for (int i = 0; i < SLOT_PER_BUCKET; i++) {
            cout << "slot " << i << "\t";
            uint64_t entry = read_from_bucket_slot(b1, i);
            if (entry == empty_entry) {
                cout << "0" << endl;
            } else {
                Item *ptr = extract_ptr(entry);
                cout << "key " << *(uint64_t *) ITEM_KEY(ptr) << "\tvalue " << *(uint64_t *) ITEM_VALUE(ptr) << endl;
            }
        }
        cout << "bucket 2 : " << b2 << endl;
        for (int i = 0; i < SLOT_PER_BUCKET; i++) {
            cout << "slot " << i << "\t";
            uint64_t entry = read_from_bucket_slot(b2, i);
            if (entry == empty_entry) {
                cout << "0" << endl;
            } else {
                Item *ptr = extract_ptr(entry);
                cout << "key " << *(uint64_t *) ITEM_KEY(ptr) << "\tvalue " << *(uint64_t *) ITEM_VALUE(ptr) << endl;
            }
        }
    }

    //for debug
    struct hash_value {
        size_type hash;
        partial_t partial;
    };

    static partial_t partial_key(const size_type hash) {
        const uint64_t hash_64bit = hash;
        const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                     static_cast<uint32_t>(hash_64bit >> 32));
        const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                     static_cast<uint16_t>(hash_32bit >> 16));
        return hash_16bit;
    }

    hash_value hashed_key(const char *key, size_type key_len) const {
        const size_type hash = str_hash()(key, key_len);
        return {hash, partial_key(hash)};
    }

    class TwoBuckets {
    public:
        TwoBuckets() = default;

        TwoBuckets(size_type i1_, size_type i2_)
                : i1(i1_), i2(i2_) {}

        size_type i1{}, i2{};

    };

    static inline size_type hashsize(const size_type hp) { return size_type(1) << hp; }

    static inline size_type hashmask(const size_type hp) { return hashsize(hp) - 1; }

    static inline size_type index_hash(const size_type hp, const size_type hash) {
        return hash & hashmask(hp);
    }

    static inline size_type alt_index(const size_type hp, const partial_t partial,
                                      const size_type index) {
        // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
        // hash constant from 64-bit MurmurHash2
        const size_type nonzero_tag = static_cast<size_type>(partial) + 1;
        return (index ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & hashmask(hp);
    }

    TwoBuckets get_two_buckets(const hash_value &hv) const {
        const size_type hp = get_hashpower();
        const size_type i1 = index_hash(hp, hv.hash);
        const size_type i2 = alt_index(hp, hv.partial, i1);
        return TwoBuckets(i1, i2);
    }
    //for debug

    void check_in_correct_position(Item *ptr, size_type index) {
        const hash_value hv = hashed_key(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr));
        TwoBuckets b = get_two_buckets(hv);
        if (index != b.i1 && index != b.i2) {
            cout << "ERROR!!!key in wrong position" << endl;
            cout << "key " << *(uint64_t *) ITEM_KEY(ptr) << "value " << *(uint64_t *) ITEM_VALUE(ptr) << endl;
            cout << "should in " << b.i1 << " " << b.i2 << endl;
            cout << "result in " << index << endl;
        }
    }

    //Count the number of items only if strong is true
    uint64_t get_item_num_and_check() {
        table_anaz_mtx.lock();
        uint64_t count = 0;
        for (size_t i = 0; i < get_size(); i++) {
            Bucket &b = buckets_[i];
            for (size_t j = 0; j < SLOT_PER_BUCKET; j++) {
                uint64_t entry = b.values_[j].load(std::memory_order_relaxed);
                debug ASSERT(!is_kick_marked(entry), "ERROR!!!There are still marked entries in the table!");
                if (entry != empty_entry) {
                    count++;
                    debug {
                        partial_t partial = extract_partial(entry);
                        const size_type nonzero_tag = static_cast<size_type>(partial) + 1;
                        size_type alt_i = (i ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & ((size_type(1) << hashpower_) - 1);
                        if (!unique_in_two_buckets(i, j, alt_i)) {
                            dump_two_buckets(i, alt_i);
                        }
                        check_in_correct_position(extract_ptr(entry), i);
                    }

                }
            }
        }
        tmp_kv_num_store_ = count;
        table_anaz_mtx.unlock();
        return count;
    }

    void get_key_position_info(std::vector<double> &kpv, size_type item_num) {
        table_anaz_mtx.lock();
        debug ASSERT(kpv.size() == SLOT_PER_BUCKET, "key_position_info length error");
        std::vector<uint64_t> count_vtr(SLOT_PER_BUCKET);
        for (size_type i = 0; i < get_size(); i++) {
            Bucket &b = buckets_[i];
            for (int j = 0; j < SLOT_PER_BUCKET; j++) {
                if (b.values_[j].load(std::memory_order_relaxed) != empty_entry) {
                    count_vtr[j]++;
                }
            }
        }

        for (size_type i = 0; i < SLOT_PER_BUCKET; i++) {
            kpv[i] = count_vtr[i] * 1.0 / item_num;
        }
        table_anaz_mtx.unlock();
    }

private:
    size_type hashpower_;

    Bucket *buckets_;

    Reclaimer rc;

    std::mutex table_anaz_mtx;

    size_type tmp_kv_num_store_;

};

}

#endif //CZL_CUCKOO_BUCKET_CONTAINER_HH
