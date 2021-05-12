#ifndef CZL_CUCKOO_CUCKOO_MAP_H
#define CZL_CUCKOO_CUCKOO_MAP_H

#include <cstring>
#include <memory>

#include "bucket_container.hh"

namespace libcuckoo {

template<class KeyEqual = str_equal_to,
        class Hash = str_hash,
        std::size_t SLOT_PER_BUCKET = DEFAULT_SLOT_PER_BUCKET>
class Cuckoohash_map {
private:

    using BC = Bucket_container<SLOT_PER_BUCKET>;
    using Bucket = typename BC::Bucket;
    using TWM = ThreadWorkManager;

    enum cuckoo_status {
        key_hit,
        key_miss,
        key_miss_with_empty_slot,
        kick_parameter
    };

    struct hash_value {
        size_type hash;
        partial_t partial;
    };

    struct table_position {
        size_type index;
        int slot;
        uint64_t entry;
        cuckoo_status status;
    };

    static partial_t partial_key(const size_type hash) {
        const uint64_t hash_64bit = hash;
        const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                     static_cast<uint32_t>(hash_64bit >> 32));
        const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                     static_cast<uint16_t>(hash_32bit >> 16));
        return hash_16bit;
    }

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

    hash_value hashed_key(const char *key, size_type key_len) const {
        const size_type hash = Hash()(key, key_len);
        return {hash, partial_key(hash)};
    }

    class TwoBuckets {
    public:
        TwoBuckets() = default;

        TwoBuckets(size_type i1_, size_type i2_)
                : i1(i1_), i2(i2_) {}

        size_type i1{}, i2{};

    };

    class RegisterHolder {
        friend class Cuckoohash_map<>;

        RegisterHolder(TWM &twm, uint64_t hash) : twm_(twm) {
            twm_.register_hash(hash);
        }

        ~RegisterHolder() {
            twm_.cancellate_hash();
        }

        TWM &twm_;
    };

    class KickInfoHolder {
        friend class Cuckoohash_map<>;

        KickInfoHolder(TWM &twm, partial_t par, uint64_t bucket, uint64_t slot) : twm_(twm) {
            twm_.store_kick_info(par, bucket, slot);
        }

        ~KickInfoHolder() {
            twm_.clean_kick_info();
        }

        TWM &twm_;
    };

    class EpochManager {
        friend class Cuckoohash_map<>;

        explicit EpochManager(BC &bc) : bc_(bc) {
            bc_.startOp();
        }

        ~EpochManager() {
            bc_.endOp();
        }

        BC &bc_;
    };

    typedef struct {
        size_type bucket;
        size_type slot;
        hash_value hv;
    } CuckooRecord;

    static constexpr uint8_t MAX_BFS_PATH_LEN = 5;

    // An array of CuckooRecords
    using CuckooRecords = std::array<CuckooRecord, MAX_BFS_PATH_LEN>;

    // A constexpr version of pow that we can use for various compile-time
    // constants and checks.
    static constexpr size_type const_pow(size_type a, size_type b) {
        return (b == 0) ? 1 : a * const_pow(a, b - 1);
    }

    // b_slot holds the information for a BFS path through the table.
    struct b_slot {
        // The bucket of the last item in the path.
        size_type bucket;
        // a compressed representation of the slots for each of the buckets in
        // the path. pathcode is sort of like a base-slot_per_bucket number, and
        // we need to hold at most MAX_BFS_PATH_LEN slots. Thus we need the
        // maximum pathcode to be at least slot_per_bucket()^(MAX_BFS_PATH_LEN).
        uint16_t pathcode;
        //TODO directly comment , maybe existing problem
//            static_assert(const_pow(slot_per_bucket(), MAX_BFS_PATH_LEN) <
//                          std::numeric_limits<decltype(pathcode)>::max(),
//                          "pathcode may not be large enough to encode a cuckoo "
//                          "path");
        // The 0-indexed position in the cuckoo path this slot occupies. It must
        // be less than MAX_BFS_PATH_LEN, and also able to hold negative values.
        int8_t depth;
        static_assert(MAX_BFS_PATH_LEN - 1 <=
                      std::numeric_limits<decltype(depth)>::max(),
                      "The depth type must able to hold a value of"
                      " MAX_BFS_PATH_LEN - 1");
        static_assert(-1 >= std::numeric_limits<decltype(depth)>::min(),
                      "The depth type must be able to hold a value of -1");

        b_slot() {}

        b_slot(const size_type b, const uint16_t p, const decltype(depth) d)
                : bucket(b), pathcode(p), depth(d) {
            assert(d < MAX_BFS_PATH_LEN);
        }
    };

    // b_queue is the queue used to store b_slots for BFS cuckoo hashing.
    class b_queue {
    public:
        b_queue() noexcept : first_(0), last_(0) {}

        void enqueue(b_slot x) {
            assert(!full());
            slots_[last_++] = x;
        }

        b_slot dequeue() {
            assert(!empty());
            assert(first_ < last_);
            b_slot &x = slots_[first_++];
            return x;
        }

        bool empty() const { return first_ == last_; }

        bool full() const { return last_ == MAX_CUCKOO_COUNT; }

    private:
        // The size of the BFS queue. It holds just enough elements to fulfill a
        // MAX_BFS_PATH_LEN search for two starting buckets, with no circular
        // wrapping-around. For one bucket, this is the geometric sum
        // sum_{k=0}^{MAX_BFS_PATH_LEN-1} slot_per_bucket()^k
        // = (1 - slot_per_bucket()^MAX_BFS_PATH_LEN) / (1 - slot_per_bucket())
        //
        // Note that if slot_per_bucket() == 1, then this simply equals
        // MAX_BFS_PATH_LEN.

        //TODO directly comment , maybe existing problem
//            static_assert(slot_per_bucket() > 0,
//                          "SLOT_PER_BUCKET must be greater than 0.");
//            static constexpr size_type MAX_CUCKOO_COUNT =
//                    2 * ((slot_per_bucket() == 1)
//                         ? MAX_BFS_PATH_LEN
//                         : (const_pow(slot_per_bucket(), MAX_BFS_PATH_LEN) - 1) /
//                           (slot_per_bucket() - 1));
        static const int MAX_CUCKOO_COUNT = 682;
        // An array of b_slots. Since we allocate just enough space to complete a
        // full search, we should never exceed the end of the array.
        b_slot slots_[MAX_CUCKOO_COUNT];
        // The index of the head of the queue in the array
        size_type first_;
        // One past the index of the last_ item of the queue in the array.
        size_type last_;
    };

    TwoBuckets get_two_buckets(const hash_value &hv) const {
        const size_type hp = hashpower();
        const size_type i1 = index_hash(hp, hv.hash);
        const size_type i2 = alt_index(hp, hv.partial, i1);
        return TwoBuckets(i1, i2);
    }

    int try_read_from_bucket(size_type bucket_index, const partial_t partial,
                             const char *key, size_type key_len, uint64_t &ret_entry) const {

        for (int i = 0; i < SLOT_PER_BUCKET; ++i) {

            uint64_t entry = bc.read_from_bucket_slot(bucket_index, i);
            if (entry == 0ul) continue;

            Item *read_ptr = extract_ptr(entry);

            if (is_kick_marked(entry) || partial == extract_partial(entry)) {
                if (KeyEqual()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), key, key_len)) {
                    ret_entry = entry;
                    return i;
                }
            }

        }
        return -1;
    }

    table_position Find_find(const char *key, size_type key_len, const partial_t partial,
                             const size_type i1, const size_type i2) const {
        uint64_t entry = 0ll;
        int slot = try_read_from_bucket(i1, partial, key, key_len, entry);
        if (slot != -1) {
            return table_position{i1, slot, entry, key_hit};
        }
        slot = try_read_from_bucket(i2, partial, key, key_len, entry);
        if (slot != -1) {
            return table_position{i2, slot, entry, key_hit};
        }
        return table_position{0, 0, 0, key_miss};
    }

    //false : key_duplicated. the slot is the position of deplicated key
    //true : have empty slot.the slot is the position of empty slot. -1 -> no empty slot
    bool try_find_insert_bucket(size_type bucket_index, int &slot,
                                const partial_t partial,
                                const char *key, size_type key_len,
                                uint64_t &ret_entry) const {
        slot = -1;
        for (int i = 0; i < SLOT_PER_BUCKET; ++i) {

            uint64_t entry = bc.read_from_bucket_slot(bucket_index, i);
            if (entry == empty_entry) {
                slot = slot == -1 ? i : slot;
                continue;
            }

            Item *read_ptr = extract_ptr(entry);

            if (is_kick_marked(entry) || partial == extract_partial(entry)) {
                if (KeyEqual()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), key, key_len)) {
                    slot = i;
                    ret_entry = entry;
                    return false;
                }
            }

        }
        return true;
    }

    // slot_search searches for a cuckoo path using breadth-first search. It
    // starts with the i1 and i2 buckets, and, until it finds a bucket with an
    // empty slot, adds each slot of the bucket in the b_slot. If the queue runs
    // out of space, it fails.
    //
    // throws hashpower_changed if it changed during the search
    b_slot slot_search(const size_type hp, const size_type i1, const size_type i2) {
        b_queue q;
        // The initial pathcode informs cuckoopath_search which bucket the path
        // starts on
        q.enqueue(b_slot(i1, 0, 0));
        q.enqueue(b_slot(i2, 1, 0));
        while (!q.empty()) {
            b_slot x = q.dequeue();
            //Bucket &b = bc[x.bucket];
            // Picks a (sort-of) random slot to start from
            size_type starting_slot = x.pathcode % SLOT_PER_BUCKET;
            for (size_type i = 0; i < SLOT_PER_BUCKET; ++i) {
                uint16_t slot = (starting_slot + i) % SLOT_PER_BUCKET;


                uint64_t entry = bc.read_from_bucket_slot(x.bucket, slot);
                if (entry == empty_entry) {
                    // We can terminate the search here
                    x.pathcode = x.pathcode * SLOT_PER_BUCKET + slot;
                    return x;
                }

                partial_t kick_partial;
                if (!is_kick_marked(entry)) {
                    kick_partial = extract_partial(entry);
                } else {
                    Item *ptr = extract_ptr(entry);
                    kick_partial = hashed_key(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr)).partial;
                }

                // If x has less than the maximum number of path components,
                // have come from if we kicked out the item at this slot.
                // create a new b_slot item, that represents the bucket we would
                //const partial_t partial = b.partial(slot);
                if (x.depth < MAX_BFS_PATH_LEN - 1) {
                    assert(!q.full());
                    b_slot y(alt_index(hp, kick_partial, x.bucket),
                             x.pathcode * SLOT_PER_BUCKET + slot, x.depth + 1);
                    q.enqueue(y);
                }
            }
        }
        // We didn't find a short-enough cuckoo path, so the search terminated.
        // Return a failure value.
        return b_slot(0, 0, -1);
    }

    int cuckoopath_search(const size_type hp, CuckooRecords &cuckoo_path, const size_type i1, const size_type i2) {
        b_slot x = slot_search(hp, i1, i2);
        if (x.depth == -1) {
            return -1;
        }
        // Fill in the cuckoo path slots from the end to the beginning.
        for (int i = x.depth; i >= 0; i--) {
            cuckoo_path[i].slot = x.pathcode % SLOT_PER_BUCKET;
            x.pathcode /= SLOT_PER_BUCKET;
        }

        CuckooRecord &first = cuckoo_path[0];
        if (x.pathcode == 0) {
            first.bucket = i1;
        } else {
            debug assert(x.pathcode == 1);
            first.bucket = i2;
        }

        {
            uint64_t first_entry = bc.read_from_bucket_slot(first.bucket, first.slot);
            if (first_entry == empty_entry) {
                // We can terminate here
                return 0;
            }

            Item *ptr = extract_ptr(first_entry);
            debug ASSERT(ptr != nullptr, "ptr null !")
            first.hv = hashed_key(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr));

            debug {
                size_type b1 = index_hash(hp, first.hv.hash);
                size_type b2 = alt_index(hp, first.hv.partial, index_hash(hp, first.hv.hash));
                ASSERT(first.bucket == b1 || first.bucket == b2, "path error")
            }
        }

        for (int i = 1; i <= x.depth; ++i) {
            CuckooRecord &curr = cuckoo_path[i];
            const CuckooRecord &prev = cuckoo_path[i - 1];
            debug {
                size_type b1 = index_hash(hp, prev.hv.hash);
                size_type b2 = alt_index(hp, prev.hv.partial, index_hash(hp, prev.hv.hash));
                ASSERT(prev.bucket == b1 || prev.bucket == b2, "kick path error")
            }

            // We get the bucket that this slot is on by computing the alternate
            // index of the previous bucket
            curr.bucket = alt_index(hp, prev.hv.partial, prev.bucket);
            //const auto lock_manager = lock_one(hp, curr.bucket, TABLE_MODE());
            //Bucket &b = bc[curr.bucket];

            uint64_t entry = bc.read_from_bucket_slot(curr.bucket, curr.slot);
            if (entry == empty_entry) {
                // We can terminate here
                return i;
            }

            Item *ptr = extract_ptr(entry);

            curr.hv = hashed_key(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr));
        }
        return x.depth;
    }

    bool cuckoopath_move(const size_type hp, CuckooRecords &cuckoo_path, size_type depth, TwoBuckets &b) {
        if (depth == 0) {
            // There is a chance that depth == 0, when try_add_to_bucket sees
            // both buckets as full and cuckoopath_search finds one empty. In
            // this case, we lock both buckets. If the slot that
            // cuckoopath_search found empty isn't empty anymore, we unlock them
            // and return false. Otherwise, the bucket is empty and insertable,
            // so we hold the locks and return true.
            record llog.kick_failure_depth0++;

            uint64_t entry = bc.read_from_bucket_slot(cuckoo_path[0].bucket, cuckoo_path[0].slot);

            if (entry == empty_entry) {
                return true;
            } else {
                return false;
            }
        }

        while (depth > 0) {
            CuckooRecord &from = cuckoo_path[depth - 1];
            CuckooRecord &to = cuckoo_path[depth];
            const size_type fb = from.bucket;
            const size_type tb = to.bucket;
            const size_type fs = from.slot;
            const size_type ts = to.slot;

            //TODO check and del duplicate key

            uint64_t mv_entry = bc.read_from_bucket_slot(fb, fs);
            if (mv_entry == empty_entry) {
                record llog.kick_failure_source_changed++;
                return false;
            }

            if (is_kick_marked(mv_entry)) {
                record llog.kick_failure_other_marked++;
                record llog.helper_triggered++;
                help_cuckoo_move({fb, (int) fs, mv_entry, kick_parameter});
                return false;
            }

            Item *mv_ptr = extract_ptr(mv_entry);
            hash_value mv_hv = hashed_key(ITEM_KEY(mv_ptr), ITEM_KEY_LEN(mv_ptr));


            if (mv_hv.hash != from.hv.hash) {
                record llog.kick_failure_source_changed++;
                return false;
            }
            debug {
                size_type b1 = index_hash(hp, mv_hv.hash);
                size_type b2 = alt_index(hp, mv_hv.partial, index_hash(hp, mv_hv.hash));
                ASSERT(fb == b1 && tb == b2 ||
                       fb == b2 && tb == b1, "kick path error")
            }

            KickInfoHolder kickInfoHolder(twm, mv_hv.hash, tb, ts);

            atomic_thread_fence(memory_order_seq_cst);

            uint64_t marked_entry = mark_entry(mv_entry, cuckoo_tid);
            if (!bc.try_update_entry(fb, fs, mv_entry, marked_entry)) {
                record llog.kick_failure_mark_cas_fail++;
                return false;
            }

            atomic_thread_fence(memory_order_seq_cst);

            if (!bc.try_update_entry(tb, ts, empty_entry, mv_entry)) {
                record llog.kick_failure_set_target_cas_fail++;
                //cas may fail because helper
                bc.try_update_entry(fb, fs, marked_entry, mv_entry);
                return false;
            }

            twm.set_redo_if_hazard(mv_hv.hash);

            atomic_thread_fence(memory_order_seq_cst);

            if (!bc.try_update_entry(fb, fs, marked_entry, empty_entry)) {
                //other thread help relocated
                record llog.kick_failure_rm_source_cas_fail++;

                return false;
            }
            depth--;
        }

        return true;
    }

    void run_cuckoo(TwoBuckets &b) {
        record llog.kick_triggered++;
        size_type hp = hashpower();
        CuckooRecords cuckoo_path;

        //LOOP CONTROL
        size_t loop_count = 0;
        while (true) {
            record llog.kick_performed++;
            loop_count++;
            debug ASSERT(loop_count < 1000000, "MAYBE DEAD LOOP");

            const int depth = cuckoopath_search(hp, cuckoo_path, b.i1, b.i2);

            record llog.kick_path_length_log[depth]++;

            if (depth < 0) {
                break;
            }

            if (cuckoopath_move(hp, cuckoo_path, depth, b)) {
                return;
            }
        }

        cerr << "table full!" << endl;
        exit(-1);
    }

    table_position Set_find(hash_value hv, TwoBuckets &b, char *key, size_type key_len) {
        int res1, res2;
        uint64_t entry = 0;
        while (true) {
            if (!try_find_insert_bucket(b.i1, res1, hv.partial, key, key_len, entry)) {
                return table_position{b.i1, res1, entry, key_hit};
            }
            if (!try_find_insert_bucket(b.i2, res2, hv.partial, key, key_len, entry)) {
                return table_position{b.i2, res2, entry, key_hit};
            }
            if (res1 != -1) {
                return table_position{b.i1, res1, entry, key_miss_with_empty_slot};
            }
            if (res2 != -1) {
                return table_position{b.i2, res2, entry, key_miss_with_empty_slot};
            }

            //We are unlucky, so let's perform cuckoo hashing.
            run_cuckoo(b);
        }
    }

    void help_cuckoo_move(table_position pos) {
        debug ASSERT(is_kick_marked(pos.entry), "helper parameter false ")

        //TODO check and delete duplicate key
        KickInfo kickInfo = twm.get_kick_info(extract_kick_thread_mark(pos.entry));
        if (kickInfo == 0ull) return;

        size_type target_bucket = kickinfo_extract_bucket(kickInfo);
        size_type target_slot = kickinfo_extract_slot(kickInfo);
        partial_t kick_par = kickinfo_extract_partial(kickInfo);

        Item *ptr = extract_ptr(pos.entry);
        hash_value hv = hashed_key(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr));
        TwoBuckets b = get_two_buckets(hv);

        bool c1 = kick_par == hv.partial;
        bool c2 = b.i1 == target_bucket && b.i2 == pos.index;
        bool c3 = b.i2 == target_bucket && b.i1 == pos.index;
        if (!(c1 && (c2 || c3))) return;

        uint64_t mv_entry = merge_entry(hv.partial, ptr);

        if (!bc.try_update_entry(target_bucket, target_slot, empty_entry, mv_entry)) {
            return;
        }

        twm.set_redo_if_hazard(hv.hash);

        atomic_thread_fence(memory_order_seq_cst);

        bc.try_update_entry(pos.index, pos.slot, pos.entry, empty_entry);
    }

    void snapshoot_bucket(size_type bucket_ind, Item *item, hash_value hv, uint64_t *res_map, int &count) {
        for (int i = 0; i < SLOT_PER_BUCKET; i++) {
            uint64_t entry = bc.read_from_bucket_slot(bucket_ind, i);
            res_map[i] = entry;

            //Statistical frequency of occurrence
            if (entry == 0ul) continue;
            Item *read_ptr = extract_ptr(entry);
            if (is_kick_marked(entry) || hv.partial == extract_partial(entry)) {
                if (KeyEqual()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), ITEM_KEY(item), ITEM_KEY_LEN(item))) {
                    count++;
                }
            }
        }
    }

    void check_anapshoot(uint64_t *entry_snapshoot, uint64_t entry, bool &self_exist, int &self_position,
                         int &first_position) {
        Item *item = extract_ptr(entry);
        for (int i = 0; i < 2 * SLOT_PER_BUCKET; i++) {
            uint64_t read_entry = entry_snapshoot[i];
            Item *read_ptr = extract_ptr(read_entry);
            if (is_kick_marked(read_entry) || extract_partial(entry) == extract_partial(read_entry)) {
                if (KeyEqual()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), ITEM_KEY(item), ITEM_KEY_LEN(item))) {
                    if (item == read_ptr) {
                        self_exist = true;
                        self_position = i;
                    }
                    if (first_position == -1) first_position = i;
                }
            }
        }
    }

    bool check_duplicate_key(TwoBuckets b, table_position pos, hash_value hv) {
        Item *item = extract_ptr(pos.entry);
        while (true) {
            uint64_t entry_snapshoot[2 * SLOT_PER_BUCKET] = {0};

            int count = 0;
            snapshoot_bucket(b.i1, item, hv, entry_snapshoot, count);
            snapshoot_bucket(b.i2, item, hv, entry_snapshoot + SLOT_PER_BUCKET, count);

            if (count == 1) {
                //insert success
                return true;
            } else if (count == 0) {
                //maybe false miss
                if (!twm.inquiry_need_redo()) {
                    //has been deleted
                    return true;
                }
                twm.clean_redo_flag();
                continue;
            } else if (count > 1) {
                //duplicate key
                bool self_exist = false;
                int first_position = -1, self_position = -1;
                check_anapshoot(entry_snapshoot, pos.entry, self_exist, self_position, first_position);
                if (!self_exist) {
                    //has been modified
                    if (twm.inquiry_need_redo()) {
                        twm.clean_redo_flag();
                        continue;
                    }
                    return true;
                } else if (self_exist && first_position != self_position) {
                    //delete self
                    if (is_kick_marked(entry_snapshoot[self_position])) {
                        uint64_t ind = self_position < SLOT_PER_BUCKET ? b.i1 : b.i2;
                        help_cuckoo_move(
                                {ind, self_position % SLOT_PER_BUCKET, entry_snapshoot[self_position], kick_parameter});
                        continue;
                    }

                    if (bc.try_update_entry(pos.index, pos.slot, pos.entry, empty_entry)) {
                        bc.deallocate(item);
                        return true;
                    } else {
                        continue;
                    }
                } else {
                    //self = first ,delete other
                    bool should_continue = false;
                    for (int i = first_position + 1; i < 2 * SLOT_PER_BUCKET; i++) {
                        uint64_t read_entry = entry_snapshoot[i];
                        Item *read_ptr = extract_ptr(read_entry);
                        if (is_kick_marked(read_entry) || extract_partial(pos.entry) == extract_partial(read_entry)) {
                            if (KeyEqual()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), ITEM_KEY(item),
                                           ITEM_KEY_LEN(item))) {
                                uint64_t ind = i < SLOT_PER_BUCKET ? b.i1 : b.i2;
                                int slot = i % SLOT_PER_BUCKET;
                                if (is_kick_marked(read_entry)) {
                                    help_cuckoo_move({ind, slot, read_entry, kick_parameter});
                                    should_continue = true;
                                    break;
                                } else {
                                    if (bc.try_update_entry(ind, slot, read_entry, empty_entry)) {
                                        bc.deallocate(extract_ptr(read_entry));
                                    } else {
                                        should_continue = true;
                                    }
                                }
                            }
                        }
                    }
                    if (!should_continue) return true;
                }
            }
        }
    }

public:
    Cuckoohash_map(size_type hp = DEFAULT_HASHPOWER, int thread_num = 0, int register_capacity = 1) :
            cuckoo_thread_num(thread_num),
            twm(thread_num, register_capacity),
            bc(hp, thread_num) {
    }

    void init_thread(int tid) {
        cuckoo_tid = tid;
        bc.init_thread(tid);
    }

    inline size_type hashpower() const { return bc.get_hashpower(); }

    static inline size_type hashmask(const size_type hp) { return hashsize(hp) - 1; }

    static inline size_type hashsize(const size_type hp) { return size_type(1) << hp; }

    inline size_type bucket_num() { return bc.get_size(); }

    inline size_type slot_num() { return bc.get_slot_num(); }

    size_type get_item_num_and_check() { return bc.get_item_num_and_check(); }

    void get_key_position_info(std::vector<double> &kpv, size_type item_num) {
        bc.get_key_position_info(kpv, item_num);
    }

    //true hit , false miss
    bool Find(char *key, size_t key_len, char *buf) {
        const hash_value hv = hashed_key(key, key_len);
        TwoBuckets b = get_two_buckets(hv);

        //RegisterHolder registerHolder(twm,hv.hash);
        twm.register_hash(hv.hash);

        EpochManager epochManager(bc);

        table_position pos;
        while (true) {
            record if (twm.inquiry_need_redo()) llog.redo_times++;
            pos = Find_find(key, key_len, hv.partial, b.i1, b.i2);
            if (pos.status == key_miss && twm.inquiry_need_redo()) {
                twm.clean_redo_flag();
                continue;
            } else {
                break;
            }
        }

        if (pos.status == key_hit) {
            //do some thing
            Item *ptr = extract_ptr(pos.entry);
            memcpy(buf, ITEM_VALUE(ptr), ITEM_VALUE_LEN(ptr));
            return true;
        }
        return false;
    }

    //true : insert
    //false : update
    bool Set(char *key, size_t key_len, char *value, size_t value_len) {
        const hash_value hv = hashed_key(key, key_len);
        TwoBuckets b = get_two_buckets(hv);

        //RegisterHolder registerHolder(twm,hv.hash);
        twm.register_hash(hv.hash);

        Item *item = bc.allocate_item(key, key_len, value, value_len);

        while (true) {
            EpochManager epochManager(bc);

            table_position pos;
            while (true) {
                record if (twm.inquiry_need_redo()) llog.redo_times++;
                pos = Set_find(hv, b, key, key_len);
                if (pos.status == key_miss_with_empty_slot && twm.inquiry_need_redo()) {
                    twm.clean_redo_flag();
                    continue;
                } else {
                    break;
                }
            }

            if (pos.status == key_miss_with_empty_slot) {
                uint64_t insert_entry = merge_entry(hv.partial, item);
                if (bc.try_update_entry(pos.index, pos.slot, empty_entry, insert_entry)) {
                    pos.entry = insert_entry;
                    return check_duplicate_key(b, pos, hv);
                }
            } else {
                debug ASSERT(pos.status == key_hit, "pos.status false");
                if (is_kick_marked(pos.entry)) {
                    help_cuckoo_move(pos);
                    continue;
                }
                if (bc.try_update_entry(pos.index, pos.slot, pos.entry, merge_entry(hv.partial, item))) {
                    bc.deallocate(extract_ptr(pos.entry));
                    return false;
                }
            }
        }
    }

    bool Delete(char *key, size_t key_len) {
        const hash_value hv = hashed_key(key, key_len);
        TwoBuckets b = get_two_buckets(hv);

        //RegisterHolder registerHolder(twm,hv.hash);
        twm.register_hash(hv.hash);
        while (true) {
            EpochManager epochManager(bc);

            table_position pos;
            while (true) {
                record if (twm.inquiry_need_redo()) llog.redo_times++;
                pos = Find_find(key, key_len, hv.partial, b.i1, b.i2);
                if (pos.status == key_miss && twm.inquiry_need_redo()) {
                    twm.clean_redo_flag();
                    continue;
                } else {
                    break;
                }
            }
            if (pos.status == key_hit) {
                if (is_kick_marked(pos.entry)) {
                    help_cuckoo_move(pos);
                    continue;
                }
                if (bc.try_update_entry(pos.index, pos.slot, pos.entry, empty_entry)) {
                    bc.deallocate(extract_ptr(pos.entry));
                    return true;
                }
            } else {
                return false;
            }
        }
    }

private:

    mutable BC bc;

    TWM twm;

    int cuckoo_thread_num;

};

}


#endif //CZL_CUCKOO_CUCKOO_MAP_H
