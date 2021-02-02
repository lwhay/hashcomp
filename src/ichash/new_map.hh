#include <cstring>
#include "new_bucket_container.hh"
#include "cuckoohash_config.hh"
#include "cuckoohash_util.hh"

#include "item.h"
#include "assert_msg.h"
#include "kick_haza_pointer.h"


namespace libcuckoo {

    static const std::size_t SLOT_PER_BUCKET = DEFAULT_SLOT_PER_BUCKET;

    static const int partial_offset = 56;
    static const uint64_t partial_mask = 0xffull << partial_offset;
    static const uint64_t ptr_mask = 0xffffffffffffull; //lower 48bit
    static const uint64_t kick_lock_mask = 1ull << (partial_offset - 1);

    thread_local int thread_id;

    //thread_local size_t kick_num_l;

    thread_local size_t kick_num_l,
                        depth0_l, // ready to kick, then find empty slot
                        kick_lock_failure_data_check_l,
                        kick_lock_failure_haza_check_l,
                        kick_lock_failure_other_lock_l,
                        kick_lock_failure_haza_check_after_l,
                        kick_lock_failure_data_check_after_l,
                        key_duplicated_after_kick_l;
    thread_local size_t kick_path_length_log_l[6];

    class new_cuckoohash_map {
    private:

        static const uint32_t kHashSeed = 7079;

        static uint64_t MurmurHash64A(const void *key, size_t len) {
            const uint64_t m = 0xc6a4a7935bd1e995ull;
            const size_t r = 47;
            uint64_t seed = kHashSeed;

            uint64_t h = seed ^(len * m);

            const auto *data = (const uint64_t *) key;
            const uint64_t *end = data + (len / 8);

            while (data != end) {
                uint64_t k = *data++;

                k *= m;
                k ^= k >> r;
                k *= m;

                h ^= k;
                h *= m;
            }

            const auto *data2 = (const unsigned char *) data;

            switch (len & 7ull) {
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

            return h;
        }

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

        using partial_t = uint8_t;

        using buckets_t = bucket_container<SLOT_PER_BUCKET>;

        using bucket = typename buckets_t::bucket;




    public:
        using size_type = typename buckets_t::size_type;

        static constexpr uint16_t slot_per_bucket() { return SLOT_PER_BUCKET; }

        new_cuckoohash_map(size_type n = DEFAULT_HASHPOWER) : buckets_(n),rehash_flag(false) {
            ;
        }

        //other hashmap must be abandon after swap
        void swap(new_cuckoohash_map &other) noexcept {
            buckets_.swap(other.buckets_);
        }

        class hashpower_changed {};
        class need_rehash {};

        enum cuckoo_status {
            ok,
            failure,
            failure_key_not_found,
            failure_key_duplicated,
            failure_table_full,
            failure_under_expansion,
        };

        struct hash_value {
            size_type hash;
            partial_t partial;
        };

        struct table_position {
            size_type index;
            size_type slot;
            cuckoo_status status;
        };

        size_type hashpower() const { return buckets_.hashpower(); }

        static inline size_type hashmask(const size_type hp) {
            return hashsize(hp) - 1;
        }

        static inline size_type hashsize(const size_type hp) {
            return size_type(1) << hp;
        }

        inline size_type bucket_num(){return buckets_.size();}

        inline size_type slot_num(){return SLOT_PER_BUCKET * buckets_.size();}


        bool check_unique(){
            size_type hp  = hashpower();
            size_t a = bucket_num();
            for(size_t i = 0 ;i < bucket_num(); i++){
                for(size_t j = 0; j < SLOT_PER_BUCKET;j ++){
                    uint64_t par_ptr = buckets_.read_from_bucket_slot(i,j);
                    if( par_ptr != (uint64_t)nullptr){
                        partial_t par = get_partial(par_ptr);
                        uint64_t ptr = get_ptr(par_ptr);
                        size_t alt_i = alt_index(hp,par,i);
                        if(!unique_in_two_bucket(i,j,alt_i)){
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        bool check_nolock(){
            size_type hp  = hashpower();
            size_t a = bucket_num();
            for(size_t i = 0 ;i < bucket_num(); i++){
                for(size_t j = 0; j < SLOT_PER_BUCKET;j ++){
                    uint64_t par_ptr = buckets_.read_from_bucket_slot(i,j);
                    if( par_ptr != (uint64_t)nullptr){
                        size_t tmp = par_ptr & ~partial_mask & ~ptr_mask;
                        if(tmp != 0ull)
                            return false;
                    }
                }
            }
            return true;
        }

        bool unique_in_two_bucket(size_type i1,size_type slot_ind,size_type i2){
            uint64_t par_ptr =  buckets_.read_from_bucket_slot(i1,slot_ind);
            partial_t par = get_partial(par_ptr);
            uint64_t ptr = get_ptr(par_ptr);
            for(int i = 0 ; i < SLOT_PER_BUCKET;i++){
                if(i == slot_ind) continue;
                uint64_t com_par_ptr = buckets_.read_from_bucket_slot(i1,i);
                if(com_par_ptr == (uint64_t) nullptr) continue;
                partial_t com_par = get_partial(com_par_ptr);
                uint64_t com_ptr = get_ptr(com_par_ptr);
                if(com_par != par){
                    continue;
                } else{
                    if(str_equal_to()(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr),ITEM_KEY(com_ptr),ITEM_KEY_LEN(com_ptr)))
                        return false;
                }
            }
            if(i1 == i2) return true;
            for(int i = 0 ; i < SLOT_PER_BUCKET;i++){
                uint64_t com_par_ptr = buckets_.read_from_bucket_slot(i2,i);
                if(com_par_ptr == (uint64_t) nullptr) continue;
                partial_t com_par = get_partial(com_par_ptr);
                uint64_t com_ptr = get_ptr(com_par_ptr);
                if(com_par != par){
                    continue;
                } else{
                    if(str_equal_to()(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr),ITEM_KEY(com_ptr),ITEM_KEY_LEN(com_ptr)))
                        return false;
                }
            }
            return true;
        }

        static partial_t partial_key(const size_type hash) {
            const uint64_t hash_64bit = hash;
            const uint32_t hash_32bit = (static_cast<uint32_t>(hash_64bit) ^
                                         static_cast<uint32_t>(hash_64bit >> 32));
            const uint16_t hash_16bit = (static_cast<uint16_t>(hash_32bit) ^
                                         static_cast<uint16_t>(hash_32bit >> 16));
            const uint8_t hash_8bit = (static_cast<uint8_t>(hash_16bit) ^
                                       static_cast<uint8_t>(hash_16bit >> 8));
            return hash_8bit;
        }


        static inline size_type index_hash(const size_type hp, const size_type hv) {
            return hv & hashmask(hp);
        }

        static inline size_type alt_index(const size_type hp, const partial_t partial,
                                          const size_type index) {
            // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
            // hash constant from 64-bit MurmurHash2
            const size_type nonzero_tag = static_cast<size_type>(partial) + 1;
            return (index ^ (nonzero_tag * 0xc6a4a7935bd1e995)) & hashmask(hp);
        }

        hash_value hashed_key(const char *key, size_type key_len) const {
            const size_type hash = str_hash()(key, key_len);
            return {hash, partial_key(hash)};
        }

        class TwoBuckets {
        public:
            TwoBuckets() {}

            TwoBuckets(size_type i1_, size_type i2_)
                    : i1(i1_), i2(i2_) {}

            size_type i1, i2;

        };



        class KickHazaManager{
        public:

            static const int HP_MAX_THREADS = 512;

            static const int ALIGN_RATIO = 128 / sizeof (size_type);

            KickHazaManager(){
                for(int i = 0; i < HP_MAX_THREADS * ALIGN_RATIO;i++) manager[i].store(0ul);
                running_max_thread = HP_MAX_THREADS;
            }

            inline void set_running_max_thread(int n){running_max_thread = n;}

            //must ensure partial correspond to an existing key
            bool inquiry_is_registerd(size_type hash){
                for(int i = 0; i < running_max_thread; i++){
                    if(i == thread_id) continue;
                    size_type store_record =  manager[i * ALIGN_RATIO].load();
                    if(is_handled(store_record) && equal_hash(store_record,hash)){
                        return true;
                    }
                }
                return false;
            }

            atomic<size_type> * register_hash(int tid,size_type hash) {
                manager[tid * ALIGN_RATIO].store(con_store_record(hash));
                return &manager[tid * ALIGN_RATIO];
            }

            bool empty(){
                for(int i  = 0 ; i < HP_MAX_THREADS ; i++){
                    size_type store_record = manager[i * ALIGN_RATIO].load();
                    if(is_handled(store_record)) return false;
                }
                return true;
            }

            inline size_type con_store_record(size_type hash){return hash | 1ul;}
            inline bool is_handled(size_type store_record){return store_record & handle_mask;}
            inline bool equal_hash(size_type store_record,size_type hash){
                ASSERT(is_handled(store_record),"compare record not be handled");
                return store_record & hash_mask == hash & hash_mask;
            }

            int running_max_thread;

            atomic<size_type> manager[HP_MAX_THREADS * ALIGN_RATIO];

            size_type hash_mask = ~0x1ul;
            size_type handle_mask = 0x1ul;

        };

        struct ParRegisterDeleter {
            void operator()(atomic<size_type> *l) const { l->store(0ul); }
        };

        using ParRegisterManager = std::unique_ptr<atomic<size_type>, ParRegisterDeleter>;

        //return true when kick lock success
        //return false when find that not the kicking slots has been modified,both have value
        //return false when other kick lock
        bool kick_lock_two(size_type b1,size_type s1,size_type b2,size_type s2){

            //ensure we lock the lower index firstly
            if(b1 > b1) std::swap(b1,b2);
            atomic<uint64_t> & atomic_par_ptr_1 = buckets_.get_atomic_par_ptr(b1,s1);
            atomic<uint64_t> & atomic_par_ptr_2 = buckets_.get_atomic_par_ptr(b2,s2);

            //LOOP CONTROL

            size_t loop_count = 0;

            while(true){

                loop_count++;

                if (loop_count >= 1000000 ){
                    cout<<"MAYBE DEAD LOOP !!!!!!!!!!!!!!!!!!!!!"<<endl;
                    cout<<"kick_lock_failure_data_check "<<kick_lock_failure_data_check_l<<endl;
                    cout<<"kick_lock_failure_haza_check "<<kick_lock_failure_haza_check_l<<endl;
                    cout<<"kick_lock_failure_other_lock "<<kick_lock_failure_other_lock_l<<endl;
                    cout<<"kick_lock_failure_haza_check_after "<<kick_lock_failure_haza_check_after_l<<endl;
                    cout<<"kick_lock_failure_data_check_after "<<kick_lock_failure_data_check_after_l<<endl;
                }
                ASSERT(loop_count<1000000,"MAYBE DEAD LOOP");


                uint64_t par_ptr_1 = atomic_par_ptr_1.load();
                partial_t par1 = get_partial(par_ptr_1);
                uint64_t ptr1 = get_ptr(par_ptr_1);


                uint64_t par_ptr_2 = atomic_par_ptr_2.load();
                partial_t par2 = get_partial(par_ptr_2);
                uint64_t ptr2 = get_ptr(par_ptr_2);

                //if both have value or kick_locked  return false

                if(par_ptr_1 != (uint64_t) nullptr && par_ptr_2 != (uint64_t) nullptr){
                    kick_lock_failure_data_check_l++;
                    return false;
                }


                //repeat unitl no target conflict
                //TODO using partial to tag only, may cause unnecessary conflict. Should use bucket-par unite tag
                //TODO return the position information using parameter.Keep loading the position until
                // it is released by other thread
                if(ptr1 != 0 && kickHazaManager.inquiry_is_registerd(hashed_key(ITEM_KEY(ptr1),ITEM_KEY_LEN(ptr1)).hash)){
                    kick_lock_failure_haza_check_l++;
                    continue;
                }
                if(ptr2 != 0 && kickHazaManager.inquiry_is_registerd(hashed_key(ITEM_KEY(ptr2),ITEM_KEY_LEN(ptr2)).hash)){
                    kick_lock_failure_haza_check_l++;
                    continue;
                }

                //repeat when lock faiure
                if(!try_kick_lock_par_ptr(atomic_par_ptr_1)) {
                    kick_lock_failure_other_lock_l++;
                    continue;
                }
                if(!try_kick_lock_par_ptr(atomic_par_ptr_2)) {
                    kick_lock_failure_other_lock_l++;
                    kick_unlock_par_ptr(atomic_par_ptr_1);
                    continue;
                }


                //check again to prevent that reader come between the first check and kick lock and finish
                //reading the first slot would be locked
                if( par_ptr_1 != 0 && kickHazaManager.inquiry_is_registerd(hashed_key(ITEM_KEY(ptr1),ITEM_KEY_LEN(ptr1)).hash)
                    ||par_ptr_2 != 0 && kickHazaManager.inquiry_is_registerd(hashed_key(ITEM_KEY(ptr2),ITEM_KEY_LEN(ptr2)).hash)) {
                    kick_unlock_par_ptr(atomic_par_ptr_1);
                    kick_unlock_par_ptr(atomic_par_ptr_2);
                    kick_lock_failure_haza_check_after_l++;
                    continue;
                }

                return true;
            }

        }



        //we dont care unlock order
        void kick_unlok_two(size_type b1,size_type s1,size_type b2,size_type s2){
            kick_unlock_par_ptr(buckets_[b1].values_[s1 * ATOMIC_ALIGN_RATIO]);
            kick_unlock_par_ptr(buckets_[b2].values_[s2 * ATOMIC_ALIGN_RATIO]);
        }


        inline partial_t get_partial(size_type par_ptr) const {
            return static_cast<partial_t>((par_ptr & partial_mask) >> partial_offset);
        }

        inline uint64_t get_ptr(size_type par_ptr) const {
            return par_ptr & ptr_mask;
        }

        TwoBuckets get_two_buckets(const hash_value &hv) const {
            // while (true) {
            const size_type hp = hashpower();
            const size_type i1 = index_hash(hp, hv.hash);
            const size_type i2 = alt_index(hp, hv.partial, i1);
            //maybe rehash here
            return TwoBuckets(i1, i2);
            //}
        }

        TwoBuckets get_two_buckets(const hash_value &hv,size_type hp) const {
            // while (true) {

            const size_type i1 = index_hash(hp, hv.hash);
            const size_type i2 = alt_index(hp, hv.partial, i1);
            //maybe rehash here
            return TwoBuckets(i1, i2);
            //}
        }

        inline uint64_t merge_partial(partial_t partial, uint64_t ptr) {
            return (((uint64_t) partial << partial_offset) | ptr);
        }

        inline bool is_kick_locked(uint64_t par_ptr) const { return kick_lock_mask & par_ptr ;}

        inline bool try_kick_lock_par_ptr(atomic<uint64_t> & atomic_par_ptr){
            uint64_t par_ptr = atomic_par_ptr.load();
            if(is_kick_locked(par_ptr)) return false;
            return atomic_par_ptr.compare_exchange_strong(par_ptr,par_ptr | kick_lock_mask);
        }

        inline void kick_unlock_par_ptr(atomic<uint64_t> & atomic_par_ptr){
            uint64_t par_ptr = atomic_par_ptr.load();
            ASSERT(is_kick_locked(par_ptr),"try kick unlock an unlocked par_ptr ");
            bool res = atomic_par_ptr.compare_exchange_strong(par_ptr, par_ptr & ~kick_lock_mask);
            ASSERT(res,"unlock failure")
        }

        int try_read_from_bucket(const bucket &b, const partial_t partial,
                                 const char *key, size_type key_len) const {

            for (int i = 0; i < static_cast<int>(slot_per_bucket()); ++i) {
                //block when kick
                size_type par_ptr;
                do{
                    par_ptr = buckets_.read_from_slot(b,i);
                }
                while(is_kick_locked(par_ptr));

                partial_t read_partial = get_partial(par_ptr);
                uint64_t read_ptr = get_ptr(par_ptr);
                if (read_ptr == (size_type) nullptr ||
                    (partial != read_partial)) {
                    continue;
                } else if (str_equal_to()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), key, key_len)) {
                    return i;
                }
            }
            return -1;
        }

        table_position cuckoo_find(const char *key, size_type key_len, const partial_t partial,
                                   const size_type i1, const size_type i2) const {
            int slot = try_read_from_bucket(buckets_[i1], partial, key, key_len);
            if (slot != -1) {
                return table_position{i1, static_cast<size_type>(slot), ok};
            }
            slot = try_read_from_bucket(buckets_[i2], partial, key, key_len);
            if (slot != -1) {
                return table_position{i2, static_cast<size_type>(slot), ok};
            }
            return table_position{0, 0, failure_key_not_found};
        }

        //false : key_duplicated. the slot is the position of deplicated key
        //true : have empty slot.the slot is the position of empty slot. -1 -> no empty slot
        bool try_find_insert_bucket(const bucket &b, int &slot,
                                    const partial_t partial, const char *key, size_type key_len) const {
            // Silence a warning from MSVC about partial being unused if is_simple.
            //(void)partial;
            slot = -1;
            for (int i = 0; i < static_cast<int>(slot_per_bucket()); ++i) {
                //block when kick
                size_type par_ptr;
                do{
                    par_ptr = buckets_.read_from_slot(b,i);
                }
                while(is_kick_locked(par_ptr));

                partial_t read_partial = get_partial(par_ptr);
                uint64_t read_ptr = get_ptr(par_ptr);

                if (read_ptr != (size_type) nullptr) {
                    if (partial != read_partial) {
                        continue;
                    }
                    if (str_equal_to()(ITEM_KEY(read_ptr), ITEM_KEY_LEN(read_ptr), key, key_len)) {
                        slot = i;
                        return false;
                    }
                } else {
                    slot = slot == -1 ? i : slot;
                }
            }
            return true;
        }

        // CuckooRecord holds one position in a cuckoo path. Since cuckoopath
        // elements only define a sequence of alternate hashings for different hash
        // values, we only need to keep track of the hash values being moved, rather
        // than the keys themselves.
        typedef struct {
            size_type bucket;
            size_type slot;
            hash_value hv;
        } CuckooRecord;

        // The maximum number of items in a cuckoo BFS path. It determines the
        // maximum number of slots we search when cuckooing.
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
            static const int  MAX_CUCKOO_COUNT = 682;
            // An array of b_slots. Since we allocate just enough space to complete a
            // full search, we should never exceed the end of the array.
            b_slot slots_[MAX_CUCKOO_COUNT];
            // The index of the head of the queue in the array
            size_type first_;
            // One past the index of the last_ item of the queue in the array.
            size_type last_;
        };

        // slot_search searches for a cuckoo path using breadth-first search. It
        // starts with the i1 and i2 buckets, and, until it finds a bucket with an
        // empty slot, adds each slot of the bucket in the b_slot. If the queue runs
        // out of space, it fails.
        //
        // throws hashpower_changed if it changed during the search
        b_slot slot_search(const size_type hp, const size_type i1,
                           const size_type i2) {
            b_queue q;
            // The initial pathcode informs cuckoopath_search which bucket the path
            // starts on
            q.enqueue(b_slot(i1, 0, 0));
            q.enqueue(b_slot(i2, 1, 0));
            while (!q.empty()) {
                b_slot x = q.dequeue();
                bucket &b = buckets_[x.bucket];
                // Picks a (sort-of) random slot to start from
                size_type starting_slot = x.pathcode % slot_per_bucket();
                for (size_type i = 0; i < slot_per_bucket(); ++i) {
                    uint16_t slot = (starting_slot + i) % slot_per_bucket();

                    //block when kick locked
                    size_type par_ptr;
                    do{
                        par_ptr = buckets_.read_from_slot(b,slot);
                    } while (is_kick_locked(par_ptr));

                    partial_t kick_partial = get_partial(par_ptr);

                    if (par_ptr == (uint64_t)nullptr) {
                        // We can terminate the search here
                        x.pathcode = x.pathcode * slot_per_bucket() + slot;
                        return x;
                    }

                    // If x has less than the maximum number of path components,
                    // have come from if we kicked out the item at this slot.
                    // create a new b_slot item, that represents the bucket we would
                    //const partial_t partial = b.partial(slot);
                    if (x.depth < MAX_BFS_PATH_LEN - 1) {
                        assert(!q.full());
                        b_slot y(alt_index(hp, kick_partial, x.bucket),
                                 x.pathcode * slot_per_bucket() + slot, x.depth + 1);
                        q.enqueue(y);
                    }
                }
            }
            // We didn't find a short-enough cuckoo path, so the search terminated.
            // Return a failure value.
            return b_slot(0, 0, -1);
        }

        int cuckoopath_search(const size_type hp, CuckooRecords &cuckoo_path,
                              const size_type i1, const size_type i2) {
            b_slot x = slot_search(hp, i1, i2);
            if (x.depth == -1) {
                return -1;
            }
            // Fill in the cuckoo path slots from the end to the beginning.
            for (int i = x.depth; i >= 0; i--) {
                cuckoo_path[i].slot = x.pathcode % slot_per_bucket();
                x.pathcode /= slot_per_bucket();
            }
            // Fill in the cuckoo_path buckets and keys from the beginning to the
            // end, using the final pathcode to figure out which bucket the path
            // starts on. Since data could have been modified between slot_search
            // and the computation of the cuckoo path, this could be an invalid
            // cuckoo_path.
            CuckooRecord &first = cuckoo_path[0];
            if (x.pathcode == 0) {
                first.bucket = i1;
            } else {
                assert(x.pathcode == 1);
                first.bucket = i2;
            }
            {

                const bucket &b = buckets_[first.bucket];

                //block when kick locked
                size_type par_ptr;
                do{
                    par_ptr = buckets_.read_from_slot(b,first.slot);
                } while (is_kick_locked(par_ptr));
                uint64_t ptr = get_ptr(par_ptr);

                if (par_ptr == (uint64_t) nullptr) {
                    // We can terminate here
                    return 0;
                }
                first.hv = hashed_key(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr));
            }
            for (int i = 1; i <= x.depth; ++i) {
                CuckooRecord &curr = cuckoo_path[i];
                const CuckooRecord &prev = cuckoo_path[i - 1];
                assert(prev.bucket == index_hash(hp, prev.hv.hash) ||
                       prev.bucket ==
                       alt_index(hp, prev.hv.partial, index_hash(hp, prev.hv.hash)));
                // We get the bucket that this slot is on by computing the alternate
                // index of the previous bucket
                curr.bucket = alt_index(hp, prev.hv.partial, prev.bucket);
                //const auto lock_manager = lock_one(hp, curr.bucket, TABLE_MODE());
                const bucket &b = buckets_[curr.bucket];

                //block when kick locked
                size_type par_ptr;
                do{
                    par_ptr = buckets_.read_from_slot(b,curr.slot);
                } while (is_kick_locked(par_ptr));

                uint64_t ptr = get_ptr(par_ptr);


                if (par_ptr == (uint64_t) nullptr) {
                    // We can terminate here
                    return i;
                }
                curr.hv = hashed_key(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr));
            }
            return x.depth;
        }


        bool cuckoopath_move(const size_type hp, CuckooRecords &cuckoo_path,
                             size_type depth, TwoBuckets &b) {
            if (depth == 0) {
                // There is a chance that depth == 0, when try_add_to_bucket sees
                // both buckets as full and cuckoopath_search finds one empty. In
                // this case, we lock both buckets. If the slot that
                // cuckoopath_search found empty isn't empty anymore, we unlock them
                // and return false. Otherwise, the bucket is empty and insertable,
                // so we hold the locks and return true.
                depth0_l++;
                const size_type bucket_i = cuckoo_path[0].bucket;
                assert(bucket_i == b.i1 || bucket_i == b.i2);
                //b = lock_two(hp, b.i1, b.i2, TABLE_MODE());

                //block when kick locked
                bucket & b = buckets_[bucket_i];
                size_type par_ptr;
                do{
                    par_ptr = buckets_.read_from_slot(b,cuckoo_path[0].slot);
                } while (is_kick_locked(par_ptr));

                if (par_ptr == (uint64_t) nullptr ){
                    return true;
                } else {
                //    b.unlock();
                    return false;
                }
            }

            while (depth > 0) {
                CuckooRecord &from = cuckoo_path[depth - 1];
                CuckooRecord &to = cuckoo_path[depth];
                const size_type fs = from.slot;
                const size_type ts = to.slot;
                TwoBuckets twob;
                //LockManager extra_manager;

                if(!kick_lock_two(from.bucket,from.slot,to.bucket,to.slot))
                    return false;

                bucket &fb = buckets_[from.bucket];
                bucket &tb = buckets_[to.bucket];

                // We plan to kick out fs, but let's check if it is still there;
                // there's a small chance we've gotten scooped by a later cuckoo. If
                // that happened, just... try again. Also the slot we are filling in
                // may have already been filled in by another thread, or the slot we
                // are moving from may be empty, both of which invalidate the swap.
                // We only need to check that the hash value is the same, because,
                // even if the keys are different and have the same hash value, then
                // the cuckoopath is still valid.


                //from par_ptr holding kick lock now
                uint64_t from_par_ptr = buckets_.read_from_slot(fb,fs);
                uint64_t from_ptr = get_ptr(from_par_ptr);
                uint64_t to_par_ptr = buckets_.read_from_slot(tb,ts);
                uint64_t to_ptr = get_ptr(to_par_ptr);

//                uint64_t from_partial =  get_partial(from_par_ptr);
//                bool from_kick_locked = from_par_ptr & kick_lock_mask;
                bool a = buckets_.read_from_slot(tb,ts) != ((uint64_t)nullptr | kick_lock_mask );
                bool b = buckets_.read_from_slot(fb,fs) == ((uint64_t) nullptr | kick_lock_mask);
                //bool c = hashed_key(ITEM_KEY(from_ptr),ITEM_KEY_LEN(from_ptr)).hash != from.hv.hash;
                //!b&&c ensure from par_ptr not empty
                ASSERT(from_par_ptr == buckets_.read_from_slot(fb,fs) ,"");
                ASSERT(to_par_ptr == buckets_.read_from_slot(tb,ts),"");

                if (a || b ||
                    hashed_key(ITEM_KEY(from_ptr),ITEM_KEY_LEN(from_ptr)).hash != from.hv.hash ){
                        kick_unlok_two(from.bucket,from.slot,to.bucket,to.slot);
                        kick_lock_failure_data_check_after_l++;
                        return false;
                }

                buckets_.set_ptr(to.bucket,to.slot,from_par_ptr);
                buckets_.set_ptr(from.bucket,from.slot,((uint64_t) nullptr | kick_lock_mask));

                kick_unlok_two(from.bucket,from.slot,to.bucket,to.slot);

                depth--;
            }

            return true;
        }


        cuckoo_status run_cuckoo(TwoBuckets &b, size_type &insert_bucket,
                                 size_type &insert_slot) {
            size_type hp = hashpower();
            CuckooRecords cuckoo_path;
            bool done = false;
            try {
                //LOOP CONTROL
                size_t loop_count = 0;
                while (!done) {
                    loop_count ++;
                    ASSERT(loop_count < 1000000,"MAYBE DEAD LOOP");
                    const int depth =
                            cuckoopath_search(hp, cuckoo_path, b.i1, b.i2);

                    kick_path_length_log_l[depth]++;
                    if (depth < 0) {
                        break;
                    }

                    //show_cuckoo_path(cuckoo_path,depth);

                    if (cuckoopath_move(hp, cuckoo_path, depth, b)) {
                        insert_bucket = cuckoo_path[0].bucket;
                        insert_slot = cuckoo_path[0].slot;

                        assert(insert_bucket == b.i1 || insert_bucket == b.i2);

                        done = true;
                        break;
                    }
                }
            } catch (hashpower_changed &) {
                // The hashpower changed while we were trying to cuckoo, which means
                // we want to retry. b.i1 and b.i2 should not be locked
                // in this case.
                return failure_under_expansion;
            }
            return done ? ok : failure;
        }

        table_position cuckoo_insert(const hash_value hv, TwoBuckets &b, char *key, size_type key_len) {
            int res1, res2;
            bucket &b1 = buckets_[b.i1];
            if (!try_find_insert_bucket(b1, res1, hv.partial, key, key_len)) {
                return table_position{b.i1, static_cast<size_type>(res1),
                                      failure_key_duplicated};
            }
            bucket &b2 = buckets_[b.i2];
            if (!try_find_insert_bucket(b2, res2, hv.partial, key, key_len)) {
                return table_position{b.i2, static_cast<size_type>(res2),
                                      failure_key_duplicated};
            }
            if (res1 != -1) {
                return table_position{b.i1, static_cast<size_type>(res1), ok};
            }
            if (res2 != -1) {
                return table_position{b.i2, static_cast<size_type>(res2), ok};
            }

            //We are unlucky, so let's perform cuckoo hashing.
            size_type insert_bucket = 0;
            size_type insert_slot = 0;
            cuckoo_status st = run_cuckoo(b, insert_bucket, insert_slot);
            kick_num_l ++;

            if (st == failure_under_expansion) {
                // The run_cuckoo operation operated on an old version of the table,
                // so we have to try again. We signal to the calling insert method
                // to try again by returning failure_under_expansion.
                return table_position{0, 0, failure_under_expansion};
            } else if (st == ok) {

                // Since we unlocked the buckets during run_cuckoo, another insert
                // could have inserted the same key into either b.i1 or
                // b.i2, so we check for that before doing the insert.
                table_position pos = cuckoo_find(key,key_len, hv.partial, b.i1, b.i2);
                if (pos.status == ok) {
                    pos.status = failure_key_duplicated;
                    return pos;
                }
                return table_position{insert_bucket, insert_slot, ok};
            }
            ASSERT(st == failure,"st type error");
            return table_position{0, 0, failure_table_full};

        }

        table_position cuckoo_insert_rehashing(buckets_t &new_buckets_,const hash_value hv, TwoBuckets &b, char *key, size_type key_len){
            int res1, res2;
            bucket &b1 = new_buckets_[b.i1];
            if (!try_find_insert_bucket(b1, res1, hv.partial, key, key_len)) {
                ASSERT(false,"rehash insert failure_key_duplicated");
            }
            bucket &b2 = new_buckets_[b.i2];
            if (!try_find_insert_bucket(b2, res2, hv.partial, key, key_len)) {
                ASSERT(false,"rehash insert failure_key_duplicated");
            }
            if (res1 != -1) {
                return table_position{b.i1, static_cast<size_type>(res1), ok};
            }
            if (res2 != -1) {
                return table_position{b.i2, static_cast<size_type>(res2), ok};
            }
            ASSERT(false,"cuckoo_insert_rehashing failure");
        }

        table_position cuckoo_insert_loop(hash_value hv, TwoBuckets &b, char *key, size_type key_len) {
            table_position pos;
            while (true) {
                const size_type hp = hashpower();
                pos = cuckoo_insert(hv, b, key, key_len);
                switch (pos.status) {
                    case ok:
                    case failure_key_duplicated:
                        return pos;
                    case failure_table_full:
                        throw need_rehash();

                        ASSERT(false,"table full,need rehash");
                        // Expand the table and try again, re-grabbing the locks
                        //cuckoo_fast_double<TABLE_MODE, automatic_resize>(hp);
                        //b = snapshot_and_lock_two<TABLE_MODE>(hv);
                        break;
                    case failure_under_expansion:
                        assert(false);
                        // The table was under expansion while we were cuckooing. Re-grab the
                        // locks and try again.
                        //b = snapshot_and_lock_two<TABLE_MODE>(hv);
                        break;
                    default:
                        assert(false);
                }
            }
        }

        inline bool check_ptr(uint64_t ptr, char *key, size_type key_len) {
            if (ptr == (uint64_t) nullptr) return false;
            return str_equal_to()(ITEM_KEY(ptr), ITEM_KEY_LEN(ptr), key, key_len);
        }

        uint64_t get_item_num() { return buckets_.get_item_num(); }
        void get_key_position_info(vector<double> & kpv){buckets_.get_key_position_info(kpv);}

        size_type move_bucket(buckets_t &old_buckets, buckets_t &new_buckets,
                         size_type old_bucket_ind) const noexcept {
            const size_t old_hp = old_buckets.hashpower();
            const size_t new_hp = new_buckets.hashpower();

            size_type move_count = 0;

            // By doubling the table size, the index_hash and alt_index of each key got
            // one bit added to the top, at position old_hp, which means anything we
            // have to move will either be at the same bucket position, or exactly
            // hashsize(old_hp) later than the current bucket.
            bucket &old_bucket = old_buckets[old_bucket_ind];

            const size_type new_bucket_ind = old_bucket_ind + hashsize(old_hp);
            size_type new_bucket_slot = 0;

            // For each occupied slot, either move it into its same position in the
            // new buckets container, or to the first available spot in the new
            // bucket in the new buckets container.
            for (size_type old_bucket_slot = 0; old_bucket_slot < slot_per_bucket();
                 ++old_bucket_slot) {
                //if (!old_bucket.occupied(old_bucket_slot)) {
                if(old_bucket.get_item_ptr(old_bucket_slot) == (uint64_t) nullptr ){
                    continue;
                }

                move_count ++;
                size_type par_ptr = old_bucket.get_item_ptr(old_bucket_slot);
                size_type ptr = get_ptr(par_ptr);
                size_type par = get_partial(par_ptr);
                const hash_value hv = hashed_key(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr));
                const size_type old_ihash = index_hash(old_hp, hv.hash);
                const size_type old_ahash = alt_index(old_hp, hv.partial, old_ihash);
                const size_type new_ihash = index_hash(new_hp, hv.hash);
                const size_type new_ahash = alt_index(new_hp, hv.partial, new_ihash);
                size_type dst_bucket_ind, dst_bucket_slot;
                if ((old_bucket_ind == old_ihash && new_ihash == new_bucket_ind) ||
                    (old_bucket_ind == old_ahash && new_ahash == new_bucket_ind)) {
                    // We're moving the key to the new bucket
                    dst_bucket_ind = new_bucket_ind;
                    dst_bucket_slot = new_bucket_slot++;
                } else {
                    // We're moving the key to the old bucket
                    assert((old_bucket_ind == old_ihash && new_ihash == old_ihash) ||
                           (old_bucket_ind == old_ahash && new_ahash == old_ahash));
                    dst_bucket_ind = old_bucket_ind;
                    dst_bucket_slot = old_bucket_slot;
                }

                new_buckets.set_ptr(dst_bucket_ind,dst_bucket_slot,par_ptr);
                old_buckets.set_ptr(old_bucket_ind,old_bucket_slot,(uint64_t) nullptr);
            }
            return move_count;
        }

        //guarantee that just one thread call this function
        //guarantee that no other thread is working on this hashtable ==> haza_manageer is all empty
        void migrate_to_new(){
            //check there are no other threads working
            ASSERT(rehash_flag.load(),"rehash not locked");
            ASSERT(kickHazaManager.empty() ,"--kickhazamanager not empty");
            ASSERT(check_unique(),"key not unique!");
            ASSERT(check_nolock(),"there are still locks in map!");
            cout<<"thread "<<thread_id<<" calling migrate function"<<endl;

            size_type start_old_num = buckets_.get_item_num();

            size_type new_hashpower = hashpower() + 1;
            buckets_t new_buckets_(new_hashpower);
            ASSERT(new_buckets_.hashpower()  ==  hashpower() +1,"--hashpower error");
            ASSERT(new_buckets_.get_item_num() == 0 ,"new bucket not empty");

            //migrate items to new buckets one by one
            size_type move_count = 0;
            for(size_type i = 0 ; i < buckets_.size(); i++){
                move_count += move_bucket(buckets_,new_buckets_,i);
            }

            ASSERT(buckets_.get_item_num() == 0 ,"old bucket not empty after move")


            size_type af_old_num = buckets_.get_item_num();
            size_type af_new_num = new_buckets_.get_item_num();

            ASSERT(move_count == start_old_num,"move num error");
            ASSERT(af_new_num == start_old_num,"migrate num error");
            ASSERT(af_old_num == 0,"migrate num error,not empty");

            buckets_.swap(new_buckets_);
            new_buckets_.set_ready_to_destory();

            ASSERT( buckets_.get_item_num() == af_new_num &&
                    new_buckets_.get_item_num() == af_old_num ,"swap buckets error");

            ASSERT(new_buckets_.get_item_num() == 0,"new buckets not empty");
            ASSERT(check_unique(),"key not unique!");
            ASSERT(check_nolock(),"there are still locks in map!");

            cout<<"-->finish rehash ,now hashpower is "<<buckets_.hashpower()<<endl;

        }

        atomic<size_type> * block_when_rehashing(const hash_value hv ){
            atomic<size_type> * tmp_handle;


            while(true){

                while( rehash_flag.load() ){pthread_yield();}

                tmp_handle = kickHazaManager.register_hash(thread_id,hv.hash);

                if(!rehash_flag.load()) break;

                tmp_handle->store(0ul);

            }

            return tmp_handle;
        }

        inline void wait_for_other_thread_finish(){
            while(!kickHazaManager.empty()){pthread_yield();}
        }


        inline bool check_insert_unique(table_position pos,TwoBuckets b,hash_value hv,Item * item){
            size_type  par_ptr;
            for(int i = 0; i < pos.slot ; i++) {
                do{
                    par_ptr = buckets_.read_from_bucket_slot(pos.index,i);
                }
                while(is_kick_locked(par_ptr));

                size_type par = get_partial(par_ptr);
                size_type ptr = get_ptr(par_ptr);
                if(par == hv.partial &&
                    str_equal_to()(ITEM_KEY(ptr),ITEM_VALUE_LEN(ptr),ITEM_KEY(item),ITEM_VALUE_LEN(item))){
                    //We don't care if we succeed
                    buckets_.try_updateKV(pos.index,pos.slot,par_ptr,0);
                    return false;
                }
            }
            if(pos.index == b.i2){
                for(int i = 0; i < slot_per_bucket() ; i++) {
                    do{
                        par_ptr = buckets_.read_from_bucket_slot(b.i1,i);
                    }
                    while(is_kick_locked(par_ptr));
                    size_type par = get_partial(par_ptr);
                    size_type ptr = get_ptr(par_ptr);
                    if(par == hv.partial &&
                       str_equal_to()(ITEM_KEY(ptr),ITEM_VALUE_LEN(ptr),ITEM_KEY(item),ITEM_VALUE_LEN(item))){
                        //We don't care if we succeed
                        buckets_.try_updateKV(pos.index,pos.slot,par_ptr,0);
                        return false;
                    }
                }
            }
            return true;
        }


        //true hit , false miss
        bool find(char *key, size_t len);

        //true insert , false key failure_key_duplicated
        bool insert(char *key, size_t key_len, char *value, size_t value_len);

        bool insert_or_assign(char *key, size_t key_len, char *value, size_t value_len);

        //true erase success, false miss
        bool erase(char *key, size_t key_len);

        atomic<bool> rehash_flag;

        mutable buckets_t buckets_;

        KickHazaManager kickHazaManager;



    };

    bool new_cuckoohash_map::find(char *key, size_t key_len) {
        const hash_value hv = hashed_key(key, key_len);

        ParRegisterManager pm(block_when_rehashing(hv));

        TwoBuckets b = get_two_buckets(hv);
        table_position pos = cuckoo_find(key, key_len, hv.partial, b.i1, b.i2);
        if (pos.status == ok) {

            //do some thing
            size_t par_ptr = buckets_.read_from_bucket_slot(pos.index,pos.slot);
            size_t ptr = get_ptr(par_ptr);
            bool a = str_equal_to()(ITEM_KEY(ptr),ITEM_KEY_LEN(ptr),key,key_len);
            ASSERT(a,"key error");

            return true;
        }
        return false;
    }



    bool new_cuckoohash_map::insert(char *key, size_t key_len, char *value, size_t value_len) {
        while(true){
            Item *item = allocate_item(key, key_len, value, value_len);
            const hash_value hv = hashed_key(key, key_len);

            ParRegisterManager pm(block_when_rehashing(hv));

            TwoBuckets b = get_two_buckets(hv);
            table_position pos;
            size_type old_hashpower = hashpower();

            try {

                pos = cuckoo_insert_loop(hv, b, key, key_len);

            }catch (need_rehash){

                pm.get()->store(0ul);

                bool old_flag = false;
                if(rehash_flag.compare_exchange_strong(old_flag,true)){

                    if(old_hashpower != hashpower()){
                        //ABA,other thread has finished rehash.release rehash_flag and redo
                        rehash_flag.store(false);
                        continue;
                    }else{
                        wait_for_other_thread_finish();

                        migrate_to_new();

                        rehash_flag.store(false);

                        continue;
                    }

                }else{
                    continue;
                }

            }

            if (pos.status == ok) {
                if (buckets_.try_insertKV(pos.index, pos.slot, merge_partial(hv.partial, (uint64_t) item))) {
                    return check_insert_unique(pos,b,hv,item);
                }
            } else {
                //key_duplicated
                return false;
            }
        }

    }

    bool new_cuckoohash_map::insert_or_assign(char *key, size_t key_len, char *value, size_t value_len) {
        Item *item = allocate_item(key, key_len, value, value_len);
        const hash_value hv = hashed_key(key, key_len);
        //protect from kick
        ParRegisterManager pm(block_when_rehashing(hv));
        while (true) {
            TwoBuckets b = get_two_buckets(hv);
            table_position pos = cuckoo_insert_loop(hv, b, key, key_len);
            if (pos.status == ok) {
                if (buckets_.try_insertKV(pos.index, pos.slot, merge_partial(hv.partial, (uint64_t) item))) {
                    return true;
                }
            } else {
                uint64_t par_ptr = buckets_.read_from_bucket_slot(pos.index,pos.slot);
                uint64_t update_ptr = get_ptr(par_ptr);
                if (check_ptr(update_ptr, key, key_len)) {
                    if (buckets_.try_updateKV(pos.index, pos.slot, par_ptr,merge_partial(hv.partial, (uint64_t) item))) {
                        return false;
                    }
                }
            }
        }
    }

    bool new_cuckoohash_map::erase(char *key, size_t key_len) {
        const hash_value hv = hashed_key(key, key_len);
        //protect from kick
        ParRegisterManager pm(block_when_rehashing(hv));
        while (true) {
            TwoBuckets b = get_two_buckets(hv);
            table_position pos = cuckoo_find(key, key_len, hv.partial, b.i1, b.i2);
            if (pos.status == ok) {
                uint64_t par_ptr = buckets_.read_from_bucket_slot(pos.index,pos.slot);
                uint64_t erase_ptr = get_ptr(par_ptr);
                if (check_ptr(erase_ptr, key, key_len)) {
                    if (buckets_.try_eraseKV(pos.index, pos.slot, par_ptr)) {
                        return true;
                    }
                }
            } else {
                //return false only when key not find

                return false;
            }
        }
    }

}
