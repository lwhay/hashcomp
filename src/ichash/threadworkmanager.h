#ifndef CZL_CUCKOO_THREADWORKMANAGER_H
#define CZL_CUCKOO_THREADWORKMANAGER_H

#include "assert_msg.h"

namespace libcuckoo {

thread_local int cuckoo_tid;
//    thread_local bool thread_initialized;

class ThreadWorkManager {
public:

    int REGISTER_CAPACITY = 1;
    static const int ALIGN_RATIO = 128 / sizeof(size_type);
    static const int ALIGN_RATIO_BOOL = 128 / sizeof(bool);

    explicit ThreadWorkManager(int thread_num, int register_capacity) : running_thread_num(thread_num),
                                                                        REGISTER_CAPACITY(register_capacity) {
        manager = new std::atomic<size_type>[running_thread_num * REGISTER_CAPACITY * ALIGN_RATIO]();
        redo_flags = new std::atomic<bool>[running_thread_num * ALIGN_RATIO_BOOL];
        kick_info = new std::atomic<KickInfo>[running_thread_num * ALIGN_RATIO];


        for (int i = 0; i < running_thread_num * REGISTER_CAPACITY * ALIGN_RATIO; i++) {
            manager[i].store(0ull, std::memory_order_relaxed);
        }

        for (int i = 0; i < running_thread_num * ALIGN_RATIO; i++) {
            kick_info[i].store(0ull, std::memory_order_relaxed);
        }
        for (int i = 0; i < running_thread_num * ALIGN_RATIO_BOOL; i++) {
            redo_flags[i].store(false, std::memory_order_relaxed);
        }

    }


    bool inquiry_need_redo() {
        return redo_flags[cuckoo_tid * ALIGN_RATIO_BOOL].load(std::memory_order_seq_cst);
    }

    void clean_redo_flag() {
        redo_flags[cuckoo_tid * ALIGN_RATIO_BOOL].store(false, std::memory_order_relaxed);
    }

    void set_redo_if_hazard(size_type hash) {
        int reg_index = hash % REGISTER_CAPACITY;
        for (int i = 0; i < running_thread_num; i++) {
            if (i == cuckoo_tid) continue;
            size_type store_record = manager[(i * REGISTER_CAPACITY + reg_index) * ALIGN_RATIO].load(
                    std::memory_order_seq_cst);
            if (is_handled(store_record) && equal_hash(store_record, hash)) {
                record llog.kick_send_redo_signal++;
                redo_flags[i * ALIGN_RATIO_BOOL].store(true, std::memory_order_seq_cst);
                //kick_send_redo_signal_l++;
                //cout<<"thread "<<i<<" redo find"<<endl;
            }
        }
    }


    void register_hash(size_type hash) {
        int reg_index = hash % REGISTER_CAPACITY;
        SOFTWARE_BARRIER;
        manager[(cuckoo_tid * REGISTER_CAPACITY + reg_index) * ALIGN_RATIO].store(con_store_record(hash),
                                                                                  std::memory_order_relaxed);
        SOFTWARE_BARRIER;
    }


    void cancellate_hash() {
        manager[cuckoo_tid * ALIGN_RATIO].store(0ul, std::memory_order_seq_cst);
    }


    bool empty() {
        for (int i = 0; i < running_thread_num; i++) {
            size_type store_record = manager[i * ALIGN_RATIO].load(std::memory_order_seq_cst);
            if (is_handled(store_record)) return false;
        }
        return true;
    }

    inline size_type con_store_record(size_type hash) { return hash | 1ul; }

    inline bool is_handled(size_type store_record) { return store_record & handle_mask; }

    inline bool equal_hash(size_type store_record, size_type hash) {
        debug ASSERT(is_handled(store_record), "compare record not be handled");
        return (store_record & hash_mask) == (hash & hash_mask);
    }

    void store_kick_info(partial_t par, uint64_t bucket, uint64_t slot) {
        kick_info[cuckoo_tid * ALIGN_RATIO].store(construct_kickinfo(bucket, slot, par));
    }

    void clean_kick_info() {
        //KickInfo * kickInfo = kick_info[cuckoo_tid * ALIGN_RATIO].load();
        //TODO reclaim kickinfo
        //delete kickInfo;
        kick_info[cuckoo_tid * ALIGN_RATIO].store(0ull);
    }

    KickInfo get_kick_info(int kick_tid) {
        return kick_info[kick_tid * ALIGN_RATIO].load();
    }


    int running_thread_num;

    std::atomic<size_type> *manager;

    std::atomic<bool> *redo_flags;

    std::atomic<KickInfo> *kick_info;

    size_type hash_mask = ~0x1ul;
    size_type handle_mask = 0x1ul;

};

}

#endif //CZL_CUCKOO_THREADWORKMANAGER_H
