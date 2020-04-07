//
// Created by jiahua on 2019/10/14.
//

#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <atomic>
#include <cassert>
#include <iostream>
#include <memory>
#include <unordered_set>
#include <thread>
#include <functional>
#include <mutex>

#include "hazard/batch_hazard.h"
#include "hash_map/thread.h"

#ifdef FAST_TABLE
#include "fast_table.h"
#include "heavyhitter/GeneralLazySS.h"
#endif

namespace trick {
namespace util {
size_t nextPowerOf2(size_t n) {
    size_t count = 0;

    // First n in the below condition
    // is for the case where n is 0
    if (n && !(n & (n - 1)))
        return n;

    while (n != 0) {
        n >>= 1ull;
        count += 1;
    }

    return 1ull << count;
}

size_t powerOf2(size_t n) {
    assert(n);
    size_t ret = 0;
    while (n != 1ull) {
        n >>= 1ull;
        ret++;
    }
    return ret;
}

//struct ThreadIndexer {
//    std::mutex mut_;
//    std::array<bool, 128> place_holders_;
//
//
//    ThreadIndexer() : place_holders_() {
//        for (auto &i : place_holders_) {
//            i = false;
//        }
//    }
//
//    size_t Get() {
//        constexpr size_t kImpossible = 1000;
//        thread_local size_t l_idx{kImpossible};
//        if (l_idx == kImpossible) {
//            std::lock_guard<std::mutex> lk(mut_);
//            for (size_t i = 0; i < place_holders_.size(); i++) {
//                if (!place_holders_[i]) {
//                    place_holders_[i] = true;
//                    l_idx = i;
//                    return (size_t) l_idx;
//                }
//            }
//            std::cerr << "Thread count exceed limit" << std::endl;
//            exit(1);
//        }
//        assert(l_idx != kImpossible);
//        return (size_t) l_idx;
//    }
//};
}

enum class InsertType {
    DOES_NOT_EXIST,
    MUST_EXIST,
    ANY
};

enum class TreeNodeType {
    DATA_NODE,
    ARRAY_NODE,
    BUCKETS_NODE
};

class TreeNode {
public:
    virtual TreeNodeType Type() const = 0;

    virtual ~TreeNode() = default;
};


template<typename KeyType, typename ValueType>
class DataNode : public TreeNode {
    using Allocator = std::allocator<uint8_t>;
    using Mutex = std::mutex;
    using KVPair = std::pair<KeyType, ValueType>;
    template<typename T> using Atom = std::atomic<T>;
public:
public:
    DataNode(const KeyType &key, const ValueType &value) : kv_pair_{key, value}
#ifndef DISABLE_INPLACE_UPDATE
            , seq_lock_{0}
#endif
    {}

    TreeNodeType Type() const override {
        return TreeNodeType::DATA_NODE;
    }

    std::pair<KeyType, ValueType> &GetValue() {
        return kv_pair_;
    }

    KVPair kv_pair_;
#ifndef DISABLE_INPLACE_UPDATE

    size_t GetVersion() {
        return seq_lock_.load(std::memory_order_relaxed);
    }

    Atom<size_t> seq_lock_;
#endif
};

template<size_t LEN>
class ArrayNode : public TreeNode {
    using Allocator = std::allocator<uint8_t>;
    using Mutex = std::mutex;
    template<typename T, size_t SIZE> using Array = std::array<T, SIZE>;
    template<typename T> using Atom = std::atomic<T>;
public:
    TreeNodeType Type() const override {
        return TreeNodeType::ARRAY_NODE;
    }

    ArrayNode() {
        for (size_t i = 0; i < LEN; i++) {
            array_[i].store(nullptr, std::memory_order_relaxed);
        }
    }

    Array<Atom<TreeNode *>, LEN> array_;

    // LEN must be power of two
    size_t GetIdx(size_t h) const {
        return h & (LEN - 1);
    }
};

#ifdef FAST_TABLE
struct ThreadHashMapStat {
    GeneralLazySS <size_t> ss_;
    size_t total_;

    ThreadHashMapStat() : ss_(0.00001), total_(0) {}

    size_t GetCount(size_t h) {
        auto p = ss_.find(h);
        if (p) {
            return p->getCount();
        }
        return 0;
    }

    void Record(size_t h) {
        ss_.put(h);
        total_++;
    }
};
#endif

template<typename KeyType, typename ValueType, typename HashFn, typename KeyEqual, typename Reclaimer = batch_hazard<TreeNode, DataNode<KeyType, ValueType>>>
class ConcurrentHashMap {
    using Allocator = std::allocator<uint8_t>;
    using Mutex = std::mutex;
//    using BucketMapT = bucket::BucketMap<KeyType, ValueType, HashFn, KeyEqual>;
    template<typename T> using Atom = std::atomic<T>;
    using DataNodeT = DataNode<KeyType, ValueType>;

    static constexpr size_t kArrayNodeSize = 16;
    static constexpr size_t kArrayNodeSizeBits = 4;
    static constexpr size_t kHashWordLength = 64;
    static constexpr size_t kMaxDepth = 10;
    static constexpr uintptr_t kHighestBit = 0x8000000000000000ull;
    static constexpr uintptr_t kValidPtrField = 0x0000ffffffffffffull;
    using ArrayNodeT = ArrayNode<kArrayNodeSize>;
public:
    ConcurrentHashMap(size_t root_size, size_t max_depth, size_t thread_cnt = 32)
#ifdef FAST_TABLE
    : ft_(65536), stat_(0)
#endif
    {
        root_size_ = util::nextPowerOf2(root_size);
        root_bits_ = util::powerOf2(root_size_);
        thread_cnt = util::nextPowerOf2(thread_cnt);
        //HazPtrInit(thread_cnt, 2);
        size_t remain = kHashWordLength - root_bits_;
        max_depth_ = std::min({kMaxDepth, remain / kArrayNodeSizeBits, max_depth});
        size_t shift = root_bits_ + 10 * kArrayNodeSizeBits;
        bucket_map_hasher_ = [shift](const KeyType &k) -> size_t {
            return (HashFn()(k) >> shift);
        };

        root_ = (Atom<TreeNode *> *) Allocator().allocate(sizeof(TreeNode *) * root_size_);
        for (size_t i = 0; i < root_size_; i++) {
            new(root_[i]) Atom<TreeNode *>;
            root_[i].store(nullptr, std::memory_order_relaxed);
        }
        ptrmgr = new Reclaimer(thread_cnt);
        for (size_t i = 0; i < thread_cnt; i++) ptrmgr->registerThread();
#ifdef FAST_TABLE
        stat_.reserve(64);
        for (size_t i = 0; i < 64; i++) {
            stat_.push_back(new ThreadHashMapStat);
        }
#endif
    }

    ~ConcurrentHashMap() {
        delete ptrmgr;
    }

#ifdef FAST_TABLE
    enum class AsyncReturnCode {
        Ok,
        Error,
        Pending,
    };

    AsyncReturnCode AsyncInsert(const KeyType &k, const ValueType &v, InsertType type = InsertType::ANY) {
        size_t h = HashFn()(k);
        size_t tid = Thread::id();
        ThreadHashMapStat *stat = stat_[tid];

        if (stat->total_ >= 5000000 && ft_.TryUpdate(h, k, v)) {
            return AsyncReturnCode::Ok;
        }

        return AsyncReturnCode::Pending;
    }

    AsyncReturnCode AsyncFind(const KeyType &k, ValueType &v) {
        size_t h = HashFn()(k);
        auto node = ft_.PinnedFind(h, k);
        if (node) {
            v = node->Value();
            return AsyncReturnCode::Ok;
        }
        return AsyncReturnCode::Pending;
    }

#endif

    bool Delete(const KeyType &k) {
        auto ptr = NullDataNodePtr();
        return DoInsert(HashFn()(k), &k, nullptr, ptr, InsertType::MUST_EXIST, false);
    }

    bool Insert(const KeyType &k, const ValueType &v, InsertType type = InsertType::ANY) {
        size_t h = HashFn()(k);
#ifdef FAST_TABLE
        size_t tid = Thread::id();
        ThreadHashMapStat *stat = stat_[tid];
        if (stat->total_ < 5000000)
            stat->Record(h);

        if (stat->total_ >= 5000000 && ft_.TryUpdate(h, k, v)) {
            return true;
        }

        if (tid == 0 && stat->total_ == 5000000) {
            stat->total_++;
            Item<size_t> *p = stat->ss_.output(true);
            size_t k = 0;
            for (size_t i = 0; i < 65536; i++) {
                size_t hash = p[i].getItem();
                if (hash != std::numeric_limits<size_t>::max()) {
                    HazPtrHolder tmp_holder;
                    Atom<TreeNode *> *locate = nullptr;
                    auto elem = FindByHash(hash, tmp_holder, locate);
                    if (elem) {
                        bool r = ft_.CheckedInsert(hash, elem->kv_pair_.first, elem->kv_pair_.second);
                        if (r) {
                            k++;
                        }
                    }
                }
            }
        }
#endif
        auto ptr = NullDataNodePtr();

        auto res = DoInsert(h, &k, &v, ptr, type);
        return res;
    }

#ifndef DISABLE_INPLACE_UPDATE

    bool InplaceUpdate(const KeyType &k, const ValueType &v, InsertType type = InsertType::ANY) {
        size_t h = HashFn()(k);
        auto ptr = NullDataNodePtr();
        auto res = DoInsert(h, &k, &v, ptr, type, true);
        return res;
    }

#endif

    bool Find(const KeyType &k, ValueType &v) {
        size_t h = HashFn()(k);

#ifdef FAST_TABLE
        {
            auto node = ft_.PinnedFind(h, k);
            if (node) {
                v = node->Value();
                return true;
            }
        }
#endif
        size_t n = 0;

        size_t idx = GetRootIdx(h);
        Atom<TreeNode *> *node_ptr = &root_[idx];
        TreeNode *node = nullptr;
        //HazPtrHolder holder;
        while (true) {
            //node = holder.Repin(*node_ptr, IsArrayNode, FilterValidPtr);
            node = ptrmgr->Repin(Thread::id(), *node_ptr, IsArrayNode, FilterValidPtr);

            if (!node) {
                ptrmgr->read(Thread::id());
                return false;
            }

            switch (node->Type()) {
                case TreeNodeType::DATA_NODE: {
                    DataNodeT *d_node = static_cast<DataNodeT *>(node);
                    if (KeyEqual()(d_node->kv_pair_.first, k)) {
#ifndef DISABLE_INPLACE_UPDATE
                        size_t ver1 = 0;
                        size_t ver2 = 0;
                        do {
                            ver1 = d_node->GetVersion();
                            v = d_node->kv_pair_.second;
                            ver2 = d_node->GetVersion();
                        } while (ver1 != ver2);
#else
                        v = d_node->kv_pair_.second;
#endif
                        ptrmgr->read(Thread::id());
                        return true;
                    } else {
                        ptrmgr->read(Thread::id());
                        return false;
                    }
                }
                case TreeNodeType::ARRAY_NODE: {
                    n++;
                    ArrayNodeT *arr_node = static_cast<ArrayNodeT *>(node);
                    idx = GetNthIdx(h, n);
                    node_ptr = &arr_node->array_[idx];
                    //ptrmgr->read(Thread::id());
                    continue;
                }
                case TreeNodeType::BUCKETS_NODE: {
                    std::cerr << "Not supported yet" << std::endl;
                    exit(1);
                }
            }
        }
    }

private:
    size_t GetRootIdx(size_t h) const {
        return (h & (root_size_ - 1));
    }

    size_t GetNthIdx(size_t h, size_t n) const {
        h >>= root_bits_;
        h >>= (n - 1) * kArrayNodeSizeBits;
        return (h & (kArrayNodeSize - 1));
    }

    static bool IsArrayNode(TreeNode *tnp) {
        return ((uintptr_t) tnp) & kHighestBit;
    }

    static TreeNode *FilterValidPtr(TreeNode *tnp) {
        return (TreeNode *) ((uintptr_t) tnp & kValidPtrField);
    }

    static TreeNode *MarkArrayNode(ArrayNodeT *anp) {
        return (TreeNode *) (((uintptr_t) anp) | kHighestBit);
    }

    /*DataNodeT *FindByHash(size_t h, HazPtrHolder &holder, Atom<TreeNode *> *&locate) {
        size_t n = 0;
        size_t curr_holder_idx = 0;

        size_t idx = GetRootIdx(h);
        Atom<TreeNode *> *node_ptr = &root_[idx];
        TreeNode *node = nullptr;
        while (true) {
            node = holder.Pin(*node_ptr, IsArrayNode, FilterValidPtr);

            if (!node) {
                return nullptr;
            }

            switch (node->Type()) {
                case TreeNodeType::DATA_NODE: {
                    DataNodeT *d_node = static_cast<DataNodeT *>(node);
                    locate = node_ptr;
                    return d_node;
                }
                case TreeNodeType::ARRAY_NODE: {
                    n++;
                    holder.Reset();
                    ArrayNodeT *arr_node = static_cast<ArrayNodeT *>(node);
                    idx = GetNthIdx(h, n);
                    node_ptr = &arr_node->array_[idx];
                    continue;
                }
                case TreeNodeType::BUCKETS_NODE: {
                    std::cerr << "Not supported yet" << std::endl;
                    exit(1);
                }
            }
        }
    }*/

    std::unique_ptr<DataNodeT, std::function<void(DataNodeT *)>>
    AllocateDataNodePtr(const KeyType *k, const ValueType *v) {
        if (!v) {
            return NullDataNodePtr();
        }
        DataNodeT *new_node = (DataNodeT *) ptrmgr->allocate(Thread::id());
        new(new_node) DataNodeT(*k, *v);
        std::unique_ptr<DataNodeT, std::function<void(DataNodeT *)>> ptr(new_node);
        return ptr;
    }


    std::unique_ptr<DataNodeT, std::function<void(DataNodeT *)>>
    NullDataNodePtr() {
        std::unique_ptr<DataNodeT, std::function<void(DataNodeT *)>> ptr(nullptr, [](DataNodeT *n) {});
        return ptr;
    }

    bool DoInsert(size_t h, const KeyType *k, const ValueType *v,
                  std::unique_ptr<DataNodeT, std::function<void(DataNodeT *)>> &ptr,
                  InsertType type, bool ipu = false) {
        assert(k);
        assert((!v && !ipu) || v);
        size_t n = 0;

        size_t idx = GetRootIdx(h);
        Atom<TreeNode *> *node_ptr = &root_[idx];
        TreeNode *node = nullptr;
        //HazPtrHolder holder;
        while (true) {
            //node = holder.Repin(*node_ptr, IsArrayNode, FilterValidPtr);
            node = ptrmgr->Repin(Thread::id(), *node_ptr, IsArrayNode, FilterValidPtr);

            if (!node) {
                if (type == InsertType::MUST_EXIST) {
                    ptrmgr->read(Thread::id());
                    return false;
                }

                if (!ptr.get()) {
                    ptr = AllocateDataNodePtr(k, v);
                }

                bool result = node_ptr->compare_exchange_strong(node, (TreeNode *) ptr.get(),
                                                                std::memory_order_relaxed);
                if (!result) {
                    continue;
                }
                assert(ptr != nullptr);
                ptr.release();
                ptrmgr->read(Thread::id());
                return true;
            }

            switch (node->Type()) {
                case TreeNodeType::DATA_NODE: {

                    DataNodeT *d_node = static_cast<DataNodeT *>(node);
                    if (KeyEqual()(d_node->kv_pair_.first, *k)) {
                        if (type == InsertType::DOES_NOT_EXIST) {
                            return false;
                        }

                        if (!ipu) {
                            if (!ptr.get()) {
                                ptr = AllocateDataNodePtr(k, v);
                            }
                            bool result = node_ptr->compare_exchange_strong(node, ptr.get(), std::memory_order_relaxed);
                            if (!result) {
                                continue;
                            }
                            ptr.release();
                            //HazPtrRetire(d_node);
                            ptrmgr->free((uint64_t) d_node);
                        } else {
#ifndef DISABLE_INPLACE_UPDATE
                            bool hold = false;
                            size_t seq = 0;
                            do {
                                do {
                                    seq = d_node->GetVersion();
                                } while (seq & 1ull); // while seq is odd, means it is being locked
                                hold = d_node->seq_lock_.compare_exchange_strong(seq, seq + 1,
                                                                                 std::memory_order_relaxed);
                            } while (!hold);
                            d_node->kv_pair_.second = *v;
                            d_node->seq_lock_.store(seq + 2, std::memory_order_relaxed);
#else
                            std::cout << "Shouldn't be here" << std::endl;
                            exit(1);
#endif
                        }
                        return true;
                    } else {
                        if (n < max_depth_ - 1) {
                            std::unique_ptr<ArrayNodeT> tmp_arr_ptr(new ArrayNodeT);
                            size_t tmp_hash = HashFn()(d_node->kv_pair_.first);
                            size_t tmp_idx = GetNthIdx(tmp_hash, n + 1);
                            size_t next_idx = GetNthIdx(h, n + 1);

                            tmp_arr_ptr->array_[tmp_idx].store(node, std::memory_order_relaxed);

                            if (!ptr.get()) {
                                ptr = AllocateDataNodePtr(k, v);
                            }

                            if (next_idx != tmp_idx) {
                                tmp_arr_ptr->array_[next_idx].store(ptr.get(), std::memory_order_relaxed);
                            }

                            bool result = node_ptr->compare_exchange_strong(node, MarkArrayNode(tmp_arr_ptr.get()),
                                                                            std::memory_order_relaxed);

                            if (result && next_idx != tmp_idx) {
                                ptr.release();
                                tmp_arr_ptr.release();
                                return true;
                            }

                            if (result) {
                                n++;
                                size_t curr_idx = GetNthIdx(h, n);
                                node_ptr = &tmp_arr_ptr->array_[curr_idx];
                                tmp_arr_ptr.release();
                            }
                            continue;
                        } else {
                            std::cerr << "Not Implemented Yet?" << std::endl;
                            exit(1);
                        }
                    }
                }
                case TreeNodeType::ARRAY_NODE: {
                    n++;
                    //holder.Reset();
                    ptrmgr->read(Thread::id());
                    ArrayNodeT *arr_node = static_cast<ArrayNodeT *>(node);
                    size_t curr_idx = GetNthIdx(h, n);
                    node_ptr = &arr_node->array_[curr_idx];
                    continue;
                }
                case TreeNodeType::BUCKETS_NODE: {
                    std::cerr << "Not Implemented Yet" << std::endl;
                    exit(1);
                }
            }

        }
    }

private:
    Atom<TreeNode *> *root_{nullptr};
    std::function<size_t(const KeyType &)> bucket_map_hasher_;
#ifdef FAST_TABLE
    FastTable<KeyType, ValueType> ft_;
    std::vector<ThreadHashMapStat *> stat_;
#endif
    size_t root_size_{0};
    size_t root_bits_{0};
    size_t max_depth_{0};
    Reclaimer *ptrmgr;
};
}