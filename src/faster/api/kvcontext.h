//
// Created by lwh on 19-5-23.
//

#ifndef HASHCOMP_KVCONTEXT_H
#define HASHCOMP_KVCONTEXT_H

#include "faster.h"
#include "../core/key_hash.h"
#include "../misc/utility.h"

using namespace FASTER::misc;
using namespace FASTER::core;

namespace FASTER {
namespace api {

class ReadContext;

class UpsertContext;

class DeleteContext;

class RmwContext;

/// This benchmark stores 8-byte keys in key-value store.
class Key {
public:
    Key(uint64_t key)
            : key_{key} {
    }

    /// Methods and operators required by the (implicit) interface:
    inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Key));
    }

    inline KeyHash GetHash() const {
        return KeyHash{Utility::GetHashCode(key_)};
    }

    /// Comparison operators.
    inline bool operator==(const Key &other) const {
        return key_ == other.key_;
    }

    inline bool operator!=(const Key &other) const {
        return key_ != other.key_;
    }

private:
    uint64_t key_;
};

/// This benchmark stores an 8-byte value in the key-value store.
class Value {
public:
    Value() : value_{0} {
    }

    Value(const Value &other) : value_{other.value_} {
    }

    Value(uint64_t value) : value_{value} {
    }

    inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Value));
    }

    friend class ReadContext;

    friend class UpsertContext;

    friend class DeleteContext;

    friend class RmwContext;

    inline uint64_t Get() const { return value_; }

    inline uint64_t Load() const { return atomic_value_.load(); }

    //inline void Set(uint64_t value) { value_ = value; }

private:
    union {
        uint64_t value_;
        std::atomic<uint64_t> atomic_value_;
    };
};

/// Class passed to store_t::Read().
class ReadContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint64_t key) : key_{key} {}

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext &other) : key_{other.key_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    // For this benchmark, we don't copy out, so these are no-ops.
    inline void Get(const value_t &value) {
        //ASSERT_TRUE(false);
        value_ = value.value_;
    }

    inline void Put(value_t &value) {
        value.value_ = value_;
    }

    inline void PutAtomic(value_t &value) {
        value.atomic_value_.store(value_);
    }

    inline void GetAtomic(const value_t &value) { atomic_value_.store(value.atomic_value_.load()); }

    inline uint64_t Return() { return value_; }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;

    union {
        uint64_t value_;
        std::atomic<uint64_t> atomic_value_;
    };
};

/// Class passed to store_t::Upsert().
class UpsertContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint64_t key, uint64_t input) : key_{key}, input_{input} {
    }

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_}, input_{other.input_} {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline static constexpr uint32_t value_size() {
        return sizeof(value_t);
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t &value) {
        value.value_ = input_;
    }

    inline bool PutAtomic(value_t &value) {
        value.atomic_value_.store(input_);
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint64_t input_;
};

class DeleteContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    DeleteContext(Key key) : key_{key} {}

    DeleteContext(const DeleteContext &other) : key_{other.key_} {}

    inline const Key &key() const {
        return key_;
    }

    inline static constexpr uint32_t value_size() {
        return sizeof(value_t);
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(value_t &value) {
        value.value_ = input_;
    }

protected:
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint64_t input_;
};

/// Class passed to store_t::RMW().
class RmwContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    RmwContext(uint64_t key, uint64_t incr) : key_{key}, incr_{incr} {
    }

    /// Copy (and deep-copy) constructor.
    RmwContext(const RmwContext &other) : key_{other.key_}, incr_{other.incr_} {
    }

    /// The implicit and explicit interfaces require a key() accessor.
    const Key &key() const {
        return key_;
    }

    inline static constexpr uint32_t value_size() {
        return sizeof(value_t);
    }

    /// Initial, non-atomic, and atomic RMW methods.
    inline void RmwInitial(value_t &value) {
        value.value_ = incr_;
    }

    inline void RmwCopy(const value_t &old_value, value_t &value) {
        value.value_ = old_value.value_ + incr_;
    }

    inline bool RmwAtomic(value_t &value) {
        value.atomic_value_.fetch_add(incr_);
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint64_t incr_;
};
}
}


#endif //HASHCOMP_KVCONTEXT_H
