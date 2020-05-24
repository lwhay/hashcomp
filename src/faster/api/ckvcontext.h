//
// Created by iclab on 9/30/19.
//

#ifndef HASHCOMP_CCKVCONTEXT_H
#define HASHCOMP_CCKVCONTEXT_H

#include "../misc/utility.h"

using namespace FASTER::misc;

namespace FASTER {
namespace api {

class Key {
public:
    Key(uint32_t key) : key_{key} {
    }

    inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Key));
    }

    inline KeyHash GetHash() const {
        std::hash<uint32_t> hash_fn;
        return KeyHash{hash_fn(key_)};
    }

    /// Comparison operators.
    inline bool operator==(const Key &other) const {
        return key_ == other.key_;
    }

    inline bool operator!=(const Key &other) const {
        return key_ != other.key_;
    }

private:
    uint32_t key_;
};

class UpsertContext;

class DeleteContext;

class ReadContext;

class alignas(16) Value {
public:
    Value() : length_{0}, value_{0} {}

    inline static constexpr uint32_t size() {
        return static_cast<uint32_t>(sizeof(Value));
    }

    friend class UpsertContext;

    friend class DeleteContext;

    friend class ReadContext;

private:
    uint8_t value_[31];
    std::atomic<uint32_t> length_;
};

class UpsertContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(Key key) : key_{key} {}

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline static constexpr uint32_t value_size() {
        return sizeof(value_t);
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        value.length_ = 5;
        std::memset(value.value_, 23, 5);
    }

    inline bool PutAtomic(Value &value) {
        // Get the lock on the value.
        bool success;
        do {
            uint8_t expected_length;
            do {
                // Spin until other the thread releases the lock.
                expected_length = value.length_.load();
            } while (expected_length == UINT8_MAX);
            // Try to get the lock.
            success = value.length_.compare_exchange_weak(expected_length, UINT8_MAX);
        } while (!success);

        std::memset(value.value_, 42, 7);
        value.length_.store(7);
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
};

class DeleteContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    DeleteContext(Key key) : key_{key} {}

    /// Copy (and deep-copy) constructor.
    DeleteContext(const DeleteContext &other) : key_{other.key_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline static constexpr uint32_t value_size() {
        return sizeof(value_t);
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        value.length_ = 5;
        std::memset(value.value_, 23, 5);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
};

class ReadContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(uint32_t key) : key_{key} {}

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext &other) : key_{other.key_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline void Get(const Value &value) {
        // All reads should be atomic (from the mutable tail).
        //ASSERT_TRUE(false);
    }

    inline void GetAtomic(const Value &value) {
        do {
            output_length = value.length_.load();
            //ASSERT_EQ(0, reinterpret_cast<size_t>(value.value_) % 16);
            output_pt1 = *reinterpret_cast<const uint64_t *>(value.value_);
            output_pt2 = *reinterpret_cast<const uint64_t *>(value.value_ + 8);
        } while (output_length != value.length_.load());
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
public:
    uint32_t output_length;
    uint64_t output_pt1;
    uint64_t output_pt2;
};
}
}
#endif //HASHCOMP_CCKVCONTEXT_H
