//
// Created by iclab on 9/30/19.
//

#ifndef HASHCOMP_CVKVCONTEXT_H
#define HASHCOMP_CVKVCONTEXT_H

#include "faster.h"
#include "../core/key_hash.h"
#include "../misc/utility.h"

using namespace FASTER::misc;

namespace FASTER {
namespace api {
/// This benchmark stores 8-byte keys in key-value store.
class Key {
public:
    Key(uint64_t key) : key_{key} {
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

class UpsertContext;

class DeleteContext;

class ReadContext;

class GenLock {
public:
    GenLock() : control_{0} {}

    GenLock(uint64_t control) : control_{control} {}

    inline GenLock &operator=(const GenLock &other) {
        control_ = other.control_;
        return *this;
    }

    union {
        struct {
            uint64_t gen_number : 62;
            uint64_t locked : 1;
            uint64_t replaced : 1;
        };
        uint64_t control_;
    };
};

static_assert(sizeof(GenLock) == 8, "sizeof(GenLock) != 8");

class AtomicGenLock {
public:
    AtomicGenLock() : control_{0} {}

    AtomicGenLock(uint64_t control) : control_{control} {}

    inline GenLock load() const {
        return GenLock{control_.load()};
    }

    inline void store(GenLock desired) {
        control_.store(desired.control_);
    }

    inline bool try_lock(bool &replaced) {
        replaced = false;
        GenLock expected{control_.load()};
        expected.locked = 0;
        expected.replaced = 0;
        GenLock desired{expected.control_};
        desired.locked = 1;

        if (control_.compare_exchange_strong(expected.control_, desired.control_)) {
            return true;
        }
        if (expected.replaced) {
            replaced = true;
        }
        return false;
    }

    inline void unlock(bool replaced) {
        if (!replaced) {
            // Just turn off "locked" bit and increase gen number.
            uint64_t sub_delta = ((uint64_t) 1 << 62) - 1;
            control_.fetch_sub(sub_delta);
        } else {
            // Turn off "locked" bit, turn on "replaced" bit, and increase gen number
            uint64_t add_delta = ((uint64_t) 1 << 63) - ((uint64_t) 1 << 62) + 1;
            control_.fetch_add(add_delta);
        }
    }

private:
    std::atomic<uint64_t> control_;
};

static_assert(sizeof(AtomicGenLock) == 8, "sizeof(AtomicGenLock) != 8");

union dummy {
    uint64_t value_;
    std::atomic<uint64_t> atomic_value_;
};

/// This benchmark stores an 8-byte value in the key-value store.
class Value {
public:
    Value() : value_{0}, gen_lock_{0} {}

    Value(const Value &other) : value_{other.value_} {}

    Value(uint64_t value) : value_{value} {}

    inline static constexpr uint32_t size() { return static_cast<uint32_t>(sizeof(Value)); }

    uint32_t length() { return sizeof(dummy); }

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

    AtomicGenLock gen_lock_;
};

/*class Value {
public:
    Value() : gen_lock_{0}, size_{0}, length_{0} {}

    Value(uint8_t *buf, uint32_t length) : gen_lock_{0}, size_(sizeof(Value) + length), length_(length) {
        value_ = new uint8_t[length];
        std::memcpy(value_, buf, length_);
    }

    Value(Value const &value) {
        gen_lock_.store(0);
        length_ = value.length_;
        size_ = sizeof(Value) + length_;
        value_ = new uint8_t[length_];
        std::memcpy(value_, value.value_, length_);
    }

    ~Value() { delete[] value_; }

    inline uint32_t size() const {
        return size_;
    }

    void reset(uint8_t *value, uint32_t length) {
        gen_lock_.store(0);
        length_ = length;
        size_ = sizeof(Value) + length;
        delete[] value_; //Might introduce memory leak here.
        value_ = new uint8_t[length_];
        std::memcpy(value_, value, length);
    }

    uint64_t get() {
        return buffer();
    }

    uint32_t length() { return length_; }

    friend class UpsertContext;

    friend class DeleteContext;

    friend class ReadContext;

private:
    AtomicGenLock gen_lock_;
    uint32_t size_;
    uint32_t length_;
    uint8_t *value_;

    inline const uint8_t *buffer() const {
        return value_;
    }

    inline uint8_t *buffer() {
        return value_;
    }
};*/

static thread_local size_t conflict_counter = 0;

class UpsertContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(uint64_t key, uint64_t input) : key_{key}, input_(input) {}

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_}, input_{other.input_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(Value);
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        value.value_ = input_;
    }

    inline bool PutAtomic(Value &value) {
        bool replaced;
        while (!value.gen_lock_.try_lock(replaced) && !replaced) {
            std::this_thread::yield();
            conflict_counter++;
        }
        if (replaced) {
            // Some other thread replaced this record.
            return false;
        }
        // In-place update overwrites length and buffer, but not size.
        value.atomic_value_.store(input_, std::memory_order::memory_order_release);
        value.gen_lock_.unlock(false);
        return true;
    }

    static size_t Conflict() { return conflict_counter; }

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

    /// Copy (and deep-copy) constructor.
    DeleteContext(const DeleteContext &other) : key_{other.key_}, length_{other.length_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(Value) + length_;
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        value.gen_lock_.store(0);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint32_t length_;
};

class ReadContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    ReadContext(Key key) : key_{key} {}

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext &other) : key_{other.key_} {}

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const { return key_; }

    inline void Get(const Value &value) {
        // All reads should be atomic (from the mutable tail).
        //ASSERT_TRUE(false);
        value_ = value.value_;
    }

    inline void GetAtomic(const Value &value) {
        GenLock before, after;
        do {
            before = value.gen_lock_.load();
            value_ = value.value_;
            do {
                after = value.gen_lock_.load();
            } while (after.locked /*|| after.replaced*/);
        } while (before.gen_number != after.gen_number);
    }

    inline uint64_t Return() { return value_; }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
public:
    uint64_t value_;
};
}
}
#endif //HASHCOMP_CVKVCONTEXT_H
