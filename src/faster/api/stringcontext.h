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

class Key {
public:
    Key(uint8_t *buf, uint32_t len) : len_{len} {
        buf_ = new uint8_t[len_];
        std::memcpy(buf_, buf, len_);
    }

    ~Key() { delete[] buf_; }

    static inline uint32_t size() {
        return static_cast<uint32_t>(sizeof(Key)) /*+ len_*/;
    }

    inline KeyHash GetHash() const {
        std::hash<uint8_t *> hash_fn;
        return KeyHash{hash_fn(buf_)};
    }

    /// Comparison operators.
    inline bool operator==(const Key &other) const {
        return len_ == other.len_ && std::memcmp(buf_, other.buf_, len_) == 0;
    }

    inline bool operator!=(const Key &other) const {
        return !(*this == other);
    }

private:
    uint32_t len_;
    uint8_t *buf_;
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

class Value {
public:
    Value() : gen_lock_{0}, size_{0}, length_{0} {}

    Value(uint8_t *buf, uint32_t length) : gen_lock_{0}, size_(sizeof(Value) + length), length_(length) {
        value_ = new uint8_t[length];
        std::memcmp(value_, buf, length_);
    }

    Value(Value const &value) {
        gen_lock_.store(0);
        length_ = value.length_;
        size_ = sizeof(Value) + length_;
        value_ = new uint8_t[length_];
        std::memcmp(value_, value.value_, length_);
    }

    ~Value() { delete[] value_; }

    inline uint32_t size() const {
        return size_;
    }

    void reset(uint8_t *value, uint32_t length) {
        length_ = length;
        size_ = sizeof(Value) + length;
        value_ = new uint8_t[length_];
        std::memcpy(value_, value, length);
    }

    uint8_t *get() {
        return buffer();
    }

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
};

class UpsertContext : public IAsyncContext {
public:
    typedef Key key_t;
    typedef Value value_t;

    UpsertContext(Key key, Value value) : key_{key}, length_{value.length_}, input_buffer(new uint8_t[length_]) {}

    /// Copy (and deep-copy) constructor.
    UpsertContext(const UpsertContext &other) : key_{other.key_}, length_{other.length_} {
        input_buffer = new uint8_t[length_];
        std::memcpy(input_buffer, other.input_buffer, length_);
    }

    ~UpsertContext() { delete[] input_buffer; }

    void reset(uint8_t *buffer) { std::memcpy(input_buffer, buffer, length_); }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline uint32_t value_size() const {
        return sizeof(Value) + length_;
    }

    inline uint8_t *get() const {
        return input_buffer;
    }

    /// Non-atomic and atomic Put() methods.
    inline void Put(Value &value) {
        value.gen_lock_.store(0);
        value.size_ = sizeof(Value) + length_;
        value.length_ = length_;
        value.reset(input_buffer, length_);
        /* if (value.buffer() == nullptr) value.reset() = new uint8_t[value.length_];
         std::memcpy(value.buffer(), input_buffer, length_);*/
    }

    inline bool PutAtomic(Value &value) {
        bool replaced;
        while (!value.gen_lock_.try_lock(replaced) && !replaced) {
            std::this_thread::yield();
        }
        if (replaced) {
            // Some other thread replaced this record.
            return false;
        }
        if (value.size_ < sizeof(Value) + length_) {
            // Current value is too small for in-place update.
            value.gen_lock_.unlock(true);
            return false;
        }
        // In-place update overwrites length and buffer, but not size.
        value.length_ = length_;
        std::memcpy(value.buffer(), input_buffer, length_);
        value.gen_lock_.unlock(false);
        return true;
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
    uint32_t length_;
    uint8_t *input_buffer;
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
        value.size_ = sizeof(Value) + length_;
        value.length_ = length_;
        std::memset(value.buffer(), 88, length_);
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

    ReadContext(Key key) : key_{key}, output_length{0} {}

    /// Copy (and deep-copy) constructor.
    ReadContext(const ReadContext &other) : key_{other.key_}, output_length{0} {}

    ~ReadContext() { delete[] output_bytes; }

    /// The implicit and explicit interfaces require a key() accessor.
    inline const Key &key() const {
        return key_;
    }

    inline void Get(const Value &value) {
        // All reads should be atomic (from the mutable tail).
        //ASSERT_TRUE(false);
    }

    inline void GetAtomic(const Value &value) {
        GenLock before, after;
        do {
            before = value.gen_lock_.load();
            output_length = value.length_;
            output_bytes = new uint8_t[output_length];
            std::memcpy(output_bytes, value.buffer(), output_length);
            do {
                after = value.gen_lock_.load();
            } while (after.locked || after.replaced);
        } while (before.gen_number != after.gen_number);
    }

protected:
    /// The explicit interface requires a DeepCopy_Internal() implementation.
    Status DeepCopy_Internal(IAsyncContext *&context_copy) {
        return IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }

private:
    Key key_;
public:
    uint8_t output_length;
    // Extract two bytes of output.
    uint8_t *output_bytes;
};
}
}
#endif //HASHCOMP_CVKVCONTEXT_H
