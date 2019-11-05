//
// Created by iclab on 10/7/19.
//

#include <thread>
#include "faster.h"
#include "gtest/gtest.h"
#include "cvkvcontext.h"

using namespace FASTER::api;
using namespace FASTER::io;
using namespace FASTER::core;

TEST(LockComparison, AtomGenLockVarTest) {
    AtomicGenLock lock;
    bool signal;
    bool success = lock.try_lock(signal);
    ASSERT_TRUE(success);
    ASSERT_EQ(lock.load().locked, 1);
    ASSERT_EQ(lock.load().replaced, 0);
    ASSERT_EQ(lock.load().gen_number, 0);
    lock.unlock(signal);
    ASSERT_FALSE(signal);
    ASSERT_EQ(lock.load().locked, 0);
    ASSERT_EQ(lock.load().replaced, 0);
    ASSERT_EQ(lock.load().gen_number, 1);
}

TEST(LockComparison, AtomGenLockMutexTest) {
    auto writer = [](AtomicGenLock *lock) {
        bool signal;
        bool success = lock->try_lock(signal);
        ASSERT_TRUE(success);
        ASSERT_EQ(lock->load().locked, 1);
        ASSERT_EQ(lock->load().replaced, 0);
        ASSERT_EQ(lock->load().gen_number, 0);
        sleep(3);
        lock->unlock(signal);
        ASSERT_FALSE(signal);
        ASSERT_EQ(lock->load().locked, 0);
        ASSERT_EQ(lock->load().replaced, 0);
        ASSERT_EQ(lock->load().gen_number, 1);
    };

    auto reader = [](AtomicGenLock *lock) {
        sleep(1);
        GenLock before = lock->load();
        ASSERT_EQ(before.locked, 1);
        ASSERT_EQ(before.replaced, 0);
        ASSERT_EQ(before.gen_number, 0);
        sleep(1);
        GenLock middle = lock->load();
        ASSERT_EQ(middle.locked, 1);
        ASSERT_EQ(middle.replaced, 0);
        ASSERT_EQ(middle.gen_number, 0);
        ASSERT_EQ(before.control_, middle.control_);
        sleep(2);
        GenLock after = lock->load();
        ASSERT_EQ(after.locked, 0);
        ASSERT_EQ(after.replaced, 0);
        ASSERT_EQ(after.gen_number, 1);
        ASSERT_NE(before.control_, after.control_);
    };

    AtomicGenLock lock;
    std::thread writeThread(writer, &lock);
    std::thread readThread(reader, &lock);
    writeThread.join();
    readThread.join();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}