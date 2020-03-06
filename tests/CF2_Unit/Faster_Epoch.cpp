//
// Created by Michael on 3/5/20.
//

#include <cstdint>
#include <vector>
#include <thread>
#include "mhash/epoch/faster/light_epoch.h"
#include "gtest/gtest.h"

using namespace FASTER::core;

TEST(FasterTest, LightEpochNaiveTest) {
    LightEpoch epoch;
    epoch.Protect();
    ASSERT_EQ(epoch.IsProtected(), true);
    epoch.Unprotect();
    ASSERT_EQ(epoch.IsProtected(), false);
}

constexpr uint64_t thread_number = 4;

TEST(FasterTest, MultiLightEpochNaiveTest) {
    LightEpoch epoch;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_number; i++) {
        threads.push_back(std::thread([](LightEpoch &epoch) {
            for (int j = 0; j < (1 << 20); j++) {
                epoch.Protect();
                ASSERT_EQ(epoch.IsProtected(), true);
                epoch.Unprotect();
                ASSERT_EQ(epoch.IsProtected(), false);
            }
        }, std::ref(epoch)));
    }
    for (int i = 0; i < thread_number; i++) threads[i].join();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}