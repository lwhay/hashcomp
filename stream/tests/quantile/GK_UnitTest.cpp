//
// Created by Michael on 2019-10-15.
//

#include "GK.h"
#include "gtest/gtest.h"

TEST(GKUnitTest, Operations) {
    GK<uint32_t> gk(0.001);
    for (int i = 0; i < 1000000; i++) {
        gk.feed(i);
    }
    ASSERT_EQ(gk.max_v, 4294967295);
    ASSERT_EQ(gk.summary.size(), 0);
    gk.finalize();
    ASSERT_EQ(gk.summary.size(), 5590);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}