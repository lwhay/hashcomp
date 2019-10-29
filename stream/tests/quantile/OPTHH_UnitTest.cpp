//
// Created by iclab on 10/29/19.
//

#include "GroupFrequent.h"
#include "LazySpaceSaving.h"
#include "gtest/gtest.h"

TEST(OddUnitTest, Operations) {
    ASSERT_EQ((10000 + 90001) | 1, 100001);
    ASSERT_EQ((10000 + 90000) | 1, 100001);
    ASSERT_EQ((10000 + 90001) | 3, 100003);
    ASSERT_EQ((10000 + 90000) | 3, 100003);
}

TEST(GFreqUnitTest, Operations) {
    GroupFrequent gf(0.001);
    for (int i = 0; i < 1000000; i++) gf.put(i);
    ASSERT_EQ(gf.size(), 112048);
    ASSERT_EQ(gf.volume(), 1000);
    ASSERT_EQ(gf.range(), 2000);
}

TEST(LazySSUnitTest, Operations) {
    LazySpaceSaving lss(0.001);
    for (int i = 0; i < 1000000; i++) lss.put(i);
    ASSERT_EQ(lss.size(), 44180);
    ASSERT_EQ(lss.volume(), 1003);
    ASSERT_EQ(lss.range(), 3009);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}