//
// Created by iclab on 10/29/19.
//

#include "GroupFrequent.h"
#include "LazySpaceSaving.h"
#include "gtest/gtest.h"

TEST(GFreqUnitTest, Operations) {
    GroupFrequent gf(0.001);
    for (int i = 0; i < 1000000; i++) gf.put(i);
    ASSERT_EQ(gf.size(), 112048);
    ASSERT_EQ(gf.volume(), 1000);
    ASSERT_EQ(gf.range(), 2000);
}

TEST(LazySSUnitTest, Operations) {

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}