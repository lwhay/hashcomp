//
// Created by iclab on 10/29/19.
//

#include "GroupFrequent.h"
#include "LazySpaceSaving.h"
#include "gtest/gtest.h"

TEST(GFreqUnitTest, Operations) {
    GroupFrequent gf(0.001);
    //gf.put(1);
}

TEST(LazySSUnitTest, Operations) {

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}