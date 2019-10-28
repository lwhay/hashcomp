//
// Created by Michael on 10/28/2019.
//

#include "Frequency.h"
#include "Frequent.h"
#include "gtest/gtest.h"

using namespace std;

TEST(FrequencyUnitTest, Operations) {
    freq_type *ft = Freq_Init(0.01);
    for (int i = 0; i < 100000; i++) {
        Freq_Update(ft, i);
    }
}

TEST(FrequentUnitTest, Operations) {
    Frequent fqt(100);
    for (int i = 0; i < 100000; i++) {
        fqt.add(i);
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}