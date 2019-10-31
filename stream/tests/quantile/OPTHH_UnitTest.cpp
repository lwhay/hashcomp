//
// Created by iclab on 10/29/19.
//

#include <unordered_map>
#include "gtest/gtest.h"
#include "generator.h"
#include "GroupFrequent.h"
#include "LazySpaceSaving.h"

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
    ASSERT_EQ(lss.find(0), nullptr);
    ASSERT_EQ(lss.find(998994), nullptr);
    int reserved = 1000000;
    int min1 = 10000000;
    int min2 = 10000000;
    int min3 = 10000000;
    int min4 = 10000000;
    int min5 = 10000000;
    uint64_t total = 0;
    for (int i = 0; i < 999991; i++) {
        if (lss.find(i) != nullptr) {
            ASSERT_EQ(lss.find(i)->getCount(), 997);
            total += lss.find(i)->getCount();
            if (i < min1) min1 = i;
            else if (i < min2) min2 = i;
            else if (i < min3) min3 = i;
            else if (i < min4) min4 = i;
            else if (i < min5) min5 = i;
        } else {
            reserved--;
        }
    }
    for (int i = 999991; i <= 999999; i++) {
        ASSERT_EQ(lss.find(i)->getCount(), 998);
        total += lss.find(i)->getCount();
    }
    ASSERT_EQ(min1, 998989);
    ASSERT_EQ(min2, 998991);
    ASSERT_EQ(min3, 998992);
    ASSERT_EQ(min4, 998993);
    ASSERT_EQ(min5, 998995);
    ASSERT_EQ(total, 1000000);
    ASSERT_EQ(reserved, 1003);
}

TEST(LazySSUnitTest, OutputTest) {
    zipf_distribution<uint32_t> gen((1 << 31), 1.0);
    std::mt19937 mt;
    LazySpaceSaving lss(0.01);
    std::unordered_map<uint32_t, uint32_t> counters;
    for (int i = 0; i < 1000000; i++) {
        uint32_t v = gen(mt);
        if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
        counters.find(v)->second++;
        lss.put(v, 1);
    }
    Counter *ordered = lss.output(true);
    for (int i = 1; i < lss.volume(); i++)
        std::cout << ordered[i].getItem() << ":" << ordered[i].getCount() << ":" << ordered[i].getDelta() << ":"
                  << ordered[i].getCount() - ordered[i].getDelta() << ":" << counters.find(ordered[i].getItem())->second
                  << std::endl;
}

TEST(LazySSUnitTest, MergeTest) {
    zipf_distribution<uint32_t> gen((1 << 31), 1.0);
    std::mt19937 mt;
    LazySpaceSaving lss(0.0001);
    std::unordered_map<uint32_t, uint32_t> counters;
    for (int i = 0; i < 1000000; i++) {
        uint32_t v = gen(mt);
        if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
        counters.find(v)->second++;
        lss.put(v, 1);
    }
    LazySpaceSaving opr(0.0001);
    for (int i = 0; i < 1000000; i++) {
        uint32_t v = gen(mt);
        if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
        counters.find(v)->second++;
        opr.put(v, 1);
    }
    Counter *output = lss.merge(opr);
    for (int i = 1; i < lss.volume(); i++) {
        std::cout << output[i].getItem() << ":" << output[i].getCount() << ":" << output[i].getDelta() << ":"
                  << output[i].getCount() - output[i].getDelta() << ":" << counters.find(output[i].getItem())->second
                  << std::endl;
    }
    delete[] output;
}

TEST(LazySSUnitTest, IterativeMergeTest) {
    zipf_distribution<uint32_t> gen((1 << 31), 1.0);
    std::mt19937 mt;
    LazySpaceSaving lss(0.0001);
    std::unordered_map<uint32_t, uint32_t> counters;
    for (int i = 0; i < 100000; i++) {
        uint32_t v = gen(mt);
        if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
        counters.find(v)->second++;
        lss.put(v, 1);
    }
    Counter *output;
    for (int r = 0; r < 10; r++) {
        LazySpaceSaving opr(0.0001);
        for (int i = 0; i < 100000; i++) {
            uint32_t v = gen(mt);
            if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
            counters.find(v)->second++;
            opr.put(v, 1);
        }
        output = lss.merge(opr, true);
    }
    for (int i = 1; i < lss.volume(); i++) {
        std::cout << output[i].getItem() << ":" << output[i].getCount() << ":" << output[i].getDelta() << ":"
                  << output[i].getCount() - output[i].getDelta() << ":" << counters.find(output[i].getItem())->second
                  << std::endl;
    }
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}