//
// Created by Michael on 11/2/19.
//

#include <unordered_map>
#include "gtest/gtest.h"
#include "generator.h"
#include "GeneralLazySS.h"

constexpr static int count_per_round = 100000;

template<typename IT>
void funcCall() {
    uint64_t max = ((uint64_t) std::numeric_limits<IT>::max() <= (1LLU << 48))
                   ? (uint64_t) std::numeric_limits<IT>::max() : (1LLU << 48);
    zipf_distribution<IT> gen(max, 1.0);
    std::mt19937 mt;
    GeneralLazySS<IT> lss(0.0001);
    std::unordered_map<IT, int> counters;
    for (int i = 0; i < count_per_round; i++) {
        IT v = gen(mt);
        if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
        counters.find(v)->second++;
        lss.put(v, 1);
    }
    Item<IT> *output;
    for (int r = 0; r < 10; r++) {
        GeneralLazySS<IT> opr(0.0001);
        for (int i = 0; i < count_per_round; i++) {
            IT v = gen(mt);
            if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
            counters.find(v)->second++;
            opr.put(v, 1);
        }
        output = lss.merge(opr, true);
    }
    for (int i = 1; i < lss.volume(); i++) {
#ifndef NDEBUG
        std::cout << output[i].getItem() << ":" << output[i].getCount() << ":" << output[i].getDelta() << ":"
                  << output[i].getCount() - output[i].getDelta() << ":" << counters.find(output[i].getItem())->second
                  << std::endl;
#else
        if (i < 10 * sizeof(IT) - 9)
            ASSERT_EQ(output[i].getCount(), counters.find(output[i].getItem())->second);
#endif
    }
}

TEST(GeneralLazySSTest, uintlimitTest) {
    ASSERT_EQ(std::numeric_limits<uint8_t>::max() / 2, 0x7F);
    ASSERT_EQ(std::numeric_limits<uint16_t>::max() / 2, 0x7FFF);
    ASSERT_EQ(std::numeric_limits<uint32_t>::max() / 2, 0x7FFFFFFF);
    ASSERT_EQ(std::numeric_limits<uint64_t>::max() / 2, 0x7FFFFFFFFFFFFFFF);
}

TEST(GeneralLazySSTest, uintvTest) {
    //funcCall<uint8_t>();
    funcCall<uint16_t>();
    funcCall<uint32_t>();
    funcCall<uint64_t>();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}