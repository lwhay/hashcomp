//
// Created by Michael on 11/2/19.
//

#include <unordered_map>
#include "gtest/gtest.h"
#include "generator.h"
#include "GeneralLazySS.h"

template<typename IT>
void funcCall() {
    zipf_distribution<IT> gen((1 << 31), 1.0);
    std::mt19937 mt;
    GeneralLazySS<IT> lss(0.0001);
    std::unordered_map<IT, IT> counters;
    for (int i = 0; i < 100000; i++) {
        IT v = gen(mt);
        if (counters.find(v) == counters.end()) counters.insert(std::make_pair(v, 0));
        counters.find(v)->second++;
        lss.put(v, 1);
    }
    Item<IT> *output;
    for (int r = 0; r < 10; r++) {
        GeneralLazySS<IT> opr(0.0001);
        for (int i = 0; i < 100000; i++) {
            IT v = gen(mt);
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

TEST(GeneralLazySSTest, uint32Test) {
    funcCall<uint32_t>();
    //funcCall<int32_t>();
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}