//
// Created by Michael on 11/2/19.
//

#include <thread>
#include <unordered_map>
#include "gtest/gtest.h"
#include "generator.h"
#include "LazySpaceSaving.h"
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

constexpr int thread_number = 4;
constexpr uint64_t itemsize = (1llu << 24);
constexpr double fPhi = 0.0001;

int actualmaxcount[thread_number];

TEST(GeneralLazySSTest, dummyTest) {
    GeneralLazySS<uint64_t> glss(0.0001);
    for (uint64_t c = 0; c < itemsize; c++) {
        glss.put(c);
        glss.put(13332, 1);
    }
    Item<uint64_t> *output = glss.output(true);
    ASSERT_EQ(output[1].getCount(), itemsize + 1);
}

void workerThread(GeneralLazySS<uint64_t> glss, int tid, bool reverse) {
    if (reverse) {
        for (uint64_t c = itemsize - 1; c != std::numeric_limits<uint64_t>::max(); c--) {
            glss.put(c);
            glss.put(13332);
        }
    } else {
        for (uint64_t c = 0; c < itemsize; c++) {
            glss.put(c);
            glss.put(13332);
        }
    }
    actualmaxcount[tid] = glss.output(true)[1].getCount();
}

TEST(GeneralLazySSTest, parallelTest) {
    std::vector<std::thread> workers;
    GeneralLazySS<uint64_t> **glsses = (GeneralLazySS<uint64_t> **) std::malloc(sizeof(GeneralLazySS<uint64_t> *) * 4);
    int tids[thread_number];
    for (int t = 0; t < thread_number; t++) {
        glsses[t] = new GeneralLazySS<uint64_t>(fPhi);
        tids[t] = t;
        workers.push_back(std::thread(workerThread, std::ref(*glsses[t]), tids[t], false));
    }
    for (int t = 0; t < thread_number; t++) {
        workers[t].join();
        std::free(glsses[t]);
        ASSERT_EQ(actualmaxcount[t], itemsize + 1);
    }
    delete[] glsses;
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}