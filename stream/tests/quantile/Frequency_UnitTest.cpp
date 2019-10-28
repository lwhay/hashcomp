//
// Created by Michael on 10/28/2019.
//

#include "../../src/heavyhitter/SpaceSaving.h"
#include "../../src/heavyhitter/Frequency.h"
#include "../../src/heavyhitter/Frequent.h"
#include "gtest/gtest.h"

using namespace std;

TEST(SSUnitTest, Operations) {
    SS<int> ss(100);
    for (int i = 0; i < 100000; i++) {
        ss.put(i);
    }
    ASSERT_EQ(ss.getCounterNumber(), 100);
}

TEST(FrequencyUnitTest, Operations) {
    freq_type *ft = Freq_Init(0.01);
    for (int i = 0; i < 100000; i++) {
        Freq_Update(ft, i);
    }
#ifndef NDEBUG
    ITEMLIST *il;
    FirstGroup(ft, il);
    while (il->nexting != nullptr) {
        cout << il->item << ":" << endl;
        il = il->nexting;
    }
#endif
}

TEST(FrequentUnitTest, Operations) {
    Frequent fqt(100);
    for (int i = 0; i < 100000; i++) {
        fqt.add(i);
    }
    ASSERT_EQ(fqt.size(), 100);
#ifndef NDEBUG
    Node *n = fqt.header();
    while (n->getNext() != nullptr) {
        cout << n->getIndex() << ":" << n->getFreq() << endl;
        n = n->getNext();
    }
#endif
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}