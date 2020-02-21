//
// Created by Michael on 2/21/20.
//

#include <iostream>
#include <string>
#include <gtest/gtest.h>
#include "tracer.h"
#include <unordered_map>
#include "concurrent_hash_map.h"
#include "dict/concurrent_dict.h"
#include "dict/slice.h"

using namespace ycsb;

TEST(YCSBLoaderTest, Deliminator) {
    YCSBLoader loader("../res/ycsb_data/run-a.dat");
    vector<YCSB_request *> ret = loader.load();
    for (int i = 0; i < loader.size(); i++) {
        std::cout << ret[i]->getOp() << " " << ret[i]->getKey() << " " << ret[i]->keyLength() << " ";
        if (ret[i]->getOp() % 2 != 0) std::cout << ret[i]->getVal() << " " << ret[i]->valLength();
        std::cout << std::endl;
    }
    for (int i = 0; i < loader.size(); i++) delete ret[i];
}

TEST(YCSBLoaderTest, CMapTester) {
    using concurrent_dict::ConcurrentDict;
    using concurrent_dict::Slice;
    std::unordered_map<string, string> umap;
    ConcurrentDict map(128, 20, 1);

    YCSBLoader loader("../res/ycsb_data/load-a.dat");
    vector<YCSB_request *> src = loader.load();
    for (int i = 0; i < loader.size(); i++) {
        if (src[i]->getOp() == YCSB_operator::insert) {
            map.Insert(Slice(src[i]->getKey()), Slice(src[i]->getVal()));
            umap.insert(std::make_pair(src[i]->getKey(), src[i]->getVal()));
        }
    }

    YCSBLoader loads("../res/ycsb_data/run-a.dat");
    vector<YCSB_request *> ret = loads.load();
    for (int i = 0; i < loads.size(); i++) {
        std::string value;
        map.Find(Slice(ret[i]->getKey()), &value);
        string realv = umap.find(ret[i]->getKey())->second;
        if (ret[i]->getOp() == YCSB_operator::lookup) ASSERT_STREQ(realv.c_str(), value.c_str());
    }
    for (int i = 0; i < loader.size(); i++) delete src[i];
    for (int i = 0; i < loads.size(); i++) delete ret[i];
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}