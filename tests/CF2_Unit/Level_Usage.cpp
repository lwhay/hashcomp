//
// Created by Michael on 2/25/20.
//

#include <cstdint>
#include <functional>
#include "gtest/gtest.h"
#include "level_hashing.h"

TEST(LevelTest, uint8TOstring) {
    level_hash *index = level_init(10);
    {
        uint64_t ik = 234234234151234323llu;
        uint64_t iv = 234234234151234323llu;
        ASSERT_EQ(level_insert(index, (uint8_t *) &ik, (uint8_t *) &iv), 0);
    }
    uint64_t uk = 234234234151234323llu;
    uint64_t ut = 234234234151234321llu;
    uint8_t *uv = new uint8_t[256];
    ASSERT_EQ(level_query(index, (uint8_t *) &uk, (uint8_t *) &uv), 1);
    ASSERT_EQ(level_query(index, (uint8_t *) &ut, (uint8_t *) &uv), 1);
    char *sk = new char[256];
    std::memset(sk, 0, 256);
    std::sprintf(sk, "%llu", 234234234151234324llu);
    char *sv = new char[256];
    std::memset(sv, 0, 256);
    std::sprintf(sv, "%llu", 234234234151234324llu);
    ASSERT_EQ(level_insert(index, (uint8_t *) sk, (uint8_t *) sv), 0);
    char *qk = new char[256];
    std::memset(qk, 0, 256);
    std::sprintf(qk, "%llu", 234234234151234324llu);
    ASSERT_EQ(level_query(index, (uint8_t *) qk, (uint8_t *) uv), 0);
    delete sk;
    sk = nullptr;
    char *qv = new char[256];
    ASSERT_EQ(level_query(index, (uint8_t *) qk, (uint8_t *) qv), 0);
    ASSERT_STREQ(sv, qv);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}