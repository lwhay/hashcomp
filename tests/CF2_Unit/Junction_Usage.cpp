//
// Created by Michael on 2019-10-14.
//

#include <cstdint>
#include <string>
#include <cstring>
#include <deque>
#include <functional>
#include <thread>
#include "gtest/gtest.h"
#include "junction/QSBR.h"
#include "junction/ConcurrentMap_Leapfrog.h"
#include "junction/ConcurrentMap_Grampa.h"

struct Foo {
    uint64_t value;
public:
    Foo(uint64_t v) : value(v) {}

    uint64_t get() { return value; }
};

/*template<typename Key, typename Value>
class FooAccess {
    Key key;
    Value value;
public:
    constexpr static Key NullKey = Key(0);

    FooAccess(Key k, Value v) : key(k), value(v) {}
};

TEST(JunctionTests, LeapfrogPtrOperations) {
    junction::QSBR::Context context = junction::DefaultQSBR.createContext();
    junction::ConcurrentMap_Leapfrog<Foo *, Foo *> jmap(128);
    jmap.assign(new Foo(1), new Foo(1));
    auto v = jmap.get(new Foo(1));
    ASSERT_EQ(v->get(), 1);
    jmap.exchange(new Foo(1), new Foo(2));
    jmap.find(new Foo(1));
    ASSERT_EQ(jmap.get(new Foo(1))->get(), 2);
    junction::DefaultQSBR.update(context);
    junction::DefaultQSBR.destroyContext(context);
}*/

TEST(JunctionTests, LeapfrogExchangeAndFind) {
    junction::QSBR::Context context = junction::DefaultQSBR.createContext();
    junction::ConcurrentMap_Leapfrog<uint64_t, Foo *> jmap(128);
    ASSERT_EQ(sizeof(Foo), sizeof(uint64_t));
    uint64_t i = 1;
    jmap.exchange(1, (Foo *) &i);
    auto v = jmap.get(1);
    ASSERT_EQ(v->get(), 1);
    i = 2;
    jmap.exchange(1, (Foo *) &i);
    jmap.find(1);
    ASSERT_EQ(jmap.get(1)->get(), 2);
    junction::DefaultQSBR.update(context);
    junction::DefaultQSBR.destroyContext(context);
}

TEST(JunctionTests, LeapfrogAssignAndFind) {
    junction::QSBR::Context context = junction::DefaultQSBR.createContext();
    junction::ConcurrentMap_Leapfrog<uint64_t, Foo *> jmap(128);
    uint64_t i = 1;
    jmap.assign(1, (Foo *) &i);
    auto v = jmap.get(1);
    ASSERT_EQ(v->get(), 1);
    for (int t = 0; t < 100; t++) {
        i = t;
        jmap.assign(1, (Foo *) &i);
        ASSERT_EQ(jmap.get(1)->get(), t);
    }
    i = 2;
    jmap.exchange(1, (Foo *) &i);
    jmap.find(1);
    ASSERT_EQ(jmap.get(1)->get(), 2);
    junction::DefaultQSBR.update(context);
    junction::DefaultQSBR.destroyContext(context);
}

TEST(JunctionTests, LeapfrogOperations) {
    junction::QSBR::Context context = junction::DefaultQSBR.createContext();
    junction::ConcurrentMap_Leapfrog<uint64_t, Foo *> jmap(128);
    jmap.assign(1, new Foo(1));
    auto v = jmap.get(1);
    ASSERT_EQ(v->get(), 1);
    jmap.exchange(1, new Foo(2));
    jmap.find(1);
    ASSERT_EQ(jmap.get(1)->get(), 2);
    junction::DefaultQSBR.update(context);
    junction::DefaultQSBR.destroyContext(context);
}

TEST(JunctionTests, LeapfrogExchangeOperations) {
    junction::QSBR::Context context = junction::DefaultQSBR.createContext();
    junction::ConcurrentMap_Leapfrog<uint64_t, Foo *> jmap(128);
    jmap.exchange(1, new Foo(1));
    auto v = jmap.get(1);
    ASSERT_EQ(v->get(), 1);
    jmap.exchange(111111111111llu, new Foo(1));
    ASSERT_EQ(jmap.get(111111111111llu)->get(), 1);
    jmap.exchange(1, new Foo(2));
    jmap.find(1);
    ASSERT_EQ(jmap.get(1)->get(), 2);
    junction::DefaultQSBR.update(context);
    junction::DefaultQSBR.destroyContext(context);
}

TEST(JunctionTests, GrampaOperations) {
    junction::QSBR::Context context = junction::DefaultQSBR.createContext();
    junction::ConcurrentMap_Grampa<int, Foo *> jmap(128);
    for (int i = 0; i < 128; i++) jmap.assign(i, new Foo(i));
    jmap.find(1);
    ASSERT_EQ(jmap.get(1)->get(), 1);
    jmap.exchange(1, new Foo(2));
    jmap.find(1);
    ASSERT_EQ(jmap.get(1)->get(), 2);
    junction::DefaultQSBR.update(context);
    junction::DefaultQSBR.destroyContext(context);
}

TEST(JunctionTests, StringTest) {
    /*junction::ConcurrentMap_Leapfrog<std::string, std::string> map;
    map.assign((turf::u8 *) "key", (turf::u8 *) "value");*/
}

TEST(JunctionTests, TurfUptrTest) {
    char *key = "key";
    char *value = "value";
    char *key1 = "key";
    junction::ConcurrentMap_Leapfrog<turf::uptr, turf::uptr> map;
    map.assign((turf::uptr) key, (turf::uptr) value);
    char oldv[6];
    std::strcpy(oldv, value);

    //Verify value is maintained by map
    value = "exception";
    turf::uptr output = 0;
    output = map.get((turf::uptr) "key");
    ASSERT_STREQ((char *) output, "value");
    ASSERT_STREQ(value, "exception");
    output = map.exchange((turf::uptr) "key", (turf::uptr) "dummy");
    ASSERT_STREQ((char *) output, "value");
    ASSERT_STREQ((char *) value, "exception");
    ASSERT_STREQ((char *) map.get((turf::uptr) key1), "dummy");

    turf::uptr ret = map.erase((turf::uptr) key);
    ASSERT_STREQ((char *) ret, "dummy");
    ASSERT_EQ(map.get((turf::uptr) key1), 0);
    ret = map.erase((turf::uptr) key);
    ASSERT_STREQ((char *) ret, nullptr);

    ASSERT_EQ(map.get((turf::uptr) key), 0);
    //Verify key is not maintained by map
    char *tk = new char[5];
    std::strcpy(tk, key);
    map.assign((turf::uptr) tk, (turf::uptr) value);
    ASSERT_EQ(map.get((turf::uptr) tk), (turf::uptr) value);
    ASSERT_EQ(map.get((turf::uptr) key), 0);
    std::memset(tk, 0, 5);
    delete tk;
    ASSERT_EQ(map.get((turf::uptr) key), 0);

    //Verify value is not maintained by map
    char *tv = new char[5];
    std::strcpy(tv, value);
    map.assign((turf::uptr) key, (turf::uptr) tv);
    std::memset(tv, 0, 5);
    delete tv;
    ASSERT_STREQ((char *) map.get((turf::uptr) key), "");
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}