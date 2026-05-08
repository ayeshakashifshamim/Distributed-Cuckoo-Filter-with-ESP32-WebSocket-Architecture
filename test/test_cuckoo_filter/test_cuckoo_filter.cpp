#include <unity.h>
#include <string.h>
#include <stdlib.h>
#include "cuckoo_filter/cuckoo_filter.h"

static CuckooFilter cf;

void setUp() { cf.clear(); }
void tearDown() {}

void test_empty_filter() {
    TEST_ASSERT_EQUAL(0, cf.size());
    TEST_ASSERT_EQUAL(CF_NUM_BUCKETS * CF_BUCKET_SIZE, cf.capacity());
    TEST_ASSERT_EQUAL(0, cf.loadPercent());
}

void test_insert_and_lookup() {
    const uint8_t key[] = "hello";
    TEST_ASSERT_EQUAL(CF_Ok, cf.add(key, 5));
    TEST_ASSERT_EQUAL(1, cf.size());
    TEST_ASSERT_EQUAL(CF_Ok, cf.contain(key, 5));
}

void test_lookup_missing() {
    const uint8_t key[] = "missing";
    TEST_ASSERT_EQUAL(CF_NotFound, cf.contain(key, 7));
}

void test_delete() {
    const uint8_t key[] = "delme";
    cf.add(key, 5);
    TEST_ASSERT_EQUAL(CF_Ok, cf.remove(key, 5));
    TEST_ASSERT_EQUAL(0, cf.size());
    TEST_ASSERT_EQUAL(CF_NotFound, cf.contain(key, 5));
}

void test_delete_missing() {
    const uint8_t key[] = "ghost";
    TEST_ASSERT_EQUAL(CF_NotFound, cf.remove(key, 5));
}

// multiple items

void test_insert_many() {
    const size_t N = 100;
    for (size_t i = 0; i < N; i++) {
        uint8_t key[4];
        memcpy(key, &i, sizeof(key));
        CF_Status s = cf.add(key, sizeof(key));
        TEST_ASSERT_EQUAL(CF_Ok, s);
    }
    TEST_ASSERT_EQUAL(N, cf.size());

    // All should be found
    for (size_t i = 0; i < N; i++) {
        uint8_t key[4];
        memcpy(key, &i, sizeof(key));
        TEST_ASSERT_EQUAL(CF_Ok, cf.contain(key, sizeof(key)));
    }
}

void test_clear() {
    const uint8_t key[] = "abc";
    cf.add(key, 3);
    cf.clear();
    TEST_ASSERT_EQUAL(0, cf.size());
    TEST_ASSERT_EQUAL(CF_NotFound, cf.contain(key, 3));
}

// capacity limit 

void test_fill_to_capacity() {
    srand(12345);
    size_t inserted = 0;
    // Try to insert up to capacity; expect most succeed
    for (size_t i = 0; i < cf.capacity(); i++) {
        uint8_t key[8];
        memcpy(key, &i, sizeof(size_t) < 8 ? sizeof(size_t) : 8);
        if (sizeof(size_t) < 8) memset(key + sizeof(size_t), 0, 8 - sizeof(size_t));
        CF_Status s = cf.add(key, 8);
        if (s == CF_Ok) inserted++;
        else break;
    }
    // typically achieve >90% occupancy
    TEST_ASSERT_GREATER_THAN(cf.capacity() * 80 / 100, inserted);
}

// load %

void test_load_percent() {
    srand(99);
    for (size_t i = 0; i < 50; i++) {
        uint8_t key[4];
        memcpy(key, &i, sizeof(key));
        cf.add(key, sizeof(key));
    }
    uint8_t pct = cf.loadPercent();
    uint8_t expected = (uint8_t)((50 * 100) / cf.capacity());
    TEST_ASSERT_EQUAL(expected, pct);
}

int main(int argc, char** argv) {
    //
    srand(42);
    UNITY_BEGIN();
    RUN_TEST(test_empty_filter);
    RUN_TEST(test_insert_and_lookup);
    RUN_TEST(test_lookup_missing);
    RUN_TEST(test_delete);
    RUN_TEST(test_delete_missing);
    RUN_TEST(test_insert_many);
    RUN_TEST(test_clear);
    RUN_TEST(test_fill_to_capacity);
    RUN_TEST(test_load_percent);
    return UNITY_END();
}
