#include <unity.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "cuckoo_filter/cuckoo_filter.h"

static CuckooFilter cf;

void setUp() { cf.clear(); }
void tearDown() {}

// ── False Positive Rate measurement ─────────────────────────────────────────
// Insert N known items, then query M items that were NOT inserted.
// Any "found" among non-inserted items is a false positive.

void test_false_positive_rate() {
    srand(54321);

    const size_t INSERT_COUNT = 500;
    const size_t QUERY_COUNT  = 10000;

    // Insert items: keys 0..INSERT_COUNT-1
    for (size_t i = 0; i < INSERT_COUNT; i++) {
        uint8_t key[8];
        memset(key, 0, sizeof(key));
        memcpy(key, &i, sizeof(size_t) < 8 ? sizeof(size_t) : 8);
        CF_Status s = cf.add(key, sizeof(key));
        TEST_ASSERT_EQUAL(CF_Ok, s);
    }

    // Query items that were NOT inserted: keys starting from INSERT_COUNT+10000
    size_t false_positives = 0;
    for (size_t i = 0; i < QUERY_COUNT; i++) {
        size_t val = INSERT_COUNT + 10000 + i;
        uint8_t key[8];
        memset(key, 0, sizeof(key));
        memcpy(key, &val, sizeof(size_t) < 8 ? sizeof(size_t) : 8);
        if (cf.contain(key, sizeof(key)) == CF_Ok) {
            false_positives++;
        }
    }

    double fpr = (double)false_positives / (double)QUERY_COUNT * 100.0;
    printf("\n=== FPR Benchmark ===\n");
    printf("  Inserted:         %zu\n", INSERT_COUNT);
    printf("  Non-member queries: %zu\n", QUERY_COUNT);
    printf("  False positives:  %zu\n", false_positives);
    printf("  FPR:              %.2f%%\n", fpr);
    printf("  Expected (8-bit): ~3%%\n");
    printf("=====================\n");

    // 8-bit fingerprint → theoretical FPR ≈ 1/2^8 * bucket_size ≈ 3.1%
    // Allow up to 6% to account for variance
    TEST_ASSERT_LESS_THAN(6.0, fpr);
}

// ── Occupancy benchmark ─────────────────────────────────────────────────────

void test_max_occupancy() {
    srand(99999);
    size_t inserted = 0;

    for (size_t i = 0; i < cf.capacity() + 100; i++) {
        uint8_t key[8];
        memset(key, 0, sizeof(key));
        memcpy(key, &i, sizeof(size_t) < 8 ? sizeof(size_t) : 8);
        CF_Status s = cf.add(key, sizeof(key));
        if (s == CF_Ok) inserted++;
        else break;
    }

    double occ = (double)inserted / (double)cf.capacity() * 100.0;
    printf("\n=== Occupancy Benchmark ===\n");
    printf("  Capacity:  %zu\n", cf.capacity());
    printf("  Inserted:  %zu\n", inserted);
    printf("  Occupancy: %.1f%%\n", occ);
    printf("===========================\n");

    // Cuckoo filters with 4-way buckets typically achieve >90%
    TEST_ASSERT_GREATER_THAN(80.0, occ);
}

int main(int argc, char** argv) {
    UNITY_BEGIN();
    RUN_TEST(test_false_positive_rate);
    RUN_TEST(test_max_occupancy);
    return UNITY_END();
}
