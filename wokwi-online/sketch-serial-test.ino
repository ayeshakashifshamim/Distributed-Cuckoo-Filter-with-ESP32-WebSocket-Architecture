// ─────────────────────────────────────────────────────────────────────────────
// sketch-serial-test.ino - Serial test of CuckooFilter without ESP-NOW
// Tests basic filter operations locally via serial output
// ─────────────────────────────────────────────────────────────────────────────
#include <Arduino.h>
#include <string.h>
#include <stdlib.h>
#include "messages.h"
#include "cuckoo_filter.h"

// ─────────────────────────────────────────────────────────────────────────────
// ─── CUCKOO FILTER IMPLEMENTATION (inlined) ────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────

uint32_t CuckooFilter::_murmur(const uint8_t* data, size_t len) {
    const uint32_t seed = 0xbc9f1d34;
    const uint32_t m    = 0x5bd1e995;
    uint32_t h = seed ^ (uint32_t)len;
    while (len >= 4) {
        uint32_t k;
        memcpy(&k, data, 4);
        k *= m;
        k ^= k >> 24;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len  -= 4;
    }
    switch (len) {
        case 3: h ^= (uint32_t)data[2] << 16;
        case 2: h ^= (uint32_t)data[1] <<  8;
        case 1: h ^= (uint32_t)data[0];
                h *= m;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

uint32_t CuckooFilter::_indexHash(uint32_t hv) { return hv & (CF_NUM_BUCKETS - 1); }
uint32_t CuckooFilter::_tagHash(uint32_t hv) {
    uint32_t tag = hv & ((1u << CF_BITS_PER_TAG) - 1);
    tag += (tag == 0);
    return tag;
}
size_t CuckooFilter::_altIndex(size_t index, uint32_t tag) {
    return _indexHash((uint32_t)(index ^ (tag * 0x5bd1e995)));
}
void CuckooFilter::_genIndexTag(const uint8_t* data, size_t len, size_t* index, uint32_t* tag) const {
    uint32_t h = _murmur(data, len);
    *index = _indexHash(h >> 8);
    *tag   = _tagHash(h);
}
bool CuckooFilter::_insertTagToBucket(size_t i, uint32_t tag, bool kickout, uint32_t& oldtag) {
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i][j] == 0) {
            _table[i][j] = (uint8_t)tag;
            return true;
        }
    }
    if (kickout) {
        size_t r = (size_t)rand() % CF_BUCKET_SIZE;
        oldtag = _table[i][r];
        _table[i][r] = (uint8_t)tag;
    }
    return false;
}
bool CuckooFilter::_findTagInBuckets(size_t i1, size_t i2, uint32_t tag) const {
    uint8_t t = (uint8_t)tag;
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i1][j] == t || _table[i2][j] == t) return true;
    }
    return false;
}
bool CuckooFilter::_deleteTagFromBucket(size_t i, uint32_t tag) {
    uint8_t t = (uint8_t)tag;
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i][j] == t) {
            _table[i][j] = 0;
            return true;
        }
    }
    return false;
}
CF_Status CuckooFilter::_addImpl(size_t i, uint32_t tag) {
    size_t curindex = i;
    uint32_t curtag = tag;
    uint32_t oldtag = 0;
    for (uint32_t count = 0; count < CF_MAX_KICKS; count++) {
        bool kickout = (count > 0);
        oldtag = 0;
        if (_insertTagToBucket(curindex, curtag, kickout, oldtag)) {
            _num_items++;
            return CF_Ok;
        }
        if (kickout) curtag = oldtag;
        curindex = _altIndex(curindex, curtag);
    }
    _victim.index = curindex;
    _victim.tag   = curtag;
    _victim.used  = true;
    return CF_Ok;
}
CuckooFilter::CuckooFilter() : _num_items(0) {
    memset(_table, 0, sizeof(_table));
    _victim = {0, 0, false};
}
CF_Status CuckooFilter::add(const uint8_t* data, size_t len) {
    if (_victim.used) return CF_NotEnoughSpace;
    size_t i;
    uint32_t tag;
    _genIndexTag(data, len, &i, &tag);
    return _addImpl(i, tag);
}
CF_Status CuckooFilter::contain(const uint8_t* data, size_t len) const {
    size_t i1, i2;
    uint32_t tag;
    _genIndexTag(data, len, &i1, &tag);
    i2 = _altIndex(i1, tag);
    bool found = (_victim.used && tag == _victim.tag && (i1 == _victim.index || i2 == _victim.index));
    if (found || _findTagInBuckets(i1, i2, tag)) return CF_Ok;
    return CF_NotFound;
}
CF_Status CuckooFilter::remove(const uint8_t* data, size_t len) {
    size_t i1, i2;
    uint32_t tag;
    _genIndexTag(data, len, &i1, &tag);
    i2 = _altIndex(i1, tag);
    if (_deleteTagFromBucket(i1, tag) || _deleteTagFromBucket(i2, tag)) {
        _num_items--;
        if (_victim.used) {
            _victim.used = false;
            _addImpl(_victim.index, _victim.tag);
        }
        return CF_Ok;
    }
    if (_victim.used && tag == _victim.tag && (i1 == _victim.index || i2 == _victim.index)) {
        _victim.used = false;
        return CF_Ok;
    }
    return CF_NotFound;
}
void CuckooFilter::clear() {
    memset(_table, 0, sizeof(_table));
    _num_items  = 0;
    _victim.used = false;
}
uint8_t CuckooFilter::loadPercent() const {
    return (uint8_t)((_num_items * 100) / capacity());
}

// ─────────────────────────────────────────────────────────────────────────────
// ─── GLOBAL TEST FILTER ────────────────────────────────────────────────────
// ─────────────────────────────────────────────────────────────────────────────
static CuckooFilter g_filter;
static uint32_t g_test_count = 0;
static uint32_t g_test_pass = 0;
static uint32_t g_test_fail = 0;

void print_status(CF_Status status) {
    switch (status) {
        case CF_Ok:           Serial.print("OK"); break;
        case CF_NotFound:     Serial.print("NOT_FOUND"); break;
        case CF_NotEnoughSpace: Serial.print("FULL"); break;
        default:              Serial.print("UNKNOWN"); break;
    }
}

void test_add(const char* label, uint16_t item, bool expect_ok) {
    g_test_count++;
    uint8_t item_bytes[2] = {(uint8_t)(item & 0xFF), (uint8_t)((item >> 8) & 0xFF)};
    CF_Status s = g_filter.add(item_bytes, 2);
    bool ok = (s == CF_Ok) == expect_ok;
    if (ok) {
        g_test_pass++;
        Serial.print("✓ PASS: ");
    } else {
        g_test_fail++;
        Serial.print("✗ FAIL: ");
    }
    Serial.printf("%s add(%04X) = ", label, item);
    print_status(s);
    Serial.printf(" [%lu/%lu]\n", (unsigned long)g_filter.size(), (unsigned long)g_filter.capacity());
}

void test_contains(const char* label, uint16_t item, bool expect_found) {
    g_test_count++;
    uint8_t item_bytes[2] = {(uint8_t)(item & 0xFF), (uint8_t)((item >> 8) & 0xFF)};
    CF_Status s = g_filter.contain(item_bytes, 2);
    bool ok = (s == CF_Ok) == expect_found;
    if (ok) {
        g_test_pass++;
        Serial.print("✓ PASS: ");
    } else {
        g_test_fail++;
        Serial.print("✗ FAIL: ");
    }
    Serial.printf("%s contains(%04X) = ", label, item);
    print_status(s);
    Serial.printf(" load=%u%%\n", g_filter.loadPercent());
}

void test_remove(const char* label, uint16_t item, bool expect_ok) {
    g_test_count++;
    uint8_t item_bytes[2] = {(uint8_t)(item & 0xFF), (uint8_t)((item >> 8) & 0xFF)};
    CF_Status s = g_filter.remove(item_bytes, 2);
    bool ok = (s == CF_Ok) == expect_ok;
    if (ok) {
        g_test_pass++;
        Serial.print("✓ PASS: ");
    } else {
        g_test_fail++;
        Serial.print("✗ FAIL: ");
    }
    Serial.printf("%s remove(%04X) = ", label, item);
    print_status(s);
    Serial.printf(" [%lu/%lu]\n", (unsigned long)g_filter.size(), (unsigned long)g_filter.capacity());
}

void setup() {
    Serial.begin(115200);
    delay(500);
    Serial.println("\n\n═══════════════════════════════════════════════════════════");
    Serial.println("    CUCKOO FILTER SERIAL TEST (No ESP-NOW)");
    Serial.println("═══════════════════════════════════════════════════════════\n");

    Serial.printf("Filter Capacity: %lu items\n", (unsigned long)g_filter.capacity());
    Serial.printf("Buckets: 256, Slots/bucket: 4, Tag bits: 8\n\n");

    // Test 1: Basic insertions
    Serial.println("─── Test 1: Basic Insertions ───");
    test_add("T1.1", 0x1234, true);
    test_add("T1.2", 0x5678, true);
    test_add("T1.3", 0xABCD, true);
    Serial.println();

    // Test 2: Lookups (should find inserted items)
    Serial.println("─── Test 2: Lookup Existing Items ───");
    test_contains("T2.1", 0x1234, true);
    test_contains("T2.2", 0x5678, true);
    test_contains("T2.3", 0xABCD, true);
    Serial.println();

    // Test 3: Lookups (should NOT find non-inserted items)
    Serial.println("─── Test 3: Lookup Non-Existing Items ───");
    test_contains("T3.1", 0x9999, false);
    test_contains("T3.2", 0xFFFF, false);
    Serial.println();

    // Test 4: Deletions
    Serial.println("─── Test 4: Delete Items ───");
    test_remove("T4.1", 0x1234, true);
    test_remove("T4.2", 0x5678, true);
    Serial.println();

    // Test 5: Verify deleted items are gone
    Serial.println("─── Test 5: Verify Deleted Items ───");
    test_contains("T5.1", 0x1234, false);
    test_contains("T5.2", 0x5678, false);
    test_contains("T5.3", 0xABCD, true);  // Should still exist
    Serial.println();

    // Test 6: Bulk insertions
    Serial.println("─── Test 6: Bulk Insertions (50 items) ───");
    for (uint16_t i = 0; i < 50; i++) {
        uint8_t item_bytes[2] = {(uint8_t)(i & 0xFF), (uint8_t)((i >> 8) & 0xFF)};
        CF_Status s = g_filter.add(item_bytes, 2);
        if (s != CF_Ok && i < 49) {
            Serial.printf("  Insert %u returned %d\n", (unsigned)i, (int)s);
        }
    }
    Serial.printf("  Added 50 items. Current load: %u/%lu (%u%%)\n",
                  (unsigned)g_filter.size(), (unsigned long)g_filter.capacity(), g_filter.loadPercent());
    Serial.println();

    // Test 7: Spot check bulk items
    Serial.println("─── Test 7: Spot Check Bulk Items ───");
    test_contains("T7.1", 0, true);
    test_contains("T7.2", 25, true);
    test_contains("T7.3", 49, true);
    test_contains("T7.4", 100, false);
    Serial.println();

    // Test 8: Clear filter
    Serial.println("─── Test 8: Clear Filter ───");
    g_filter.clear();
    Serial.printf("After clear: size=%lu, load=%u%%\n",
                  (unsigned long)g_filter.size(), g_filter.loadPercent());
    test_contains("T8.1", 0, false);
    test_contains("T8.2", 25, false);
    Serial.println();

    // Summary
    Serial.println("═══════════════════════════════════════════════════════════");
    Serial.printf("TEST SUMMARY:\n");
    Serial.printf("  Total Tests:  %lu\n", (unsigned long)g_test_count);
    Serial.printf("  Passed:       %lu ✓\n", (unsigned long)g_test_pass);
    Serial.printf("  Failed:       %lu ✗\n", (unsigned long)g_test_fail);
    Serial.printf("  Pass Rate:    %u%%\n", g_test_count > 0 ? (unsigned)((g_test_pass * 100) / g_test_count) : 0);
    Serial.println("═══════════════════════════════════════════════════════════\n");

    if (g_test_fail == 0) {
        Serial.println("✓ ALL TESTS PASSED!\n");
    } else {
        Serial.printf("✗ FAILURES DETECTED: %lu tests failed\n\n", (unsigned long)g_test_fail);
    }
}

void loop() {
    delay(1000);
}
