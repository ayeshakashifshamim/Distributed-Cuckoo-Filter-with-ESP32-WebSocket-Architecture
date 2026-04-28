// test_protocol — binary layout compliance for the M2 wire protocol.
// Asserts every packed struct has stable, cross-platform size and field
// offsets so an ESP32 slave and a desktop test harness interpret the same
// bytes identically.
#include <unity.h>
#include <string.h>
#include "protocol/messages.h"

void setUp() {}
void tearDown() {}

// ── Struct sizes ────────────────────────────────────────────────────────────

void test_header_size()        { TEST_ASSERT_EQUAL(6,  sizeof(MsgHeader)); }
void test_insert_size()        { TEST_ASSERT_EQUAL(15, sizeof(MsgInsert)); }
void test_ack_size()           { TEST_ASSERT_EQUAL(8,  sizeof(MsgAck)); }
void test_chain_insert_size()  { TEST_ASSERT_EQUAL(16, sizeof(MsgChainInsert)); }
void test_chain_result_size()  { TEST_ASSERT_EQUAL(9,  sizeof(MsgChainResult)); }
void test_tag_query_size()     { TEST_ASSERT_EQUAL(9,  sizeof(MsgTagQuery)); }
void test_hard_reset_size()    { TEST_ASSERT_EQUAL(6,  sizeof(MsgHardReset)); }
void test_bucket_entry_size()  { TEST_ASSERT_EQUAL(6,  sizeof(BucketEntry)); }

void test_bucket_read_size() {
    // 6 header + 1 count + 20*2 buckets = 47
    TEST_ASSERT_EQUAL(6 + 1 + MAX_BUCKET_BATCH * sizeof(uint16_t),
                      sizeof(MsgBucketBatchRead));
}

void test_bucket_data_size() {
    // 6 header + 1 count + 20*6 entries = 127
    TEST_ASSERT_EQUAL(6 + 1 + MAX_BUCKET_BATCH * sizeof(BucketEntry),
                      sizeof(MsgBucketBatchData));
}

void test_batch_under_espnow_limit() {
    TEST_ASSERT_LESS_OR_EQUAL(250, sizeof(MsgBucketBatchData));
    TEST_ASSERT_LESS_OR_EQUAL(250, sizeof(MsgBucketBatchWrite));
}

// ── Field offsets (no padding) ──────────────────────────────────────────────

void test_header_offsets() {
    MsgHeader h; auto* base = (uint8_t*)&h;
    TEST_ASSERT_EQUAL(0, (uint8_t*)&h.type   - base);
    TEST_ASSERT_EQUAL(1, (uint8_t*)&h.seq    - base);
    TEST_ASSERT_EQUAL(2, (uint8_t*)&h.src_id - base);
    TEST_ASSERT_EQUAL(4, (uint8_t*)&h.dst_id - base);
}

void test_chain_insert_offsets() {
    MsgChainInsert m; auto* base = (uint8_t*)&m;
    TEST_ASSERT_EQUAL(6,  (uint8_t*)&m.bucket   - base);
    TEST_ASSERT_EQUAL(8,  (uint8_t*)&m.tag      - base);
    TEST_ASSERT_EQUAL(9,  (uint8_t*)&m.op_id    - base);
    TEST_ASSERT_EQUAL(13, (uint8_t*)&m.hop      - base);
    TEST_ASSERT_EQUAL(14, (uint8_t*)&m.max_hops - base);
    TEST_ASSERT_EQUAL(15, (uint8_t*)&m.try_only - base);
}

void test_chain_result_offsets() {
    MsgChainResult m; auto* base = (uint8_t*)&m;
    TEST_ASSERT_EQUAL(6, (uint8_t*)&m.ack_seq     - base);
    TEST_ASSERT_EQUAL(7, (uint8_t*)&m.status      - base);
    TEST_ASSERT_EQUAL(8, (uint8_t*)&m.evicted_tag - base);
}

// ── Round-trip ──────────────────────────────────────────────────────────────

void test_insert_round_trip() {
    MsgInsert orig;
    memset(&orig, 0, sizeof(orig));
    orig.hdr.type   = MSG_INSERT;
    orig.hdr.seq    = 42;
    orig.hdr.dst_id = 0x0001;
    memcpy(orig.item, "testdata", 8);
    orig.item_len = 8;

    uint8_t wire[sizeof(MsgInsert)];
    memcpy(wire, &orig, sizeof(wire));
    const auto* parsed = (const MsgInsert*)wire;

    TEST_ASSERT_EQUAL(MSG_INSERT, parsed->hdr.type);
    TEST_ASSERT_EQUAL(42,         parsed->hdr.seq);
    TEST_ASSERT_EQUAL(0x0001,     parsed->hdr.dst_id);
    TEST_ASSERT_EQUAL(8,          parsed->item_len);
    TEST_ASSERT_EQUAL_MEMORY("testdata", parsed->item, 8);
}

int main(int, char**) {
    UNITY_BEGIN();
    RUN_TEST(test_header_size);
    RUN_TEST(test_insert_size);
    RUN_TEST(test_ack_size);
    RUN_TEST(test_chain_insert_size);
    RUN_TEST(test_chain_result_size);
    RUN_TEST(test_tag_query_size);
    RUN_TEST(test_hard_reset_size);
    RUN_TEST(test_bucket_entry_size);
    RUN_TEST(test_bucket_read_size);
    RUN_TEST(test_bucket_data_size);
    RUN_TEST(test_batch_under_espnow_limit);
    RUN_TEST(test_header_offsets);
    RUN_TEST(test_chain_insert_offsets);
    RUN_TEST(test_chain_result_offsets);
    RUN_TEST(test_insert_round_trip);
    return UNITY_END();
}
