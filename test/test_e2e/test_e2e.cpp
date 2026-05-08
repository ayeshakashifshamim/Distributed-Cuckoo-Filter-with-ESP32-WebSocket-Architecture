// test_e2e.cpp



// sequential end-to-end coverage for the distributed cuckoo filter.
// simulates a master and NUM_SLAVES slaves in a single process
// SlaveStorage + Dispatcher pair - the "transport" is a function-pointer hack
// that steers slave replies back into the test harness's reply collectors
// tests are ordered. DONT CHANGE THE ORDER AS THEY AARE DEPENDANT. ok

#include <unity.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "config.h"
#include "protocol/messages.h"
#include "slave_storage.h"
#include "dispatcher/dispatcher.h"


// simulated cluster
static SlaveStorage  g_storage[NUM_SLAVES];
static Dispatcher*   g_disp[NUM_SLAVES] = {nullptr};
static uint8_t       g_route_primary[GLOBAL_BUCKET_COUNT];
static bool          g_route_locked[GLOBAL_BUCKET_COUNT];
static bool          g_slave_alive[NUM_SLAVES] = {};  // test_00 - sets all to true

static uint8_t g_reply_status      = 0;
static uint8_t g_reply_seq         = 0;
static uint8_t g_reply_evicted_tag = 0;
static bool    g_reply_ready       = false;

static MsgBucketBatchData g_batch_data;
static bool               g_batch_data_ready = false;

static uint8_t g_active_slave = 0;

// slave-side transport: routed by g_active_slave (set by deliver()).
static void transport_from_slave(const uint8_t* data, size_t len) {
    (void)len;
    uint8_t type = data[0];
    if (type == MSG_ACK) {
        const MsgAck* a = (const MsgAck*)data;
        g_reply_status = a->status;
        g_reply_seq    = a->ack_seq;
        g_reply_evicted_tag = 0;
        g_reply_ready  = true;
    } else if (type == MSG_CHAIN_RESULT) {
        const MsgChainResult* r = (const MsgChainResult*)data;
        g_reply_status = r->status;
        g_reply_seq    = r->ack_seq;
        g_reply_evicted_tag = r->evicted_tag;
        g_reply_ready  = true;
    } else if (type == MSG_BUCKET_BATCH_DATA) {
        memcpy(&g_batch_data, data, sizeof(g_batch_data));
        g_batch_data_ready = true;
    }
}

static void deliver(uint8_t idx, const void* buf, size_t len) {
    g_active_slave = idx;
    g_reply_ready = false;
    g_batch_data_ready = false;
    g_disp[idx]->onMessage((const uint8_t*)buf, len);
}

// Master-side helpers

static uint8_t  g_seq = 0;
static uint32_t g_op_id = 1;

static uint32_t murmur(const uint8_t* data, size_t len) {
    const uint32_t seed = 0xbc9f1d34, m = 0x5bd1e995;
    uint32_t h = seed ^ (uint32_t)len;
    while (len >= 4) {
        uint32_t k; memcpy(&k, data, 4);
        k *= m; k ^= k >> 24; k *= m;
        h *= m; h ^= k;
        data += 4; len -= 4;
    }
    switch (len) {
        case 3: h ^= (uint32_t)data[2] << 16; /* fallthrough */
        case 2: h ^= (uint32_t)data[1] << 8;  /* fallthrough */
        case 1: h ^= (uint32_t)data[0]; h *= m;
    }
    h ^= h >> 13; h *= m; h ^= h >> 15;
    return h;
}
static uint8_t  tag_of(uint32_t h)     { uint8_t t = (uint8_t)(h & 0xFF); return t ? t : 1; }
static uint16_t bucket1_of(uint32_t h) { return (uint16_t)((h >> 8) & (GLOBAL_BUCKET_COUNT - 1)); }
static uint16_t alt_bucket(uint16_t b, uint8_t tag) {
    return (uint16_t)((b ^ (uint32_t)(tag * 0x5bd1e995u)) & (GLOBAL_BUCKET_COUNT - 1));
}

static bool send_chain_insert(uint8_t slave, uint16_t b, uint8_t tag,
                              bool try_only,
                              uint8_t* out_status, uint8_t* out_evicted) {
    if (!g_slave_alive[slave]) {
        *out_status = STATUS_UNAVAILABLE; *out_evicted = 0; return false;
    }
    MsgChainInsert m; memset(&m, 0, sizeof(m));
    m.hdr.type   = MSG_CHAIN_INSERT;
    m.hdr.seq    = ++g_seq;
    m.hdr.src_id = MASTER_ID;
    m.hdr.dst_id = (uint16_t)(slave + 1);
    m.bucket = b; m.tag = tag;
    m.op_id  = g_op_id; m.hop = 0;
    m.max_hops = MAX_CHAIN_HOPS;
    m.try_only = try_only ? 1 : 0;
    deliver(slave, &m, sizeof(m));
    if (!g_reply_ready) return false;
    *out_status  = g_reply_status;
    *out_evicted = g_reply_evicted_tag;
    return true;
}

static bool send_tag_query(uint8_t slave, uint8_t cmd, uint16_t b, uint8_t tag,
                           uint8_t* out_status) {
    if (!g_slave_alive[slave]) { *out_status = STATUS_UNAVAILABLE; return false; }
    MsgTagQuery m; memset(&m, 0, sizeof(m));
    m.hdr.type   = cmd;
    m.hdr.seq    = ++g_seq;
    m.hdr.src_id = MASTER_ID;
    m.hdr.dst_id = (uint16_t)(slave + 1);
    m.bucket = b; m.tag = tag;
    deliver(slave, &m, sizeof(m));
    if (!g_reply_ready) return false;
    *out_status = g_reply_status;
    return true;
}

static uint8_t insert_key(const uint8_t* key, uint8_t key_len) {
    uint32_t h  = murmur(key, key_len);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);
    g_op_id++;

    for (int i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (g_route_locked[b]) continue;
        uint8_t st = STATUS_ERROR, ev = 0;
        send_chain_insert(g_route_primary[b], b, tg, /*try_only=*/true, &st, &ev);
        if (st == STATUS_OK) return STATUS_OK;
    }

    uint16_t cur = ((rand() & 1) ? b1 : b2);
    uint8_t  ctg = tg;
    for (uint8_t hop = 0; hop < MAX_CHAIN_HOPS; hop++) {
        if (g_route_locked[cur]) return STATUS_RETRY;
        uint8_t st = STATUS_ERROR, ev = 0;
        send_chain_insert(g_route_primary[cur], cur, ctg, /*try_only=*/false, &st, &ev);
        if (st == STATUS_OK)     return STATUS_OK;
        if (st != STATUS_KICKED) return st;
        ctg = ev;
        cur = alt_bucket(cur, ctg);
    }
    return STATUS_FULL;
}

static uint8_t lookup_key(const uint8_t* key, uint8_t key_len) {
    uint32_t h  = murmur(key, key_len);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);
    bool any_unavailable = false;
    for (int i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        uint8_t st = STATUS_ERROR;
        if (!send_tag_query(g_route_primary[b], MSG_TAG_LOOKUP, b, tg, &st)) {
            any_unavailable = true; continue;
        }
        if (st == STATUS_OK) return STATUS_OK;
    }
    return any_unavailable ? STATUS_UNAVAILABLE : STATUS_NOT_FOUND;
}

static uint8_t delete_key(const uint8_t* key, uint8_t key_len) {
    uint32_t h  = murmur(key, key_len);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);
    bool any_unavailable = false;
    for (int i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        uint8_t st = STATUS_ERROR;
        if (!send_tag_query(g_route_primary[b], MSG_TAG_DELETE, b, tg, &st)) {
            any_unavailable = true; continue;
        }
        if (st == STATUS_OK) return STATUS_OK;
    }
    return any_unavailable ? STATUS_UNAVAILABLE : STATUS_NOT_FOUND;
}

// test fixture

void setUp() {}
void tearDown() {}


// a. Boot — routing table is b % NUM_SLAVES, every slave clean.
void test_00_routing_and_storage_init() {
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        g_storage[i].init();
        g_slave_alive[i] = true;
        static Dispatcher* slots[NUM_SLAVES];
        if (!g_disp[i]) {
            slots[i] = new Dispatcher(g_storage[i], (uint16_t)(i + 1), MASTER_ID,
                                      transport_from_slave);
            g_disp[i] = slots[i];
        } else {
            g_disp[i]->clearDedup();
        }
    }
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT; b++) {
        g_route_primary[b] = (uint8_t)(b % NUM_SLAVES);
        g_route_locked[b]  = false;
    }
    TEST_ASSERT_EQUAL(0, g_route_primary[0]);
    TEST_ASSERT_EQUAL(NUM_SLAVES - 1, g_route_primary[NUM_SLAVES - 1]);
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        TEST_ASSERT_EQUAL(LOCAL_CAPACITY, g_storage[i].free_slots());
        TEST_ASSERT_EQUAL(0, g_storage[i].item_count());
    }
}

// b. Single insert lands, looks up, deletes.
void test_01_insert_lookup_delete() {
    const char* k = "alpha";
    TEST_ASSERT_EQUAL(STATUS_OK, insert_key((const uint8_t*)k, 5));
    TEST_ASSERT_EQUAL(STATUS_OK, lookup_key((const uint8_t*)k, 5));
    TEST_ASSERT_EQUAL(STATUS_OK, delete_key((const uint8_t*)k, 5));
    TEST_ASSERT_EQUAL(STATUS_NOT_FOUND, lookup_key((const uint8_t*)k, 5));
}

// c. Many distinct keys insert + lookup.
void test_02_many_inserts_and_lookups() {
    const uint32_t N = 200;
    for (uint32_t i = 0; i < N; i++) {
        TEST_ASSERT_EQUAL(STATUS_OK, insert_key((const uint8_t*)&i, sizeof(i)));
    }
    for (uint32_t i = 0; i < N; i++) {
        TEST_ASSERT_EQUAL(STATUS_OK, lookup_key((const uint8_t*)&i, sizeof(i)));
    }
    for (uint32_t i = 0; i < N; i++) delete_key((const uint8_t*)&i, sizeof(i));
}

// d. try_only=true returns STATUS_FULL on full bucket without evicting.
void test_03_try_only_does_not_evict() {
    srand(1);
    const uint16_t target = 7;
    const uint8_t owner   = g_route_primary[target];

    uint8_t tags[4] = {11, 22, 33, 44};
    for (uint8_t i = 0; i < 4; i++) {
        uint8_t st = STATUS_ERROR, ev = 0;
        send_chain_insert(owner, target, tags[i], /*try_only=*/true, &st, &ev);
        TEST_ASSERT_EQUAL(STATUS_OK, st);
    }
    uint8_t st = STATUS_ERROR, ev = 0;
    send_chain_insert(owner, target, 99, /*try_only=*/true, &st, &ev);
    TEST_ASSERT_EQUAL(STATUS_FULL, st);
    for (uint8_t i = 0; i < 4; i++) {
        uint8_t lst = STATUS_ERROR;
        send_tag_query(owner, MSG_TAG_LOOKUP, target, tags[i], &lst);
        TEST_ASSERT_EQUAL(STATUS_OK, lst);
    }
    // Same bucket with try_only=false → STATUS_KICKED, one original tag gone.
    send_chain_insert(owner, target, 99, /*try_only=*/false, &st, &ev);
    TEST_ASSERT_EQUAL(STATUS_KICKED, st);
    TEST_ASSERT_TRUE(ev == 11 || ev == 22 || ev == 33 || ev == 44);

    MsgBucketBatchClear cl;
    memset(&cl, 0, sizeof(cl));
    cl.hdr.type = MSG_BUCKET_BATCH_CLEAR;
    cl.hdr.seq  = ++g_seq;
    cl.hdr.dst_id = owner + 1;
    cl.count = 1;
    cl.buckets[0] = target;
    deliver(owner, &cl, sizeof(cl));
    TEST_ASSERT_TRUE(g_reply_ready);
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);
}

// e. Dedup: replaying a CHAIN_INSERT with the same seq gives identical reply.
void test_04_dedup_replays_cached_result() {
    const uint16_t b = 3;
    const uint8_t owner = g_route_primary[b];
    g_disp[owner]->clearDedup();

    MsgChainInsert m;
    memset(&m, 0, sizeof(m));
    m.hdr.type   = MSG_CHAIN_INSERT;
    m.hdr.seq    = 77;
    m.hdr.src_id = MASTER_ID;
    m.hdr.dst_id = owner + 1;
    m.bucket     = b;
    m.tag        = 55;
    m.op_id      = 12345;
    m.max_hops   = MAX_CHAIN_HOPS;
    m.try_only   = 0;
    deliver(owner, &m, sizeof(m));
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);

    deliver(owner, &m, sizeof(m));
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);

    uint8_t* tags = g_storage[owner].bucket_at(g_storage[owner].index_of(b)).tags;
    uint8_t count = 0;
    for (uint8_t i = 0; i < 4; i++) if (tags[i] == 55) count++;
    TEST_ASSERT_EQUAL(1, count);
}

// f. Bucket batch round trip: read → write on another slave → clear on source.
void test_05_bucket_batch_migration() {
    const uint16_t buckets[3] = {0, NUM_SLAVES, NUM_SLAVES * 2};  // owned by slave 0
    for (uint8_t i = 0; i < 3; i++) TEST_ASSERT_EQUAL(0, g_route_primary[buckets[i]]);

    for (uint8_t i = 0; i < 3; i++) {
        uint8_t st = STATUS_ERROR, ev = 0;
        send_chain_insert(0, buckets[i], (uint8_t)(100 + i), true, &st, &ev);
        TEST_ASSERT_EQUAL(STATUS_OK, st);
    }

    MsgBucketBatchRead rd;
    memset(&rd, 0, sizeof(rd));
    rd.hdr.type = MSG_BUCKET_BATCH_READ;
    rd.hdr.seq  = ++g_seq;
    rd.hdr.dst_id = 1;
    rd.count = 3;
    for (uint8_t i = 0; i < 3; i++) rd.buckets[i] = buckets[i];
    deliver(0, &rd, sizeof(rd));
    TEST_ASSERT_TRUE(g_batch_data_ready);
    TEST_ASSERT_EQUAL(3, g_batch_data.count);

    MsgBucketBatchWrite wr;
    memset(&wr, 0, sizeof(wr));
    wr.hdr.type = MSG_BUCKET_BATCH_WRITE;
    wr.hdr.seq  = ++g_seq;
    wr.hdr.dst_id = 2;
    wr.count = g_batch_data.count;
    memcpy(wr.entries, g_batch_data.entries, wr.count * sizeof(BucketEntry));
    deliver(1, &wr, sizeof(wr));
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);

    MsgBucketBatchClear cl;
    memset(&cl, 0, sizeof(cl));
    cl.hdr.type = MSG_BUCKET_BATCH_CLEAR;
    cl.hdr.seq  = ++g_seq;
    cl.hdr.dst_id = 1;
    cl.count = 3;
    for (uint8_t i = 0; i < 3; i++) cl.buckets[i] = buckets[i];
    deliver(0, &cl, sizeof(cl));
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);

    for (uint8_t i = 0; i < 3; i++) {
        TEST_ASSERT_EQUAL(-1, g_storage[0].index_of(buckets[i]));
        TEST_ASSERT_NOT_EQUAL(-1, g_storage[1].index_of(buckets[i]));
    }

    for (uint8_t i = 0; i < 3; i++) g_route_primary[buckets[i]] = 1;
    for (uint8_t i = 0; i < 3; i++) {
        uint8_t st = STATUS_ERROR;
        send_tag_query(1, MSG_TAG_LOOKUP, buckets[i], (uint8_t)(100 + i), &st);
        TEST_ASSERT_EQUAL(STATUS_OK, st);
    }

    MsgBucketBatchClear cl2 = cl;
    cl2.hdr.seq = ++g_seq;
    cl2.hdr.dst_id = 2;
    deliver(1, &cl2, sizeof(cl2));
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);
    for (uint8_t i = 0; i < 3; i++) g_route_primary[buckets[i]] = 0;
}

// g. Route-lock returns STATUS_RETRY from the master-level insert.
void test_06_locked_bucket_returns_retry() {
    uint32_t k = 0xBEEFCAFEu;
    uint32_t h  = murmur((const uint8_t*)&k, 4);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);

    g_route_locked[b1] = true;
    g_route_locked[b2] = true;
    uint8_t st = insert_key((const uint8_t*)&k, 4);
    g_route_locked[b1] = false;
    g_route_locked[b2] = false;

    TEST_ASSERT_EQUAL(STATUS_RETRY, st);
}

// h. Hard reset wipes a slave completely.
void test_07_hard_reset_wipes_slave() {
    const uint16_t b = 5;
    const uint8_t owner = g_route_primary[b];

    uint8_t st = STATUS_ERROR, ev = 0;
    send_chain_insert(owner, b, 77, true, &st, &ev);
    TEST_ASSERT_EQUAL(STATUS_OK, st);
    TEST_ASSERT_NOT_EQUAL(-1, g_storage[owner].index_of(b));

    MsgHardReset rst;
    memset(&rst, 0, sizeof(rst));
    rst.hdr.type = MSG_HARD_RESET;
    rst.hdr.seq  = ++g_seq;
    rst.hdr.dst_id = owner + 1;
    deliver(owner, &rst, sizeof(rst));
    TEST_ASSERT_EQUAL(STATUS_OK, g_reply_status);

    TEST_ASSERT_EQUAL(-1, g_storage[owner].index_of(b));
    TEST_ASSERT_EQUAL(0, g_storage[owner].item_count());
    TEST_ASSERT_EQUAL(LOCAL_CAPACITY, g_storage[owner].free_slots());

    uint8_t lst = STATUS_ERROR;
    send_tag_query(owner, MSG_TAG_LOOKUP, b, 77, &lst);
    TEST_ASSERT_EQUAL(STATUS_NOT_FOUND, lst);
}

// i. Heavy load — measure FPR end-to-end.
void test_08_load_and_fpr() {
    for (uint8_t i = 0; i < NUM_SLAVES; i++) g_storage[i].init();
    for (uint8_t i = 0; i < NUM_SLAVES; i++) g_disp[i]->clearDedup();

    const uint32_t N = 400;
    bool ok_flags[N] = {false};
    uint32_t inserted = 0;
    for (uint32_t i = 0; i < N; i++) {
        if (insert_key((const uint8_t*)&i, sizeof(i)) == STATUS_OK) {
            ok_flags[i] = true;
            inserted++;
        }
    }
    TEST_ASSERT_GREATER_THAN(N * 9 / 10, inserted);

    for (uint32_t i = 0; i < N; i++) {
        if (!ok_flags[i]) continue;
        TEST_ASSERT_EQUAL(STATUS_OK, lookup_key((const uint8_t*)&i, sizeof(i)));
    }

    const uint32_t Q = 2000;
    uint32_t fp = 0;
    for (uint32_t i = 0; i < Q; i++) {
        uint32_t k = N + 100000 + i;
        if (lookup_key((const uint8_t*)&k, sizeof(k)) == STATUS_OK) fp++;
    }
    double fpr = 100.0 * (double)fp / (double)Q;
    printf("[E2E] FPR: %.2f%% (%u/%u)\n", fpr, (unsigned)fp, (unsigned)Q);
    TEST_ASSERT_LESS_THAN(8.0, fpr);
}

// j. UNAVAILABLE propagation: kill both owners → lookup says UNAVAILABLE.
void test_09_unavailable_propagates() {
    for (uint8_t i = 0; i < NUM_SLAVES; i++) g_storage[i].init();
    for (uint8_t i = 0; i < NUM_SLAVES; i++) g_disp[i]->clearDedup();
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT; b++) {
        g_route_primary[b] = (uint8_t)(b % NUM_SLAVES);
        g_route_locked[b]  = false;
    }
    for (uint8_t i = 0; i < NUM_SLAVES; i++) g_slave_alive[i] = true;

    const char* k = "ghost";
    TEST_ASSERT_EQUAL(STATUS_OK, insert_key((const uint8_t*)k, 5));

    uint32_t h = murmur((const uint8_t*)k, 5);
    uint8_t tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);
    g_slave_alive[g_route_primary[b1]] = false;
    g_slave_alive[g_route_primary[b2]] = false;

    TEST_ASSERT_EQUAL(STATUS_UNAVAILABLE, lookup_key((const uint8_t*)k, 5));
    TEST_ASSERT_EQUAL(STATUS_UNAVAILABLE, delete_key((const uint8_t*)k, 5));

    g_slave_alive[g_route_primary[b1]] = true;
    g_slave_alive[g_route_primary[b2]] = true;
    TEST_ASSERT_EQUAL(STATUS_OK, lookup_key((const uint8_t*)k, 5));
}


int main(int, char**) {
    srand(0xC0FFEE);
    UNITY_BEGIN();
    RUN_TEST(test_00_routing_and_storage_init);
    RUN_TEST(test_01_insert_lookup_delete);
    RUN_TEST(test_02_many_inserts_and_lookups);
    RUN_TEST(test_03_try_only_does_not_evict);
    RUN_TEST(test_04_dedup_replays_cached_result); //
    RUN_TEST(test_05_bucket_batch_migration);
    RUN_TEST(test_06_locked_bucket_returns_retry);
    RUN_TEST(test_07_hard_reset_wipes_slave);
    RUN_TEST(test_08_load_and_fpr);
    RUN_TEST(test_09_unavailable_propagates);
    return UNITY_END();
}
