// master.cpp — Distributed cuckoo filter coordinator.
//
// Responsibilities:
//   - Maintains bucket→node ownership mapping (bucket_owner[])
//   - Drives distributed insert (kickout chain) / lookup / delete by forwarding
//     to the relevant slave via ESP-NOW and waiting for a synchronous reply.
//   - Monitors slave liveness via heartbeats and triggers reactive bucket
//     rebalancing when load imbalance or overload is detected.
//
// Firmware build: master only (SLAVE_BUILD must not be defined).

#if !defined(NATIVE_BUILD) && defined(MASTER_BUILD)

#include <Arduino.h>
#include <WiFi.h>
#include <string.h>
#include <stdlib.h>
#include "config.h"
#include "protocol/messages.h"
#include "transport/transport.h"

// ════════════════════════════════════════════════════════════════════════════
// Node registry
// ════════════════════════════════════════════════════════════════════════════

struct NodeRecord {
    uint8_t        id;
    const uint8_t* mac;
    bool           alive;
    uint32_t       last_heartbeat_ms;
    uint32_t       item_count;
    uint16_t       capacity;
    uint8_t        load_pct;
};

static NodeRecord node_registry[NUM_SLAVES];

// bucket_owner[b] → owning slave index (0-based).  bucket_busy[b] is set
// while a bucket is mid-migration so that concurrent ops return STATUS_RETRY.
static uint8_t bucket_owner[GLOBAL_BUCKET_COUNT];
static bool    bucket_busy [GLOBAL_BUCKET_COUNT];

// ════════════════════════════════════════════════════════════════════════════
// Sequence-numbered reply synchronisation
// ════════════════════════════════════════════════════════════════════════════

static uint8_t           g_seq = 0;
static volatile bool     g_reply_ready = false;
static volatile uint8_t  g_reply_status = STATUS_ERROR;
static volatile uint8_t  g_reply_seq    = 0;
static volatile uint8_t  g_reply_evicted_tag = 0;

static volatile bool          g_batch_data_ready = false;
static MsgBucketBatchData     g_batch_data_buf;

static uint32_t g_operation_id = 1;

// Auto-rebalance: heartbeat handlers set a flag; loop() processes it on a
// cooldown timer so that migration storms are avoided.
static volatile bool g_rebalance_pending = false;
static uint32_t      g_last_rebalance_ms = 0;

// ════════════════════════════════════════════════════════════════════════════
// MurmurHash2 (32-bit) — deterministic, no OS dependencies, ESP32-safe
// ════════════════════════════════════════════════════════════════════════════

static uint32_t murmur_hash(const uint8_t* data, size_t len) {
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
        case 2: h ^= (uint32_t)data[1] <<  8; /* fallthrough */
        case 1: h ^= (uint32_t)data[0]; h *= m;
    }
    h ^= h >> 13; h *= m; h ^= h >> 15;
    return h;
}

// Fingerprint extracted from the hash's low byte.  Zero is remapped to 1 so
// that slot 0 in each bucket remains the "empty" sentinel.
static uint8_t  fingerprint_of(uint32_t h)  { uint8_t t = (uint8_t)(h & 0xFF); return t ? t : 1; }

// Primary bucket index: upper 24 bits of the hash, masked to GLOBAL_BUCKET_COUNT.
static uint16_t primary_bucket_of(uint32_t h) { return (uint16_t)((h >> 8) & (GLOBAL_BUCKET_COUNT - 1)); }

// Alternate bucket under the standard cuckoo XOR mixing formula.
static uint16_t alternate_bucket(uint16_t b, uint8_t tag) {
    return (uint16_t)((b ^ (uint32_t)(tag * 0x5bd1e995u)) & (GLOBAL_BUCKET_COUNT - 1));
}

// ════════════════════════════════════════════════════════════════════════════
// Bucket-to-node assignment
//
// PROBLEM: the old sequential modulo mapping
//   bucket_owner[b] = b % NUM_SLAVES
// produced contiguous ranges per slave.  Any non-uniformity in hash-driven
// item distribution (or in the cuckoo kickout chain) therefore concentrated
// load onto particular slaves.
//
// SOLUTION: hash-based scatter.
//   bucket_owner[b] = murmur_hash(&b, sizeof(b)) % NUM_SLAVES
// This distributes consecutive bucket indices across all slaves so that hash
// skew is averaged out rather than being mapped onto a single slave's range.
// ════════════════════════════════════════════════════════════════════════════

static void assign_buckets_to_nodes() {
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT; b++) {
        uint32_t h = murmur_hash((const uint8_t*)&b, sizeof(b));
        bucket_owner[b] = (uint8_t)(h % NUM_SLAVES);
        bucket_busy[b]  = false;
    }
}

static NodeRecord* find_node_by_id(uint16_t src_id) {
    for (uint8_t i = 0; i < NUM_SLAVES; i++)
        if (node_registry[i].id == (uint8_t)src_id) return &node_registry[i];
    return nullptr;
}

// ════════════════════════════════════════════════════════════════════════════
// Low-level send / await helpers
// ════════════════════════════════════════════════════════════════════════════

static bool await_ack(uint8_t req_seq, uint32_t timeout_ms) {
    uint32_t start = millis();
    while ((millis() - start) < timeout_ms) {
        if (g_reply_ready && g_reply_seq == req_seq) {
            g_reply_ready = false;
            return true;
        }
        delay(2);
    }
    return false;
}

static bool await_batch_data(uint32_t timeout_ms) {
    uint32_t start = millis();
    while ((millis() - start) < timeout_ms) {
        if (g_batch_data_ready) { g_batch_data_ready = false; return true; }
        delay(2);
    }
    return false;
}

// ── Outbound message builders ───────────────────────────────────────────────

static bool propagate_cuckoo_insert(uint8_t slave_idx, uint16_t bucket, uint8_t tag,
                                   uint32_t op_id, uint8_t hop, bool try_only,
                                   uint8_t* out_status, uint8_t* out_evicted) {
    NodeRecord& s = node_registry[slave_idx];
    if (!s.alive) { *out_status = STATUS_UNAVAILABLE; return false; }

    MsgChainInsert msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.type  = MSG_CHAIN_INSERT;
    msg.hdr.seq   = ++g_seq;
    msg.hdr.src_id = MASTER_ID;
    msg.hdr.dst_id = s.id;
    msg.bucket    = bucket;
    msg.tag       = tag;
    msg.op_id     = op_id;
    msg.hop       = hop;
    msg.max_hops  = MAX_CHAIN_HOPS;
    msg.try_only  = try_only ? 1 : 0;

    g_reply_ready = false;
    transport_send(s.mac, (const uint8_t*)&msg, sizeof(msg));
    if (!await_ack(g_seq, 1000)) { *out_status = STATUS_UNAVAILABLE; return false; }
    *out_status  = g_reply_status;
    *out_evicted = g_reply_evicted_tag;
    return true;
}

static bool query_bucket_tag(uint8_t slave_idx, uint8_t cmd, uint16_t bucket, uint8_t tag,
                             uint8_t* out_status) {
    NodeRecord& s = node_registry[slave_idx];
    if (!s.alive) { *out_status = STATUS_UNAVAILABLE; return false; }

    MsgTagQuery msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.type   = cmd;
    msg.hdr.seq    = ++g_seq;
    msg.hdr.src_id = MASTER_ID;
    msg.hdr.dst_id = s.id;
    msg.bucket     = bucket;
    msg.tag        = tag;

    g_reply_ready = false;
    transport_send(s.mac, (const uint8_t*)&msg, sizeof(msg));
    if (!await_ack(g_seq, 1000)) { *out_status = STATUS_UNAVAILABLE; return false; }
    *out_status = g_reply_status;
    return true;
}

static bool transmit_hard_reset(uint8_t slave_idx) {
    NodeRecord& s = node_registry[slave_idx];
    MsgHardReset msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.type   = MSG_HARD_RESET;
    msg.hdr.seq    = ++g_seq;
    msg.hdr.src_id = MASTER_ID;
    msg.hdr.dst_id = s.id;

    g_reply_ready = false;
    transport_send(s.mac, (const uint8_t*)&msg, sizeof(msg));
    return await_ack(g_seq, 1500) && g_reply_status == STATUS_OK;
}

// ════════════════════════════════════════════════════════════════════════════
// Distributed operations
// ════════════════════════════════════════════════════════════════════════════

// Phase 1 — attempt non-kickout insert into primary then alternate bucket.
// Returns early on first STATUS_OK.
static uint8_t try_soft_insert(uint32_t h, uint8_t tg, uint16_t b1, uint16_t b2, uint32_t op) {
    for (uint8_t i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (bucket_busy[b]) continue;
        uint8_t owner = bucket_owner[b];
        if (!node_registry[owner].alive) continue;
        uint8_t st = STATUS_ERROR, ev = 0;
        if (!propagate_cuckoo_insert(owner, b, tg, op, 0, /*try_only=*/true, &st, &ev)) continue;
        if (st == STATUS_OK) return STATUS_OK;
    }
    return STATUS_FULL;
}

// Phase 2 — hard insert with kickout chain.  A random candidate bucket is
// selected; each STATUS_KICKED reply carries the evicted tag which is then
// inserted into its alternate bucket.  Chain halts at MAX_CHAIN_HOPS.
static uint8_t execute_kickout_chain(uint8_t tg, uint16_t b1, uint16_t b2, uint32_t op) {
    uint16_t cur = ((rand() & 1) ? b1 : b2);
    uint8_t  ctg = tg;
    for (uint8_t hop = 0; hop < MAX_CHAIN_HOPS; hop++) {
        if (bucket_busy[cur]) return STATUS_RETRY;
        uint8_t owner = bucket_owner[cur];
        if (!node_registry[owner].alive) return STATUS_UNAVAILABLE;
        uint8_t st = STATUS_ERROR, ev = 0;
        if (!propagate_cuckoo_insert(owner, cur, ctg, op, hop, /*try_only=*/false, &st, &ev))
            return STATUS_UNAVAILABLE;
        if (st == STATUS_OK)     return STATUS_OK;
        if (st != STATUS_KICKED) return st;
        ctg = ev;
        cur = alternate_bucket(cur, ctg);
    }
    return STATUS_FULL;
}

// Returns STATUS_OK, STATUS_FULL, STATUS_RETRY, STATUS_UNAVAILABLE.
static uint8_t distributed_insert_key(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) return STATUS_ERROR;
    uint32_t h  = murmur_hash(key, key_len);
    uint8_t  tg = fingerprint_of(h);
    uint16_t b1 = primary_bucket_of(h);
    uint16_t b2 = alternate_bucket(b1, tg);
    uint32_t op = g_operation_id++;

    uint8_t st = try_soft_insert(h, tg, b1, b2, op);
    if (st != STATUS_FULL) return st;
    return execute_kickout_chain(tg, b1, b2, op);
}

static uint8_t distributed_lookup_key(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) return STATUS_ERROR;
    uint32_t h  = murmur_hash(key, key_len);
    uint8_t  tg = fingerprint_of(h);
    uint16_t b1 = primary_bucket_of(h);
    uint16_t b2 = alternate_bucket(b1, tg);

    bool any_unavailable = false;
    for (uint8_t i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (bucket_busy[b]) return STATUS_RETRY;
        uint8_t st = STATUS_ERROR;
        if (!query_bucket_tag(bucket_owner[b], MSG_TAG_LOOKUP, b, tg, &st)) {
            any_unavailable = true; continue;
        }
        if (st == STATUS_OK) return STATUS_OK;
    }
    return any_unavailable ? STATUS_UNAVAILABLE : STATUS_NOT_FOUND;
}

static uint8_t distributed_delete_key(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) return STATUS_ERROR;
    uint32_t h  = murmur_hash(key, key_len);
    uint8_t  tg = fingerprint_of(h);
    uint16_t b1 = primary_bucket_of(h);
    uint16_t b2 = alternate_bucket(b1, tg);

    bool any_unavailable = false;
    for (uint8_t i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (bucket_busy[b]) return STATUS_RETRY;
        uint8_t st = STATUS_ERROR;
        if (!query_bucket_tag(bucket_owner[b], MSG_TAG_DELETE, b, tg, &st)) {
            any_unavailable = true; continue;
        }
        if (st == STATUS_OK) return STATUS_OK;
    }
    return any_unavailable ? STATUS_UNAVAILABLE : STATUS_NOT_FOUND;
}

// ════════════════════════════════════════════════════════════════════════════
// Reactive bucket rebalancer
//
// Triggered when any slave exceeds REBALANCE_THRESHOLD load or when the
// max-min load gap across live slaves exceeds IMBALANCE_THRESHOLD.  Runs
// three ESP-NOW round-trips (read → write → clear) to migrate a batch of
// buckets from the most-loaded to the least-loaded slave.
// ════════════════════════════════════════════════════════════════════════════

static void unlock_buckets(const uint16_t* batch, uint8_t n) {
    for (uint8_t i = 0; i < n; i++) bucket_busy[batch[i]] = false;
}

static void cmd_rebalance() {
    if (NUM_SLAVES < 2) {
        Serial.println("[MASTER] REBALANCE: need >=2 slaves"); return;
    }
    int8_t  src = -1, dst = -1;
    uint8_t max_load = 0, min_load = 101;
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        if (!node_registry[i].alive) continue;
        if (node_registry[i].load_pct > max_load) { max_load = node_registry[i].load_pct; src = (int8_t)i; }
        if (node_registry[i].load_pct < min_load) { min_load = node_registry[i].load_pct; dst = (int8_t)i; }
    }
    if (src < 0 || dst < 0 || src == dst) {
        Serial.println("[MASTER] REBALANCE: no candidates"); return;
    }
    bool overload  = max_load >= REBALANCE_THRESHOLD;
    bool imbalance = (max_load - min_load) >= IMBALANCE_THRESHOLD;
    if (!overload && !imbalance) {
        Serial.println("[MASTER] REBALANCE: cluster balanced"); return;
    }
    Serial.printf("[MASTER] REBALANCE %s: src=0x%02X (%u%%) dst=0x%02X (%u%%)\n",
                 overload ? "overload" : "imbalance",
                 node_registry[src].id, max_load, node_registry[dst].id, min_load);

    // Verify destination has at least one free physical slot.
    uint16_t dst_owned = 0;
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT; b++)
        if (bucket_owner[b] == (uint8_t)dst) dst_owned++;
    if (dst_owned >= LOCAL_CAPACITY) {
        Serial.println("[MASTER] REBALANCE: dst has no free slots"); return;
    }
    uint8_t dst_free = (uint8_t)(LOCAL_CAPACITY - dst_owned);

    // Collect up to dst_free non-locked buckets from the source.
    uint16_t batch[MAX_BUCKET_BATCH];
    uint8_t  n = 0;
    uint8_t  cap = (dst_free < MAX_BUCKET_BATCH) ? dst_free : MAX_BUCKET_BATCH;
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT && n < cap; b++) {
        if (bucket_owner[b] == (uint8_t)src && !bucket_busy[b]) batch[n++] = b;
    }
    if (n == 0) { Serial.println("[MASTER] REBALANCE: no eligible buckets"); return; }

    for (uint8_t i = 0; i < n; i++) bucket_busy[batch[i]] = true;

    // Round 1 — read bucket contents from source.
    MsgBucketBatchRead rd;
    memset(&rd, 0, sizeof(rd));
    rd.hdr.type = MSG_BUCKET_BATCH_READ;
    rd.hdr.seq  = ++g_seq;
    rd.hdr.src_id = MASTER_ID;
    rd.hdr.dst_id = node_registry[src].id;
    rd.count = n;
    memcpy(rd.buckets, batch, n * sizeof(uint16_t));

    g_batch_data_ready = false;
    transport_send(node_registry[src].mac, (const uint8_t*)&rd, sizeof(rd));
    if (!await_batch_data(1500)) { unlock_buckets(batch, n); Serial.println("[MASTER] REBALANCE: read timeout"); return; }

    // Round 2 — write contents into destination.
    MsgBucketBatchWrite wr;
    memset(&wr, 0, sizeof(wr));
    wr.hdr.type = MSG_BUCKET_BATCH_WRITE;
    wr.hdr.seq  = ++g_seq;
    wr.hdr.src_id = MASTER_ID;
    wr.hdr.dst_id = node_registry[dst].id;
    wr.count = g_batch_data_buf.count;
    memcpy(wr.entries, g_batch_data_buf.entries, wr.count * sizeof(BucketEntry));

    g_reply_ready = false;
    transport_send(node_registry[dst].mac, (const uint8_t*)&wr, sizeof(wr));
    if (!await_ack(g_seq, 1500) || g_reply_status != STATUS_OK) {
        unlock_buckets(batch, n); Serial.println("[MASTER] REBALANCE: write failed"); return;
    }

    // Round 3 — clear source.
    MsgBucketBatchClear cl;
    memset(&cl, 0, sizeof(cl));
    cl.hdr.type = MSG_BUCKET_BATCH_CLEAR;
    cl.hdr.seq  = ++g_seq;
    cl.hdr.src_id = MASTER_ID;
    cl.hdr.dst_id = node_registry[src].id;
    cl.count = n;
    memcpy(cl.buckets, batch, n * sizeof(uint16_t));

    g_reply_ready = false;
    transport_send(node_registry[src].mac, (const uint8_t*)&cl, sizeof(cl));
    if (!await_ack(g_seq, 1500) || g_reply_status != STATUS_OK) {
        unlock_buckets(batch, n); Serial.println("[MASTER] REBALANCE: clear failed"); return;
    }

    // Publish new ownership and unlock.
    for (uint8_t i = 0; i < n; i++) {
        bucket_owner[batch[i]] = (uint8_t)dst;
        bucket_busy[batch[i]]  = false;
    }
    Serial.printf("[MASTER] REBALANCE done: %u buckets 0x%02X -> 0x%02X\n",
                  n, node_registry[src].id, node_registry[dst].id);
}

// ════════════════════════════════════════════════════════════════════════════
// Serial command handlers
// ════════════════════════════════════════════════════════════════════════════

static void cmd_insert(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) { Serial.println("[USAGE] i <key>"); return; }
    if (key_len > 8) key_len = 8;

    char keyStr[9] = {0};
    memcpy(keyStr, key, key_len);

    uint8_t st = distributed_insert_key(key, key_len);
    const char* m = (st==STATUS_OK)?"SUCCESS":(st==STATUS_FULL)?"FULL":
                    (st==STATUS_RETRY)?"RETRY":(st==STATUS_UNAVAILABLE)?"UNAVAILABLE":"ERROR";
    Serial.printf("[INSERT] %s -> %s\n", keyStr, m);
}

static void cmd_delete(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) { Serial.println("[USAGE] d <key>"); return; }
    if (key_len > 8) key_len = 8;

    char keyStr[9] = {0};
    memcpy(keyStr, key, key_len);

    uint8_t st = distributed_delete_key(key, key_len);
    const char* m = (st==STATUS_OK)?"OK":(st==STATUS_NOT_FOUND)?"NOT FOUND":
                    (st==STATUS_RETRY)?"RETRY":(st==STATUS_UNAVAILABLE)?"UNAVAILABLE":"ERROR";
    Serial.printf("[DELETE] %s -> %s\n", keyStr, m);
}

static void cmd_lookup(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) { Serial.println("[USAGE] l <key>"); return; }
    if (key_len > 8) key_len = 8;

    char keyStr[9] = {0};
    memcpy(keyStr, key, key_len);

    uint8_t st = distributed_lookup_key(key, key_len);
    const char* m = (st==STATUS_OK)?"FOUND":(st==STATUS_NOT_FOUND)?"NOT FOUND":
                    (st==STATUS_RETRY)?"RETRY":(st==STATUS_UNAVAILABLE)?"UNAVAILABLE":"ERROR";
    Serial.printf("[LOOKUP] %s -> %s\n", keyStr, m);
}

static void cmd_status() {
    Serial.println("\n-- STATUS --");
    Serial.println("NODE   STATUS   ITEMS / CAP   PING");
    uint32_t now = millis();
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        NodeRecord& s = node_registry[i];
        uint32_t ping = s.alive ? (now - s.last_heartbeat_ms) : 0;
        Serial.printf("0x%02X   %-6s   %lu / %-5u   %lums\n",
                      s.id, s.alive ? "UP" : "DOWN",
                      (unsigned long)s.item_count, s.capacity,
                      (unsigned long)ping);
    }
    Serial.println();
}

static void cmd_benchmark(int n) {
    if (n <= 0) { Serial.println("[MASTER] Usage: b <n>"); return; }
    uint32_t t0 = micros(), ok = 0;
    for (int i = 0; i < n; i++) {
        uint32_t k = (uint32_t)i;
        if (distributed_insert_key((const uint8_t*)&k, 4) == STATUS_OK) ok++;
    }
    uint32_t dt = micros() - t0;
    Serial.printf("[MASTER] BENCH: inserts=%d ok=%lu total=%luus avg=%luus\n",
                  n, (unsigned long)ok, (unsigned long)dt, (unsigned long)(dt / (uint32_t)n));
}

static void cmd_fpr(int n) {
    if (n <= 0) { Serial.println("[MASTER] Usage: f <n>"); return; }
    for (int i = 0; i < n; i++) {
        uint32_t k = (uint32_t)i;
        distributed_insert_key((const uint8_t*)&k, 4);
    }
    uint32_t fp = 0;
    for (int i = 0; i < n; i++) {
        uint32_t k = (uint32_t)(n + 10000 + i);
        if (distributed_lookup_key((const uint8_t*)&k, 4) == STATUS_OK) fp++;
    }
    float fpr = (float)fp * 100.0f / (float)n;
    Serial.printf("[MASTER] FPR: %.2f%% (%lu/%d)\n", fpr, (unsigned long)fp, n);
}

static void handle_serial() {
    if (!Serial.available()) return;
    char buf[64] = {0};
    uint8_t idx = 0;
    while (Serial.available() && idx < (uint8_t)sizeof(buf) - 1) {
        char c = (char)Serial.read();
        if (c == '\n' || c == '\r') break;
        buf[idx++] = c;
    }
    if (idx == 0) return;

    char cmd = buf[0];
    const char* arg = (idx > 2) ? (buf + 2) : "";
    uint8_t arg_len = (uint8_t)strlen(arg);
    if      (cmd == 'i') cmd_insert((const uint8_t*)arg, arg_len);
    else if (cmd == 'd') cmd_delete((const uint8_t*)arg, arg_len);
    else if (cmd == 'l') cmd_lookup((const uint8_t*)arg, arg_len);
    else if (cmd == 's') cmd_status();
    else if (cmd == 'b') cmd_benchmark(atoi(arg));
    else if (cmd == 'f') cmd_fpr(atoi(arg));
    else if (cmd == 'r') cmd_rebalance();
    else {
        Serial.println("\n-- MENU --");
        Serial.println("i <key> : Insert");
        Serial.println("d <key> : Delete");
        Serial.println("l <key> : Lookup");
        Serial.println("s       : Status");
        Serial.println("b <n>   : Bench");
        Serial.println("f <n>   : FPR Test");
        Serial.println("r       : Rebalance");
        Serial.println();
    }
}

// ════════════════════════════════════════════════════════════════════════════
// ESP-NOW receive callback — WiFi task context, must not block.
// ════════════════════════════════════════════════════════════════════════════

void on_recv(const uint8_t* mac, const uint8_t* data, int len) {
    (void)mac;
    if (len < 1) return;
    uint8_t type = data[0];

    if (type == MSG_ACK && len >= (int)sizeof(MsgAck)) {
        const MsgAck* a = (const MsgAck*)data;
        g_reply_status = a->status;
        g_reply_seq    = a->ack_seq;
        g_reply_evicted_tag = 0;
        g_reply_ready  = true;
        return;
    }
    if (type == MSG_CHAIN_RESULT && len >= (int)sizeof(MsgChainResult)) {
        const MsgChainResult* r = (const MsgChainResult*)data;
        g_reply_status = r->status;
        g_reply_seq    = r->ack_seq;
        g_reply_evicted_tag = r->evicted_tag;
        g_reply_ready  = true;
        return;
    }
    if (type == MSG_BUCKET_BATCH_DATA && len >= (int)sizeof(MsgBucketBatchData)) {
        memcpy(&g_batch_data_buf, data, sizeof(g_batch_data_buf));
        g_batch_data_ready = true;
        return;
    }
    if (type == MSG_HEARTBEAT && len >= (int)sizeof(MsgHeartbeat)) {
        const MsgHeartbeat* hb = (const MsgHeartbeat*)data;
        NodeRecord* sl = find_node_by_id(hb->hdr.src_id);
        if (!sl) return;
        bool was_dead = !sl->alive;
        sl->alive      = true;
        sl->last_heartbeat_ms = millis();
        sl->item_count = hb->item_count;
        sl->capacity   = hb->capacity;
        sl->load_pct   = hb->load_pct;
        if (was_dead) {
            Serial.printf("[MASTER] Slave 0x%02X ONLINE\n", sl->id);
            if (hb->load_pct != 0) {
                sl->alive = false;
                uint8_t idx = (uint8_t)(sl - node_registry);
                if (transmit_hard_reset(idx)) {
                    sl->alive = true;
                    Serial.printf("[MASTER] Slave 0x%02X reset OK\n", sl->id);
                } else {
                    Serial.printf("[MASTER] Slave 0x%02X reset FAILED\n", sl->id);
                }
            }
            g_rebalance_pending = true;
        } else if (sl->alive) {
            if (hb->load_pct > REBALANCE_THRESHOLD) {
                g_rebalance_pending = true;
            } else if (NUM_SLAVES >= 2) {
                uint8_t hi = 0, lo = 101;
                for (uint8_t i = 0; i < NUM_SLAVES; i++) {
                    if (!node_registry[i].alive) continue;
                    if (node_registry[i].load_pct > hi) hi = node_registry[i].load_pct;
                    if (node_registry[i].load_pct < lo) lo = node_registry[i].load_pct;
                }
                if (hi - lo >= IMBALANCE_THRESHOLD) g_rebalance_pending = true;
            }
        }
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Arduino entry points
// ════════════════════════════════════════════════════════════════════════════

void setup() {
    Serial.begin(115200);
    delay(300);

    WiFi.mode(WIFI_STA);
    Serial.printf("[MASTER] MAC: %s\n", WiFi.macAddress().c_str());

    transport_init(on_recv);

    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        node_registry[i].id          = (uint8_t)(i + 1);
        node_registry[i].mac        = SLAVE_MACS[i];
        node_registry[i].alive      = true;
        node_registry[i].last_heartbeat_ms = millis();
        node_registry[i].item_count     = 0;
        node_registry[i].capacity   = LOCAL_CAPACITY * 4;
        node_registry[i].load_pct  = 0;
        transport_add_peer(SLAVE_MACS[i]);
    }

    assign_buckets_to_nodes();
    Serial.println("[MASTER] Ready. Commands: i/d/l <key> | s | b/f <n> | r");
}

void loop() {
    uint32_t now = millis();
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        if (node_registry[i].alive && (now - node_registry[i].last_heartbeat_ms) > HEARTBEAT_TIMEOUT_MS) {
            node_registry[i].alive = false;
            Serial.printf("[MASTER] Slave 0x%02X OFFLINE\n", node_registry[i].id);
        }
    }

    if (g_rebalance_pending && (now - g_last_rebalance_ms) >= REBALANCE_COOLDOWN_MS) {
        g_rebalance_pending = false;
        g_last_rebalance_ms = now;
        cmd_rebalance();
    }

    handle_serial();
    delay(10);
}

#endif // !NATIVE_BUILD && MASTER_BUILD
