// master.cpp — Distributed cuckoo filter coordinator (Milestone 2).
//
// Owns the global routing table g_route_primary[b] → slave_index, issues
// chain-insert / tag-lookup / tag-delete operations to the owning slave, and
// runs a bucket-level rebalancer driven by heartbeat load reports.
//
// This file compiles only for the master firmware build.

#if !defined(NATIVE_BUILD) && defined(MASTER_BUILD)

#include <Arduino.h>
#include <WiFi.h>
#include <string.h>
#include <stdlib.h>
#include "config.h"
#include "protocol/messages.h"
#include "transport/transport.h"

// ── Master-local state ──────────────────────────────────────────────────────

struct SlaveInfo {
    uint8_t        id;
    const uint8_t* mac;
    bool           alive;
    uint32_t       last_hb_ms;
    uint32_t       item_count;
    uint16_t       capacity;
    uint8_t        load_pct;
};

static SlaveInfo g_slaves[NUM_SLAVES];

// Flat routing table: global bucket → slave index. `locked` flag is set during
// rebalancing so lookups/inserts/deletes return STATUS_RETRY for buckets in
// flight.
static uint8_t g_route_primary[GLOBAL_BUCKET_COUNT];
static bool    g_route_locked [GLOBAL_BUCKET_COUNT];

// ── Reply synchronisation (single outstanding request at a time) ────────────

static uint8_t           g_seq = 0;
static volatile bool     g_reply_ready = false;
static volatile uint8_t  g_reply_status = STATUS_ERROR;
static volatile uint8_t  g_reply_seq    = 0;
static volatile uint8_t  g_reply_evicted_tag = 0;

static volatile bool            g_batch_data_ready = false;
static MsgBucketBatchData       g_batch_data_buf;

static uint32_t g_chain_op_id = 1;

// Auto-rebalance scheduling — heartbeat flags work, loop() runs the migration.
static volatile bool g_rebalance_pending = false;
static uint32_t      g_last_rebalance_ms = 0;

// ── Hashing (master only — slaves receive explicit bucket + tag) ────────────

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
        case 2: h ^= (uint32_t)data[1] << 8;  /* fallthrough */
        case 1: h ^= (uint32_t)data[0]; h *= m;
    }
    h ^= h >> 13; h *= m; h ^= h >> 15;
    return h;
}

static uint8_t  tag_of(uint32_t h)  { uint8_t t = (uint8_t)(h & 0xFF); return t ? t : 1; }
static uint16_t bucket1_of(uint32_t h) { return (uint16_t)((h >> 8) & (GLOBAL_BUCKET_COUNT - 1)); }
static uint16_t alt_bucket(uint16_t b, uint8_t tag) {
    return (uint16_t)((b ^ (uint32_t)(tag * 0x5bd1e995u)) & (GLOBAL_BUCKET_COUNT - 1));
}

// ── Routing table helpers ───────────────────────────────────────────────────

static void routing_init() {
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT; b++) {
        g_route_primary[b] = (uint8_t)(b % NUM_SLAVES);
        g_route_locked[b]  = false;
    }
}

static SlaveInfo* slave_by_src(uint16_t src_id) {
    for (uint8_t i = 0; i < NUM_SLAVES; i++)
        if (g_slaves[i].id == (uint8_t)src_id) return &g_slaves[i];
    return nullptr;
}

// ── Low-level send/wait ─────────────────────────────────────────────────────

static bool wait_for_reply(uint8_t req_seq, uint32_t timeout_ms) {
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

static bool wait_for_batch_data(uint32_t timeout_ms) {
    uint32_t start = millis();
    while ((millis() - start) < timeout_ms) {
        if (g_batch_data_ready) { g_batch_data_ready = false; return true; }
        delay(2);
    }
    return false;
}

static bool send_chain_insert(uint8_t slave_idx, uint16_t bucket, uint8_t tag,
                              uint32_t op_id, uint8_t hop, bool try_only,
                              uint8_t* out_status, uint8_t* out_evicted) {
    SlaveInfo& s = g_slaves[slave_idx];
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
    if (!wait_for_reply(g_seq, 1000)) { *out_status = STATUS_UNAVAILABLE; return false; }
    *out_status  = g_reply_status;
    *out_evicted = g_reply_evicted_tag;
    return true;
}

static bool send_tag_query(uint8_t slave_idx, uint8_t cmd, uint16_t bucket, uint8_t tag,
                           uint8_t* out_status) {
    SlaveInfo& s = g_slaves[slave_idx];
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
    if (!wait_for_reply(g_seq, 1000)) { *out_status = STATUS_UNAVAILABLE; return false; }
    *out_status = g_reply_status;
    return true;
}

static bool send_hard_reset(uint8_t slave_idx) {
    SlaveInfo& s = g_slaves[slave_idx];
    MsgHardReset msg;
    memset(&msg, 0, sizeof(msg));
    msg.hdr.type   = MSG_HARD_RESET;
    msg.hdr.seq    = ++g_seq;
    msg.hdr.src_id = MASTER_ID;
    msg.hdr.dst_id = s.id;

    g_reply_ready = false;
    transport_send(s.mac, (const uint8_t*)&msg, sizeof(msg));
    return wait_for_reply(g_seq, 1500) && g_reply_status == STATUS_OK;
}

// ── Distributed operations ─────────────────────────────────────────────────

static uint8_t distributed_insert_key(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) return STATUS_ERROR;
    uint32_t h  = murmur_hash(key, key_len);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);
    uint32_t op = g_chain_op_id++;

    // Phase 1 — soft try b1 then b2 (no kickout).
    for (uint8_t i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (g_route_locked[b]) continue;
        uint8_t owner = g_route_primary[b];
        if (!g_slaves[owner].alive) continue;
        uint8_t st = STATUS_ERROR, ev = 0;
        if (!send_chain_insert(owner, b, tg, op, 0, /*try_only=*/true, &st, &ev)) continue;
        if (st == STATUS_OK) return STATUS_OK;
    }

    // Phase 2 — force chain starting at a random candidate.
    uint16_t cur = ((rand() & 1) ? b1 : b2);
    uint8_t  ctg = tg;
    for (uint8_t hop = 0; hop < MAX_CHAIN_HOPS; hop++) {
        if (g_route_locked[cur]) return STATUS_RETRY;
        uint8_t owner = g_route_primary[cur];
        if (!g_slaves[owner].alive) return STATUS_UNAVAILABLE;
        uint8_t st = STATUS_ERROR, ev = 0;
        if (!send_chain_insert(owner, cur, ctg, op, hop, /*try_only=*/false, &st, &ev))
            return STATUS_UNAVAILABLE;
        if (st == STATUS_OK)     return STATUS_OK;
        if (st != STATUS_KICKED) return st;
        ctg = ev;
        cur = alt_bucket(cur, ctg);
    }
    return STATUS_FULL;
}

static uint8_t distributed_lookup_key(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) return STATUS_ERROR;
    uint32_t h  = murmur_hash(key, key_len);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);

    bool any_unavailable = false;
    for (uint8_t i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (g_route_locked[b]) return STATUS_RETRY;
        uint8_t st = STATUS_ERROR;
        if (!send_tag_query(g_route_primary[b], MSG_TAG_LOOKUP, b, tg, &st)) {
            any_unavailable = true; continue;
        }
        if (st == STATUS_OK) return STATUS_OK;
    }
    return any_unavailable ? STATUS_UNAVAILABLE : STATUS_NOT_FOUND;
}

static uint8_t distributed_delete_key(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) return STATUS_ERROR;
    uint32_t h  = murmur_hash(key, key_len);
    uint8_t  tg = tag_of(h);
    uint16_t b1 = bucket1_of(h);
    uint16_t b2 = alt_bucket(b1, tg);

    bool any_unavailable = false;
    for (uint8_t i = 0; i < 2; i++) {
        uint16_t b = (i == 0) ? b1 : b2;
        if (g_route_locked[b]) return STATUS_RETRY;
        uint8_t st = STATUS_ERROR;
        if (!send_tag_query(g_route_primary[b], MSG_TAG_DELETE, b, tg, &st)) {
            any_unavailable = true; continue;
        }
        if (st == STATUS_OK) return STATUS_OK;
    }
    return any_unavailable ? STATUS_UNAVAILABLE : STATUS_NOT_FOUND;
}

// ── Rebalancing (3 round-trip bucket migration) ────────────────────────────

static void unlock_batch(const uint16_t* batch, uint8_t n) {
    for (uint8_t i = 0; i < n; i++) g_route_locked[batch[i]] = false;
}

static void cmd_rebalance() {
    if (NUM_SLAVES < 2) {
        Serial.println("[MASTER] REBALANCE: need >=2 slaves"); return;
    }
    int8_t  src = -1, dst = -1;
    uint8_t max_load = 0, min_load = 101;
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        if (!g_slaves[i].alive) continue;
        if (g_slaves[i].load_pct > max_load) { max_load = g_slaves[i].load_pct; src = (int8_t)i; }
        if (g_slaves[i].load_pct < min_load) { min_load = g_slaves[i].load_pct; dst = (int8_t)i; }
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
                  g_slaves[src].id, max_load, g_slaves[dst].id, min_load);

    // Guard: dst must have free physical slots to receive buckets.
    uint16_t dst_owned = 0;
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT; b++)
        if (g_route_primary[b] == (uint8_t)dst) dst_owned++;
    if (dst_owned >= LOCAL_CAPACITY) {
        Serial.println("[MASTER] REBALANCE: dst has no free slots"); return;
    }
    uint8_t dst_free = (uint8_t)(LOCAL_CAPACITY - dst_owned);

    // Pick up to MAX_BUCKET_BATCH (and no more than dst_free) buckets currently
    // owned by src that aren't already locked by another in-flight operation.
    uint16_t batch[MAX_BUCKET_BATCH];
    uint8_t  n = 0;
    uint8_t  cap = (dst_free < MAX_BUCKET_BATCH) ? dst_free : MAX_BUCKET_BATCH;
    for (uint16_t b = 0; b < GLOBAL_BUCKET_COUNT && n < cap; b++) {
        if (g_route_primary[b] == (uint8_t)src && !g_route_locked[b]) batch[n++] = b;
    }
    if (n == 0) { Serial.println("[MASTER] REBALANCE: no eligible buckets"); return; }

    for (uint8_t i = 0; i < n; i++) g_route_locked[batch[i]] = true;

    // Round 1 — read.
    MsgBucketBatchRead rd;
    memset(&rd, 0, sizeof(rd));
    rd.hdr.type = MSG_BUCKET_BATCH_READ;
    rd.hdr.seq  = ++g_seq;
    rd.hdr.src_id = MASTER_ID;
    rd.hdr.dst_id = g_slaves[src].id;
    rd.count = n;
    memcpy(rd.buckets, batch, n * sizeof(uint16_t));

    g_batch_data_ready = false;
    transport_send(g_slaves[src].mac, (const uint8_t*)&rd, sizeof(rd));
    if (!wait_for_batch_data(1500)) { unlock_batch(batch, n); Serial.println("[MASTER] REBALANCE: read timeout"); return; }

    // Round 2 — write to dst.
    MsgBucketBatchWrite wr;
    memset(&wr, 0, sizeof(wr));
    wr.hdr.type = MSG_BUCKET_BATCH_WRITE;
    wr.hdr.seq  = ++g_seq;
    wr.hdr.src_id = MASTER_ID;
    wr.hdr.dst_id = g_slaves[dst].id;
    wr.count = g_batch_data_buf.count;
    memcpy(wr.entries, g_batch_data_buf.entries, wr.count * sizeof(BucketEntry));

    g_reply_ready = false;
    transport_send(g_slaves[dst].mac, (const uint8_t*)&wr, sizeof(wr));
    if (!wait_for_reply(g_seq, 1500) || g_reply_status != STATUS_OK) {
        unlock_batch(batch, n); Serial.println("[MASTER] REBALANCE: write failed"); return;
    }

    // Round 3 — clear on src.
    MsgBucketBatchClear cl;
    memset(&cl, 0, sizeof(cl));
    cl.hdr.type = MSG_BUCKET_BATCH_CLEAR;
    cl.hdr.seq  = ++g_seq;
    cl.hdr.src_id = MASTER_ID;
    cl.hdr.dst_id = g_slaves[src].id;
    cl.count = n;
    memcpy(cl.buckets, batch, n * sizeof(uint16_t));

    g_reply_ready = false;
    transport_send(g_slaves[src].mac, (const uint8_t*)&cl, sizeof(cl));
    if (!wait_for_reply(g_seq, 1500) || g_reply_status != STATUS_OK) {
        unlock_batch(batch, n); Serial.println("[MASTER] REBALANCE: clear failed"); return;
    }

    for (uint8_t i = 0; i < n; i++) {
        g_route_primary[batch[i]] = (uint8_t)dst;
        g_route_locked [batch[i]] = false;
    }
    Serial.printf("[MASTER] REBALANCE done: %u buckets 0x%02X -> 0x%02X\n",
                  n, g_slaves[src].id, g_slaves[dst].id);
}

// ── Serial command handlers ─────────────────────────────────────────────────

static void cmd_insert(const uint8_t* key, uint8_t key_len) {
    if (key_len == 0) { Serial.println("[USAGE] i <key>"); return; }
    if (key_len > 8) key_len = 8;
    
    // Convert key to printable string safely
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
        SlaveInfo& s = g_slaves[i];
        uint32_t ping = s.alive ? (now - s.last_hb_ms) : 0;
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

// ── Receive callback ───────────────────────────────────────────────────────

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
        SlaveInfo* sl = slave_by_src(hb->hdr.src_id);
        if (!sl) return;
        bool was_dead = !sl->alive;
        sl->alive      = true;
        sl->last_hb_ms = millis();
        sl->item_count = hb->item_count;
        sl->capacity   = hb->capacity;
        sl->load_pct   = hb->load_pct;
        if (was_dead) {
            Serial.printf("[MASTER] Slave 0x%02X ONLINE\n", sl->id);
            // Reset stale state if the slave came back with leftover RAM.
            if (hb->load_pct != 0) {
                sl->alive = false;
                uint8_t idx = (uint8_t)(sl - g_slaves);
                if (send_hard_reset(idx)) {
                    sl->alive = true;
                    Serial.printf("[MASTER] Slave 0x%02X reset OK\n", sl->id);
                } else {
                    Serial.printf("[MASTER] Slave 0x%02X reset FAILED\n", sl->id);
                }
            }
            // New-node-join may have created an imbalance — flag for the loop.
            g_rebalance_pending = true;
        } else if (sl->alive) {
            if (hb->load_pct > REBALANCE_THRESHOLD) {
                g_rebalance_pending = true;
            } else if (NUM_SLAVES >= 2) {
                uint8_t hi = 0, lo = 101;
                for (uint8_t i = 0; i < NUM_SLAVES; i++) {
                    if (!g_slaves[i].alive) continue;
                    if (g_slaves[i].load_pct > hi) hi = g_slaves[i].load_pct;
                    if (g_slaves[i].load_pct < lo) lo = g_slaves[i].load_pct;
                }
                if (hi - lo >= IMBALANCE_THRESHOLD) g_rebalance_pending = true;
            }
        }
    }
}

// ── Arduino entry points ───────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(300);

    WiFi.mode(WIFI_STA);
    Serial.printf("[MASTER] MAC: %s\n", WiFi.macAddress().c_str());

    transport_init(on_recv);

    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        g_slaves[i].id          = (uint8_t)(i + 1);
        g_slaves[i].mac         = SLAVE_MACS[i];
        g_slaves[i].alive       = true;
        g_slaves[i].last_hb_ms  = millis();
        g_slaves[i].item_count  = 0;
        g_slaves[i].capacity    = LOCAL_CAPACITY * 4;
        g_slaves[i].load_pct    = 0;
        transport_add_peer(SLAVE_MACS[i]);
    }

    routing_init();
    Serial.println("[MASTER] Ready. Commands: i/d/l <key> | s | b/f <n> | r");
}

void loop() {
    uint32_t now = millis();
    for (uint8_t i = 0; i < NUM_SLAVES; i++) {
        if (g_slaves[i].alive && (now - g_slaves[i].last_hb_ms) > HEARTBEAT_TIMEOUT_MS) {
            g_slaves[i].alive = false;
            Serial.printf("[MASTER] Slave 0x%02X OFFLINE\n", g_slaves[i].id);
        }
    }

    // Auto-rebalance (cooldown-gated). Runs in loop() — never inside on_recv.
    if (g_rebalance_pending && (now - g_last_rebalance_ms) >= REBALANCE_COOLDOWN_MS) {
        g_rebalance_pending = false;
        g_last_rebalance_ms = now;
        cmd_rebalance();
    }

    handle_serial();
    delay(10);
}

#endif // !NATIVE_BUILD && MASTER_BUILD
