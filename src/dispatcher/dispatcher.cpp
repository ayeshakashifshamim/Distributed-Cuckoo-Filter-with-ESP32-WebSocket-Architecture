// dispatcher.cpp — Slave-side message handler.
//
// Decoupled from the transport layer (tests inject a fake send_fn) and from the
// single-node CuckooFilter (which is only used by unit tests).  All distributed
// state lives in slave_storage; this class only routes and serialises replies.
//
// Deduplication:
//   Recent (src_id, seq, op_id) tuples are cached so that retried requests
//   (e.g. after a dropped ACK) replay the stored reply rather than re-executing
//   the operation.  Chain-insert uses op_id; non-chain ops use op_id=0.

#include "dispatcher.h"
#include "../config.h"
#include <string.h>
#include <stdlib.h>

namespace {

constexpr uint8_t SLOT_COUNT = 4;  // == CF_BUCKET_SIZE

// Tag 0 is the "empty slot" sentinel; valid fingerprints are always 1..255.
// These helpers scan a fixed-size tag array for vacancy or a specific fingerprint.

int seek_vacant_slot(const uint8_t tags[SLOT_COUNT]) {
    for (uint8_t i = 0; i < SLOT_COUNT; i++) if (tags[i] == 0) return (int)i;
    return -1;
}

int locate_fingerprint(const uint8_t tags[SLOT_COUNT], uint8_t tag) {
    for (uint8_t i = 0; i < SLOT_COUNT; i++) if (tags[i] == tag) return (int)i;
    return -1;
}

}  // namespace

Dispatcher::Dispatcher(SlaveStorage& storage, uint16_t node_id,
                       uint16_t master_id, TransportSendFn send_fn)
    : _storage(storage), _node_id(node_id), _master_id(master_id),
      _send(send_fn), _seq(0), _dedup_idx(0) {
    memset(_dedup, 0, sizeof(_dedup));
}

// ── Reply helpers ────────────────────────────────────────────────────────

void Dispatcher::_sendAck(uint8_t ack_seq, uint8_t status) {
    MsgAck ack;
    ack.hdr.type   = MSG_ACK;
    ack.hdr.seq    = _seq++;
    ack.hdr.src_id = _node_id;
    ack.hdr.dst_id = _master_id;
    ack.ack_seq    = ack_seq;
    ack.status     = status;
    _send((const uint8_t*)&ack, sizeof(ack));
}

void Dispatcher::_sendChainResult(uint8_t ack_seq, uint8_t status, uint8_t evicted_tag) {
    MsgChainResult res;
    res.hdr.type    = MSG_CHAIN_RESULT;
    res.hdr.seq     = _seq++;
    res.hdr.src_id  = _node_id;
    res.hdr.dst_id  = _master_id;
    res.ack_seq     = ack_seq;
    res.status      = status;
    res.evicted_tag = evicted_tag;
    _send((const uint8_t*)&res, sizeof(res));
}

void Dispatcher::sendHeartbeat() {
    MsgHeartbeat hb;
    hb.hdr.type   = MSG_HEARTBEAT;
    hb.hdr.seq    = _seq++;
    hb.hdr.src_id = _node_id;
    hb.hdr.dst_id = _master_id;
    hb.item_count = _storage.item_count();
    hb.capacity   = (uint16_t)(LOCAL_CAPACITY * SLOT_COUNT);
    uint32_t cap  = hb.capacity ? hb.capacity : 1;
    hb.load_pct   = (uint8_t)((hb.item_count * 100u) / cap);
    _send((const uint8_t*)&hb, sizeof(hb));
}

// ── Dedup cache ──────────────────────────────────────────────────────────

void Dispatcher::clearDedup() {
    memset(_dedup, 0, sizeof(_dedup));
    _dedup_idx = 0;
}

bool Dispatcher::_dedupHit(uint16_t src_id, uint8_t seq, uint32_t op_id,
                           uint8_t& status, uint8_t& evicted) const {
    for (uint8_t i = 0; i < DEDUP_SIZE; i++) {
        const DedupEntry& e = _dedup[i];
        if (e.valid && e.src_id == src_id && e.seq == seq && e.op_id == op_id) {
            status  = e.status;
            evicted = e.evicted_tag;
            return true;
        }
    }
    return false;
}

void Dispatcher::_dedupRecord(uint16_t src_id, uint8_t seq, uint32_t op_id,
                              uint8_t status, uint8_t evicted) {
    _dedup[_dedup_idx] = {src_id, seq, status, evicted, op_id, true};
    _dedup_idx = (_dedup_idx + 1) % DEDUP_SIZE;
}

// ── Chain insert ──────────────────────────────────────────────────────────
//
// try_only=true: attempt insert without kickout.  Returns STATUS_FULL
// immediately if the bucket has no empty slot.
//
// try_only=false: forcibly insert and report the evicted fingerprint so the
// master can continue the kickout chain at the alternate bucket.

void Dispatcher::process_cuckoo_insert(const MsgChainInsert* msg) {
    uint8_t cs, ce;
    if (_dedupHit(msg->hdr.src_id, msg->hdr.seq, msg->op_id, cs, ce)) {
        _sendChainResult(msg->hdr.seq, cs, ce);
        return;
    }

    int16_t slot = _storage.index_of(msg->bucket);
    if (slot < 0) {
        slot = _storage.acquire_slot(msg->bucket);
        if (slot < 0) {
            _dedupRecord(msg->hdr.src_id, msg->hdr.seq, msg->op_id, STATUS_FULL, 0);
            _sendChainResult(msg->hdr.seq, STATUS_FULL, 0);
            return;
        }
    }

    uint8_t* tags = _storage.bucket_at(slot).tags;

    int vacant = seek_vacant_slot(tags);
    if (vacant >= 0) {
        tags[vacant] = msg->tag;
        _storage.on_tag_added();
        _dedupRecord(msg->hdr.src_id, msg->hdr.seq, msg->op_id, STATUS_OK, 0);
        _sendChainResult(msg->hdr.seq, STATUS_OK, 0);
        return;
    }

    if (msg->try_only) {
        _dedupRecord(msg->hdr.src_id, msg->hdr.seq, msg->op_id, STATUS_FULL, 0);
        _sendChainResult(msg->hdr.seq, STATUS_FULL, 0);
        return;
    }

    // Force insert: displace a random resident tag; master will re-insert it.
    uint8_t r = (uint8_t)(rand() % SLOT_COUNT);
    uint8_t evicted = tags[r];
    tags[r] = msg->tag;
    _dedupRecord(msg->hdr.src_id, msg->hdr.seq, msg->op_id, STATUS_KICKED, evicted);
    _sendChainResult(msg->hdr.seq, STATUS_KICKED, evicted);
}

// ── Tag lookup / delete ───────────────────────────────────────────────────

void Dispatcher::process_bucket_lookup(const MsgTagQuery* msg) {
    int16_t slot = _storage.index_of(msg->bucket);
    if (slot < 0) { _sendAck(msg->hdr.seq, STATUS_NOT_FOUND); return; }
    bool found = locate_fingerprint(_storage.bucket_at(slot).tags, msg->tag) >= 0;
    _sendAck(msg->hdr.seq, found ? STATUS_OK : STATUS_NOT_FOUND);
}

void Dispatcher::process_bucket_delete(const MsgTagQuery* msg) {
    // Tag delete is idempotent — same (src_id, seq) retry replays the cached
    // status until that cache entry ages out, at which point a second delete
    // will correctly return STATUS_NOT_FOUND.
    uint8_t cs, ce;
    if (_dedupHit(msg->hdr.src_id, msg->hdr.seq, 0, cs, ce)) {
        _sendAck(msg->hdr.seq, cs);
        return;
    }

    int16_t slot = _storage.index_of(msg->bucket);
    uint8_t status;
    if (slot < 0) {
        status = STATUS_NOT_FOUND;
    } else {
        uint8_t* tags = _storage.bucket_at(slot).tags;
        int hit = locate_fingerprint(tags, msg->tag);
        if (hit >= 0) {
            tags[hit] = 0;
            _storage.on_tag_removed();
            _storage.release_if_empty(msg->bucket);
            status = STATUS_OK;
        } else {
            status = STATUS_NOT_FOUND;
        }
    }
    _dedupRecord(msg->hdr.src_id, msg->hdr.seq, 0, status, 0);
    _sendAck(msg->hdr.seq, status);
}

// ── Bucket batch migration ───────────────────────────────────────────────

void Dispatcher::process_bucket_read(const MsgBucketBatchRead* msg) {
    MsgBucketBatchData out;
    memset(&out, 0, sizeof(out));
    out.hdr.type   = MSG_BUCKET_BATCH_DATA;
    out.hdr.seq    = _seq++;
    out.hdr.src_id = _node_id;
    out.hdr.dst_id = _master_id;
    uint8_t n = msg->count > MAX_BUCKET_BATCH ? MAX_BUCKET_BATCH : msg->count;
    out.count = n;
    for (uint8_t i = 0; i < n; i++) {
        uint16_t b = msg->buckets[i];
        out.entries[i].global_bucket = b;
        int16_t slot = _storage.index_of(b);
        if (slot >= 0) memcpy(out.entries[i].tags, _storage.bucket_at(slot).tags, SLOT_COUNT);
        else           memset(out.entries[i].tags, 0, SLOT_COUNT);
    }
    _send((const uint8_t*)&out, sizeof(out));
}

void Dispatcher::process_bucket_write(const MsgBucketBatchWrite* msg) {
    uint8_t n = msg->count > MAX_BUCKET_BATCH ? MAX_BUCKET_BATCH : msg->count;
    for (uint8_t i = 0; i < n; i++) {
        const BucketEntry& e = msg->entries[i];
        int16_t slot = _storage.acquire_slot(e.global_bucket);
        if (slot < 0) continue;
        uint8_t* tags = _storage.bucket_at(slot).tags;
        for (uint8_t j = 0; j < SLOT_COUNT; j++) {
            if (tags[j] != 0) _storage.on_tag_removed();
            tags[j] = e.tags[j];
            if (tags[j] != 0) _storage.on_tag_added();
        }
    }
    _sendAck(msg->hdr.seq, STATUS_OK);
}

void Dispatcher::process_bucket_clear(const MsgBucketBatchClear* msg) {
    uint8_t n = msg->count > MAX_BUCKET_BATCH ? MAX_BUCKET_BATCH : msg->count;
    for (uint8_t i = 0; i < n; i++) _storage.force_clear(msg->buckets[i]);
    _sendAck(msg->hdr.seq, STATUS_OK);
}

// ── Hard reset ─────────────────────────────────────────────────────────────

void Dispatcher::process_hard_reset(const MsgHardReset* msg) {
    _storage.init();
    clearDedup();
    _sendAck(msg->hdr.seq, STATUS_OK);
}

// ── Top-level dispatch ─────────────────────────────────────────────────────

void Dispatcher::onMessage(const uint8_t* buf, size_t len) {
    if (len < sizeof(MsgHeader)) return;
    const MsgHeader* hdr = (const MsgHeader*)buf;

    switch (hdr->type) {
        case MSG_PING:
            _sendAck(hdr->seq, STATUS_OK);
            break;
        case MSG_CHAIN_INSERT:
            if (len >= sizeof(MsgChainInsert))
                process_cuckoo_insert((const MsgChainInsert*)buf);
            break;
        case MSG_TAG_LOOKUP:
            if (len >= sizeof(MsgTagQuery))
                process_bucket_lookup((const MsgTagQuery*)buf);
            break;
        case MSG_TAG_DELETE:
            if (len >= sizeof(MsgTagQuery))
                process_bucket_delete((const MsgTagQuery*)buf);
            break;
        case MSG_BUCKET_BATCH_READ:
            if (len >= sizeof(MsgBucketBatchRead))
                process_bucket_read((const MsgBucketBatchRead*)buf);
            break;
        case MSG_BUCKET_BATCH_WRITE:
            if (len >= sizeof(MsgBucketBatchWrite))
                process_bucket_write((const MsgBucketBatchWrite*)buf);
            break;
        case MSG_BUCKET_BATCH_CLEAR:
            if (len >= sizeof(MsgBucketBatchClear))
                process_bucket_clear((const MsgBucketBatchClear*)buf);
            break;
        case MSG_HARD_RESET:
            if (len >= sizeof(MsgHardReset))
                process_hard_reset((const MsgHardReset*)buf);
            break;
        default:
            break;
    }
}
