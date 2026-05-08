#pragma once
// dispatcher.h — Slave-side message handler.
//
// Decoupled from transport (tests inject a fake send_fn) and from the local
// CuckooFilter class (which is only used by unit tests).  All distributed
// state lives in slave_storage; this class only routes and serialises replies.

#include <stdint.h>
#include <stddef.h>
#include "../protocol/messages.h"
#include "../slave_storage.h"

typedef void (*TransportSendFn)(const uint8_t* data, size_t len);

// Ring buffer of recently processed (src_id, seq, op_id) tuples so that retried
// requests (dropped ACKs) replay the cached reply instead of being re-executed.
static constexpr uint8_t DEDUP_SIZE = 64;

struct DedupEntry {
    uint16_t src_id;
    uint8_t  seq;
    uint8_t  status;
    uint8_t  evicted_tag;
    uint32_t op_id;         // chain-insert correlator; 0 for other messages
    bool     valid;
};

class Dispatcher {
public:
    Dispatcher(SlaveStorage& storage, uint16_t node_id, uint16_t master_id,
               TransportSendFn send_fn);

    // Process one incoming wire buffer.
    void onMessage(const uint8_t* buf, size_t len);

    // Emit a heartbeat to master (called from the slave's main loop).
    void sendHeartbeat();

    // Reset dedup state — used by tests between cases.
    void clearDedup();

    uint8_t seq() const { return _seq; }

private:
    SlaveStorage&   _storage;
    uint16_t        _node_id;
    uint16_t        _master_id;
    TransportSendFn _send;
    uint8_t         _seq;

    DedupEntry _dedup[DEDUP_SIZE];
    uint8_t    _dedup_idx;

    void _sendAck(uint8_t ack_seq, uint8_t status);
    void _sendChainResult(uint8_t ack_seq, uint8_t status, uint8_t evicted_tag);

    bool _dedupHit(uint16_t src_id, uint8_t seq, uint32_t op_id,
                   uint8_t& status, uint8_t& evicted) const;
    void _dedupRecord(uint16_t src_id, uint8_t seq, uint32_t op_id,
                      uint8_t status, uint8_t evicted);

    void process_cuckoo_insert(const MsgChainInsert* msg);
    void process_bucket_lookup(const MsgTagQuery* msg);
    void process_bucket_delete(const MsgTagQuery* msg);
    void process_bucket_read(const MsgBucketBatchRead* msg);
    void process_bucket_write(const MsgBucketBatchWrite* msg);
    void process_bucket_clear(const MsgBucketBatchClear* msg);
    void process_hard_reset(const MsgHardReset* msg);
};
