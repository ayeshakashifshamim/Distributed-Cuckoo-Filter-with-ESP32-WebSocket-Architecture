#pragma once
// dispatcher.h — Slave-side message handler.
// The dispatcher is decoupled from the transport (tests inject a fake send_fn)
// and from the single-node CuckooFilter class. All distributed state lives in
// slave_storage.{h,cpp}; the local CuckooFilter is only used by unit tests.

#include <stdint.h>
#include <stddef.h>
#include "../protocol/messages.h"
#include "../slave_storage.h"

typedef void (*TransportSendFn)(const uint8_t* data, size_t len);

// Ring buffer of recently processed (src_id, seq) tuples so that retried
// requests (dropped ACKs) replay the cached reply instead of being re-executed.
static constexpr uint8_t DEDUP_SIZE = 64;

struct DedupEntry {
    uint16_t src_id;
    uint8_t  seq;
    uint8_t  status;
    uint8_t  evicted_tag;   // cached for MSG_CHAIN_RESULT retries
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

    // op_id=0 is the "no correlator" value used for non-chain messages.
    bool _dedupHit(uint16_t src_id, uint8_t seq, uint32_t op_id,
                   uint8_t& status, uint8_t& evicted);
    void _dedupRecord(uint16_t src_id, uint8_t seq, uint32_t op_id,
                      uint8_t status, uint8_t evicted);

    void _handleChainInsert(const MsgChainInsert* msg);
    void _handleTagLookup(const MsgTagQuery* msg);
    void _handleTagDelete(const MsgTagQuery* msg);
    void _handleBucketBatchRead(const MsgBucketBatchRead* msg);
    void _handleBucketBatchWrite(const MsgBucketBatchWrite* msg);
    void _handleBucketBatchClear(const MsgBucketBatchClear* msg);
    void _handleHardReset(const MsgHardReset* msg);
};
