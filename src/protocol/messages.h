#pragma once
// wire protocol for Distributed Cuckoo Filter
//   All structs are 1-byte packed so binary layout is identical on ESP32 and
//   desktop. Field order is contract.

#include <stdint.h>
#include "../config.h"

#define MSG_DELETE              0x02
#define MSG_LOOKUP              0x03
#define MSG_SYNC                0x04
#define MSG_ACK                 0x05
#define MSG_PING                0x06
#define MSG_HEARTBEAT           0x07
#define MSG_CHAIN_INSERT        0x0A
#define MSG_CHAIN_RESULT        0x0B
#define MSG_TAG_LOOKUP          0x0C
#define MSG_TAG_DELETE          0x0D
#define MSG_BUCKET_BATCH_READ   0x0E
#define MSG_BUCKET_BATCH_DATA   0x0F
#define MSG_BUCKET_BATCH_WRITE  0x10
#define MSG_BUCKET_BATCH_CLEAR  0x11
#define MSG_HARD_RESET          0x12

#define CMD_PING    MSG_PING
#define CMD_INSERT  MSG_INSERT
#define CMD_LOOKUP  MSG_LOOKUP
#define CMD_DELETE  MSG_DELETE
#define CMD_STATUS  MSG_SYNC

#define STATUS_OK          0
#define STATUS_FULL        1
#define STATUS_NOT_FOUND   2
#define STATUS_ERROR       3
#define STATUS_RETRY       4   // bucket locked for migration
#define STATUS_UNAVAILABLE 5   // owner offline
#define STATUS_KICKED      6   // insert displaced a resident tag

#define ACK_OK          STATUS_OK
#define ACK_FOUND       STATUS_OK
#define ACK_NOT_FOUND   STATUS_NOT_FOUND
#define ACK_FULL        STATUS_FULL
#define ACK_ERR         STATUS_ERROR
#define ACK_RETRY       STATUS_RETRY
#define ACK_UNAVAILABLE STATUS_UNAVAILABLE
#define ACK_KICKED      STATUS_KICKED

// 8-bit slave broadcast selector (low byte of MsgHeader.dst_id).
#define BROADCAST_ID 0xFF

#pragma pack(push, 1)

// Common 6-byte header on every message.
struct __attribute__((packed)) MsgHeader {
    uint8_t  type;      // MSG_* constant
    uint8_t  seq;       // rolling sequence number, used for dedup
    uint16_t src_id;    // sender node ID
    uint16_t dst_id;    // destination node ID (low byte = slave ID or BROADCAST_ID)
};

// User-level operations (master to slave) 
struct __attribute__((packed)) MsgInsert { MsgHeader hdr; uint8_t item[8]; uint8_t item_len; };
struct __attribute__((packed)) MsgDelete { MsgHeader hdr; uint8_t item[8]; uint8_t item_len; };
struct __attribute__((packed)) MsgLookup { MsgHeader hdr; uint8_t item[8]; uint8_t item_len; };

// Slave status (slave to master) 
struct __attribute__((packed)) MsgSync {
    MsgHeader hdr;
    uint32_t  item_count;
    uint16_t  capacity;
    uint8_t   load_pct;
};

struct __attribute__((packed)) MsgHeartbeat {
    MsgHeader hdr;
    uint32_t  item_count;
    uint16_t  capacity;
    uint8_t   load_pct;
};

// ack
struct __attribute__((packed)) MsgAck {
    MsgHeader hdr;
    uint8_t   ack_seq;   // mirrors seq from the originating request
    uint8_t   status;    // STATUS_*
};

// Kickout chain 
// try_only=1 → slave accepts only if a slot is free; returns STATUS_FULL on full
// try_only=0 → slave evicts a random resident tag on full, returns STATUS_KICKED
struct __attribute__((packed)) MsgChainInsert {
    MsgHeader hdr;
    uint16_t  bucket;    // global bucket index
    uint8_t   tag;       // fingerprint to place
    uint32_t  op_id;     // logical transaction ID spanning a chain
    uint8_t   hop;       // 0-based hop index
    uint8_t   max_hops;  // hard stop bound
    uint8_t   try_only;  // 0 = force (kickout ok), 1 = soft (no kickout)
};

struct __attribute__((packed)) MsgChainResult {
    MsgHeader hdr;
    uint8_t   ack_seq;
    uint8_t   status;      // STATUS_OK / STATUS_KICKED / STATUS_FULL / ...
    uint8_t   evicted_tag; // valid only when status == STATUS_KICKED
};

// Direct bucket queries (master to slave) 
struct __attribute__((packed)) MsgTagQuery {
    MsgHeader hdr;     // type = MSG_TAG_LOOKUP or MSG_TAG_DELETE
    uint16_t  bucket;
    uint8_t   tag;
};

// Bucket migration (rebalancing, 3 round trips per batch) 
// Sizes at MAX_BUCKET_BATCH = 20 stay under the 250-byte ESP-NOW limit
struct __attribute__((packed)) BucketEntry {
    uint16_t global_bucket;
    uint8_t  tags[4];  // must match CF_BUCKET_SIZE
};

struct __attribute__((packed)) MsgBucketBatchRead {
    MsgHeader hdr;
    uint8_t   count;
    uint16_t  buckets[MAX_BUCKET_BATCH];
};

struct __attribute__((packed)) MsgBucketBatchData {
    MsgHeader   hdr;
    uint8_t     count;
    BucketEntry entries[MAX_BUCKET_BATCH];
};

struct __attribute__((packed)) MsgBucketBatchWrite {
    MsgHeader   hdr;
    uint8_t     count;
    BucketEntry entries[MAX_BUCKET_BATCH];
};

struct __attribute__((packed)) MsgBucketBatchClear {
    MsgHeader hdr;
    uint8_t   count;
    uint16_t  buckets[MAX_BUCKET_BATCH];
};

// Hard reset (master to slave on reconnect with non-zero load) 
struct __attribute__((packed)) MsgHardReset {
    MsgHeader hdr;
};

#pragma pack(pop)
