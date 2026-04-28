#pragma once
// ─────────────────────────────────────────────────────────────────────────────
// CuckooFilter — ESP32 adaptation of the reference implementation
//   - No dynamic allocation  (static arrays, fixed at compile time)
//   - No std::string / std::random_device / openssl / __int128
//   - Uses MurmurHash2 (inline) + alt-index formula from the reference
//   - Victim cache preserved exactly as in the reference (handles edge case
//     where the last insertion cannot find a free slot after kMaxCuckooCount)
// ─────────────────────────────────────────────────────────────────────────────
#include <stdint.h>
#include <stddef.h>

// ── Tuning (change only these) ───────────────────────────────────────────────
#define CF_NUM_BUCKETS   256u   // must be power of 2; 256*4*1 = 1 KB
#define CF_BUCKET_SIZE     4u   // tags per bucket (4 is standard)
#define CF_BITS_PER_TAG    8u   // fingerprint width in bits (8 → ~3 % FP rate)
#define CF_MAX_KICKS     500u   // max displacement attempts before "full"
// ─────────────────────────────────────────────────────────────────────────────

static_assert((CF_NUM_BUCKETS & (CF_NUM_BUCKETS - 1)) == 0,
              "CF_NUM_BUCKETS must be a power of 2");

enum CF_Status {
    CF_Ok            = 0,
    CF_NotFound      = 1,
    CF_NotEnoughSpace = 2,
};

class CuckooFilter {
public:
    CuckooFilter();

    /** Insert item bytes. Returns CF_Ok or CF_NotEnoughSpace. */
    CF_Status add(const uint8_t* data, size_t len);

    /** Lookup item bytes. Returns CF_Ok (probably present) or CF_NotFound. */
    CF_Status contain(const uint8_t* data, size_t len) const;

    /** Delete item bytes. Returns CF_Ok or CF_NotFound. */
    CF_Status remove(const uint8_t* data, size_t len);

    /** Remove all entries. */
    void clear();

    size_t   size()        const { return _num_items; }
    size_t   capacity()    const { return CF_NUM_BUCKETS * CF_BUCKET_SIZE; }
    uint8_t  loadPercent() const;

private:
    // Table: [bucket][slot] each slot holds one tag (1 byte for 8-bit tags)
    uint8_t  _table[CF_NUM_BUCKETS][CF_BUCKET_SIZE];
    size_t   _num_items;

    // Victim cache — exactly as in the reference
    struct { size_t index; uint32_t tag; bool used; } _victim;

    // ── Internal helpers ─────────────────────────────────────────────────────
    static uint32_t _murmur(const uint8_t* data, size_t len);
    static uint32_t _indexHash(uint32_t hv);
    static uint32_t _tagHash(uint32_t hv);
    static size_t   _altIndex(size_t index, uint32_t tag);

    void     _genIndexTag(const uint8_t* data, size_t len,
                          size_t* index, uint32_t* tag) const;
    bool     _insertTagToBucket(size_t i, uint32_t tag,
                                bool kickout, uint32_t& oldtag);
    bool     _findTagInBuckets(size_t i1, size_t i2, uint32_t tag) const;
    bool     _deleteTagFromBucket(size_t i, uint32_t tag);
    CF_Status _addImpl(size_t i, uint32_t tag);
};
