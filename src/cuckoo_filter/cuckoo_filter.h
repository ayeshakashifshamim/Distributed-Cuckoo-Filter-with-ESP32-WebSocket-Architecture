#pragma once
// ─────────────────────────────────────────────────────────────────────────────
// CuckooFilter — embedded adaptation
//   - No dynamic allocation  (static arrays, fixed at compile time)
//   - No std::string / std::random_device / openssl / __int128
//   - MurmurHash2 inline + XOR-based alternate-bucket formula
//   - Victim cache handles the edge case where displacement is exhausted
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

    CF_Status add(const uint8_t* data, size_t len);
    CF_Status contain(const uint8_t* data, size_t len) const;
    CF_Status remove(const uint8_t* data, size_t len);
    void clear();

    size_t   size()        const { return _num_items; }
    size_t   capacity()    const { return CF_NUM_BUCKETS * CF_BUCKET_SIZE; }
    uint8_t  loadPercent() const;

private:
    uint8_t  _table[CF_NUM_BUCKETS][CF_BUCKET_SIZE];
    size_t   _num_items;

    struct { size_t index; uint32_t tag; bool used; } _victim;

    // ── Internal helpers ─────────────────────────────────────────────────────
    static uint32_t hash_bytes(const uint8_t* data, size_t len);
    static uint32_t mask_bucket_idx(uint32_t hv);
    static uint32_t derive_fingerprint(uint32_t hv);
    static size_t   alt_index(size_t index, uint32_t tag);

    void     _split_hash(const uint8_t* data, size_t len,
                         size_t* index, uint32_t* tag) const;
    bool     _slot_insert(size_t i, uint32_t tag,
                           bool kickout, uint32_t& oldtag);
    bool     _find_fingerprint(size_t i1, size_t i2, uint32_t tag) const;
    bool     _erase_fingerprint(size_t i, uint32_t tag);
    CF_Status _try_insert(size_t i, uint32_t tag);
};
