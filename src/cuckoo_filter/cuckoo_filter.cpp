// cuckoo_filter.cpp — CuckooFilter implementation.
//
// Probabilistic set membership structure.  Each slot stores an 8-bit fingerprint
// derived from the item's MurmurHash.  Buckets are 4 slots wide.  Insertion uses
// cuckoo displacement (kickout chain) to handle collisions; a victim cache holds
// one overflow item when the chain is exhausted.
//
// Properties:
//   - False-positive rate: ~3% (8-bit fingerprints)
//   - No external dependencies — runs on ESP32 with minimal RAM.
//   - Single-byte fingerprints, victim cache, and up to 500 displacement attempts
//     per insert.

#include "cuckoo_filter.h"
#include <string.h>   // memset
#include <stdlib.h>   // rand()

// ════════════════════════════════════════════════════════════════════════════
// MurmurHash2 (32-bit) — deterministic, portable, no OS calls
// ════════════════════════════════════════════════════════════════════════════

uint32_t CuckooFilter::hash_bytes(const uint8_t* data, size_t len) {
    const uint32_t seed = 0xbc9f1d34;
    const uint32_t m    = 0x5bd1e995;
    uint32_t h = seed ^ (uint32_t)len;

    while (len >= 4) {
        uint32_t k;
        memcpy(&k, data, 4);
        k *= m;
        k ^= k >> 24;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len  -= 4;
    }
    switch (len) {
        case 3: h ^= (uint32_t)data[2] << 16; /* fall through */
        case 2: h ^= (uint32_t)data[1] <<  8; /* fall through */
        case 1: h ^= (uint32_t)data[0];
                h *= m;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

// ════════════════════════════════════════════════════════════════════════════
// Index and fingerprint derivation
// ════════════════════════════════════════════════════════════════════════════

// Fast modulo: GLOBAL_BUCKET_COUNT is a power of 2.
uint32_t CuckooFilter::mask_bucket_idx(uint32_t hv) {
    return hv & (CF_NUM_BUCKETS - 1);
}

// Low 8 bits of the hash, remapped so that 0 → 1.  Slot 0 in each bucket is
// the "empty" sentinel, so fingerprint 0 is illegal.
uint32_t CuckooFilter::derive_fingerprint(uint32_t hv) {
    uint32_t tag = hv & ((1u << CF_BITS_PER_TAG) - 1);
    tag += (tag == 0);
    return tag;
}

// Standard cuckoo alternate-bucket formula (XOR with tag-dependent value).
size_t CuckooFilter::alt_index(size_t index, uint32_t tag) {
    return mask_bucket_idx((uint32_t)(index ^ (tag * 0x5bd1e995)));
}

// Parse an item into its primary bucket index and fingerprint.
void CuckooFilter::_split_hash(const uint8_t* data, size_t len,
                                size_t* index, uint32_t* tag) const {
    uint32_t h = hash_bytes(data, len);
    *index = mask_bucket_idx(h >> 8);
    *tag   = derive_fingerprint(h);
}

// ════════════════════════════════════════════════════════════════════════════
// Bucket primitives
// ════════════════════════════════════════════════════════════════════════════

// Try to place `tag` in bucket `i`.  Returns true on success.  If the bucket
// is full and kickout=true, a random slot is displaced and its old value is
// written to `oldtag`.
bool CuckooFilter::_slot_insert(size_t i, uint32_t tag,
                                 bool kickout, uint32_t& oldtag) {
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i][j] == 0) {
            _table[i][j] = (uint8_t)tag;
            return true;
        }
    }
    if (kickout) {
        size_t r = (size_t)rand() % CF_BUCKET_SIZE;
        oldtag = _table[i][r];
        _table[i][r] = (uint8_t)tag;
    }
    return false;
}

// Scan both primary and alternate buckets for a matching fingerprint.
bool CuckooFilter::_find_fingerprint(size_t i1, size_t i2, uint32_t tag) const {
    uint8_t t = (uint8_t)tag;
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i1][j] == t || _table[i2][j] == t)
            return true;
    }
    return false;
}

// Remove the first matching fingerprint from a bucket.  Returns true on success.
bool CuckooFilter::_erase_fingerprint(size_t i, uint32_t tag) {
    uint8_t t = (uint8_t)tag;
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i][j] == t) {
            _table[i][j] = 0;
            return true;
        }
    }
    return false;
}

// ════════════════════════════════════════════════════════════════════════════
// Core insert with displacement
// ════════════════════════════════════════════════════════════════════════════

CF_Status CuckooFilter::_try_insert(size_t i, uint32_t tag) {
    size_t   curindex = i;
    uint32_t curtag   = tag;
    uint32_t oldtag   = 0;

    for (uint32_t attempt = 0; attempt < CF_MAX_KICKS; attempt++) {
        bool kicked = (attempt > 0);
        oldtag = 0;
        if (_slot_insert(curindex, curtag, kicked, oldtag)) {
            _num_items++;
            return CF_Ok;
        }
        if (kicked) curtag = oldtag;
        curindex = alt_index(curindex, curtag);
    }
    // Exhausted displacement budget — store displaced item in victim cache.
    _victim.index = curindex;
    _victim.tag   = curtag;
    _victim.used  = true;
    return CF_Ok;
}

// ════════════════════════════════════════════════════════════════════════════
// Public API
// ════════════════════════════════════════════════════════════════════════════

CuckooFilter::CuckooFilter() : _num_items(0) {
    memset(_table, 0, sizeof(_table));
    _victim = {0, 0, false};
}

CF_Status CuckooFilter::add(const uint8_t* data, size_t len) {
    if (_victim.used) return CF_NotEnoughSpace;
    size_t   i;
    uint32_t tag;
    _split_hash(data, len, &i, &tag);
    return _try_insert(i, tag);
}

CF_Status CuckooFilter::contain(const uint8_t* data, size_t len) const {
    size_t   i1, i2;
    uint32_t tag;
    _split_hash(data, len, &i1, &tag);
    i2 = alt_index(i1, tag);

    bool found = (_victim.used &&
                  tag == _victim.tag &&
                  (i1 == _victim.index || i2 == _victim.index));

    if (found || _find_fingerprint(i1, i2, tag))
        return CF_Ok;
    return CF_NotFound;
}

CF_Status CuckooFilter::remove(const uint8_t* data, size_t len) {
    size_t   i1, i2;
    uint32_t tag;
    _split_hash(data, len, &i1, &tag);
    i2 = alt_index(i1, tag);

    if (_erase_fingerprint(i1, tag) || _erase_fingerprint(i2, tag)) {
        _num_items--;
        if (_victim.used) {
            _victim.used = false;
            _try_insert(_victim.index, _victim.tag);
        }
        return CF_Ok;
    }
    if (_victim.used && tag == _victim.tag &&
        (i1 == _victim.index || i2 == _victim.index)) {
        _victim.used = false;
        return CF_Ok;
    }
    return CF_NotFound;
}

void CuckooFilter::clear() {
    memset(_table, 0, sizeof(_table));
    _num_items  = 0;
    _victim.used = false;
}

uint8_t CuckooFilter::loadPercent() const {
    return (uint8_t)((_num_items * 100) / capacity());
}
