// cuckoo_filter.cpp — CuckooFilter implementation: MurmurHash2, bucket ops, kick-out insertion
#include "cuckoo_filter.h"
#include <string.h>   // memset
#include <stdlib.h>   // rand() — seeded with esp_random() in main.cpp

// ─────────────────────────────────────────────────────────────────────────────
// MurmurHash2 (32-bit) — same algorithm family used in the reference repo.
// Works on byte arrays, no OS dependencies, runs fine on ESP32.
// ─────────────────────────────────────────────────────────────────────────────
uint32_t CuckooFilter::_murmur(const uint8_t* data, size_t len) {
    const uint32_t seed = 0xbc9f1d34;
    const uint32_t m    = 0x5bd1e995;
    uint32_t h = seed ^ (uint32_t)len;

    while (len >= 4) {
        uint32_t k;
        memcpy(&k, data, 4);   // safe unaligned read
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

// ─── Index / tag helpers (mirrored from the reference) ───────────────────────

uint32_t CuckooFilter::_indexHash(uint32_t hv) {
    return hv & (CF_NUM_BUCKETS - 1);   // fast modulo for power-of-2
}

uint32_t CuckooFilter::_tagHash(uint32_t hv) {
    uint32_t tag = hv & ((1u << CF_BITS_PER_TAG) - 1);
    tag += (tag == 0);   // tag must never be 0 (0 means empty slot)
    return tag;
}

// Alt-index formula straight from the reference (0x5bd1e995 = MurmurHash2 constant)
size_t CuckooFilter::_altIndex(size_t index, uint32_t tag) {
    return _indexHash((uint32_t)(index ^ (tag * 0x5bd1e995)));
}

void CuckooFilter::_genIndexTag(const uint8_t* data, size_t len,
                                size_t* index, uint32_t* tag) const {
    uint32_t h = _murmur(data, len);
    *index = _indexHash(h >> 8);   // use upper bits for index
    *tag   = _tagHash(h);          // use lower bits for tag
}

// ─── Bucket operations ────────────────────────────────────────────────────────

bool CuckooFilter::_insertTagToBucket(size_t i, uint32_t tag,
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

bool CuckooFilter::_findTagInBuckets(size_t i1, size_t i2, uint32_t tag) const {
    uint8_t t = (uint8_t)tag;
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i1][j] == t || _table[i2][j] == t)
            return true;
    }
    return false;
}

bool CuckooFilter::_deleteTagFromBucket(size_t i, uint32_t tag) {
    uint8_t t = (uint8_t)tag;
    for (size_t j = 0; j < CF_BUCKET_SIZE; j++) {
        if (_table[i][j] == t) {
            _table[i][j] = 0;
            return true;
        }
    }
    return false;
}

// ─── Core add logic (mirrored from reference AddImpl) ────────────────────────

CF_Status CuckooFilter::_addImpl(size_t i, uint32_t tag) {
    size_t   curindex = i;
    uint32_t curtag   = tag;
    uint32_t oldtag   = 0;

    for (uint32_t count = 0; count < CF_MAX_KICKS; count++) {
        bool kickout = (count > 0);
        oldtag = 0;
        if (_insertTagToBucket(curindex, curtag, kickout, oldtag)) {
            _num_items++;
            return CF_Ok;
        }
        if (kickout) curtag = oldtag;
        curindex = _altIndex(curindex, curtag);
    }
    // Store the displaced tag in victim cache (reference behaviour)
    _victim.index = curindex;
    _victim.tag   = curtag;
    _victim.used  = true;
    return CF_Ok;   // reference returns Ok here (victim holds the item)
}

// ─── Public API ──────────────────────────────────────────────────────────────

CuckooFilter::CuckooFilter() : _num_items(0) {
    memset(_table, 0, sizeof(_table));
    _victim = {0, 0, false};
}

CF_Status CuckooFilter::add(const uint8_t* data, size_t len) {
    if (_victim.used) return CF_NotEnoughSpace;
    size_t   i;
    uint32_t tag;
    _genIndexTag(data, len, &i, &tag);
    return _addImpl(i, tag);
}

CF_Status CuckooFilter::contain(const uint8_t* data, size_t len) const {
    size_t   i1, i2;
    uint32_t tag;
    _genIndexTag(data, len, &i1, &tag);
    i2 = _altIndex(i1, tag);

    bool found = (_victim.used &&
                  tag == _victim.tag &&
                  (i1 == _victim.index || i2 == _victim.index));

    if (found || _findTagInBuckets(i1, i2, tag))
        return CF_Ok;
    return CF_NotFound;
}

CF_Status CuckooFilter::remove(const uint8_t* data, size_t len) {
    size_t   i1, i2;
    uint32_t tag;
    _genIndexTag(data, len, &i1, &tag);
    i2 = _altIndex(i1, tag);

    if (_deleteTagFromBucket(i1, tag) || _deleteTagFromBucket(i2, tag)) {
        _num_items--;
        // Re-insert victim if one exists (reference behaviour)
        if (_victim.used) {
            _victim.used = false;
            _addImpl(_victim.index, _victim.tag);
        }
        return CF_Ok;
    }
    // Check victim cache
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
