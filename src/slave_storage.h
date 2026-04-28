#pragma once
// slave_storage.h — Distributed cuckoo filter storage for one slave node.
//
// A slave hosts an arbitrary, time-varying subset of GLOBAL_BUCKET_COUNT
// logical buckets. To keep RAM bounded and allocation-free after boot:
//   local_index[global_bucket] → physical slot index in bucket_pool, or -1
//   bucket_pool[slot].tags[4]  → the CF_BUCKET_SIZE fingerprints for the bucket
//   free_stack                 → physical slots currently available
//
// The single heap allocation happens once in init().
#include <stdint.h>
#include <vector>

struct BucketData {
    uint8_t tags[4];   // CF_BUCKET_SIZE fingerprints
};

class SlaveStorage {
public:
    void init();                                    // (re)initialise to empty

    int16_t     index_of(uint16_t global_bucket) const;
    BucketData& bucket_at(int16_t slot)          { return _pool[(size_t)slot]; }

    int16_t     acquire_slot(uint16_t global_bucket);
    bool        release_if_empty(uint16_t global_bucket);
    void        force_clear(uint16_t global_bucket);

    uint16_t    free_slots()  const { return (uint16_t)_free.size(); }
    uint32_t    item_count()  const { return _items; }

    void        on_tag_added()   { _items++; }
    void        on_tag_removed() { if (_items) _items--; }

private:
    std::vector<int16_t>    _index;   // size = GLOBAL_BUCKET_COUNT
    std::vector<BucketData> _pool;    // size = LOCAL_CAPACITY
    std::vector<uint16_t>   _free;    // LIFO stack of free slots
    uint32_t                _items = 0;
};
