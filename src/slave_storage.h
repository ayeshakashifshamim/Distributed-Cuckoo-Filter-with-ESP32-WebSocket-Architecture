#pragma once
// slave_storage.h — Per-slave bucket backing store.
//
// Each slave holds a dynamic, time-varying subset of the GLOBAL_BUCKET_COUNT
// logical bucket namespace. RAM is kept bounded via indirection:
//
//   bucket_to_slot[global_bucket] → physical slot index in bucket_pool, or -1
//   bucket_pool[slot].tags[4]     → the 4 fingerprints for the bucket
//   free_stack                    → LIFO stack of currently unused physical slots
//
// init() performs the single heap allocation; after that the structure is
// allocation-free.
#include <stdint.h>
#include <vector>

struct BucketData {
    uint8_t tags[4];
};

class SlaveStorage {
public:
    void init();

    int16_t     index_of(uint16_t global_bucket) const;
    BucketData& bucket_at(int16_t slot)          { return _pool[(size_t)slot]; }

    int16_t     acquire_slot(uint16_t global_bucket);
    bool        release_if_empty(uint16_t global_bucket);
    void        force_clear(uint16_t global_bucket);

    uint16_t    free_slots()  const { return (uint16_t)_free_stack.size(); }
    uint32_t    item_count()  const { return _items; }

    void        on_tag_added()   { _items++; }
    void        on_tag_removed() { if (_items) _items--; }

private:
    std::vector<int16_t>    _bucket_to_slot;
    std::vector<BucketData> _pool;
    std::vector<uint16_t>   _free_stack;
    uint32_t                _items = 0;
};
