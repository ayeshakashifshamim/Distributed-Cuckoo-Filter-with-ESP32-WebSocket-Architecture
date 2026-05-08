// slave_storage.cpp — per-slave backing store for distributed buckets.
#include "slave_storage.h"
#include "config.h"
#include <string.h>

namespace {
bool is_empty(const BucketData& b) {
    for (uint8_t i = 0; i < 4; i++) if (b.tags[i] != 0) return false;
    return true;
}
}  // namespace

void SlaveStorage::init() {
    _bucket_to_slot.assign(GLOBAL_BUCKET_COUNT, -1);
    _pool.assign(LOCAL_CAPACITY, BucketData{});
    _free_stack.clear();
    _free_stack.reserve(LOCAL_CAPACITY);
    for (uint16_t i = LOCAL_CAPACITY; i > 0; i--) _free_stack.push_back((uint16_t)(i - 1));
    _items = 0;
}

int16_t SlaveStorage::index_of(uint16_t global_bucket) const {
    if (global_bucket >= GLOBAL_BUCKET_COUNT) return -1;
    return _bucket_to_slot[global_bucket];
}

int16_t SlaveStorage::acquire_slot(uint16_t global_bucket) {
    if (global_bucket >= GLOBAL_BUCKET_COUNT) return -1;
    if (_bucket_to_slot[global_bucket] >= 0) return _bucket_to_slot[global_bucket];
    if (_free_stack.empty()) return -1;

    uint16_t slot = _free_stack.back();
    _free_stack.pop_back();
    memset(_pool[slot].tags, 0, sizeof(_pool[slot].tags));
    _bucket_to_slot[global_bucket] = (int16_t)slot;
    return (int16_t)slot;
}

bool SlaveStorage::release_if_empty(uint16_t global_bucket) {
    int16_t slot = index_of(global_bucket);
    if (slot < 0) return true;
    if (!is_empty(_pool[slot])) return false;
    _bucket_to_slot[global_bucket] = -1;
    _free_stack.push_back((uint16_t)slot);
    return true;
}

void SlaveStorage::force_clear(uint16_t global_bucket) {
    int16_t slot = index_of(global_bucket);
    if (slot < 0) return;
    BucketData& b = _pool[slot];
    for (uint8_t i = 0; i < 4; i++) if (b.tags[i] != 0) { if (_items) _items--; b.tags[i] = 0; }
    _bucket_to_slot[global_bucket] = -1;
    _free_stack.push_back((uint16_t)slot);
}
