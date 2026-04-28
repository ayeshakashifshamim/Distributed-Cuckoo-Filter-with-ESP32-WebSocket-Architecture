// config.h — Central configuration for all nodes (MACs, IDs, tuning).
// Distributed cuckoo filter operates on a flat GLOBAL_BUCKET_COUNT address
// space. The master-side routing table maps each global bucket to a slave,
// and each slave stores only the buckets it owns (LOCAL_CAPACITY slots).
#pragma once
#include <stdint.h>

// ── Node IDs ──────────────────────────────────────────────────────────────────
#define MASTER_ID   0x0000
#define NUM_SLAVES  1      // must be a power of 2 (1, 2, 4, or 8)

// ── MAC addresses ─────────────────────────────────────────────────────────────
// Replace with real MACs before flashing. Print WiFi.macAddress() on each board.
//   Master  : ESP32-WROVER      (8C:94:DF:94:24:48)
//   Slave 1 : ESP-WROOM-32      (30:76:F5:BA:37:2C)
//   Slave 2 : ESP32             (8C:94:DF:94:24:48)
static const uint8_t MASTER_MAC[6] = {0x8C, 0x94, 0xDF, 0x94, 0x24, 0x48};

static const uint8_t SLAVE_MACS[NUM_SLAVES][6] = {
    {0x30, 0x76, 0xF5, 0xBA, 0x37, 0x2C},  // Slave 0x01
};

// ── Cluster capacity ──────────────────────────────────────────────────────────
// Each slave hosts BASE_BUCKETS_PER_SLAVE buckets. Total global capacity scales
// linearly with NUM_SLAVES. Both factors must be powers of 2 so the product is
// also a power of 2 (required by the alt-bucket XOR mask).
constexpr uint16_t BASE_BUCKETS_PER_SLAVE = 256;
constexpr uint16_t GLOBAL_BUCKET_COUNT    = BASE_BUCKETS_PER_SLAVE * NUM_SLAVES;
constexpr uint16_t LOCAL_CAPACITY         = BASE_BUCKETS_PER_SLAVE;

static_assert((GLOBAL_BUCKET_COUNT & (GLOBAL_BUCKET_COUNT - 1)) == 0,
              "GLOBAL_BUCKET_COUNT must be a power of 2: NUM_SLAVES must be 1, 2, 4, or 8");

// ── Distributed tuning ────────────────────────────────────────────────────────
constexpr uint8_t  REBALANCE_THRESHOLD       = 85;    // load_pct that triggers overload rebalance
constexpr uint8_t  IMBALANCE_THRESHOLD       = 20;    // max-min load gap that triggers rebalance
constexpr uint32_t REBALANCE_COOLDOWN_MS     = 15000; // min ms between auto-rebalance attempts
constexpr uint8_t  MAX_BUCKET_BATCH          = 20;    // buckets per migrate call
constexpr uint8_t  MAX_CHAIN_HOPS            = 5;     // distributed kickout chain hard stop

// ── Heartbeat timing ──────────────────────────────────────────────────────────
#define HEARTBEAT_INTERVAL_MS  2000
#define HEARTBEAT_TIMEOUT_MS   6000

// ── LED pins (shared by all slave boards) ─────────────────────────────────────
#define PIN_GREEN  21
#define PIN_BLUE   22
#define PIN_RED    23
