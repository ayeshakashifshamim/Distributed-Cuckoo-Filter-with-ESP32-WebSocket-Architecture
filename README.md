# Distributed Cuckoo Filter with ESP32

A distributed implementation of a Cuckoo Filter across ESP32 microcontrollers using ESP-NOW and/or WebSocket communication for scalable approximate membership queries in IoT environments.

## Project Overview

This project implements a probabilistic data structure (Cuckoo Filter) distributed across multiple ESP32 devices, enabling efficient set membership testing with minimal memory footprint. The system uses a master-slave architecture where filter operations (insertions, deletions, lookups) are coordinated across resource-constrained devices.

### Key Features

- **Cuckoo Filter Implementation**: Space-efficient probabilistic data structure supporting INSERT, DELETE, and LOOKUP operations
- **Distributed Architecture**: Master-slave coordination for distributed filter operations
- **ESP32-Based**: Runs on ESP32 microcontrollers (simulated via Wokwi)
- **Dual Communication**: Support for ESP-NOW and/or WebSocket protocols
- **Hash-Based Partitioning**: Scalable distribution of filter data across multiple nodes
- **Transport-Agnostic Protocol**: Binary message format for reliable distributed operations

## Technical Specifications

### Hardware
- **Platform**: ESP32 (ESP32-D0WD/D0WDQ6/D2WD/S0WD)
- **CPU**: Xtensa LX6 dual-core (up to 600 MIPS)
- **SRAM**: 520 KB total
- **Simulator**: Wokwi ESP32 simulator

### Filter Parameters (Milestone 1)
- **Buckets**: 4096
- **Entries per bucket**: 4
- **Fingerprint size**: 16 bits
- **Memory footprint**: ~32 KB
- **Expected FPR**: ~0.012%

## Project Structure

```
Project/
├── ideas/                          # Planning and design documents
│   ├── cuckoo-filter.md           # Project requirements and milestones
│   ├── ESP32.PDF                  # ESP32 datasheet (official PDF)
│   ├── ESP32.txt                  # ESP32 datasheet (text extract)
│   └── plans/                     # Implementation plans
│       ├── milestone1-esp32-cuckoo-filter-explainer.md
│       └── milestone1-wokwi-cuckoo-filter-protocol-plan.md
├── src/                           # Source code (ready for implementation)
├── tests/                         # Test harness (ready for implementation)
├── .gitignore                     # Git ignore patterns
└── README.md                      # This file
```

## Milestones

### Milestone 1: Single-Node Filter & Communication Infrastructure ✅ (Complete)
- Cuckoo Filter on ESP32 with MurmurHash2, kickout mechanism, victim cache
- ESP-NOW transport with 1-byte-packed binary protocol
- Master/slave coordination via INSERT/DELETE/LOOKUP/SYNC/ACK/PING/HEARTBEAT
- Native test harness with FPR measurement

**Focus**: Distributed data structure design, message-passing architecture

### Milestone 2: Distributed Filter Operations ✅ (Complete)
- Hash-based partitioning over a flat global bucket address space (`g_route_primary[b] → slave_index`)
- Distributed INSERT with dual soft-try then remote kickout chain (`MSG_CHAIN_INSERT.try_only`)
- Cooperative distributed DELETE; LOOKUP aggregates b1/b2 with `STATUS_UNAVAILABLE` propagation
- Bucket-level migration via batched 3-round-trip protocol (`READ → WRITE → CLEAR`) with per-bucket locks
- Auto-rebalance from heartbeats on overload (`load_pct > REBALANCE_THRESHOLD`) and on imbalance (covers new-node-join), cooldown-gated
- Hard-reset on slave reconnect to wipe stale RAM before re-routing
- Slave-side `SlaveStorage` (heap-reserved once at boot) decouples logical bucket IDs from physical RAM slots

**Focus**: Partitioning strategies, distributed consistency.
Design rationale: see `milestone2-distributed-cuckoo-refactor.md`.

## Protocol Operations

### Core Operations
1. **INSERT**: Add an item to the filter with kickout mechanism
2. **DELETE**: Remove an item from the filter
3. **LOOKUP**: Check membership (probabilistic)
4. **SYNC**: Synchronize filter state across nodes (digest-only in M1)

### Message Format
Transport-agnostic binary framing with optional JSON mapping for debugging.

## Development Setup

### Prerequisites
- Arduino IDE or PlatformIO
- Wokwi ESP32 simulator account
- ESP32 board support package

### Building & Testing
Instructions will be added as implementation progresses.

## Course Information

**Course**: PDC (Parallel and Distributed Computing)  
**Institution**: Institute of Business Administration  
**Semester**: 6 (Spring 2026)  
**Project**: 19 - Distributed Cuckoo Filter

## Design Principles

- **Deterministic**: Testable single-node behavior before networking
- **Idempotent**: Operations survive retries (common in ESP-NOW)
- **Resource-Conscious**: Conservative memory usage (32 KB filter size)
- **Transport-Agnostic**: Protocol decoupled from physical transport layer

## References

- Cuckoo Filter paper: Fan et al., "Cuckoo Filter: Practically Better Than Bloom"
- ESP32 Technical Reference Manual
- ESP-NOW Protocol Documentation

## License

Academic project - Institute of Business Administration

## Contributors

Team members working on this PDC course project.

---

**Status**: Milestone 2 complete — distributed partitioning, remote kickout chains, cooperative deletion, batched bucket migration, auto-rebalance on overload+imbalance, and hard-reset rejoin are implemented and tested. See `milestone2-distributed-cuckoo-refactor.md` for the design.
