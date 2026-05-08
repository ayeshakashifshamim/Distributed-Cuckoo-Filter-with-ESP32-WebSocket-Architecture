# Distributed Cuckoo Filter on ESP32

A probabilistic set-membership data structure distributed across ESP32 nodes, communicating over ESP-NOW. Built for resource-constrained IoT environments as a PDC course project at IBA Karachi.

**Group 08** — Ayesha Kashif (29155) · Syed Hamza Ahsan (28903) · Kabeer Jafri (29027) · Qazi Muhammad Waiz (28982)  
**Course**: PDC, Semester 6, Spring 2026

---

## What It Does

A Cuckoo Filter is split across multiple ESP32 nodes. A single master node routes INSERT, DELETE, and LOOKUP operations to the slave that owns each bucket partition. Slaves maintain local filter state and reply with ACKs. Everything runs over ESP-NOW — no TCP, no WebSocket, no IP stack.

### Why ESP-NOW (not WebSocket)

Milestones 1 and 2 used WebSocket over TCP. We switched to ESP-NOW for Milestone 3 because:

- **Memory**: maintaining simultaneous WebSocket connections required per-client TCP state and heap allocations that pushed the master close to its memory ceiling with the filter already resident
- **Overhead**: WebSocket frames add 8–14 bytes per message on top of TCP/IP — for our 6–20 byte payloads that framing cost was a large fraction of every packet

ESP-NOW is connectionless, operates at the 802.11 MAC layer, and has zero framing overhead. Peer registration is a one-time MAC write at boot.

---

## Filter Parameters

| Parameter | Value |
|---|---|
| Buckets per node | 256 |
| Slots per bucket | 4 |
| Fingerprint width | 8 bits |
| RAM per node | ~1 KB |
| False positive rate | ~3% |
| Max kickout attempts | 500 |
| Designed slave count | 1, 2, 4, or 8 (must be power of 2) |
| Hardware tested with | 1 master + 1 slave |

---

## Repository Layout

```
src/
  config.h                ← NODE COUNT + MAC ADDRESSES — edit before flashing
  master.cpp              ← master firmware: routing, rebalancing, CLI
  slave.cpp               ← slave firmware: filter ops, heartbeat, LEDs
  cuckoo_filter/
    cuckoo_filter.h/.cpp  ← single-node filter (MurmurHash2, kickout, victim cache)
  dispatcher/
    dispatcher.h/.cpp     ← transport-agnostic packet router + dedup cache
  protocol/
    messages.h            ← packed binary wire structs (all < 250 B)
  transport/
    transport.h/.cpp      ← ESP-NOW thin wrapper (skipped on native/desktop builds)
  slave_storage.h/.cpp    ← per-slave bucket pool (heap-reserved once at boot)

test/
  test_cuckoo_filter/     ← filter unit tests
  test_e2e/               ← full cluster simulation (in-process, no hardware needed)
  test_protocol/          ← wire-format and struct layout tests
  test_benchmark/         ← FPR and occupancy benchmarks
  TEST_MATRIX.md          ← full test case table

sim/
  master/                 ← Wokwi config for master node (VS Code extension)
  slave/                  ← Wokwi config for slave node

docs/
  final_report.pdf
  individual_contribution.pdf
  presentation_video.mp4
```

---

## Setup From Scratch

### 1. Install prerequisites

- [VS Code](https://code.visualstudio.com/)
- [PlatformIO IDE extension](https://marketplace.visualstudio.com/items?itemName=platformio.platformio-ide)
- [Wokwi VS Code extension](https://marketplace.visualstudio.com/items?itemName=wokwi.wokwi-vscode) (simulation only)

### 2. Clone and switch to the final branch

```bash
git clone https://github.com/ayeshakashifshamim/Distributed-Cuckoo-Filter-with-ESP32-WebSocket-Architecture.git
cd Distributed-Cuckoo-Filter-with-ESP32-WebSocket-Architecture
git checkout final
```

Open the folder in VS Code. PlatformIO will automatically install the ESP32 toolchain on first build.

### 3. Read each board's MAC address

Flash this sketch to each ESP32 and open Serial Monitor at 115200 baud:

```cpp
#include <WiFi.h>
void setup() {
    Serial.begin(115200);
    WiFi.mode(WIFI_STA);
    Serial.println(WiFi.macAddress());
}
void loop() {}
```

Copy the printed MAC for each board — you will need them in the next step.

> **Linux users**: if the port is not accessible, run:
> ```bash
> sudo chmod a+rw /dev/ttyUSB0   # repeat for ttyUSB1, ttyUSB2 as needed
> ```
> If a port is locked by a ghost process:
> ```bash
> fuser -k /dev/ttyUSB0
> ```

### 4. Edit `src/config.h`

This is the only file you need to change before flashing.

```cpp
// Number of slave nodes — must be 1, 2, 4, or 8
#define NUM_SLAVES  1

// Paste the MACs you read in step 3
static const uint8_t MASTER_MAC[6] = {0x8C, 0x94, 0xDF, 0x94, 0x24, 0x48};

static const uint8_t SLAVE_MACS[NUM_SLAVES][6] = {
    {0x30, 0x76, 0xF5, 0xBA, 0x37, 0x2C},  // Slave 0x01
};
```

> **Important**: the `SLAVE_MACS` array must have exactly `NUM_SLAVES` rows. If you change `NUM_SLAVES` from 1 to 2, add a second MAC row. Mismatch causes a compiler error.

> **Important**: wrong or placeholder MACs cause silent one-way ESP-NOW failures — inserts will return `UNAVAILABLE`. Always use real hardware MACs.

### 5. Flash the boards

**On Linux** (specify port explicitly to avoid detection issues):

```bash
# Master
pio run -e master --target upload --upload-port /dev/ttyUSB0

# Slave 0x01
pio run -e slave --target upload --upload-port /dev/ttyUSB1
```

**On macOS**:

```bash
pio run -e master --target upload
pio run -e slave --target upload
```

> If uploads freeze midway, the baud rate is already set to 115200 in `platformio.ini` for stability. Do not increase it.

### 6. Open Serial Monitor on the master

In VS Code PlatformIO sidebar → Serial Monitor, or:

```bash
pio device monitor -e master --port /dev/ttyUSB0 --baud 115200
```

Wait ~6 seconds. You should see:

```
[MASTER] Slave 0x01 ONLINE
```

---

## Running Tests (No Hardware Needed)

All four test suites compile and run natively on your laptop:

```bash
pio test -e native
```

Expected output: **28/28 PASSED**

---

## Serial CLI — Master Node

Open Serial Monitor at **115200 baud** with **Newline** line ending.

| Command | Action |
|---|---|
| `i <key>` | Insert key into the distributed filter |
| `l <key>` | Lookup key (probabilistic) |
| `d <key>` | Delete key |
| `s` | Show slave status, load percentages, bucket counts |
| `b <n>` | Benchmark: insert n items, print latency + distribution |
| `f <n>` | FPR test: insert n items, measure false positive rate |

Example session:

```
i hello        → [MASTER] INSERT hello → Slave 0x01 OK
l hello        → [MASTER] LOOKUP hello → FOUND
d hello        → [MASTER] DELETE hello → OK
l hello        → [MASTER] LOOKUP hello → NOT FOUND
f 200          → FPR: 2.5% (5/200)
```

---

## Wokwi Simulation (No Physical Hardware)

The `sim/` folder contains Wokwi configs for both master and slave nodes.

1. Install the [Wokwi VS Code extension](https://marketplace.visualstudio.com/items?itemName=wokwi.wokwi-vscode)
2. Build the firmware first: `pio run -e master` and `pio run -e slave`
3. Open `sim/master/diagram.json` → press **F1** → `Wokwi: Start Simulator`
4. Repeat for `sim/slave/diagram.json` in a second VS Code window

---

## Adding More Slave Nodes

1. Set `NUM_SLAVES` to 2, 4, or 8 in `src/config.h`
2. Add one MAC row per new slave to `SLAVE_MACS`
3. Flash each new slave board with `pio run -e slave --target upload`
4. For slave 0x02 onward, add `-D MY_SLAVE_ID=0x02` to that board's build flags in `platformio.ini`

The master will automatically begin routing to all registered slaves and will trigger rebalancing when any node's load exceeds 85%.

---

## Milestones

### Milestone 1 — Single-Node Filter + Communication ✅
Single-node Cuckoo Filter with MurmurHash2, kickout loop, and victim cache. WebSocket transport (later replaced). Master/slave coordination with INSERT / DELETE / LOOKUP / SYNC / ACK / PING / HEARTBEAT. Native test harness with FPR measurement.

### Milestone 2 — Distributed Operations ✅
Hash-based bucket partitioning across slaves. Distributed INSERT with kickout chains. Cooperative DELETE. Three-tier LOOKUP (primary → broadcast → local). Bucket migration via batched READ → WRITE → CLEAR protocol. Auto-rebalance on overload and slave join. Hard-reset on slave reconnect.

### Milestone 3 — ESP-NOW Migration + Hardware Testing ✅
Full rewrite of the transport layer from WebSocket to ESP-NOW. Packed binary protocol replacing JSON. Hardware tested with 1 master + 1 slave on physical ESP32 boards. Multi-slave behaviour verified in Wokwi simulation (laptop limited to 2 USB ports).

---

## Design Principles

- **Deterministic**: single-node filter verified before any networking is added
- **Idempotent**: all operations tolerate retries (ESP-NOW gives no delivery guarantee — the dispatcher's dedup cache handles duplicates on the slave side)
- **Memory-bounded**: heap allocated once at boot via `SlaveStorage`; no dynamic allocation during operation
- **Layered**: `transport.cpp` is the only file that touches ESP-NOW; everything above it is testable on desktop

---

## License

Academic project — Institute of Business Administration, Karachi.
