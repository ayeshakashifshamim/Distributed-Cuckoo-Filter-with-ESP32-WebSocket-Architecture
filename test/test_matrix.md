# Test Matrix — Distributed Cuckoo Filter

## Native Tests (no hardware required)

| Suite | Coverage |
|---|---|
| `test_cuckoo_filter` | Single-node filter correctness: insert, lookup, delete, clear, capacity, load % |
| `test_benchmark` | Single-node FPR (must be < 6% at 8-bit tags) and max occupancy (must be > 80%) |
| `test_protocol` | Wire-format compliance: packed struct sizes, field offsets, ESP-NOW payload ceiling, round-trip parse |
| `test_e2e` | Full in-process cluster: routing, insert/lookup/delete, try_only semantics, dedup replay, bucket-batch migration, route-lock RETRY, hard-reset, distributed FPR, UNAVAILABLE propagation |

## Hardware Tests (ESP32 boards)

| # | Test | Procedure | Expected |
|---|---|---|---|
| 1 | ESP-NOW liveness | Boot master + slaves; run `s` | All configured slaves show ALIVE |
| 2 | Insert via CLI | `i hello` on master | Routed to owning slave; ACK OK |
| 3 | Lookup via CLI | `l hello` after insert | Slave responds FOUND |
| 4 | Delete via CLI | `d hello` then `l hello` | Second query returns NOT FOUND |
| 5 | Benchmark | `b 200` | Reports total and avg insert latency |
| 6 | FPR run | `f 500` | Reported FPR at or below ~5% |
| 7 | Manual rebalance | `r` while one slave is overloaded | Logs `REBALANCE done: N buckets src→dst` |
| 8 | Auto-rebalance on overload | Push one slave past `REBALANCE_THRESHOLD` via inserts | Rebalance fires from heartbeat path (cooldown-gated) |
| 9 | Auto-rebalance on join | Power on a previously absent slave | Rebalance fires within cooldown window to distribute load to new node |
| 10 | Hard reset on reconnect | Power-cycle a slave mid-workload | Master resets it on reconnect; logs `Slave 0xXX reset OK` |

## Design References

| Topic | Document |
|---|---|
| M2 design and refactor rationale | `../docs/implementation/milestone2-distributed-cuckoo-refactor.md` |
| M3 ESP-NOW migration notes | `../docs/implementation/milestone3.md` |
| Project brief and milestone specs | `../ideas/cuckoo-filter.md` |

## Running the Tests

```bash
# All native suites on desktop:
pio test -e native

# Firmware builds:
pio run -e master
pio run -e slave
pio run -e slave2    # second slave: add -D MY_SLAVE_ID=0x02 to build flags
```
