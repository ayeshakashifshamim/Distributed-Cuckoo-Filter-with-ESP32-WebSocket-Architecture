# Test Matrix — Distributed Cuckoo Filter (Milestone 2)

## Native tests (no hardware)

| Suite | Coverage |
|-------|----------|
| `test_cuckoo_filter` | Single-node filter correctness (insert / lookup / delete, clear, capacity, load %) |
| `test_benchmark`     | Single-node FPR (< 6 % at 8-bit tags) and max occupancy (> 80 %) |
| `test_protocol`      | M2 wire-format: packed struct sizes, field offsets, ESP-NOW payload limits, round-trip parse |
| `test_e2e`           | End-to-end against an in-process cluster: routing, insert/lookup/delete, `try_only` semantics, dedup replay, bucket-batch migration, route-lock RETRY, hard-reset, distributed FPR, UNAVAILABLE propagation |

## Hardware tests (ESP32 boards)

| # | Test | Procedure | Expected |
|---|------|-----------|----------|
| 1 | ESP-NOW liveness | Boot master + slaves; `s` on master | All configured slaves show ALIVE |
| 2 | Insert via CLI   | `i hello` on master | Routed to owning primary; ACK OK |
| 3 | Lookup via CLI   | `l hello` after insert | Slave responds FOUND |
| 4 | Delete via CLI   | `d hello` then `l hello` | NOT FOUND |
| 5 | Benchmark        | `b 200` | Reports total / avg insert latency |
| 6 | FPR run          | `f 500` | Master prints FPR ≲ ~5 % |
| 7 | Manual rebalance | `r` while overloaded | Logs `REBALANCE done: N buckets src→dst` |
| 8 | Auto-rebalance overload | Push load past `REBALANCE_THRESHOLD` on one slave | Rebalance fires from heartbeat path (cooldown-gated) |
| 9 | Auto-rebalance join | Power on a previously dead slave | Rebalance fires within cooldown window to even out the joiner |
| 10 | Hard reset | Power-cycle a slave mid-workload | Master resets it on reconnect (`Slave 0xXX reset OK`) |

## Design references

| Topic | Document |
|---|---|
| M2 design / refactor plan | `../milestone2-distributed-cuckoo-refactor.md` |
| Project brief (milestones) | `../ideas/cuckoo-filter.md` |

## Running tests

```
# Native (desktop):
pio test -e native

# Firmware:
pio run -e master
pio run -e slave
pio run -e slave2    # second slave with MY_SLAVE_ID=0x02
```
