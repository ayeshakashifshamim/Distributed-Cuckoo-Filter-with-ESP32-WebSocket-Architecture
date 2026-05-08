// Copyright (c) 2026 - Kabeer's Network Opensource Ecosystem
// slave.cpp — ESP-NOW slave node for distributed cuckoo filter.
//
// Responsibilities:
//   - Owns a local set of buckets (storage via SlaveStorage).
//   - Handles incoming messages: CHAIN_INSERT, TAG_LOOKUP, TAG_DELETE,
//     BUCKET_BATCH_*, and HARD_RESET from the master.
//   - Sends periodic heartbeats to the master so that it can track load and
//     detect node failure.
//
// LED contract:
//   - Blue flash on each received ESP-NOW frame.
//   - Green = idle (has free slots); Red = no free slots.
//   - Red blink pattern on malformed frame.
//
// Firmware build: slave only (MASTER_BUILD must not be defined).

#if !defined(NATIVE_BUILD) && defined(SLAVE_BUILD)

#include <Arduino.h>
#include <WiFi.h>
#include <esp_wifi.h>
#include <string.h>
#include <stdlib.h>
#include "config.h"
#include "protocol/messages.h"
#include "slave_storage.h"
#include "dispatcher/dispatcher.h"
#include "transport/transport.h"

// MY_SLAVE_ID is passed at compile time: -D MY_SLAVE_ID=0x02
#ifndef MY_SLAVE_ID
#define MY_SLAVE_ID 0x01
#endif

// ── Module-local state ────────────────────────────────────────────────────

static SlaveStorage  bucket_pool;
static Dispatcher*    msg_router = nullptr;
static uint8_t       own_mac[6];

static volatile bool flag_rx  = false;   // set in WiFi task, cleared in loop()
static volatile bool flag_err = false;   // set on bad frame

// ── LED helpers ────────────────────────────────────────────────────────────

// setIdleLeds() reflects current free-slot state:
//   green HIGH → at least one free slot; red HIGH → completely full.
static void setIdleLeds() {
    bool full = (bucket_pool.free_slots() == 0);
    digitalWrite(PIN_BLUE,  LOW);
    digitalWrite(PIN_GREEN, full ? LOW  : HIGH);
    digitalWrite(PIN_RED,   full ? HIGH : LOW);
}

// Brief blue pulse on successful frame receive.
static void flashBlue() {
    digitalWrite(PIN_BLUE, HIGH); delay(60); digitalWrite(PIN_BLUE, LOW);
}

// Red blink pattern on malformed frame.
static void blinkRed() {
    for (int i = 0; i < 4; i++) {
        digitalWrite(PIN_RED, HIGH); delay(70);
        digitalWrite(PIN_RED, LOW);  delay(70);
    }
}

// Triple blue flash on boot to indicate slave identity.
static void bootBluex3() {
    for (int i = 0; i < 3; i++) {
        digitalWrite(PIN_BLUE, HIGH); delay(90);
        digitalWrite(PIN_BLUE, LOW);  delay(90);
    }
}

// ── Transport send helper ─────────────────────────────────────────────────

static void send_to_master(const uint8_t* data, size_t len) {
    transport_send(MASTER_MAC, data, len);
}

// ── ESP-NOW receive callback ───────────────────────────────────────────────
//
// Runs in WiFi task context — must not block or call any Arduino delay.
//
// Frames destined for a different slave or for the broadcast ID are ignored.
// Loopback frames (originating from own MAC) are also ignored.
static void on_recv(const uint8_t* mac, const uint8_t* data, int len) {
    if (len < (int)sizeof(MsgHeader)) { flag_err = true; return; }
    if (memcmp(mac, own_mac, 6) == 0) return;

    const MsgHeader* hdr = (const MsgHeader*)data;
    uint8_t dst = (uint8_t)(hdr->dst_id & 0xFF);
    if (dst != MY_SLAVE_ID && dst != BROADCAST_ID) return;

    flag_rx = true;
    msg_router->onMessage(data, (size_t)len);
}

// ── Arduino entry points ──────────────────────────────────────────────────

void setup() {
    Serial.begin(115200);
    delay(250);
    srand(esp_random());

    pinMode(PIN_GREEN, OUTPUT); digitalWrite(PIN_GREEN, LOW);
    pinMode(PIN_BLUE,  OUTPUT); digitalWrite(PIN_BLUE,  LOW);
    pinMode(PIN_RED,   OUTPUT); digitalWrite(PIN_RED,   LOW);

    WiFi.mode(WIFI_STA);
    esp_wifi_get_mac(WIFI_IF_STA, own_mac);

    bucket_pool.init();

    transport_init(on_recv);
    transport_add_peer(MASTER_MAC);

    static Dispatcher disp(bucket_pool, MY_SLAVE_ID, MASTER_ID, send_to_master);
    msg_router = &disp;

    Serial.printf("[SLAVE %u] ready  local_capacity=%u\n",
                  (unsigned)MY_SLAVE_ID, (unsigned)LOCAL_CAPACITY);

    bootBluex3();
    setIdleLeds();
}

void loop() {
    if (flag_err) { flag_err = false; blinkRed();  setIdleLeds(); }
    if (flag_rx)  { flag_rx  = false; flashBlue(); setIdleLeds(); }

    static uint32_t last_hb = 0;
    if (millis() - last_hb >= HEARTBEAT_INTERVAL_MS) {
        last_hb = millis();
        msg_router->sendHeartbeat();
    }

    setIdleLeds();
    delay(10);
}

#endif // !NATIVE_BUILD && SLAVE_BUILD
