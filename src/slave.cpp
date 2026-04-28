// slave.cpp — ESP-NOW slave node for distributed cuckoo filter (M2).
// Receives CHAIN_INSERT / TAG_LOOKUP / TAG_DELETE / BUCKET_BATCH_* / HARD_RESET
// from master and delegates to Dispatcher. LEDs: blue=rx, green=idle, red=full.

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

// Override per board at compile time: -D MY_SLAVE_ID=0x02
#ifndef MY_SLAVE_ID
#define MY_SLAVE_ID 0x01
#endif

static SlaveStorage  g_storage;
static Dispatcher*   g_dispatcher = nullptr;
static uint8_t       g_own_mac[6];
static volatile bool g_led_rx  = false;
static volatile bool g_led_bad = false;

// ── LED helpers (only called from loop()) ───────────────────────────────────
static void setIdleLeds() {
    bool full = (g_storage.free_slots() == 0);
    digitalWrite(PIN_BLUE,  LOW);
    digitalWrite(PIN_GREEN, full ? LOW  : HIGH);
    digitalWrite(PIN_RED,   full ? HIGH : LOW);
}
static void flashBlue() {
    digitalWrite(PIN_BLUE, HIGH); delay(60); digitalWrite(PIN_BLUE, LOW);
}
static void blinkRed() {
    for (int i = 0; i < 4; i++) {
        digitalWrite(PIN_RED, HIGH); delay(70);
        digitalWrite(PIN_RED, LOW);  delay(70);
    }
}
static void bootBluex3() {
    for (int i = 0; i < 3; i++) {
        digitalWrite(PIN_BLUE, HIGH); delay(90);
        digitalWrite(PIN_BLUE, LOW);  delay(90);
    }
}

static void send_to_master(const uint8_t* data, size_t len) {
    transport_send(MASTER_MAC, data, len);
}

// ESP-NOW receive callback — runs in WiFi task, must not block.
static void on_recv(const uint8_t* mac, const uint8_t* data, int len) {
    if (len < (int)sizeof(MsgHeader)) { g_led_bad = true; return; }
    if (memcmp(mac, g_own_mac, 6) == 0) return;       // ignore loopback

    const MsgHeader* hdr = (const MsgHeader*)data;
    uint8_t dst = (uint8_t)(hdr->dst_id & 0xFF);
    if (dst != MY_SLAVE_ID && dst != BROADCAST_ID) return;

    g_led_rx = true;
    g_dispatcher->onMessage(data, (size_t)len);
}

void setup() {
    Serial.begin(115200);
    delay(250);
    srand(esp_random());

    pinMode(PIN_GREEN, OUTPUT); digitalWrite(PIN_GREEN, LOW);
    pinMode(PIN_BLUE,  OUTPUT); digitalWrite(PIN_BLUE,  LOW);
    pinMode(PIN_RED,   OUTPUT); digitalWrite(PIN_RED,   LOW);

    WiFi.mode(WIFI_STA);
    esp_wifi_get_mac(WIFI_IF_STA, g_own_mac);

    g_storage.init();

    transport_init(on_recv);
    transport_add_peer(MASTER_MAC);

    static Dispatcher disp(g_storage, MY_SLAVE_ID, MASTER_ID, send_to_master);
    g_dispatcher = &disp;

    Serial.printf("[SLAVE %u] ready  local_capacity=%u\n",
                  (unsigned)MY_SLAVE_ID, (unsigned)LOCAL_CAPACITY);

    bootBluex3();
    setIdleLeds();
}

void loop() {
    if (g_led_bad) { g_led_bad = false; blinkRed();  setIdleLeds(); }
    if (g_led_rx)  { g_led_rx  = false; flashBlue(); setIdleLeds(); }

    static uint32_t last_hb = 0;
    if (millis() - last_hb >= HEARTBEAT_INTERVAL_MS) {
        last_hb = millis();
        g_dispatcher->sendHeartbeat();
    }

    setIdleLeds();
    delay(10);
}

#endif // !NATIVE_BUILD && SLAVE_BUILD
