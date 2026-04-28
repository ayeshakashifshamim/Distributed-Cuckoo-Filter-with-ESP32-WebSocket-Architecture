// transport.cpp — ESP-NOW transport layer implementation
// Skipped entirely on native (desktop) builds — no WiFi hardware available.
#ifndef NATIVE_BUILD

#include "transport.h"
#include <Arduino.h>
#include <WiFi.h>
#include <esp_wifi.h>
#include <esp_now.h>
#include <string.h>

// ── Internal state ────────────────────────────────────────────────────────────

// The single application-level receive callback registered by the caller.
static TransportRecvCb s_recv_cb = nullptr;

// ── ESP-NOW callbacks ─────────────────────────────────────────────────────────

// Adapts the ESP-NOW receive signature to our TransportRecvCb type.
static void _espnow_recv_cb(const uint8_t* mac, const uint8_t* data, int len) {
    if (s_recv_cb) {
        s_recv_cb(mac, data, len);
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

void transport_init(TransportRecvCb on_recv) {
    s_recv_cb = on_recv;

    WiFi.mode(WIFI_STA);            // ESP-NOW requires STA mode
    WiFi.disconnect();

    // Force ALL boards to be on the same WiFi channel (1) to ensure they can hear each other.
    // If boards boot onto different default channels, ESP-NOW will fail silently or be one-way.
    esp_wifi_set_promiscuous(true);
    esp_wifi_set_channel(1, WIFI_SECOND_CHAN_NONE);
    esp_wifi_set_promiscuous(false);

    if (esp_now_init() != ESP_OK) {
        Serial.println("[TRANSPORT] esp_now_init failed");
        return;
    }

    esp_now_register_recv_cb(_espnow_recv_cb);
}

bool transport_send(const uint8_t* mac, const uint8_t* data, size_t len) {
    esp_err_t rc = esp_now_send(mac, data, len);
    return (rc == ESP_OK);
}

void transport_add_peer(const uint8_t* mac) {
    esp_now_peer_info_t peer{};
    memcpy(peer.peer_addr, mac, 6);
    peer.channel = 0;       // 0 = use current Wi-Fi channel
    peer.encrypt = false;

    esp_err_t rc = esp_now_add_peer(&peer);
    if (rc != ESP_OK) {
        Serial.printf("[TRANSPORT] add_peer failed rc=%d\n", (int)rc);
    }
}

#endif // NATIVE_BUILD
