// transport.h — Thin ESP-NOW wrapper (init, send, receive)
// Single responsibility: isolates all WiFi/ESP-NOW calls from business logic.
#pragma once
#include <stdint.h>
#include <stddef.h>

// Callback invoked when a packet arrives (mac = sender's MAC, 6 bytes)
typedef void (*TransportRecvCb)(const uint8_t* mac, const uint8_t* data, int len);

#ifndef NATIVE_BUILD

// Initialise WiFi in STA mode and ESP-NOW, then register the receive callback.
void transport_init(TransportRecvCb on_recv);

// Send `len` bytes to `mac` via ESP-NOW. Returns true on success.
bool transport_send(const uint8_t* mac, const uint8_t* data, size_t len);

// Register a peer MAC so ESP-NOW will accept sends to it.
void transport_add_peer(const uint8_t* mac);

#endif // NATIVE_BUILD
