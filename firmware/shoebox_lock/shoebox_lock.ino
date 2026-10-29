// ============================================================
//  Прошивка контроллера шкафа с тормозными башмаками
//  Плата: ESP32 (ESP32-DevKitC / WROOM-32)
//
//  Библиотеки (Arduino IDE -> Управление библиотеками):
//    - "WebSockets" by Markus Sattler (links2004)
//    - "ArduinoJson" by Benoit Blanchon (v7)
//
//  Логика:
//    1. ESP32 подключается к Wi-Fi и держит постоянное WebSocket-соединение
//       с сервером (устройство само звонит на сервер, «белый» IP на станции
//       не нужен).
//    2. Сервер присылает команду {"type":"cmd","cmd":"open"} — замок
//       открывается.
//    3. Бригада открывает дверь, берёт башмаки, закрывает дверь —
//       замок закрывается автоматически (по геркону).
//       Если дверь не открыли за UNLOCK_WINDOW_SEC — закрывается сам.
//    4. Любое изменение (замок, дверь) сразу отправляется на сервер,
//       плюс состояние раз в HEARTBEAT_SEC.
//    5. Нет связи — шкаф остаётся закрытым. Резерв — механический ключ.
// ============================================================

#include <WiFi.h>
#include <WebSocketsClient.h>
#include <ArduinoJson.h>
#include "config.h"

#define FW_VERSION "1.0.0"

WebSocketsClient ws;

enum LockState { LOCKED, UNLOCKED };
LockState lockState = LOCKED;

bool doorOpen = false;           // устойчивое (после антидребезга) состояние двери
bool doorRaw = false;
unsigned long doorRawChangedAt = 0;

unsigned long unlockedAt = 0;    // когда открыли замок
bool doorOpenedDuringUnlock = false;
bool relayOn = false;
String lastCmdId = "";

unsigned long lastHeartbeat = 0;
bool wsConnected = false;

// ------------------------------------------------------------
//  Управление реле
// ------------------------------------------------------------
void setRelay(bool on) {
  relayOn = on;
  digitalWrite(PIN_LOCK_RELAY, on ? RELAY_ACTIVE_LEVEL : !RELAY_ACTIVE_LEVEL);
}

// Физически открыть/закрыть замок с учётом его типа
void applyLock(LockState s) {
  if (LOCK_FAIL_SECURE) {
    setRelay(s == UNLOCKED);   // электромеханический: ток = открыт
  } else {
    setRelay(s == LOCKED);     // электромагнит: ток = закрыт
  }
}

// ------------------------------------------------------------
//  Отправка сообщений на сервер
// ------------------------------------------------------------
void sendJson(JsonDocument& doc) {
  if (!wsConnected) return;
  String out;
  serializeJson(doc, out);
  ws.sendTXT(out);
}

void sendState() {
  JsonDocument doc;
  doc["type"] = "state";
  doc["lock"] = lockState == LOCKED ? "locked" : "unlocked";
  doc["door"] = doorOpen ? "open" : "closed";
  doc["rssi"] = WiFi.RSSI();
  doc["uptime"] = millis() / 1000;
  doc["fw"] = FW_VERSION;
  sendJson(doc);
  lastHeartbeat = millis();
}

void sendEvent(const char* event, const char* detail = "") {
  JsonDocument doc;
  doc["type"] = "event";
  doc["event"] = event;
  doc["detail"] = detail;
  sendJson(doc);
  Serial.printf("[event] %s %s\n", event, detail);
}

void sendAck(const String& cmdId, bool ok, const char* msg) {
  JsonDocument doc;
  doc["type"] = "ack";
  doc["id"] = cmdId;
  doc["ok"] = ok;
  doc["msg"] = msg;
  sendJson(doc);
}

// ------------------------------------------------------------
//  Логика замка
// ------------------------------------------------------------
void unlock(const char* reason) {
  lockState = UNLOCKED;
  unlockedAt = millis();
  doorOpenedDuringUnlock = doorOpen;
  applyLock(UNLOCKED);
  sendEvent("unlocked", reason);
  sendState();
}

void lock(const char* reason) {
  lockState = LOCKED;
  applyLock(LOCKED);
  sendEvent("locked", reason);
  sendState();
}

void updateDoor() {
  bool raw = digitalRead(PIN_DOOR_SENSOR) == HIGH;  // геркон разомкнут = дверь открыта
  if (raw != doorRaw) {
    doorRaw = raw;
    doorRawChangedAt = millis();
  }
  if (raw != doorOpen && millis() - doorRawChangedAt > 50) {
    doorOpen = raw;
    if (doorOpen) {
      if (lockState == UNLOCKED) {
        doorOpenedDuringUnlock = true;
        sendEvent("door_opened");
      } else {
        // Дверь открыта, хотя замок закрыт: механический ключ или взлом
        sendEvent("door_opened_while_locked", "ключ или вскрытие");
      }
    } else {
      sendEvent("door_closed");
    }
    sendState();
  }
}

void updateLockTimers() {
  if (lockState != UNLOCKED) return;
  unsigned long elapsed = millis() - unlockedAt;

  // Дверь открыли и закрыли — закрываем замок (через 1 с, чтобы дверь «села»)
  if (doorOpenedDuringUnlock && !doorOpen && millis() - doorRawChangedAt > 1000) {
    lock("door_closed");
    return;
  }
  // Дверь так и не открыли
  if (!doorOpenedDuringUnlock && elapsed > UNLOCK_WINDOW_SEC * 1000UL) {
    lock("timeout");
    return;
  }
  // Защита от бесконечно открытого шкафа
  if (elapsed > UNLOCK_MAX_SEC * 1000UL) {
    lock("max_time");
    return;
  }
  // Импульсный режим для электромеханической защёлки
  if (LOCK_FAIL_SECURE && UNLOCK_PULSE_SEC > 0 && relayOn &&
      elapsed > UNLOCK_PULSE_SEC * 1000UL) {
    setRelay(false);
  }
}

// ------------------------------------------------------------
//  Приём команд с сервера
// ------------------------------------------------------------
void handleMessage(uint8_t* payload, size_t length) {
  JsonDocument doc;
  if (deserializeJson(doc, payload, length)) return;
  if (doc["type"] != "cmd") return;

  String cmd = doc["cmd"] | "";
  String id = doc["id"] | "";

  // Повтор той же команды (например, после переподключения) не выполняем дважды
  if (id.length() && id == lastCmdId) {
    sendAck(id, true, "duplicate");
    return;
  }
  lastCmdId = id;

  if (cmd == "open") {
    unlock("remote");
    sendAck(id, true, "opened");
  } else if (cmd == "close") {
    lock("remote");
    sendAck(id, true, "closed");
  } else if (cmd == "status") {
    sendState();
    sendAck(id, true, "status");
  } else {
    sendAck(id, false, "unknown command");
  }
}

void onWsEvent(WStype_t type, uint8_t* payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED:
      wsConnected = true;
      Serial.println("[ws] connected");
      sendEvent("online", FW_VERSION);
      sendState();
      break;
    case WStype_DISCONNECTED:
      wsConnected = false;
      Serial.println("[ws] disconnected");
      break;
    case WStype_TEXT:
      handleMessage(payload, length);
      break;
    default:
      break;
  }
}

// ------------------------------------------------------------
//  Связь
// ------------------------------------------------------------
void connectWifi() {
  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);
  Serial.printf("[wifi] connecting to %s", WIFI_SSID);
  unsigned long start = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - start < 20000) {
    delay(500);
    Serial.print(".");
  }
  Serial.println(WiFi.status() == WL_CONNECTED ? " ok" : " failed (retrying in background)");
}

void connectServer() {
  String path = String("/ws/device?id=") + DEVICE_ID + "&token=" + DEVICE_TOKEN;
  if (USE_TLS) {
    ws.beginSSL(SERVER_HOST, SERVER_PORT, path.c_str());
  } else {
    ws.begin(SERVER_HOST, SERVER_PORT, path.c_str());
  }
  ws.onEvent(onWsEvent);
  ws.setReconnectInterval(5000);
  ws.enableHeartbeat(15000, 5000, 2);  // ping каждые 15 с, 2 пропуска = обрыв
}

void updateStatusLed() {
  // Связь есть — горит постоянно, нет — мигает
  if (wsConnected) {
    digitalWrite(PIN_STATUS_LED, HIGH);
  } else {
    digitalWrite(PIN_STATUS_LED, (millis() / 300) % 2);
  }
}

// ------------------------------------------------------------
void setup() {
  Serial.begin(115200);
  pinMode(PIN_LOCK_RELAY, OUTPUT);
  pinMode(PIN_DOOR_SENSOR, INPUT_PULLUP);
  pinMode(PIN_STATUS_LED, OUTPUT);

  applyLock(LOCKED);  // при старте шкаф всегда закрыт
  doorOpen = doorRaw = digitalRead(PIN_DOOR_SENSOR) == HIGH;

  Serial.printf("\nShoebox lock %s, id=%s\n", FW_VERSION, DEVICE_ID);
  connectWifi();
  connectServer();
}

void loop() {
  ws.loop();
  updateDoor();
  updateLockTimers();
  updateStatusLed();

  if (millis() - lastHeartbeat > HEARTBEAT_SEC * 1000UL) {
    sendState();
  }
}
