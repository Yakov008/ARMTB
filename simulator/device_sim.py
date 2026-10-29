"""
Эмулятор контроллера шкафа — ведёт себя как прошивка ESP32.
Нужен, чтобы проверить сервер и интерфейс без реального устройства.

Запуск:
    python device_sim.py korshuniha-1 secret-token-korshuniha-1 [ws://localhost:8000]

Команды в консоли (имитация действий бригады):
    o  — открыть дверь
    c  — закрыть дверь
    q  — выход
"""
import asyncio
import json
import sys
import time

import websockets

UNLOCK_WINDOW_SEC = 60
UNLOCK_MAX_SEC = 600
HEARTBEAT_SEC = 10


class Cabinet:
    def __init__(self):
        self.lock = "locked"
        self.door = "closed"
        self.unlocked_at = 0.0
        self.door_opened_during_unlock = False
        self.door_closed_at = 0.0
        self.ws = None
        self.last_cmd_id = ""

    async def send(self, msg: dict):
        if self.ws:
            await self.ws.send(json.dumps(msg, ensure_ascii=False))

    async def send_state(self):
        await self.send({"type": "state", "lock": self.lock, "door": self.door,
                         "rssi": -55, "uptime": int(time.monotonic()), "fw": "sim-1.0"})

    async def event(self, name: str, detail: str = ""):
        print(f"[event] {name} {detail}")
        await self.send({"type": "event", "event": name, "detail": detail})

    async def unlock(self, reason: str):
        self.lock = "unlocked"
        self.unlocked_at = time.monotonic()
        self.door_opened_during_unlock = self.door == "open"
        print(">>> РЕЛЕ: замок ОТКРЫТ")
        await self.event("unlocked", reason)
        await self.send_state()

    async def do_lock(self, reason: str):
        self.lock = "locked"
        print(">>> РЕЛЕ: замок ЗАКРЫТ")
        await self.event("locked", reason)
        await self.send_state()

    async def set_door(self, state: str):
        if state == self.door:
            return
        self.door = state
        if state == "open":
            if self.lock == "unlocked":
                self.door_opened_during_unlock = True
                await self.event("door_opened")
            else:
                await self.event("door_opened_while_locked", "ключ или вскрытие")
        else:
            self.door_closed_at = time.monotonic()
            await self.event("door_closed")
        await self.send_state()

    async def tick(self):
        if self.lock != "unlocked":
            return
        elapsed = time.monotonic() - self.unlocked_at
        if self.door_opened_during_unlock and self.door == "closed" and time.monotonic() - self.door_closed_at > 1:
            await self.do_lock("door_closed")
        elif not self.door_opened_during_unlock and elapsed > UNLOCK_WINDOW_SEC:
            await self.do_lock("timeout")
        elif elapsed > UNLOCK_MAX_SEC:
            await self.do_lock("max_time")

    async def handle(self, raw: str):
        msg = json.loads(raw)
        if msg.get("type") != "cmd":
            return
        cmd, cid = msg.get("cmd"), msg.get("id", "")
        if cid and cid == self.last_cmd_id:
            await self.send({"type": "ack", "id": cid, "ok": True, "msg": "duplicate"})
            return
        self.last_cmd_id = cid
        print(f"<<< команда с сервера: {cmd}")
        if cmd == "open":
            await self.unlock("remote")
        elif cmd == "close":
            await self.do_lock("remote")
        elif cmd == "status":
            await self.send_state()
        else:
            await self.send({"type": "ack", "id": cid, "ok": False, "msg": "unknown command"})
            return
        await self.send({"type": "ack", "id": cid, "ok": True, "msg": cmd})


async def keyboard(cab: Cabinet):
    loop = asyncio.get_running_loop()
    while True:
        line = (await loop.run_in_executor(None, sys.stdin.readline)).strip().lower()
        if line == "o":
            await cab.set_door("open")
        elif line == "c":
            await cab.set_door("closed")
        elif line == "q":
            raise SystemExit
        elif line:
            print("o — открыть дверь, c — закрыть дверь, q — выход")


async def timers(cab: Cabinet):
    last_hb = 0.0
    while True:
        await cab.tick()
        if time.monotonic() - last_hb > HEARTBEAT_SEC:
            await cab.send_state()
            last_hb = time.monotonic()
        await asyncio.sleep(0.2)


async def run(device_id: str, token: str, base: str):
    cab = Cabinet()
    url = f"{base}/ws/device?id={device_id}&token={token}"
    asyncio.create_task(keyboard(cab))
    asyncio.create_task(timers(cab))
    while True:
        try:
            async with websockets.connect(url) as ws:
                cab.ws = ws
                print(f"[ws] подключено к {base} как {device_id}")
                await cab.event("online", "sim-1.0")
                await cab.send_state()
                async for raw in ws:
                    await cab.handle(raw)
        except (OSError, websockets.ConnectionClosed) as e:
            print(f"[ws] нет связи ({e}), повтор через 5 с")
        cab.ws = None
        await asyncio.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    asyncio.run(run(sys.argv[1], sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else "ws://localhost:8000"))
