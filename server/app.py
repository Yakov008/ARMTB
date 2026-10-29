"""
Сервер управления шкафами с тормозными башмаками.

- Принимает WebSocket-подключения от контроллеров шкафов (ESP32) — /ws/device
- Даёт веб-интерфейс диспетчеру (ДНЦ) — http://<сервер>:8000
- Ведёт журнал всех операций в SQLite (events.db)

Запуск:          python app.py
Добавить ДНЦ:    python app.py adduser <логин>
"""
import asyncio
import getpass
import hashlib
import json
import os
import secrets
import sqlite3
import sys
import time
import uuid
import webbrowser
from contextlib import asynccontextmanager
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

# В собранном exe данные лежат рядом с exe, а статика — внутри сборки
if getattr(sys, "frozen", False):
    DATA_DIR = Path(sys.executable).parent
    STATIC_DIR = Path(sys._MEIPASS) / "static"
else:
    DATA_DIR = Path(__file__).parent
    STATIC_DIR = DATA_DIR / "static"

DEVICES_FILE = DATA_DIR / "devices.json"
USERS_FILE = DATA_DIR / "users.json"
DB_FILE = DATA_DIR / "events.db"

PORT = int(os.environ.get("LOCK_PORT", "8000"))
OFFLINE_AFTER_SEC = 35   # нет вестей от шкафа дольше — считаем «нет связи»
ACK_TIMEOUT_SEC = 5      # сколько ждём подтверждения команды от шкафа
SESSION_TTL_SEC = 12 * 3600

DEFAULT_DEVICES = {
    "devices": [
        {"id": "korshuniha-1", "name": "Коршуниха-Ангарская, шкаф №1", "token": "secret-token-korshuniha-1"},
        {"id": "chernaya-1", "name": "Чёрная, шкаф №1", "token": "secret-token-chernaya-1"},
    ]
}


# ------------------------------------------------------------------
#  Пользователи (диспетчеры)
# ------------------------------------------------------------------
def hash_password(password: str, salt: str) -> str:
    return hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 200_000).hex()


def load_users() -> dict:
    if USERS_FILE.exists():
        return json.loads(USERS_FILE.read_text(encoding="utf-8"))
    return {}


def save_user(login: str, password: str, name: str = ""):
    users = load_users()
    salt = secrets.token_hex(16)
    users[login] = {"salt": salt, "hash": hash_password(password, salt), "name": name or login}
    USERS_FILE.write_text(json.dumps(users, ensure_ascii=False, indent=2), encoding="utf-8")


def check_password(login: str, password: str) -> bool:
    user = load_users().get(login)
    if not user:
        return False
    return secrets.compare_digest(user["hash"], hash_password(password, user["salt"]))


def ensure_first_user():
    if load_users():
        return
    password = secrets.token_urlsafe(8)
    save_user("admin", password, "Администратор")
    print("=" * 60, flush=True)
    print("  Создан первый пользователь:  admin")
    print(f"  Пароль:                      {password}")
    print("  Добавить диспетчера:  python app.py adduser <логин>")
    print("=" * 60, flush=True)


# ------------------------------------------------------------------
#  Журнал событий
# ------------------------------------------------------------------
def db():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with db() as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts REAL NOT NULL,
                device_id TEXT,
                user TEXT,
                kind TEXT NOT NULL,
                detail TEXT
            )"""
        )


def log_event(device_id: str, kind: str, detail: str = "", user: str = "") -> dict:
    ts = time.time()
    with db() as conn:
        cur = conn.execute(
            "INSERT INTO events (ts, device_id, user, kind, detail) VALUES (?, ?, ?, ?, ?)",
            (ts, device_id, user, kind, detail),
        )
    return {"id": cur.lastrowid, "ts": ts, "device_id": device_id, "user": user, "kind": kind, "detail": detail}


# ------------------------------------------------------------------
#  Подключённые шкафы
# ------------------------------------------------------------------
class Hub:
    def __init__(self):
        self.devices: dict[str, dict] = {}
        self.sockets: dict[str, WebSocket] = {}
        self.pending: dict[str, asyncio.Future] = {}
        self.ui_clients: set[WebSocket] = set()

    def load(self):
        if not DEVICES_FILE.exists():
            DEVICES_FILE.write_text(json.dumps(DEFAULT_DEVICES, ensure_ascii=False, indent=2), encoding="utf-8")
        cfg = json.loads(DEVICES_FILE.read_text(encoding="utf-8"))
        for d in cfg["devices"]:
            self.devices[d["id"]] = {
                "id": d["id"],
                "name": d.get("name", d["id"]),
                "token": d["token"],
                "online": False,
                "lock": "unknown",
                "door": "unknown",
                "rssi": None,
                "fw": None,
                "last_seen": None,
            }

    def public(self, dev: dict) -> dict:
        return {k: v for k, v in dev.items() if k != "token"}

    def snapshot(self) -> list:
        return [self.public(d) for d in self.devices.values()]

    async def broadcast(self, msg: dict):
        dead = []
        for client in self.ui_clients:
            try:
                await client.send_json(msg)
            except Exception:
                dead.append(client)
        for client in dead:
            self.ui_clients.discard(client)

    async def push_devices(self):
        await self.broadcast({"type": "devices", "devices": self.snapshot()})

    async def push_event(self, event: dict):
        await self.broadcast({"type": "event", "event": event})

    async def send_command(self, device_id: str, cmd: str) -> dict:
        sock = self.sockets.get(device_id)
        if not sock:
            raise HTTPException(409, "Нет связи со шкафом")
        cmd_id = uuid.uuid4().hex[:12]
        fut = asyncio.get_running_loop().create_future()
        self.pending[cmd_id] = fut
        try:
            await sock.send_json({"type": "cmd", "cmd": cmd, "id": cmd_id})
            return await asyncio.wait_for(fut, ACK_TIMEOUT_SEC)
        except asyncio.TimeoutError:
            raise HTTPException(504, "Шкаф не подтвердил команду")
        finally:
            self.pending.pop(cmd_id, None)


hub = Hub()
sessions: dict[str, dict] = {}


async def watchdog():
    """Помечает шкафы без связи."""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        changed = False
        for dev in hub.devices.values():
            if dev["online"] and (dev["last_seen"] is None or now - dev["last_seen"] > OFFLINE_AFTER_SEC):
                dev["online"] = False
                changed = True
                await hub.push_event(log_event(dev["id"], "offline", "нет данных от шкафа"))
        if changed:
            await hub.push_devices()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    hub.load()
    ensure_first_user()
    task = asyncio.create_task(watchdog())
    yield
    task.cancel()


app = FastAPI(title="Удалённое управление шкафами ТБ", lifespan=lifespan)


# ------------------------------------------------------------------
#  Авторизация диспетчера
# ------------------------------------------------------------------
def current_user(token: str | None) -> dict | None:
    s = sessions.get(token or "")
    if not s or time.time() - s["created"] > SESSION_TTL_SEC:
        return None
    return s


def require_user(request: Request) -> dict:
    user = current_user(request.cookies.get("session"))
    if not user:
        raise HTTPException(401, "Требуется вход")
    return user


class LoginIn(BaseModel):
    login: str
    password: str


class OpenIn(BaseModel):
    train: str = ""
    comment: str = ""


@app.post("/api/login")
async def login(data: LoginIn, response: Response):
    if not check_password(data.login, data.password):
        log_event("", "login_failed", data.login)
        raise HTTPException(401, "Неверный логин или пароль")
    token = secrets.token_urlsafe(32)
    name = load_users()[data.login]["name"]
    sessions[token] = {"login": data.login, "name": name, "created": time.time()}
    response.set_cookie("session", token, httponly=True, samesite="strict", max_age=SESSION_TTL_SEC)
    log_event("", "login", "", data.login)
    return {"login": data.login, "name": name}


@app.post("/api/logout")
async def logout(request: Request, response: Response):
    sessions.pop(request.cookies.get("session", ""), None)
    response.delete_cookie("session")
    return {"ok": True}


@app.get("/api/me")
async def me(request: Request):
    u = require_user(request)
    return {"login": u["login"], "name": u["name"]}


# ------------------------------------------------------------------
#  Управление шкафами
# ------------------------------------------------------------------
@app.get("/api/devices")
async def list_devices(request: Request):
    require_user(request)
    return hub.snapshot()


@app.post("/api/devices/{device_id}/open")
async def open_device(device_id: str, data: OpenIn, request: Request):
    user = require_user(request)
    if device_id not in hub.devices:
        raise HTTPException(404, "Шкаф не найден")
    detail = f"поезд {data.train or '—'}; {data.comment}".strip("; ")
    await hub.push_event(log_event(device_id, "open_requested", detail, user["login"]))
    ack = await hub.send_command(device_id, "open")
    return {"ok": ack.get("ok", False), "msg": ack.get("msg", "")}


@app.post("/api/devices/{device_id}/close")
async def close_device(device_id: str, request: Request):
    user = require_user(request)
    if device_id not in hub.devices:
        raise HTTPException(404, "Шкаф не найден")
    await hub.push_event(log_event(device_id, "close_requested", "", user["login"]))
    ack = await hub.send_command(device_id, "close")
    return {"ok": ack.get("ok", False), "msg": ack.get("msg", "")}


@app.get("/api/events")
async def events(request: Request, device: str = "", limit: int = 200):
    require_user(request)
    q = "SELECT * FROM events"
    args: list = []
    if device:
        q += " WHERE device_id = ?"
        args.append(device)
    q += " ORDER BY id DESC LIMIT ?"
    args.append(min(limit, 2000))
    with db() as conn:
        return [dict(r) for r in conn.execute(q, args)]


# ------------------------------------------------------------------
#  WebSocket: живые обновления для интерфейса
# ------------------------------------------------------------------
@app.websocket("/ws/ui")
async def ws_ui(ws: WebSocket):
    if not current_user(ws.cookies.get("session")):
        await ws.close(code=4401)
        return
    await ws.accept()
    hub.ui_clients.add(ws)
    await ws.send_json({"type": "devices", "devices": hub.snapshot()})
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        hub.ui_clients.discard(ws)


# ------------------------------------------------------------------
#  WebSocket: подключение шкафа (ESP32)
# ------------------------------------------------------------------
@app.websocket("/ws/device")
async def ws_device(ws: WebSocket, id: str = "", token: str = ""):
    dev = hub.devices.get(id)
    if not dev or not secrets.compare_digest(dev["token"], token):
        log_event(id, "auth_failed", ws.client.host if ws.client else "")
        await ws.close(code=4403)
        return
    await ws.accept()

    old = hub.sockets.get(id)
    if old:
        try:
            await old.close()
        except Exception:
            pass
    hub.sockets[id] = ws
    dev["online"] = True
    dev["last_seen"] = time.time()
    await hub.push_event(log_event(id, "online", ws.client.host if ws.client else ""))
    await hub.push_devices()

    try:
        while True:
            msg = await ws.receive_json()
            dev["last_seen"] = time.time()
            mtype = msg.get("type")

            if mtype == "state":
                was = (dev["online"], dev["lock"], dev["door"])
                dev["online"] = True
                dev["lock"] = msg.get("lock", dev["lock"])
                dev["door"] = msg.get("door", dev["door"])
                dev["rssi"] = msg.get("rssi")
                dev["fw"] = msg.get("fw")
                if was != (dev["online"], dev["lock"], dev["door"]):
                    await hub.push_devices()

            elif mtype == "event":
                if msg.get("event") == "online":
                    continue  # уже записали при подключении
                await hub.push_event(log_event(id, msg.get("event", "?"), msg.get("detail", "")))

            elif mtype == "ack":
                fut = hub.pending.get(msg.get("id", ""))
                if fut and not fut.done():
                    fut.set_result(msg)
    except (WebSocketDisconnect, RuntimeError, ValueError):
        pass
    finally:
        if hub.sockets.get(id) is ws:
            del hub.sockets[id]
            dev["online"] = False
            await hub.push_event(log_event(id, "offline", "соединение разорвано"))
            await hub.push_devices()


# ------------------------------------------------------------------
#  Веб-интерфейс
# ------------------------------------------------------------------
@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "adduser":
        login = sys.argv[2]
        name = input("ФИО диспетчера: ").strip()
        password = getpass.getpass("Пароль: ")
        if password != getpass.getpass("Повторите пароль: "):
            print("Пароли не совпадают")
            sys.exit(1)
        save_user(login, password, name)
        print(f"Пользователь {login} сохранён")
        return

    init_db()
    ensure_first_user()
    print(f"Сервер запущен: http://localhost:{PORT}")
    if "--no-browser" not in sys.argv:
        webbrowser.open(f"http://localhost:{PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
