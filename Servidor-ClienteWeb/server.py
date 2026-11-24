"""
Plataforma web de control remoto (servidor).

- Panel web con login: controla la iluminación de la Raspberry y muestra confirmaciones.
- La Raspberry se conecta por TCP (TCP_PORT) y habla JSON, un mensaje por línea.
  Alternativa con un solo túnel: la misma conversación por WebSocket en /device.
- Alerta de movimiento: cuando la Raspberry avisa, se envía un correo.

Uso:  pip install -r requirements.txt  ->  copiar .env.example a .env  ->  python server.py
"""
import asyncio
import hmac
import json
import logging
import os
import secrets
import smtplib
import socket
import ssl
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from urllib.parse import urlparse

import uvicorn
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware

BASE = Path(__file__).parent


# ---------- Configuración (.env) ----------
def load_env():
    f = BASE / ".env"
    if f.exists():
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


load_env()
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
DEVICE_TOKEN = os.getenv("DEVICE_TOKEN", "")
SECRET_KEY = os.getenv("SECRET_KEY") or secrets.token_hex(32)
SECURE_COOKIES = os.getenv("SECURE_COOKIES", "1") == "1"
HTTP_PORT = int(os.getenv("HTTP_PORT", "8000"))
TCP_PORT = int(os.getenv("TCP_PORT", "5000"))
TCP_HOST = os.getenv("TCP_HOST", "0.0.0.0")
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "").replace(" ", "")
ALERT_TO = os.getenv("ALERT_TO", "")
MOTION_COOLDOWN = int(os.getenv("MOTION_COOLDOWN", "60"))
ACK_TIMEOUT = 5  # segundos que se espera la confirmación de la Raspberry
COL = timezone(timedelta(hours=-5))  # Colombia

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[logging.FileHandler(BASE / "activity.log", encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("control")


# ---------- Estado ----------
class State:
    device = None                              # conexión activa de la Raspberry
    light = {"state": None, "level": 100}      # último estado confirmado
    last_mail = 0.0


S = State()
panels: set[WebSocket] = set()
history: deque = deque(maxlen=100)
pending: dict[str, asyncio.Future] = {}
cmd_lock = asyncio.Lock()


async def broadcast(obj: dict):
    data = json.dumps(obj)
    for p in list(panels):
        try:
            await p.send_text(data)
        except Exception:
            panels.discard(p)


async def emit(kind: str, text: str):
    """Registra en archivo, guarda en el historial y lo muestra en los paneles."""
    ev = {"type": "log", "ts": time.time(), "kind": kind, "text": text}
    history.append(ev)
    log.info("[%s] %s", kind, text)
    await broadcast(ev)


def state_msg() -> dict:
    d = S.device
    return {"type": "state", "online": d is not None, "name": d.name if d else None, "light": S.light}


async def push_state():
    await broadcast(state_msg())


def parse(raw) -> dict:
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def valid_level(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= 100


# ---------- Conexiones de la Raspberry (TCP y WebSocket) ----------
class TcpDevice:
    transport = "TCP"

    def __init__(self, reader, writer):
        self.reader, self.writer, self.name = reader, writer, "raspberry"
        self.peer = "{}:{}".format(*(writer.get_extra_info("peername") or ("?", 0))[:2])

    async def send(self, obj: dict):
        self.writer.write((json.dumps(obj) + "\n").encode())
        await self.writer.drain()

    async def close(self):
        self.writer.close()


class WsDevice:
    transport = "WebSocket"

    def __init__(self, ws: WebSocket):
        self.ws, self.name = ws, "raspberry"
        self.peer = f"{ws.client.host}:{ws.client.port}" if ws.client else "?"

    async def send(self, obj: dict):
        await self.ws.send_text(json.dumps(obj))

    async def close(self):
        try:
            await self.ws.close()
        except Exception:
            pass


async def run_device(dev, recv):
    """Autentica con token, registra el dispositivo y procesa sus mensajes."""
    try:
        hello = await asyncio.wait_for(recv(), 10)
    except asyncio.TimeoutError:
        hello = None
    token = str((hello or {}).get("token", ""))
    if not hello or hello.get("type") != "hello" or not hmac.compare_digest(token.encode(), DEVICE_TOKEN.encode()):
        log.warning("Conexión de dispositivo rechazada desde %s", dev.peer)
        await asyncio.sleep(1)  # frena intentos de adivinar el token
        await dev.close()
        return

    dev.name = str(hello.get("device") or "raspberry")[:40]
    old, S.device = S.device, dev
    if old:
        await old.close()
    try:
        await dev.send({"type": "hello_ok"})
    except (ConnectionError, OSError, RuntimeError):
        pass
    await emit("ok", f"Raspberry conectada: {dev.name} ({dev.transport}, {dev.peer})")
    await push_state()
    try:
        while (m := await recv()) is not None:
            await on_device_message(m)
    finally:
        if S.device is dev:
            S.device = None
            for f in pending.values():
                if not f.done():
                    f.set_result({"ok": False, "error": "La Raspberry se desconectó"})
            await emit("error", "Raspberry desconectada")
            await push_state()
        await dev.close()


async def on_device_message(m: dict):
    t = m.get("type")
    if t == "ack":
        fut = pending.get(str(m.get("id")))
        if fut and not fut.done():
            fut.set_result(m)
    elif t == "status":
        apply_light(m)
        await push_state()
    elif t == "event" and m.get("event") == "motion":
        await handle_motion()


def apply_light(m: dict):
    light = dict(S.light)
    if m.get("state") in ("on", "off"):
        light["state"] = m["state"]
    if valid_level(m.get("level")):
        light["level"] = m["level"]
    S.light = light


async def handle_tcp(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    sock = writer.get_extra_info("socket")
    if sock:  # keepalive: detecta caídas silenciosas a través del túnel
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    async def recv():
        try:
            line = await reader.readline()
        except (ConnectionError, ValueError):
            return None
        return parse(line) if line else None

    try:
        await run_device(TcpDevice(reader, writer), recv)
    finally:
        writer.close()


# ---------- Comandos de iluminación con confirmación ----------
async def send_command(state: str, level: int) -> dict:
    async with cmd_lock:
        dev = S.device
        if dev is None:
            return {"ok": False, "error": "La Raspberry no está conectada"}
        cid = uuid.uuid4().hex[:8]
        fut = asyncio.get_running_loop().create_future()
        pending[cid] = fut
        try:
            await dev.send({"type": "cmd", "id": cid, "action": "light", "state": state, "level": level})
            ack = await asyncio.wait_for(fut, ACK_TIMEOUT)
        except asyncio.TimeoutError:
            return {"ok": False, "error": "La Raspberry no confirmó a tiempo"}
        except (ConnectionError, OSError, RuntimeError):
            return {"ok": False, "error": "Se perdió la conexión con la Raspberry"}
        finally:
            pending.pop(cid, None)
        if ack.get("ok") is True:
            apply_light({"state": ack.get("state", state), "level": ack.get("level", level)})
            return {"ok": True}
        return {"ok": False, "error": str(ack.get("error") or "La Raspberry reportó un fallo")[:200]}


async def do_light(state: str, level: int):
    res = await send_command(state, level)
    if res["ok"]:
        if S.light["state"] == "on":
            text = f"Confirmado: luz encendida al {S.light['level']}%"
        else:
            text = "Confirmado: luz apagada"
        await emit("ok", text)
        await push_state()
    else:
        text = f"No se ejecutó la acción: {res['error']}"
        await emit("error", text)
    await broadcast({"type": "result", "ok": res["ok"], "text": text})


# ---------- Alerta de movimiento por correo ----------
def _smtp_send(subject: str, body: str):
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, SMTP_USER, ALERT_TO
    msg.set_content(body)
    ctx = ssl.create_default_context()
    if SMTP_PORT == 465:
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=20) as s:
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            s.starttls(context=ctx)
            s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)


async def send_mail(subject: str, body: str) -> bool:
    if not (SMTP_USER and SMTP_PASSWORD and ALERT_TO):
        await emit("error", "Correo no configurado (revisa SMTP_USER, SMTP_PASSWORD y ALERT_TO en .env)")
        return False
    try:
        await asyncio.to_thread(_smtp_send, subject, body)
    except Exception as e:
        await emit("error", f"No se pudo enviar el correo: {e}")
        return False
    await emit("ok", f"Correo enviado a {ALERT_TO}")
    return True


async def handle_motion():
    await emit("motion", "Movimiento detectado")
    now = time.time()
    if now - S.last_mail < MOTION_COOLDOWN:
        await emit("info", f"Correo omitido: ya se envió uno hace menos de {MOTION_COOLDOWN} s")
        return
    S.last_mail = now
    when = datetime.now(COL).strftime("%Y-%m-%d %H:%M:%S")
    name = S.device.name if S.device else "raspberry"
    ok = await send_mail("Alerta: movimiento detectado", f"El sensor de {name} detectó movimiento el {when} (hora de Colombia).")
    if not ok:
        S.last_mail = 0.0  # permite reintentar con el próximo evento


# ---------- Aplicación web ----------
@asynccontextmanager
async def lifespan(_: FastAPI):
    srv = await asyncio.start_server(handle_tcp, TCP_HOST, TCP_PORT)
    log.info("TCP en %s:%s | Panel en http://localhost:%s", TCP_HOST, TCP_PORT, HTTP_PORT)
    try:
        yield
    finally:
        srv.close()
        if S.device:
            await S.device.close()


app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, max_age=8 * 3600, same_site="lax", https_only=SECURE_COOKIES)


def page(name: str) -> HTMLResponse:
    return HTMLResponse((BASE / name).read_text(encoding="utf-8"), headers={"Cache-Control": "no-store"})


@app.get("/")
async def index(request: Request):
    return page("index.html") if request.session.get("user") else RedirectResponse("/login")


@app.get("/login")
async def login_page(request: Request):
    return RedirectResponse("/") if request.session.get("user") else page("login.html")


fails: dict[str, tuple[int, float]] = {}  # ip -> (intentos fallidos, bloqueado hasta)


def client_ip(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    return xff.split(",")[-1].strip() if xff else (request.client.host if request.client else "?")


@app.post("/api/login")
async def login(request: Request):
    ip = client_ip(request)
    count, until = fails.get(ip, (0, 0.0))
    if until > time.time():
        return JSONResponse({"error": "Demasiados intentos. Espera un minuto."}, status_code=429)
    try:
        body = await request.json()
    except ValueError:
        body = {}
    if not isinstance(body, dict):
        body = {}
    user, pwd = str(body.get("user", "")), str(body.get("password", ""))
    ok_user = hmac.compare_digest(user.encode(), ADMIN_USER.encode())
    ok_pwd = hmac.compare_digest(pwd.encode(), ADMIN_PASSWORD.encode())
    if not (ok_user and ok_pwd):
        count += 1
        fails[ip] = (0, time.time() + 60) if count >= 5 else (count, 0.0)
        log.warning("Login fallido desde %s", ip)
        return JSONResponse({"error": "Usuario o contraseña incorrectos"}, status_code=401)
    fails.pop(ip, None)
    request.session.clear()
    request.session["user"] = user
    log.info("Login correcto desde %s", ip)
    return {"ok": True}


@app.post("/api/logout")
async def logout(request: Request):
    request.session.clear()
    return {"ok": True}


@app.get("/api/me")
async def me(request: Request):
    if not request.session.get("user"):
        return JSONResponse({"error": "no autenticado"}, status_code=401)
    return {"user": request.session["user"]}


@app.websocket("/ws/panel")
async def panel(ws: WebSocket):
    origin = ws.headers.get("origin")
    if not ws.session.get("user") or (origin and urlparse(origin).netloc != ws.headers.get("host")):
        await ws.close(code=4401)
        return
    await ws.accept()
    panels.add(ws)
    await ws.send_text(json.dumps({"type": "snapshot", "history": list(history), "state": state_msg()}))
    try:
        while True:
            try:
                msg = json.loads(await ws.receive_text())
            except ValueError:
                continue
            if not isinstance(msg, dict):
                continue
            if msg.get("type") == "light":
                if msg.get("state") in ("on", "off") and valid_level(msg.get("level")):
                    await do_light(msg["state"], msg["level"])
            elif msg.get("type") == "test_mail":
                await send_mail("Prueba de correo", "Este es un correo de prueba de la plataforma de control remoto.")
    except WebSocketDisconnect:
        pass
    finally:
        panels.discard(ws)


@app.websocket("/device")
async def device_ws(ws: WebSocket):
    """Alternativa a TCP: la Raspberry usa el mismo protocolo JSON por WebSocket (un solo túnel HTTP)."""
    await ws.accept()

    async def recv():
        try:
            return parse(await ws.receive_text())
        except (WebSocketDisconnect, RuntimeError, KeyError):
            return None

    await run_device(WsDevice(ws), recv)


if __name__ == "__main__":
    if not ADMIN_PASSWORD or not DEVICE_TOKEN:
        raise SystemExit("Falta ADMIN_PASSWORD o DEVICE_TOKEN. Copia .env.example a .env y complétalo.")
    uvicorn.run(app, host="127.0.0.1", port=HTTP_PORT, proxy_headers=False)
