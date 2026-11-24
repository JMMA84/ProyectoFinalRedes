import json
import os
import socket
import threading
import time
from pathlib import Path
from gpiozero import PWMLED, Button

# Cargar variables desde .env
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# --- HARDWARE ---
# LED PWM en GPIO 18 (pin físico 12) + resistencia + GND
# Switch entre GPIO 26 (pin físico 37) y GND (pin físico 39)
PIN_LED = 18
PIN_SWITCH = 26

led = PWMLED(PIN_LED)
switch = Button(PIN_SWITCH, bounce_time=0.25)

# --- RED ---
SERVER_HOST = os.getenv("SERVER_HOST", "127.0.0.1")
SERVER_PORT = int(os.getenv("TCP_PORT", "5000"))
DEVICE_TOKEN = os.getenv("DEVICE_TOKEN", "")
DEVICE_NAME = os.getenv("DEVICE_NAME", "raspberry-pi-5")

sock_activo = None
sock_lock = threading.Lock()
ultimo_nivel = 100


def enviar_json(obj: dict):
    """Serializa a JSON + salto de línea (NDJSON) y envía por el socket TCP."""
    with sock_lock:
        if sock_activo:
            try:
                trama = json.dumps(obj) + "\n"
                sock_activo.sendall(trama.encode("utf-8"))
                print(f"[TX -> Server] {trama.strip()}")
            except OSError as e:
                print(f"[Error TX] {e}")


def al_presionar_switch():
    """Dispara el evento de movimiento hacia el servidor al presionar el switch."""
    print("[HW] Switch presionado (GPIO 26 / Pin 37) -> Enviando alerta...")
    enviar_json({"type": "event", "event": "motion"})


switch.when_pressed = al_presionar_switch


def procesar_mensaje(msg: dict):
    global ultimo_nivel
    tipo = msg.get("type")

    if tipo == "hello_ok":
        print("[NET] Autenticación aceptada por el servidor.")
        estado_actual = "on" if led.value > 0 else "off"
        nivel_actual = int(round(led.value * 100)) if led.value > 0 else ultimo_nivel
        enviar_json({"type": "status", "state": estado_actual, "level": nivel_actual})

    elif tipo == "cmd" and msg.get("action") == "light":
        cid = msg.get("id")
        estado = msg.get("state")
        nivel = msg.get("level", ultimo_nivel)

        try:
            nivel = int(nivel)
            if not (0 <= nivel <= 100):
                raise ValueError("Nivel fuera de rango (0-100)")

            if estado == "on":
                ultimo_nivel = nivel
                led.value = nivel / 100.0
                nivel_real = int(round(led.value * 100))
                estado_real = "on" if nivel_real > 0 else "off"
            elif estado == "off":
                ultimo_nivel = nivel
                led.off()
                nivel_real = nivel
                estado_real = "off"
            else:
                raise ValueError(f"Estado inválido: {estado}")

            print(f"[HW] LED actualizado -> estado={estado_real}, nivel={nivel_real}%")
            enviar_json({
                "type": "ack",
                "id": cid,
                "ok": True,
                "state": estado_real,
                "level": nivel_real,
            })

        except Exception as e:
            print(f"[Error HW] {e}")
            enviar_json({
                "type": "ack",
                "id": cid,
                "ok": False,
                "error": str(e),
            })


def bucle_conexion():
    global sock_activo
    if not DEVICE_TOKEN:
        raise SystemExit("Falta DEVICE_TOKEN en el archivo .env de la Raspberry Pi.")

    while True:
        try:
            print(f"[NET] Conectando por TCP a {SERVER_HOST}:{SERVER_PORT}...")
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((SERVER_HOST, SERVER_PORT))

            with sock_lock:
                sock_activo = s

            enviar_json({
                "type": "hello",
                "device": DEVICE_NAME,
                "token": DEVICE_TOKEN,
            })

            buffer = ""
            while True:
                chunk = s.recv(1024)
                if not chunk:
                    print("[NET] Conexión cerrada por el servidor.")
                    break
                buffer += chunk.decode("utf-8")
                while "\n" in buffer:
                    linea, buffer = buffer.split("\n", 1)
                    if linea.strip():
                        print(f"[RX <- Server] {linea.strip()}")
                        try:
                            procesar_mensaje(json.loads(linea))
                        except json.JSONDecodeError:
                            print("[Error] JSON malformado recibido.")

        except (ConnectionRefusedError, TimeoutError, OSError) as e:
            print(f"[NET] Sin conexión ({e}). Reintentando en 4s...")
        finally:
            with sock_lock:
                if sock_activo:
                    sock_activo.close()
                    sock_activo = None
            time.sleep(4)


if __name__ == "__main__":
    try:
        bucle_conexion()
    except KeyboardInterrupt:
        print("\nDeteniendo cliente IoT...")
    finally:
        led.off()
        led.close()
        switch.close()