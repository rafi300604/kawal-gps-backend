"""Subscriber MQTT -- jalur pengganti HTTPS untuk device yang kirim lewat
GSM/seluler (hemat kuota data: satu koneksi persisten, bukan buka sesi TLS
baru tiap kirim -- lihat FIRMWARE_CONTRACT.md).

Payload di topic `telemetry/<SN>` adalah HEX MENTAH (bukan dibungkus JSON)
-- identitas device datang dari topic, bukan dari payload."""
import asyncio
import os
import socket

import paho.mqtt.client as mqtt

from . import telemetry
from .database import async_session

_MQTT_CLIENT = None
_MAIN_LOOP = None


def _get_mqtt_config():
    broker = os.getenv("GPS_MQTT_BROKER", "")
    # Default 1883 (listener INTERNAL, anonymous, tanpa TLS -- lihat
    # mqtt-gps/config/mosquitto.conf) -- backend jalan di network Docker yang
    # sama dengan broker, tidak perlu lewat listener publik 8883 (yang pakai
    # cert self-signed + auth, khusus device eksternal dari internet).
    port = int(os.getenv("GPS_MQTT_PORT", "1883"))
    client_id = os.getenv("GPS_MQTT_CLIENT_ID") or f"kawal-gps-backend-{socket.gethostname()}-{os.getpid()}"
    return {
        "broker": broker,
        "port": port,
        "user": os.getenv("GPS_MQTT_USER", ""),
        "password": os.getenv("GPS_MQTT_PASSWORD", ""),
        "client_id": client_id,
        "topic": "telemetry/+",
        "use_tls": os.getenv("GPS_MQTT_TLS", "false").lower() == "true",
    }


def _extract_sn_from_topic(topic: str) -> str | None:
    parts = topic.strip("/").split("/")
    if len(parts) == 2 and parts[0] == "telemetry":
        return parts[1]
    return None


async def _handle_telemetry_message(sn: str, hex_payload: str):
    try:
        decoded = telemetry.decode_compact_telemetry(hex_payload)
    except Exception as exc:
        print(f"[mqtt] Gagal decode payload dari topic telemetry/{sn}: {exc}", flush=True)
        return
    async with async_session() as db:
        await telemetry.persist_telemetry(
            db,
            sn=sn,
            lat=decoded["lat"],
            lng=decoded["lng"],
            heading=decoded["heading"],
            speed_knots=decoded["speed_knots"],
            timestamp=decoded["timestamp"],
            data_source=decoded["data_source"],
            power_source=decoded["power_source"],
            tamper=decoded["tamper"],
        )
    print(f"[mqtt] Tersimpan: sn={sn} lat={decoded['lat']} lng={decoded['lng']}", flush=True)


def _on_connect(client, userdata, flags, rc, properties=None):
    config = _get_mqtt_config()
    if rc == 0:
        print(f"[mqtt] Terhubung; subscribe ke {config['topic']}", flush=True)
        client.subscribe(config["topic"])
    else:
        print(f"[mqtt] Gagal connect, kode {rc}", flush=True)


def _on_disconnect(client, userdata, rc, properties=None):
    print(f"[mqtt] Terputus, kode {rc}", flush=True)


def _on_message(client, userdata, msg):
    sn = _extract_sn_from_topic(msg.topic)
    if sn is None:
        print(f"[mqtt] Topic tidak dikenali: {msg.topic}", flush=True)
        return
    try:
        hex_payload = msg.payload.decode("ascii").strip()
    except UnicodeDecodeError:
        print(f"[mqtt] Payload bukan teks ascii/hex di topic {msg.topic}", flush=True)
        return

    global _MAIN_LOOP
    if _MAIN_LOOP is not None and _MAIN_LOOP.is_running():
        asyncio.run_coroutine_threadsafe(_handle_telemetry_message(sn, hex_payload), _MAIN_LOOP)
    else:
        try:
            asyncio.run(_handle_telemetry_message(sn, hex_payload))
        except Exception as exc:
            print(f"[mqtt] Error memproses pesan dari {sn}: {exc}", flush=True)


def start_mqtt_client(loop=None):
    global _MQTT_CLIENT, _MAIN_LOOP
    if loop is not None:
        _MAIN_LOOP = loop
    elif _MAIN_LOOP is None:
        try:
            _MAIN_LOOP = asyncio.get_running_loop()
        except RuntimeError:
            pass

    if _MQTT_CLIENT is not None:
        return _MQTT_CLIENT

    config = _get_mqtt_config()
    if not config["broker"]:
        print("[mqtt] GPS_MQTT_BROKER belum diset -- subscriber MQTT tidak dijalankan.", flush=True)
        return None

    print(
        f"[mqtt] Inisialisasi client broker={config['broker']}:{config['port']} "
        f"tls={config['use_tls']} client_id={config['client_id']}",
        flush=True,
    )

    try:
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION1,
            client_id=config["client_id"],
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
    except (AttributeError, TypeError):
        client = mqtt.Client(client_id=config["client_id"], clean_session=True, protocol=mqtt.MQTTv311)

    if config["user"] or config["password"]:
        client.username_pw_set(config["user"], config["password"])
    if config["use_tls"]:
        client.tls_set()

    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    client.on_message = _on_message

    for attempt in range(1, 11):
        try:
            client.connect(config["broker"], config["port"], keepalive=60)
            client.loop_start()
            _MQTT_CLIENT = client
            print(f"[mqtt] Client jalan; broker={config['broker']}:{config['port']}", flush=True)
            return client
        except Exception as exc:
            print(f"[mqtt] Broker belum siap (percobaan {attempt}/10): {exc}; coba lagi 2 detik", flush=True)
            import time
            time.sleep(2)

    print("[mqtt] Broker MQTT tidak tersedia setelah beberapa percobaan; lanjut tanpa MQTT.", flush=True)
    return None


def stop_mqtt_client():
    global _MQTT_CLIENT
    if _MQTT_CLIENT is None:
        return
    try:
        _MQTT_CLIENT.disconnect()
    finally:
        _MQTT_CLIENT.loop_stop()
        _MQTT_CLIENT = None
