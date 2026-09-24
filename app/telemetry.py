"""Decode + persist logic untuk telemetry GPS, dipakai BERSAMA oleh dua jalur
transport (HTTP `POST /telemetry` di main.py, dan MQTT topic `telemetry/<SN>`
di gps_mqtt_client.py) -- supaya logic upsert device/alert kuota/alert tamper
TIDAK terduplikasi (dan berisiko divergen) di dua tempat berbeda.
"""
from datetime import datetime, timedelta, timezone

import struct

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import models

# Notifikasi "kuota SIM diduga habis" -- lihat persist_telemetry(). Modem GSM
# tidak punya cara membaca sisa kuota asli dari operator, jadi firmware
# (skripsi.ino) mengirim heuristik lewat field `dataSource` yang sudah ada
# (bukan field angka baru) begitu mendeteksi sesi data GSM gagal berkali-kali
# padahal jaringan seluler terdaftar normal -- lihat FIRMWARE_CONTRACT.md.
# Cooldown mencegah satu baris device_alerts baru dibuat di SETIAP transmisi
# selama kondisinya masih berlangsung (device bisa kirim tiap beberapa
# detik), yang akan membanjiri riwayat alert dengan ratusan baris identik.
QUOTA_WARNING_KEYWORD = "kuota"
QUOTA_ALERT_COOLDOWN = timedelta(hours=12)

# Format kompak per 2026-09-15 (lihat FIRMWARE_CONTRACT.md) -- 17 byte
# big-endian: lat(i32) lng(i32) heading(u16) speedKnots(u16) timestamp(u32)
# flags(u8). Dipakai baik di body HTTP (field `d`, hex-encode di dalam JSON)
# MAUPUN payload MQTT (hex mentah, tanpa wrapper JSON -- identitas device
# datang dari topic `telemetry/<SN>`, bukan diulang di payload).
COMPACT_TELEMETRY_STRUCT = struct.Struct(">iiHHIB")
FLAG_WIFI = 0b0001
FLAG_QUOTA_WARNING = 0b0010
FLAG_POWER_AKI = 0b0100
FLAG_TAMPER = 0b1000
FLAG_RESERVED_MASK = 0b11110000


def decode_compact_telemetry(hex_payload: str) -> dict:
    try:
        raw = bytes.fromhex(hex_payload)
    except ValueError:
        raise HTTPException(status_code=400, detail="Field 'd' bukan hex yang valid")
    if len(raw) != COMPACT_TELEMETRY_STRUCT.size:
        raise HTTPException(
            status_code=400,
            detail=f"Field 'd' harus {COMPACT_TELEMETRY_STRUCT.size} byte "
            f"({COMPACT_TELEMETRY_STRUCT.size * 2} karakter hex), dapat {len(raw)} byte",
        )
    lat_raw, lng_raw, heading, speed_raw, ts_raw, flags = COMPACT_TELEMETRY_STRUCT.unpack(raw)
    if flags & FLAG_RESERVED_MASK:
        raise HTTPException(status_code=400, detail="Bit cadangan (bit4-7) di flags harus 0")

    is_wifi = bool(flags & FLAG_WIFI)
    quota_warning = bool(flags & FLAG_QUOTA_WARNING)
    if quota_warning and not is_wifi:
        raise HTTPException(
            status_code=400, detail="quota_warning (bit1) cuma valid kalau dataSource WIFI (bit0=1)"
        )

    if is_wifi:
        data_source = "WIFI (GSM: kuota diduga habis)" if quota_warning else "WIFI"
    else:
        data_source = "GSM"

    return {
        "lat": lat_raw / 1_000_000,
        "lng": lng_raw / 1_000_000,
        "heading": float(heading),
        "speed_knots": speed_raw / 100,
        "timestamp": datetime.fromtimestamp(ts_raw, tz=timezone.utc),
        "data_source": data_source,
        "power_source": "AKI" if flags & FLAG_POWER_AKI else "BATTERY",
        "tamper": bool(flags & FLAG_TAMPER),
    }


async def persist_telemetry(
    db: AsyncSession,
    *,
    sn: str,
    lat: float,
    lng: float,
    heading: float,
    speed_knots: float,
    timestamp: datetime,
    data_source: str,
    power_source: str | None,
    tamper: bool,
    name: str | None = None,
    status: str | None = None,
) -> models.Device:
    """Upsert `devices` (state terkini) + insert baris `telemetry` +
    alert kuota/tamper kalau relevan. Dipanggil oleh ingest_telemetry()
    (HTTP) dan gps_mqtt_client.py (MQTT) -- SATU jalur logic, supaya
    perilaku identik apa pun transport yang dipakai device."""
    result = await db.execute(select(models.Device).where(models.Device.sn == sn))
    device = result.scalar_one_or_none()

    if device is None:
        device = models.Device(sn=sn)
        db.add(device)

    if name is not None:
        device.name = name
    if power_source is not None:
        device.power_source = power_source
    device.data_source = data_source
    if status is not None:
        device.status = status

    device.last_lat = lat
    device.last_lng = lng
    device.last_speed_knots = speed_knots
    device.last_heading = heading
    device.last_transmitted_at = timestamp

    if QUOTA_WARNING_KEYWORD in data_source.lower():
        cutoff = datetime.now(timezone.utc) - QUOTA_ALERT_COOLDOWN
        recent_alert = await db.execute(
            select(models.DeviceAlert.id)
            .where(
                models.DeviceAlert.device_sn == sn,
                models.DeviceAlert.type == "low_quota",
                models.DeviceAlert.created_at >= cutoff,
            )
            .limit(1)
        )
        if recent_alert.scalar_one_or_none() is None:
            db.add(
                models.DeviceAlert(
                    device_sn=sn,
                    type="low_quota",
                    message=f"Device {sn} melaporkan: {data_source}",
                    value=None,
                )
            )

    if tamper:
        # Alert tamper TIDAK pakai cooldown berbasis waktu seperti kuota --
        # itu kejadian kritis/diskrit, bukan kondisi berkelanjutan yang perlu
        # di-suppress. Sebagai gantinya, deteksi rising edge: catat baris
        # device_alerts baru cuma kalau titik SEBELUMNYA (yang paling baru,
        # kalau ada) belum tamper.
        previous = await db.execute(
            select(models.Telemetry.tamper)
            .where(models.Telemetry.device_sn == sn)
            .order_by(models.Telemetry.ts.desc())
            .limit(1)
        )
        was_already_tampered = previous.scalar_one_or_none() or False
        if not was_already_tampered:
            db.add(
                models.DeviceAlert(
                    device_sn=sn,
                    type="tamper",
                    message=f"Device {sn} diusik (tamper) pada "
                    f"{timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC",
                    value=None,
                )
            )

    row = models.Telemetry(
        device_sn=sn,
        ts=timestamp,
        lat=lat,
        lng=lng,
        speed_knots=speed_knots,
        heading=heading,
        data_source=data_source,
        power_source=power_source,
        tamper=tamper,
    )
    db.add(row)
    await db.commit()
    await db.refresh(device)
    return device
