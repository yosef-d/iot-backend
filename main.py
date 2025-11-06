# main.py
import os
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from psycopg.types.json import Json

from db import fetchone, fetchall, execute  # <- tus helpers

# =========================================================
# CONFIG
# =========================================================

SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# UUID de device ya existente en tu tabla
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")

app = FastAPI(title="IoT Ingest API", version="1.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://iot-frontend-iota.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================================================
# MODELOS
# =========================================================

class ReadingIn(BaseModel):
    """Formato original del backend (el que ya usabas desde PowerShell)."""
    lat: float
    lon: float
    alt: float | None = None
    time: datetime | None = None


class MinimalReading(BaseModel):
    """
    Formato mínimo FINAL de tus JSON:
    {
      "latitude":  19.5049,
      "longitude": -99.1467,
      "altitude":  2242,         // opcional
      "timestamp": "2025-11-01T17:30:00Z"  // opcional
    }
    """
    latitude: float
    longitude: float
    altitude: Optional[float] = None
    timestamp: Optional[datetime] = None

    # Validadores suaves por si llegan strings numéricos
    @field_validator("latitude", mode="before")
    @classmethod
    def _lat_cast(cls, v):
        return float(v)

    @field_validator("longitude", mode="before")
    @classmethod
    def _lon_cast(cls, v):
        return float(v)

    @field_validator("altitude", mode="before")
    @classmethod
    def _alt_cast(cls, v):
        if v is None or v == "":
            return None
        return float(v)


class BulkMinimal(BaseModel):
    items: List[MinimalReading]


# =========================================================
# HELPERS
# =========================================================

def require_token(authorization: str | None):
    """Valida 'Authorization: Bearer <token>'."""
    if not authorization:
        raise HTTPException(status_code=401, detail="missing_token")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="invalid_auth_header")
    if parts[1] != SAFE_TOKEN:
        raise HTTPException(status_code=401, detail="invalid_token")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def insert_reading(
    lat: float,
    lon: float,
    alt: float | None,
    read_at: datetime | None,
    payload_dict: dict,
) -> int:
    """Inserta una lectura en tabla readings y retorna id."""
    row = execute(
        """
        INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at, payload)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            str(DEFAULT_DEVICE_ID),
            float(lat),
            float(lon),
            float(alt) if alt is not None else None,
            read_at,
            Json(payload_dict),
        ),
    )
    return int(row["id"])


# =========================================================
# ENDPOINTS
# =========================================================

@app.get("/")
def root():
    return {"ok": True, "service": "iot-backend", "uptime": utcnow().isoformat()}


@app.get("/health")
def health():
    """Comprueba que la app corre y que la DB responde."""
    try:
        row = fetchone("SELECT 1 AS ok;")
        return {"ok": True, "db": bool(row and row.get("ok") == 1)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e}")


# ---------- Ingest (formato original) ----------
@app.post("/ingest")
def ingest(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    require_token(authorization)
    try:
        new_id = insert_reading(
            lat=payload.lat,
            lon=payload.lon,
            alt=payload.alt,
            read_at=payload.time,
            payload_dict={
                "lat": payload.lat,
                "lon": payload.lon,
                "alt": payload.alt,
                "time": payload.time.isoformat() if payload.time else None,
            },
        )
        return {"inserted_id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e}")


# ---------- Ingest mínimo: 1 elemento ----------
@app.post("/ingest_min")
def ingest_min(
    item: MinimalReading,
    authorization: str | None = Header(default=None),
):
    """
    Acepta el formato mínimo final (latitude, longitude, altitude?, timestamp?) para un solo punto.
    """
    require_token(authorization)
    try:
        new_id = insert_reading(
            lat=item.latitude,
            lon=item.longitude,
            alt=item.altitude,
            read_at=item.timestamp,
            payload_dict={
                "latitude": item.latitude,
                "longitude": item.longitude,
                "altitude": item.altitude,
                "timestamp": item.timestamp.isoformat() if item.timestamp else None,
            },
        )
        return {"inserted_id": new_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e}")


# ---------- Ingest mínimo: bulk ----------
@app.post("/ingest_min/bulk")
def ingest_min_bulk(
    batch: BulkMinimal,
    authorization: str | None = Header(default=None),
):
    """
    Acepta:
    { "items": [ {latitude, longitude, altitude?, timestamp?}, ... ] }
    Inserta todo y regresa conteo e ids.
    """
    require_token(authorization)
    inserted_ids: List[int] = []
    try:
        for it in batch.items:
            rid = insert_reading(
                lat=it.latitude,
                lon=it.longitude,
                alt=it.altitude,
                read_at=it.timestamp,
                payload_dict={
                    "latitude": it.latitude,
                    "longitude": it.longitude,
                    "altitude": it.altitude,
                    "timestamp": it.timestamp.isoformat() if it.timestamp else None,
                },
            )
            inserted_ids.append(rid)

        return {"count": len(inserted_ids), "ids": inserted_ids}
    except Exception as e:
        # si algo sale mal, informa cuántos iban bien
        raise HTTPException(
            status_code=500,
            detail={"error": f"db_error: {e}", "inserted_so_far": len(inserted_ids)},
        )


# ---------- Lecturas recientes (para la tabla) ----------
@app.get("/readings/recent")
def recent(
    limit: int = 50,
    device: UUID | None = None,
):
    if device is None:
        device = DEFAULT_DEVICE_ID
    try:
        rows = fetchall(
            """
            SELECT id, device_id, lat, lon, alt_m, read_at, ts
            FROM readings
            WHERE device_id = %s
            ORDER BY ts DESC
            LIMIT %s;
            """,
            (str(device), limit),
        )
        return {"items": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e}")


# ---------- Track (para el mapa) ----------
@app.get("/readings/track")
def track(
    device: UUID = Query(..., description="UUID del dispositivo"),
    start: datetime | None = Query(None, description="inicio ISO"),
    end: datetime | None = Query(None, description="fin ISO"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    clauses = ["device_id = %s"]
    params: list = [str(device)]

    if start is not None:
        clauses.append("ts >= %s")
        params.append(start)
    if end is not None:
        clauses.append("ts <= %s")
        params.append(end)

    order_sql = "ASC" if order.lower() == "asc" else "DESC"

    where_sql = " AND ".join(clauses)
    query = f"""
        SELECT id, device_id, lat, lon, alt_m, read_at, ts
        FROM readings
        WHERE {where_sql}
        ORDER BY ts {order_sql};
    """
    try:
        rows = fetchall(query, tuple(params))
        return {"items": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e}")
