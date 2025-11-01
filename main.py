import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.types.json import Json

# estos sí existen en tu repo
from db import fetchone, fetchall, execute

# =========================================================
# CONFIG
# =========================================================

SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# el que vimos en Neon
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")

app = FastAPI(title="IoT Ingest API", version="1.0.0")

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
    lat: float
    lon: float
    alt: float | None = None
    time: datetime | None = None


# =========================================================
# HELPERS
# =========================================================

def require_token(authorization: str | None):
    if not authorization:
        raise HTTPException(status_code=401, detail="missing_token")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="invalid_auth_header")
    if parts[1] != SAFE_TOKEN:
        raise HTTPException(status_code=401, detail="invalid_token")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def dt_to_iso(x):
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.isoformat()
    return str(x)


# =========================================================
# ENDPOINTS BÁSICOS
# =========================================================

@app.get("/")
def root():
    return {"ok": True, "service": "iot-backend", "uptime": utcnow().isoformat()}


@app.get("/health")
def health():
    row = fetchone("SELECT 1 AS ok;")
    return {"ok": True, "db": bool(row and row.get("ok") == 1)}


@app.post("/ingest_lite")
def ingest_lite(payload: ReadingIn, authorization: str | None = Header(default=None)):
    require_token(authorization)

    row = execute(
        """
        INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at, payload)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            str(DEFAULT_DEVICE_ID),
            float(payload.lat),
            float(payload.lon),
            float(payload.alt) if payload.alt is not None else None,
            payload.time,
            Json(
                {
                    "lat": payload.lat,
                    "lon": payload.lon,
                    "alt": payload.alt,
                    "time": payload.time.isoformat() if payload.time else None,
                }
            ),
        ),
    )
    return {"inserted_id": row["id"]}


@app.post("/ingest")
def ingest(payload: ReadingIn, authorization: str | None = Header(default=None)):
    require_token(authorization)

    row = execute(
        """
        INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at, payload)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            str(DEFAULT_DEVICE_ID),
            float(payload.lat),
            float(payload.lon),
            float(payload.alt) if payload.alt is not None else None,
            payload.time,
            Json(
                {
                    "lat": payload.lat,
                    "lon": payload.lon,
                    "alt": payload.alt,
                    "time": payload.time.isoformat() if payload.time else None,
                }
            ),
        ),
    )
    return {"inserted_id": row["id"]}


@app.get("/readings/recent")
def recent(limit: int = 50, device: UUID | None = None):
    if device is None:
        device = DEFAULT_DEVICE_ID

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


# =========================================================
# NUEVO: TRACK (VERSIÓN ULTRA SIMPLE)
# =========================================================

@app.get("/readings/track")
def readings_track():
    """
    Devuelve TODOS los puntos del device por defecto en orden ASC.
    Sin filtros, sin fechas, sin distancia, sin nada.
    """
    device = str(DEFAULT_DEVICE_ID)

    try:
        rows = fetchall(
            """
            SELECT id, device_id, lat, lon, alt_m, read_at, ts
            FROM readings
            WHERE device_id = %s
            ORDER BY ts ASC;
            """,
            (device,),
        )
    except Exception as e:
        # Esto debe aparecer en los logs de Railway
        print("[/readings/track] DB ERROR:", e)
        raise HTTPException(status_code=500, detail="db_error")

    points: list[dict] = []
    for r in rows:
        points.append(
            {
                "id": r["id"],
                "device_id": r["device_id"],
                "lat": float(r["lat"]) if r["lat"] is not None else None,
                "lon": float(r["lon"]) if r["lon"] is not None else None,
                "alt_m": float(r["alt_m"]) if r["alt_m"] is not None else None,
                "read_at": dt_to_iso(r["read_at"]),
                "ts": dt_to_iso(r["ts"]),
            }
        )

    return {
        "device": device,
        "count": len(points),
        "points": points,
    }
