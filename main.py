import os
import math
from datetime import datetime, timezone
from uuid import UUID
from typing import Optional, Any

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.types.json import Json

# 👇 helpers que ya tienes en tu proyecto
from db import fetchone, fetchall, execute

# =========================================================
# CONFIGURACIÓN BÁSICA
# =========================================================

# token que usas en PowerShell
SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# device por defecto que ya existe en tu tabla
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")

app = FastAPI(title="IoT Ingest API", version="1.0.0")

# permitir frontend local y vercel
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "https://iot-frontend-iota.vercel.app",
        # si quieres ser más relajado, puedes dejar "*"
        # "*",
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
    """
    Valida 'Authorization: Bearer <token>'.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="missing_token")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="invalid_auth_header")
    if parts[1] != SAFE_TOKEN:
        raise HTTPException(status_code=401, detail="invalid_token")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Distancia en metros entre dos puntos lat/lon.
    """
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# =========================================================
# ENDPOINTS
# =========================================================

@app.get("/")
def root():
    return {"ok": True, "service": "iot-backend", "uptime": utcnow().isoformat()}


@app.get("/health")
def health():
    """
    Comprueba que la app corre y que la DB responde.
    """
    row = fetchone("SELECT 1 AS ok;")
    return {"ok": True, "db": bool(row and row.get("ok") == 1)}


@app.post("/ingest_lite")
def ingest_lite(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Versión mínima: guarda lat/lon (+ alt si viene).
    """
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
def ingest(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Versión completa: igual a ingest_lite pero explícita.
    """
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
def recent(
    limit: int = 50,
    device: Optional[str] = None,
):
    """
    Últimas lecturas para mostrar en la tabla del frontend.
    Si no mandas ?device=, usamos el UUID por defecto.
    """
    if device is None:
        # usa el que estás usando en ingest
        device = str(DEFAULT_DEVICE_ID)

    rows = fetchall(
        """
        SELECT id, device_id, lat, lon, alt_m, read_at, ts
        FROM readings
        WHERE device_id = %s
        ORDER BY ts DESC
        LIMIT %s;
        """,
        (device, limit),
    )
    return {"items": rows}


@app.get("/readings/track")
def readings_track(
    device: Optional[str] = Query(None, description="ID lógico o UUID del dispositivo"),
    start: datetime | None = Query(None, description="inicio ISO"),
    end: datetime | None = Query(None, description="fin ISO"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
    limit: int = Query(500, ge=1, le=2000),
):
    """
    Devuelve los puntos de una ruta para poder pintarla en el mapa.
    - Si mandas ?device=ipn-zac-sim-01 → filtra por ese device_id
    - Si NO mandas device → toma TODO (o el UUID por defecto, ver abajo)
    - Puedes mandar start y end en ISO para acotar
    - Devuelve también un summary con distancia y duración
    """
    clauses = []
    params: list[Any] = []

    if device:
        # filtrar por el device que mandó el frontend
        clauses.append("device_id = %s")
        params.append(device)
    else:
        # si no mandó device, usamos el de siempre
        clauses.append("device_id = %s")
        params.append(str(DEFAULT_DEVICE_ID))

    if start is not None:
        clauses.append("ts >= %s")
        params.append(start)
    if end is not None:
        clauses.append("ts <= %s")
        params.append(end)

    where_sql = " AND ".join(clauses) if clauses else "TRUE"
    order_sql = "ASC" if order.lower() == "asc" else "DESC"

    query = f"""
        SELECT id, device_id, lat, lon, alt_m, read_at, ts
        FROM readings
        WHERE {where_sql}
        ORDER BY read_at {order_sql} NULLS LAST, ts {order_sql}
        LIMIT %s;
    """
    params.append(limit)

    rows = fetchall(query, tuple(params))

    # -------- normalizamos para el frontend --------
    points: list[dict[str, Any]] = []
    for r in rows:
        points.append(
            {
                "id": r["id"],
                "device_id": r["device_id"],
                "lat": float(r["lat"]) if r["lat"] is not None else None,
                "lon": float(r["lon"]) if r["lon"] is not None else None,
                "alt_m": float(r["alt_m"]) if r["alt_m"] is not None else None,
                "read_at": r["read_at"].isoformat() if r["read_at"] else None,
                "ts": r["ts"].isoformat() if r["ts"] else None,
            }
        )

    # -------- cálculo de distancia y duración --------
    total_dist = 0.0
    for i in range(1, len(points)):
        p1 = points[i - 1]
        p2 = points[i]
        if (
            p1["lat"] is not None
            and p1["lon"] is not None
            and p2["lat"] is not None
            and p2["lon"] is not None
        ):
            total_dist += haversine_m(p1["lat"], p1["lon"], p2["lat"], p2["lon"])

    # duración
    duration_s = None
    if points:
        first_time = points[0]["read_at"] or points[0]["ts"]
        last_time = points[-1]["read_at"] or points[-1]["ts"]
        if first_time and last_time:
            t1 = datetime.fromisoformat(first_time.replace("Z", "+00:00"))
            t2 = datetime.fromisoformat(last_time.replace("Z", "+00:00"))
            duration_s = (t2 - t1).total_seconds()

    return {
        "device": device or str(DEFAULT_DEVICE_ID),
        "points": points,
        "summary": {
            "count": len(points),
            "distance_m": round(total_dist, 2),
            "duration_s": duration_s,
        },
    }
