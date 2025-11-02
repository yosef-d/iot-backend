import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.types.json import Json

# 👇 importa tus helpers reales
from db import fetchone, fetchall, execute

# =========================================================
# CONFIG
# =========================================================

SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# este es el device que ya existe en tu DB
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")


app = FastAPI(title="IoT Ingest API", version="1.0.0")

# CORS para que el Vercel y el localhost puedan pegarle
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
    """
    Este es el formato que ESPERAMOS recibir.
    OJO: si en PowerShell lo mandas así:
        { "lat": 19.5, "lon": -99.14, "alt": 2242, "time": "2025-11-01T14:30:00Z" }
    esto entra perfecto.
    """
    lat: float
    lon: float
    alt: float | None = None
    time: datetime | None = None


# =========================================================
# HELPERS
# =========================================================

def require_token(authorization: str | None):
    """Valida Authorization: Bearer ..."""
    if not authorization:
        raise HTTPException(status_code=401, detail="missing_token")
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="invalid_auth_header")
    if parts[1] != SAFE_TOKEN:
        raise HTTPException(status_code=401, detail="invalid_token")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _insert_reading(
    *,
    device_id: UUID,
    lat: float,
    lon: float,
    alt: float | None,
    read_at: datetime | None,
    raw_payload: dict,
):
    """
    Función auxiliar que realmente mete el registro.
    La separamos para poder envolverla en try/except en los endpoints.
    """
    row = execute(
        """
        INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at, payload)
        VALUES (%s, NOW(), %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            str(device_id),
            float(lat),
            float(lon),
            float(alt) if alt is not None else None,
            read_at,
            Json(raw_payload),
        ),
    )
    return row["id"]


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


# =========================================================
# INGEST (dos variantes)
# =========================================================

@app.post("/ingest_lite")
def ingest_lite(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Variante mínima. La dejamos por compatibilidad.
    """
    require_token(authorization)

    # construimos el payload crudo que guardaremos en JSON
    payload_json = {
        "lat": payload.lat,
        "lon": payload.lon,
        "alt": payload.alt,
        "time": payload.time.isoformat() if payload.time else None,
    }

    try:
        new_id = _insert_reading(
            device_id=DEFAULT_DEVICE_ID,
            lat=payload.lat,
            lon=payload.lon,
            alt=payload.alt,
            read_at=payload.time,
            raw_payload=payload_json,
        )
        return {"inserted_id": new_id}
    except Exception as e:
        # 🔴 AQUI es donde ANTES se te perdía el error
        # ahora lo regresamos TAL CUAL
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


@app.post("/ingest")
def ingest(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Endpoint principal. Úsame para PowerShell.
    """
    require_token(authorization)

    payload_json = {
        "lat": payload.lat,
        "lon": payload.lon,
        "alt": payload.alt,
        "time": payload.time.isoformat() if payload.time else None,
    }

    try:
        new_id = _insert_reading(
            device_id=DEFAULT_DEVICE_ID,
            lat=payload.lat,
            lon=payload.lon,
            alt=payload.alt,
            read_at=payload.time,
            raw_payload=payload_json,
        )
        return {"inserted_id": new_id}
    except Exception as e:
        # 🔴 ESTE es el que te estaba devolviendo 500 sin decirte nada
        # ahora sí vas a ver QUÉ dijo Postgres
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


# =========================================================
# LISTADO / TABLA
# =========================================================

@app.get("/readings/recent")
def recent(
    limit: int = 50,
    device: UUID | None = None,
):
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
# TRACK PARA EL MAPA
# =========================================================

@app.get("/readings/track")
def track(
    device: UUID | None = Query(None, description="UUID del dispositivo"),
    start: datetime | None = Query(None, description="inicio ISO"),
    end: datetime | None = Query(None, description="fin ISO"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    """
    Devuelve la ruta (todos los puntos) de un device.
    Si no mandas ?device=... te mando el DEFAULT.
    """
    if device is None:
        device = DEFAULT_DEVICE_ID

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
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


# =========================================================
# FIN
# =========================================================
