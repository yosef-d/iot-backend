import os
from datetime import datetime, timezone
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# =========================================================
# CONFIG GLOBAL
# =========================================================

# URL de tu Neon (ya la tienes en Railway como env)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_mVBjRlX5w0zF@ep-winter-bread-advp2uvx-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require",
)

# Token que usas en PowerShell
SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# Device fijo que ya existe en tu tabla
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")

app = FastAPI(title="IoT Ingest API", version="1.0.0")

# CORS para local y Vercel
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
# PEQUEÑO HELPER DE DB (aquí ya forzamos dict_row)
# =========================================================

def db_fetchone(sql: str, params: tuple | None = None):
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchone()

def db_fetchall(sql: str, params: tuple | None = None):
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or ())
            return cur.fetchall()

def db_execute_returning(sql: str, params: tuple | None = None):
    """Para INSERT ... RETURNING ..."""
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params or ())
            row = cur.fetchone()
            conn.commit()
            return row

def db_execute(sql: str, params: tuple | None = None):
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            conn.commit()


# =========================================================
# MODELOS
# =========================================================

class ReadingIn(BaseModel):
    lat: float
    lon: float
    alt: float | None = None
    time: datetime | None = None  # opcional


# =========================================================
# UTILIDADES
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


# =========================================================
# ENDPOINTS
# =========================================================

@app.get("/")
def root():
    return {"ok": True, "service": "iot-backend", "uptime": utcnow().isoformat()}


@app.get("/health")
def health():
    """
    Verifica que la API y la DB respondan.
    Aquí YA no usamos db.py, vamos directo.
    """
    try:
        row = db_fetchone("SELECT 1 AS ok;")
        return {"ok": True, "db": bool(row and row["ok"] == 1)}
    except Exception as e:
        # si algo truena, devolvemos detalle
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


@app.post("/ingest")
def ingest(payload: ReadingIn, authorization: str | None = Header(default=None)):
    """
    Inserta una lectura simple.
    """
    require_token(authorization)

    row = db_execute_returning(
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
            {
                "lat": payload.lat,
                "lon": payload.lon,
                "alt": payload.alt,
                "time": payload.time.isoformat() if payload.time else None,
            },
        ),
    )
    return {"inserted_id": row["id"]}


@app.get("/readings/recent")
def readings_recent(limit: int = 50, device: UUID | None = None):
    """
    Devuelve las últimas N lecturas para la tabla del frontend.
    """
    if device is None:
        device = DEFAULT_DEVICE_ID

    rows = db_fetchall(
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


@app.get("/readings/track")
def readings_track(
    device: UUID | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    """
    Devuelve TODA la ruta (o intervalo) de un device para el mapa.
    Si no mandas device, usamos el DEFAULT.
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

    where_sql = " AND ".join(clauses)
    order_sql = "ASC" if order.lower() == "asc" else "DESC"

    query = f"""
        SELECT id, device_id, lat, lon, alt_m, read_at, ts
        FROM readings
        WHERE {where_sql}
        ORDER BY ts {order_sql};
    """

    rows = db_fetchall(query, tuple(params))
    return {"items": rows}
