import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# estos helpers ya los tienes en db.py
from db import fetchone, fetchall, execute

# =========================================================
# CONFIG GENERAL
# =========================================================

# token que usas en PowerShell
SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# este es el device que ya existe en tu tabla readings en Neon
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")

app = FastAPI(title="IoT Ingest API", version="1.0.0")

# CORS: tu localhost y tu Vercel
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
    """
    Valida que venga:  Authorization: Bearer <token>
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


# =========================================================
# ENDPOINTS BÁSICOS
# =========================================================

@app.get("/")
def root():
    return {
        "ok": True,
        "service": "iot-backend",
        "uptime": utcnow().isoformat(),
    }


@app.get("/health")
def health():
    """
    Comprueba que la app corre y que la DB responde.
    """
    try:
        row = fetchone("SELECT 1 AS ok;")
        return {"ok": True, "db": bool(row and row.get("ok") == 1)}
    except Exception as e:
        # si hay un problema de conexión a la db
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


# =========================================================
# INGESTA
# =========================================================

@app.post("/ingest_lite")
def ingest_lite(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Versión mínima: guarda lat/lon (+ alt si viene) con el device por default.
    """
    require_token(authorization)

    try:
        row = execute(
            """
            INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at)
            VALUES (%s, NOW(), %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                str(DEFAULT_DEVICE_ID),
                float(payload.lat),
                float(payload.lon),
                float(payload.alt) if payload.alt is not None else None,
                payload.time,
            ),
        )
        return {"inserted_id": row["id"]}
    except Exception as e:
        # aquí es donde antes te daba 500 sin explicar
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


@app.post("/ingest")
def ingest(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Versión completa (igual que la lite pero más explícita).
    La dejamos así para que tu PowerShell la use.
    """
    require_token(authorization)

    try:
        row = execute(
            """
            INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at)
            VALUES (%s, NOW(), %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                str(DEFAULT_DEVICE_ID),
                float(payload.lat),
                float(payload.lon),
                float(payload.alt) if payload.alt is not None else None,
                payload.time,
            ),
        )
        return {"inserted_id": row["id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


# =========================================================
# LECTURAS
# =========================================================

@app.get("/readings/recent")
def recent(
    limit: int = 50,
    device: UUID | None = None,
):
    """
    Últimas lecturas para mostrar en la tabla del frontend.
    Si no mandas device, usa el que definimos arriba.
    """
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
        raise HTTPException(status_code=500, detail=f"db_error: {e!s}")


@app.get("/readings/track")
def track(
    device: UUID | None = Query(None, description="UUID del dispositivo"),
    start: datetime | None = Query(None, description="inicio ISO"),
    end: datetime | None = Query(None, description="fin ISO"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    """
    Devuelve los puntos de una ruta para un device en un intervalo.

    - si no mandas ?device=... te pongo el device por default
    - si mandas start/end te filtro
    - order = asc|desc
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
