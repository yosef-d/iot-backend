import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.types.json import Json

# 👇 estos vienen de tu db.py en Railway
from db import fetchone, fetchall, execute

# =========================================================
# CONFIG
# =========================================================

# el token que usas en PowerShell
SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

# el device fijo que ya existe en tu tabla
DEFAULT_DEVICE_ID = UUID("0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9")

app = FastAPI(title="IoT Ingest API", version="1.0.0")

# CORS: localhost + vercel
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


def get_inserted_id(db_result):
    """
    Tu Railway está devolviendo tuplas tipo (123,)
    y a veces diccionarios tipo {"id": 123}.
    Esto lo hace tolerante.
    """
    if db_result is None:
        return None
    # psycopg puede devolver Row, que soporta acceso por índice
    if isinstance(db_result, (list, tuple)):
        # ejemplo: (123,)
        return db_result[0]
    if isinstance(db_result, dict):
        return db_result.get("id")
    # último recurso
    return db_result


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
    Aquí NO usamos row.get(...) porque en tu caso viene como tupla.
    """
    try:
        row = fetchone("SELECT 1 AS ok;")
    except Exception:
        raise HTTPException(status_code=500, detail="db_error")

    ok_value = None
    if isinstance(row, (list, tuple)):
        # (1,)
        ok_value = row[0]
    elif isinstance(row, dict):
        ok_value = row.get("ok")

    return {"ok": True, "db": ok_value == 1}


@app.post("/ingest_lite")
def ingest_lite(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Versión mínima: guarda lat/lon (+ alt si viene).
    """
    require_token(authorization)

    try:
        db_res = execute(
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
    except Exception as e:
        # esto ayuda mucho cuando algo truena en Railway
        raise HTTPException(status_code=500, detail=f"db_insert_error: {e!s}")

    inserted_id = get_inserted_id(db_res)
    return {"inserted_id": inserted_id}


@app.post("/ingest")
def ingest(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
    """
    Versión completa: igual a ingest_lite pero explícita.
    """
    require_token(authorization)

    try:
        db_res = execute(
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"db_insert_error: {e!s}")

    inserted_id = get_inserted_id(db_res)
    return {"inserted_id": inserted_id}


@app.get("/readings/recent")
def recent(
    limit: int = 50,
    device: UUID | None = None,
):
    """
    Últimas lecturas para mostrar en la tabla del frontend.
    """
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


@app.get("/readings/track")
def track(
    device: UUID | None = Query(None, description="UUID del dispositivo; si no, usa el default"),
    start: datetime | None = Query(None, description="inicio ISO"),
    end: datetime | None = Query(None, description="fin ISO"),
    order: str = Query("asc", pattern="^(asc|desc)$"),
):
    """
    Devuelve los puntos de una ruta para un device en un intervalo.
    Sirve para que el frontend pinte el trayecto en un mapa.
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

    rows = fetchall(query, tuple(params))
    return {"items": rows}
