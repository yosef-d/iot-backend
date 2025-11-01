from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime, timezone
from typing import Optional
from db import fetchone, fetchall, execute, ping
from psycopg import sql, Json
import os


app = FastAPI(title="IoT Ingest API", version="1.0.0")

# origenes permitidos (frontend público)
allowed_origins = [
    "https://iot-frontend-iota.vercel.app",
    "https://iot-frontend-iota.vercel.app/",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Auth por token de dispositivo (Bearer) ---
def get_device_id(authorization: Optional[str] = Header(None)) -> str:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = authorization.split(" ", 1)[1].strip()
    row = fetchone(
        "SELECT id FROM public.devices WHERE api_token = %s AND active = TRUE",
        (token,)
    )
    if not row:
        raise HTTPException(status_code=401, detail="Invalid token")
    return row[0]  # UUID

# --- Modelo de entrada ---
class ReadingIn(BaseModel):
    lat: float
    lon: float
    alt: Optional[float] = None
    time: Optional[str] = None  # ISO-8601

    @field_validator("lat")
    @classmethod
    def check_lat(cls, v):
        if v < -90 or v > 90:
            raise ValueError("lat out of range")
        return v

    @field_validator("lon")
    @classmethod
    def check_lon(cls, v):
        if v < -180 or v > 180:
            raise ValueError("lon out of range")
        return v

@app.get("/")
def root():
    return {"ok": True, "service": "iot-backend"}

@app.get("/health")
def health():
    row = fetchone("SELECT 1")
    return {"ok": True, "db": row[0] == 1}

@app.post("/ingest")
def ingest(payload: ReadingIn, device_id: str = Depends(get_device_id)):
    # Parseo opcional del tiempo de lectura
    read_at: Optional[datetime] = None
    if payload.time:
        try:
            read_at = datetime.fromisoformat(payload.time.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid time format (ISO-8601 expected)")

    # Construir parámetros asegurando tipos y JSON adaptado
    from uuid import UUID
    from psycopg.types.json import Json
    try:
        dev_id_str = str(device_id)  # por si viene como UUID
        lat_f = float(payload.lat)
        lon_f = float(payload.lon)
        alt_f = float(payload.alt) if payload.alt is not None else None
        payload_json = Json(payload.model_dump())

        params = (dev_id_str, lat_f, lon_f, alt_f, read_at, payload_json)

        # Trazas de tipos al log
        
        row = execute(
            """
            INSERT INTO public.readings (device_id, lat, lon, alt_m, read_at, payload)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            params,
            returning=True
        )
        return {"inserted_id": row[0]}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ingest_failed: {e}")

@app.get("/readings/recent")
def recent(
    limit: int = Query(50, ge=1, le=500),
    device: Optional[str] = None
):
    if device:
        q = """
            SELECT id, device_id, lat, lon, alt_m, read_at, ts
            FROM public.readings
            WHERE device_id = %s
            ORDER BY COALESCE(read_at, ts) DESC
            LIMIT %s
        """
        rows = fetchall(q, (device, limit))
    else:
        q = """
            SELECT id, device_id, lat, lon, alt_m, read_at, ts
            FROM public.readings
            ORDER BY COALESCE(read_at, ts) DESC
            LIMIT %s
        """
        rows = fetchall(q, (limit,))

    def to_obj(r):
        return {
            "id": r[0],
            "device_id": str(r[1]),
            "lat": r[2],
            "lon": r[3],
            "alt_m": r[4],
            "read_at": r[5].isoformat() if r[5] else None,
            "ts": r[6].isoformat() if r[6] else None,
        }
    return {"items": [to_obj(r) for r in rows]}

# CORS abierto temporal para frontend
try:
    from fastapi.middleware.cors import CORSMiddleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
except Exception:
    pass
@app.post("/ingest_lite")
def ingest_lite(payload: ReadingIn, device_id: str = Depends(get_device_id)):
    try:
        row = execute(
            """
            INSERT INTO public.readings (device_id, lat, lon)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (device_id, payload.lat, payload.lon),
            returning=True
        )
        return {"inserted_id": row[0]}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ingest_lite_failed: {e}")

@app.post("/ingest_lite")
def ingest_lite(payload: ReadingIn, device_id: str = Depends(get_device_id)):
    try:
        params = (str(device_id), float(payload.lat), float(payload.lon))
        print("INGEST_LITE TYPES:", [type(p).__name__ for p in params])
        row = execute(
            """
            INSERT INTO public.readings (device_id, lat, lon)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            params,
            returning=True
        )
        return {"inserted_id": row[0]}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"ingest_lite_failed: {e}")

