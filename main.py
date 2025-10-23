from fastapi import FastAPI, Header, HTTPException, Depends, Query
from pydantic import BaseModel, field_validator
from typing import Optional, List
from datetime import datetime
from db import fetchone, fetchall, execute
from psycopg.types.json import Json

app = FastAPI(title="IoT Ingest API", version="1.0.0")

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
    return row[0]

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

@app.get("/health")
def health():
    row = fetchone("SELECT 1")
    return {"ok": True, "db": row[0] == 1}

@app.post("/ingest")
def ingest(payload: ReadingIn, device_id: str = Depends(get_device_id)):
    read_at: Optional[datetime] = None
    if payload.time:
        try:
            read_at = datetime.fromisoformat(payload.time.replace("Z", "+00:00"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid time format (ISO-8601 expected)")

    row = execute(
        """
        INSERT INTO public.readings (device_id, lat, lon, alt_m, read_at, payload)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb)
        RETURNING id
        """,
        (
            device_id,
            payload.lat,
            payload.lon,
            payload.alt,
            read_at,
            Json({"lat": payload.lat, "lon": payload.lon, "alt": payload.alt, "time": payload.time}),
        ),
        returning=True
    )
    return {"inserted_id": row[0]}

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

