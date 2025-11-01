import os
from datetime import datetime, timezone
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from psycopg.types.json import Json

from db import fetchone, fetchall, execute

SAFE_TOKEN = os.getenv(
    "SAFE_TOKEN",
    "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG",
)

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

class ReadingIn(BaseModel):
    lat: float
    lon: float
    alt: float | None = None
    time: datetime | None = None


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


@app.get("/")
def root():
    return {"ok": True, "service": "iot-backend", "uptime": utcnow().isoformat()}


@app.get("/health")
def health():
    row = fetchone("SELECT 1 AS ok;")
    return {"ok": True, "db": bool(row and row.get("ok") == 1)}


@app.post("/ingest_lite")
def ingest_lite(
    payload: ReadingIn,
    authorization: str | None = Header(default=None),
):
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

# ⬇️ ⬇️ ⬇️ AQUÍ REEMPLAZAS TU /readings/track VIEJO POR ESTE ⬇️ ⬇️ ⬇️
@app.get("/readings/track")
def readings_track(device: str | None = None):
    """
    Devuelve los puntos de un dispositivo en orden de tiempo
    + un pequeño resumen (distancia y duración).
    """
    if device is None or device.strip() == "":
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
        print("[/readings/track] ERROR consultando DB:", e)
        raise HTTPException(status_code=500, detail="db_error")

    points: list[dict] = []
    for r in rows:
        lat = float(r["lat"]) if r["lat"] is not None else None
        lon = float(r["lon"]) if r["lon"] is not None else None

        def to_iso(x):
            if not x:
                return None
            if isinstance(x, datetime):
                return x.isoformat()
            return str(x)

        points.append(
            {
                "id": r["id"],
                "device_id": r["device_id"],
                "lat": lat,
                "lon": lon,
                "alt_m": float(r["alt_m"]) if r["alt_m"] is not None else None,
                "read_at": to_iso(r["read_at"]),
                "ts": to_iso(r["ts"]),
            }
        )

    def haversine_m(lat1, lon1, lat2, lon2):
        import math
        R = 6371000.0
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlmb = math.radians(lon2 - lon1)
        a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

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

    duration_s = None
    if len(points) >= 2:
        start_s = points[0]["read_at"] or points[0]["ts"]
        end_s = points[-1]["read_at"] or points[-1]["ts"]
        try:
            if start_s and end_s:
                t1 = datetime.fromisoformat(start_s.replace("Z", "+00:00"))
                t2 = datetime.fromisoformat(end_s.replace("Z", "+00:00"))
                duration_s = (t2 - t1).total_seconds()
        except Exception as e:
            print("[/readings/track] ERROR parseando fechas:", e)
            duration_s = None

    return {
        "device": device,
        "points": points,
        "summary": {
            "count": len(points),
            "distance_m": round(total_dist, 2),
            "duration_s": duration_s,
        },
    }
# ⬆️ ⬆️ ⬆️ HASTA AQUÍ
