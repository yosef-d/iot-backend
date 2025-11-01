<<<<<<< HEAD
# main.py
from fastapi import FastAPI, HTTPException, Header
=======
﻿from fastapi import FastAPI, HTTPException, Header
>>>>>>> fee32d4 (remove ping import from main)
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
<<<<<<< HEAD
from datetime import datetime, timezone
=======
from db import fetchone, fetchall, execute
from psycopg import sql, Json
import os
>>>>>>> fee32d4 (remove ping import from main)

from db import fetchone, fetchall, execute  # 👈 OJO: aquí ya NO va 'ping'

SAFE_TOKEN = "XK8q1vR3pN6tY9bM2fH5wJ7cL0dS4gA8zQ1eV6uP9kT3nR5mB8yC2hF7xL0aD4sG"

app = FastAPI(title="IoT Ingest API")

# 👇 orígenes permitidos (Vercel + local)
origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "https://iot-frontend-iota.vercel.app",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {"ok": True, "msg": "iot-backend up"}


@app.get("/health")
def health():
    # mismo patrón que ya te había salido bien: probar la DB con un SELECT 1
    try:
        row = fetchone("SELECT 1;")
        db_ok = bool(row and row[0] == 1)
    except Exception:
        db_ok = False
    return {"ok": True, "db": db_ok}


from pydantic import BaseModel


class ReadingIn(BaseModel):
    lat: float
    lon: float
    alt: Optional[float] = None
    time: Optional[str] = None


def _require_token(auth_header: Optional[str]):
    if not auth_header:
        raise HTTPException(status_code=401, detail="missing_token")
    # esperamos formato: "Bearer <token>"
    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer" or parts[1] != SAFE_TOKEN:
        raise HTTPException(status_code=401, detail="invalid_token")


@app.post("/ingest_lite")
def ingest_lite(
    payload: ReadingIn,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
):
    _require_token(authorization)

    # timestamp del servidor
    ts = datetime.now(timezone.utc)

    row = execute(
        """
        INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at, payload)
        VALUES (
            '0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9',
            %s, %s, %s, %s, %s, %s
        )
        RETURNING id;
        """,
        (
            ts,
            payload.lat,
            payload.lon,
            payload.alt,
            None,
            None,
        ),
    )
    inserted_id = row[0] if row else None
    return {"inserted_id": inserted_id}


@app.post("/ingest")
def ingest(
    payload: ReadingIn,
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
):
    _require_token(authorization)

    # si viene time en el JSON, úsalo; si no, usa ahora()
    if payload.time:
        try:
            read_at = datetime.fromisoformat(payload.time.replace("Z", "+00:00"))
        except Exception:
            read_at = None
    else:
        read_at = None

    ts = datetime.now(timezone.utc)

    row = execute(
        """
        INSERT INTO readings (device_id, ts, lat, lon, alt_m, read_at, payload)
        VALUES (
            '0aef3bcc-b74b-47ce-9514-7eeb87bcb1a9',
            %s, %s, %s, %s, %s, %s
        )
        RETURNING id;
        """,
        (
            ts,
            payload.lat,
            payload.lon,
            payload.alt,
            read_at,
            {
                "lat": payload.lat,
                "lon": payload.lon,
                "alt": payload.alt,
                "time": payload.time,
            },
        ),
    )
    inserted_id = row[0] if row else None
    return {"inserted_id": inserted_id}


@app.get("/readings/recent")
def recent(limit: int = 50, device: Optional[str] = None):
    if device:
        rows = fetchall(
            """
            SELECT id, device_id, ts, lat, lon, alt_m, read_at, payload
            FROM readings
            WHERE device_id = %s
            ORDER BY ts DESC
            LIMIT %s;
            """,
            (device, limit),
        )
    else:
        rows = fetchall(
            """
            SELECT id, device_id, ts, lat, lon, alt_m, read_at, payload
            FROM readings
            ORDER BY ts DESC
            LIMIT %s;
            """,
            (limit,),
        )

    items = []
    for r in rows or []:
        items.append(
            {
                "id": r[0],
                "device_id": r[1],
                "ts": r[2],
                "lat": r[3],
                "lon": r[4],
                "alt_m": r[5],
                "read_at": r[6],
                "payload": r[7],
            }
        )
    return {"items": items}
