import os
from functools import lru_cache
import psycopg
from psycopg_pool import ConnectionPool
from psycopg.types.json import Json  # <-- IMPORTANTE

def _normalize_url(url: str) -> str:
    if "channel_binding=require" in url:
        url = url.replace("channel_binding=require", "")
        while "&&" in url:
            url = url.replace("&&", "&")
        url = url.rstrip("?&")
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def _conninfo() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return _normalize_url(url)

@lru_cache
def get_pool() -> ConnectionPool:
    conninfo = _conninfo()
    return ConnectionPool(conninfo=conninfo, min_size=1, max_size=5, open=True)

def _adapt_params(params):
    if params is None:
        return ()
    # Si es dict (named params) -> convertir cada dict anidado a Json
    if isinstance(params, dict):
        return {k: (Json(v) if isinstance(v, dict) else v) for k, v in params.items()}
    # Si es tupla/lista -> convertir dicts dentro a Json
    try:
        return tuple(Json(p) if isinstance(p, dict) else p for p in params)
    except TypeError:
        # Si no es iterable (param único)
        return Json(params) if isinstance(params, dict) else params

def fetchone(query, params=None):
    ap = _adapt_params(params)
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, ap)
            return cur.fetchone()

def fetchall(query, params=None):
    ap = _adapt_params(params)
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, ap)
            return cur.fetchall()

def execute(query, params=None, returning=False):
    ap = _adapt_params(params)
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, ap)
            if returning:
                return cur.fetchone()
            conn.commit()
