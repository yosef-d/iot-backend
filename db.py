import os
from functools import lru_cache
import psycopg
from psycopg_pool import ConnectionPool

def _normalize_url(url: str) -> str:
    # quita channel_binding si aparece
    if "channel_binding=require" in url:
        url = url.replace("channel_binding=require", "")
        while "&&" in url:
            url = url.replace("&&", "&")
        url = url.rstrip("?&")
    # fuerza ssl si no viene
    if "sslmode=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}sslmode=require"
    return url

def _conninfo() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    if not url:
        # No explotes en import; el error se verá al primer uso si falta la var
        raise RuntimeError("DATABASE_URL not set")
    return _normalize_url(url)

@lru_cache
def get_pool() -> ConnectionPool:
    conninfo = _conninfo()
    return ConnectionPool(conninfo=conninfo, min_size=1, max_size=5, open=True)

def fetchone(query, params=None):
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchone()

def fetchall(query, params=None):
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def execute(query, params=None, returning=False):
    pool = get_pool()
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if returning:
                return cur.fetchone()
            conn.commit()
