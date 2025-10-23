import os
import psycopg
from psycopg_pool import ConnectionPool

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# Forzar SSL si no viene en la URL (Neon lo requiere)
if "sslmode=" not in DATABASE_URL:
    sep = "&" if "?" in DATABASE_URL else "?"
    DATABASE_URL = f"{DATABASE_URL}{sep}sslmode=require"

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=5, open=True)

def fetchone(query, params=None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchone()

def fetchall(query, params=None):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def execute(query, params=None, returning=False):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            if returning:
                return cur.fetchone()
            conn.commit()
