# db.py
import os
import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg.connect(DATABASE_URL)

def fetchone(query: str, params: tuple | None = None):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            return cur.fetchone()

def fetchall(query: str, params: tuple | None = None):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def execute(query: str, params: tuple | None = None):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(query, params or ())
            conn.commit()
            # si quieres que regrese algo cuando hay RETURNING:
            try:
                return cur.fetchone()
            except psycopg.ProgrammingError:
                return None
