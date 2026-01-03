import os
import psycopg2

def neon_conn():
    url = os.getenv("NEON_DATABASE_URL")
    if not url:
        raise RuntimeError("NEON_DATABASE_URL is not set")
    return psycopg2.connect(url)
