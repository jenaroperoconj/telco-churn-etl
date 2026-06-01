import os
import sqlite3
from contextlib import contextmanager
from urllib.parse import urlparse


DEFAULT_DATABASE_URL = "sqlite:///data/processed/telco_churn.db"


def database_url() -> str:
    return os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)


def is_postgres(url: str | None = None) -> bool:
    value = url or database_url()
    return value.startswith("postgres://") or value.startswith("postgresql://")


@contextmanager
def connect():
    url = database_url()
    if is_postgres(url):
        import psycopg
        from psycopg.rows import dict_row

        with psycopg.connect(url, row_factory=dict_row) as conn:
            yield conn
    else:
        parsed = urlparse(url)
        path = parsed.path.lstrip("/") if parsed.scheme == "sqlite" else url
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def placeholder() -> str:
    return "%s" if is_postgres() else "?"


def serial_type() -> str:
    return "BIGSERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
