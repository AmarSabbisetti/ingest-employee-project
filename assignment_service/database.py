import sqlite3
from contextlib import contextmanager
from typing import Iterator

from assignment_service.config import DB_DIR, DB_PATH

@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    DB_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        yield conn
    finally:
        conn.close()