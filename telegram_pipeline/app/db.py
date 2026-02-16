import os
import sqlite3
from pathlib import Path
from typing import Optional

DB_PATH = Path("data/risk_commander.sqlite")

def check_write_permission():
    """Enforce kill-switch for DB writes."""
    if os.environ.get("ALLOW_WRITE") != "1":
        raise RuntimeError("WRITE blocked. Set ALLOW_WRITE=1 to run this command.")

def get_connection(db_path: str = str(DB_PATH), write: Optional[bool] = None) -> sqlite3.Connection:
    """
    Returns a SQLite connection.
    If write is None: Auto-detect (write=True if ALLOW_WRITE=1, else False)
    If write is True: Enforces ALLOW_WRITE=1
    If write is False: Attempts read-only mode
    """
    if write is None:
        write = (os.environ.get("ALLOW_WRITE") == "1")

    if write:
        check_write_permission()
        # Standard connect (read-write)
        # Ensure directory exists only if writing
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(db_path)
    else:
        # Read-only mode using URI
        if not Path(db_path).exists():
             # If DB missing and RO requested, standard connect to create empty file or fail gracefully
             conn = sqlite3.connect(db_path)
        else:
             uri_path = Path(db_path).absolute().as_uri()
             conn = sqlite3.connect(f"{uri_path}?mode=ro", uri=True)

    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db(conn: Optional[sqlite3.Connection] = None):
    """Initialize DB with schema."""
    # If conn provided, use it. If not, get one with write=True
    close_conn = False
    if conn is None:
        conn = get_connection(write=True)
        close_conn = True
    
    try:
        schema_path = Path(__file__).parent / "schema.sql"
        with open(schema_path, "r", encoding="utf-8") as f:
            conn.executescript(f.read())
        conn.commit()
    finally:
        if close_conn:
            conn.close()
