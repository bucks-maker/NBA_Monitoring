"""SQLite connection management with WAL mode and schema initialization."""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA_PATH = Path(__file__).parent / "schema.sql"


def get_connection(
    db_path: Path,
    thread_safe: bool = False,
) -> sqlite3.Connection:
    """Create and initialize a SQLite connection.

    Args:
        db_path: Path to the database file.
        thread_safe: If True, allow cross-thread usage.

    Returns:
        Initialized connection with WAL mode and schema applied.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path), check_same_thread=not thread_safe)
    conn.execute("PRAGMA journal_mode=WAL")

    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())

    conn.commit()
    return conn


def get_row_connection(
    db_path: Path,
    thread_safe: bool = False,
) -> sqlite3.Connection:
    """Like get_connection but with Row factory for dict-like access."""
    conn = get_connection(db_path, thread_safe=thread_safe)
    conn.row_factory = sqlite3.Row
    return conn
