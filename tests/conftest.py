"""Shared test fixtures: in-memory DB, mock clients."""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from src.db.connection import SCHEMA_PATH


@pytest.fixture
def mem_conn():
    """In-memory SQLite connection with schema applied."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA journal_mode=WAL")
    with open(SCHEMA_PATH) as f:
        conn.executescript(f.read())
    conn.commit()
    yield conn
    conn.close()
