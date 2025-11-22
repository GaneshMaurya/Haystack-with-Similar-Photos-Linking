"""
Directory models and DB helpers (Haystack-style).

Directory stores *only logical metadata*:
- photo_id -> logical_volume, alt_key, cookie, status, replicas

Directory does NOT track offsets/sizes — Stores do.

Flow:
1. allocate_write(): choose a writable logical volume, create photo row in 'alloc' state.
2. commit_write(): mark row active.
3. get_photo(): return logical metadata.
4. mark_deleted(): soft-delete a photo.
"""

import sqlite3
from contextlib import closing
import os
import json
import logging
from typing import List

logging.basicConfig(level=logging.INFO)

DB = os.environ.get("DB_PATH", "data/directory.db")


# --------------------------------------------
# INITIALIZATION
# --------------------------------------------

def init_db():
    os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()

        # Logical volumes: maps an LV to a set of physical store nodes
        c.execute("""
        CREATE TABLE IF NOT EXISTS volumes (
            logical_volume TEXT PRIMARY KEY,
            replicas TEXT NOT NULL,
            capacity_bytes INTEGER DEFAULT 1000000000,
            used_bytes INTEGER DEFAULT 0,
            is_writable INTEGER DEFAULT 1,
            status TEXT DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")

        # Photos: pure logical metadata (no offsets/sizes)
        c.execute("""
        CREATE TABLE IF NOT EXISTS photos (
            photo_id INTEGER PRIMARY KEY AUTOINCREMENT,
            logical_volume TEXT NOT NULL,
            alt_key TEXT DEFAULT 'orig',
            cookie TEXT,
            replicas TEXT,
            status TEXT DEFAULT 'alloc',  -- alloc -> pending append, active -> OK, deleted
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")

        # Optional table: can remove since we use synchronous replication
        c.execute("""
        CREATE TABLE IF NOT EXISTS replication_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            photo_id INTEGER NOT NULL,
            replica_node TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")

        # Insert a default logical volume (for development)
        c.execute("INSERT OR IGNORE INTO volumes (logical_volume, replicas) VALUES (?, ?)",
                  ("lv-1", "store1,store2,store3"))

        # Migration safety for alt_key
        c.execute("PRAGMA table_info(photos)")
        cols = [r[1] for r in c.fetchall()]
        if "alt_key" not in cols:
            logging.info("Adding alt_key column to photos")
            c.execute("ALTER TABLE photos ADD COLUMN alt_key TEXT DEFAULT 'orig'")

        conn.commit()


# --------------------------------------------
# INTERNAL HELPERS
# --------------------------------------------

def choose_volume_for_write(size: int) -> dict:
    """
    Select a writable logical volume. For now, first writable LV.
    Real Haystack uses free space, load, allocation policy, etc.
    """
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("""
            SELECT logical_volume, replicas
            FROM volumes
            WHERE is_writable = 1
            LIMIT 1
        """)
        row = c.fetchone()
        if not row:
            raise RuntimeError("No writable logical volume available")

        logical_volume, replicas = row
        return {
            "logical_volume": logical_volume,
            "replicas": replicas.split(",")
        }


# --------------------------------------------
# WRITE: allocate_write
# --------------------------------------------

def allocate_write(size: int, alt_key: str = "orig") -> dict:
    """
    Main Haystack-style directory allocation:
    - choose logical volume
    - assign new photo_id
    - generate cookie
    - return ALL replicas (Store nodes)
    """

    from uuid import uuid4
    cookie = uuid4().hex[:16]

    vol = choose_volume_for_write(size)
    logical_volume = vol["logical_volume"]
    replicas = vol["replicas"]

    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("""
            INSERT INTO photos (logical_volume, alt_key, cookie, replicas, status)
            VALUES (?, ?, ?, ?, ?)
        """, (logical_volume, alt_key, cookie, ",".join(replicas), "alloc"))

        photo_id = c.lastrowid
        conn.commit()

    # Return EXACT fields expected by API Gateway
    return {
        "photo_id": photo_id,
        "logical_volume": logical_volume,
        "cookie": cookie,
        "replicas": replicas
    }


# --------------------------------------------
# WRITE: commit_write
# --------------------------------------------

def commit_write(photo_id: int):
    """
    After Stores append successfully, mark photo as active.
    """

    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()

        c.execute("SELECT status FROM photos WHERE photo_id = ?", (photo_id,))
        row = c.fetchone()
        if not row:
            raise RuntimeError("photo_id not found")

        c.execute("UPDATE photos SET status = 'active' WHERE photo_id = ?", (photo_id,))
        conn.commit()

    return {"status": "committed", "photo_id": photo_id}


# --------------------------------------------
# READ: get_photo
# --------------------------------------------

def get_photo(photo_id: int) -> dict | None:
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()

        c.execute("""
            SELECT photo_id, logical_volume, alt_key, cookie, replicas, status
            FROM photos
            WHERE photo_id = ?
        """, (photo_id,))

        row = c.fetchone()
        if not row:
            return None

        return {
            "photo_id": row[0],
            "logical_volume": row[1],
            "alt_key": row[2],
            "cookie": row[3],
            "replicas": row[4].split(",") if row[4] else [],
            "status": row[5]
        }


# --------------------------------------------
# WRITE: mark_deleted
# --------------------------------------------

def mark_deleted(photo_id: int):
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()

        c.execute("SELECT status FROM photos WHERE photo_id = ?", (photo_id,))
        if not c.fetchone():
            raise RuntimeError("photo not found")

        c.execute("""
            UPDATE photos
            SET status = 'deleted'
            WHERE photo_id = ?
        """, (photo_id,))

        conn.commit()

    return {"status": "deleted", "photo_id": photo_id}


# --------------------------------------------
# DEBUG helper
# --------------------------------------------

def list_all_photos() -> dict:
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT photo_id, logical_volume, alt_key, status FROM photos")
        rows = c.fetchall()

        return {
            "total": len(rows),
            "photos": [
                {
                    "photo_id": r[0],
                    "logical_volume": r[1],
                    "alt_key": r[2],
                    "status": r[3]
                }
                for r in rows
            ]
        }
