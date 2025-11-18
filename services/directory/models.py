"""
Directory models and DB helpers.

This directory stores *only logical metadata*:
- photo_id -> logical_volume, alt_key, cookie, status, replicas
It does NOT store offsets or sizes. Those are maintained by Store nodes.

Functions:
- init_db(): create tables
- allocate_write(size, alt_key): reserve a photo_id and logical volume
- commit_write(photo_id, checksum): mark photo active (and optionally set replication tasks)
- get_photo(photo_id): return logical metadata for a photo
- mark_deleted(photo_id): soft-delete
- list_all_photos(): debug helper
"""

import sqlite3
from contextlib import closing
import os
import json
import logging
from typing import List

logging.basicConfig(level=logging.INFO)
# Default DB path (relative). Adjust via DB_PATH env var if needed.
DB = os.environ.get("DB_PATH", "data/directory.db")

def init_db():
    os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        # volumes: logical volume mapping to store node IDs (replicas)
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
        # photos: minimal logical metadata only
        c.execute("""
        CREATE TABLE IF NOT EXISTS photos (
            photo_id INTEGER PRIMARY KEY AUTOINCREMENT,
            logical_volume TEXT NOT NULL,
            alt_key TEXT DEFAULT 'orig',
            cookie TEXT,
            replicas TEXT,
            status TEXT DEFAULT 'alloc',  -- alloc -> pending append, active -> available, deleted -> soft deleted
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        # replication_state: optional to track which replicas have completed
        c.execute("""
        CREATE TABLE IF NOT EXISTS replication_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            photo_id INTEGER NOT NULL,
            replica_node TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending', -- pending, complete, failed
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        # Insert a default logical volume if not present (for dev)
        c.execute("INSERT OR IGNORE INTO volumes (logical_volume, replicas) VALUES (?, ?)",
                  ("lv-1", "store1,store2,store3"))
        # migration: ensure older DBs get the alt_key column (and other safe ALTERs if needed)
        try:
            c.execute("PRAGMA table_info(photos)")
            existing_cols = [r[1] for r in c.fetchall()]
            if "alt_key" not in existing_cols:
                logging.info("migrating DB: adding alt_key column to photos")
                c.execute("ALTER TABLE photos ADD COLUMN alt_key TEXT DEFAULT 'orig'")
        except Exception:
            logging.exception("DB migration failed (non-fatal)")
        conn.commit()


def choose_volume_for_write(size: int) -> dict:
    """
    Very simple policy for demo:
    choose first writable volume. Real Haystack uses free space etc.
    """
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT logical_volume, replicas, used_bytes, capacity_bytes FROM volumes WHERE is_writable=1 LIMIT 1")
        row = c.fetchone()
        if not row:
            raise RuntimeError("no writable volume available")
        logical_volume, replicas, used_bytes, capacity_bytes = row
        return {"logical_volume": logical_volume, "replicas": replicas.split(",")}


def allocate_write(size: int, alt_key: str = "orig") -> dict:
    """
    Reserve a photo_id and return logical volume, cookie, primary and replicas.
    This returns a *photo_id* (key) that the API will use when appending to the Store.
    This keeps the Store index consistent (store indexes by photo_id).
    """
    from uuid import uuid4
    cookie = uuid4().hex[:16]
    vol = choose_volume_for_write(size)
    logical_volume = vol["logical_volume"]
    replicas = vol["replicas"]
    # Insert a row into photos with status 'alloc' and return the new photo_id.
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO photos (logical_volume, alt_key, cookie, replicas, status) VALUES (?, ?, ?, ?, ?)",
            (logical_volume, alt_key, cookie, ",".join(replicas), "alloc")
        )
        photo_id = c.lastrowid
        conn.commit()
    # for simplicity, pick primary as the first replica
    primary = replicas[0] if replicas else None
    return {
        "photo_id": photo_id,
        "logical_volume": logical_volume,
        "cookie": cookie,
        "primary_store": primary,
        "replicas": replicas
    }


def commit_write(photo_id: int):
    """
    Mark photo as active (available for reads) once store append succeeded.
    Directory does not store offsets/sizes — stores do.
    Create initial replication_state entries for background workers.
    """
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        # make sure photo exists and is in 'alloc' state
        c.execute("SELECT status, replicas FROM photos WHERE photo_id = ?", (photo_id,))
        row = c.fetchone()
        if not row:
            raise RuntimeError("photo_id not found")
        status, replicas = row
        # mark active
        c.execute("UPDATE photos SET status = 'active' WHERE photo_id = ?", (photo_id,))
        # create replication_state rows
        if replicas:
            for r in replicas.split(","):
                c.execute("INSERT INTO replication_state (photo_id, replica_node, status) VALUES (?, ?, ?)",
                          (photo_id, r, "pending"))
        conn.commit()
    return {"status": "committed", "photo_id": photo_id}


def get_photo(photo_id: int) -> dict | None:
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT photo_id, logical_volume, alt_key, cookie, replicas, status FROM photos WHERE photo_id = ?", (photo_id,))
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


def mark_deleted(photo_id: int):
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT status FROM photos WHERE photo_id = ?", (photo_id,))
        if not c.fetchone():
            raise RuntimeError("photo not found")
        c.execute("UPDATE photos SET status = 'deleted' WHERE photo_id = ?", (photo_id,))
        conn.commit()
    return {"status": "deleted", "photo_id": photo_id}


def list_all_photos() -> dict:
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT photo_id, logical_volume, alt_key, status FROM photos")
        rows = c.fetchall()
        return {
            "total": len(rows),
            "photos": [
                {"photo_id": r[0], "logical_volume": r[1], "alt_key": r[2], "status": r[3]}
                for r in rows
            ]
        }
