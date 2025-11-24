"""
Directory models and DB helpers (Haystack-style).

Directory stores *only logical metadata*:
- photo_id -> logical_volume, alt_key, cookie, status, replicas

UPDATED LOGIC:
- Supports updating volume usage (used_bytes).
- Implements rollover logic in choose_volume_for_write: checks capacity,
  marks old LV read-only, and creates the next sequential LV (lv-2, lv-3, etc.).
- Adds synchronization helpers for bootstrapping new replicas (dump/sync).
"""

import sqlite3
from contextlib import closing
import os
import json
import logging
from typing import List, Dict, Any, Tuple
import re # Added for sequential LV ID generation

logging.basicConfig(level=logging.INFO)

DB = os.environ.get("DB_PATH", "data/directory.db")
# Read LV_CAPACITY_BYTES from environment (defaults to 500 KB)
DEFAULT_LV_CAPACITY_BYTES = int(os.environ.get("LV_CAPACITY_BYTES", 500000))


# --------------------------------------------
# INITIALIZATION
# --------------------------------------------

def init_db():
    os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()

        # Logical volumes: Capacity is set by f-string, resolving the sqlite3 error.
        c.execute(f"""
        CREATE TABLE IF NOT EXISTS volumes (
            logical_volume TEXT PRIMARY KEY,
            replicas TEXT NOT NULL,
            capacity_bytes INTEGER DEFAULT {DEFAULT_LV_CAPACITY_BYTES},
            used_bytes INTEGER DEFAULT 0,
            is_writable INTEGER DEFAULT 1,
            status TEXT DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")

        # Photos table creation remains the same
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

        # Replication state table (Optional)
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

        # Migration safety for alt_key
        c.execute("PRAGMA table_info(photos)")
        cols = [r[1] for r in c.fetchall()]
        if "alt_key" not in cols:
            logging.info("Adding alt_key column to photos")
            c.execute("ALTER TABLE photos ADD COLUMN alt_key TEXT DEFAULT 'orig'")

        conn.commit()


# --------------------------------------------
# INTERNAL HELPERS (LV Rollover and Usage)
# --------------------------------------------

def get_next_lv_id() -> str:
    """Finds the largest existing LV ID (e.g., lv-1) and returns the next one (lv-2)."""
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT logical_volume FROM volumes ORDER BY logical_volume DESC LIMIT 1")
        last_lv = c.fetchone()
        
        if last_lv:
            match = re.match(r'lv-(\d+)', last_lv[0])
            if match:
                next_id = int(match.group(1)) + 1
                return f"lv-{next_id}"
        
        return "lv-1" # Default if no volumes exist


def update_volume_usage(logical_volume: str, size_to_add: int) -> bool:
    """
    Updates the used_bytes for the given volume. 
    Returns True if update succeeded and False if the volume is now full (rollover needed).
    """
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        
        c.execute("SELECT used_bytes, capacity_bytes FROM volumes WHERE logical_volume = ?", (logical_volume,))
        row = c.fetchone()
        
        if not row:
            logging.warning(f"Attempted to update usage for non-existent volume: {logical_volume}")
            return False

        used, capacity = row
        new_used = used + size_to_add
        
        # Haystack policy: Check if the new size exceeds 95% threshold 
        if new_used > capacity * 0.95:
            # Mark the volume as read-only (is_writable = 0)
            c.execute("UPDATE volumes SET is_writable = 0, used_bytes = ? WHERE logical_volume = ?", 
                      (new_used, logical_volume))
            conn.commit()
            logging.info(f"VOLUME FULL POLICY TRIGGERED: {logical_volume} marked read-only.")
            return False # Rollover needed
        
        # Normal update
        c.execute("UPDATE volumes SET used_bytes = ? WHERE logical_volume = ?", 
                  (new_used, logical_volume))
        conn.commit()
        return True


def insert_new_volume(logical_volume: str, replicas: List[str]):
    """Inserts a new writable volume into the database."""
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        # Ensure capacity_bytes is set correctly
        c.execute("INSERT INTO volumes (logical_volume, replicas, capacity_bytes) VALUES (?, ?, ?)",
                  (logical_volume, ",".join(replicas), DEFAULT_LV_CAPACITY_BYTES))
        conn.commit()
    logging.info(f"Inserted new volume: {logical_volume} with replicas: {replicas}")


def volume_exists(logical_volume: str) -> bool:
    """Checks if a logical volume already exists."""
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT 1 FROM volumes WHERE logical_volume = ?", (logical_volume,))
        return c.fetchone() is not None


def choose_volume_for_write(size: int, available_stores: List[str]) -> dict:
    """
    Selects the current writable logical volume. If none exist, or if the current one
    is full, it creates the next sequential LV.
    """
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        
        # 1. Find the current writable volume
        c.execute("""
            SELECT logical_volume, replicas, used_bytes, capacity_bytes
            FROM volumes
            WHERE is_writable = 1
            LIMIT 1
        """)
        row = c.fetchone()
        
        # 2. Check if the current volume is nearing capacity
        if row:
            logical_volume, replicas_raw, used, capacity = row
            
            # Policy Check: If the current write would push us past the 95% threshold, trigger rollover
            if used + size > capacity * 0.95:
                logging.warning(f"LV {logical_volume} is too full (used={used}, cap={capacity}). Triggering rollover.")
                
                # Mark current volume as read-only
                c.execute("UPDATE volumes SET is_writable = 0 WHERE logical_volume = ?", (logical_volume,))
                conn.commit()
                
                row = None # Force creation of new LV
            else:
                 # If writable and not full, use it.
                 return {
                    "logical_volume": logical_volume,
                    "replicas": replicas_raw.split(",")
                 }

        # 3. If no writable volume found (row is None), create a new one (LV rollover)
        if not row:
            if not available_stores:
                 raise RuntimeError("No available physical stores to create a new logical volume.")

            new_lv_id = get_next_lv_id()
            
            # Insert new volume and commit (using the model helper function)
            insert_new_volume(new_lv_id, available_stores)
            
            logging.info(f"LV Rollover SUCCESS: Created new writable volume: {new_lv_id}")
            
            return {
                "logical_volume": new_lv_id,
                "replicas": available_stores
            }
            
        raise RuntimeError("Volume selection logic failed.")


# --------------------------------------------
# WRITE: allocate_write
# --------------------------------------------

def allocate_write(size: int, alt_key: str = "orig", available_stores: List[str] = []) -> dict:
    """
    Main Haystack-style directory allocation:
    - choose logical volume (and potentially trigger rollover)
    """

    from uuid import uuid4
    cookie = uuid4().hex[:16]

    # Uses the enhanced volume chooser
    vol = choose_volume_for_write(size, available_stores)
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

        # Helper to convert SQLite row (tuple) to dictionary
        columns = ["photo_id", "logical_volume", "alt_key", "cookie", "replicas", "status"]
        result = dict(zip(columns, row))
        
        result["replicas"] = result["replicas"].split(",") if result["replicas"] else []
        
        return result


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
# REPLICATION & SYNC HELPERS (NEW)
# --------------------------------------------

def get_all_photos_data() -> List[Dict[str, Any]]:
    """Retrieves all photo records for a full data dump (Snapshot Sync)."""
    with closing(sqlite3.connect(DB)) as conn:
        conn.row_factory = sqlite3.Row # Allows fetching results by column name
        c = conn.cursor()
        c.execute("""
            SELECT photo_id, logical_volume, alt_key, cookie, replicas, status, created_at
            FROM photos
        """)
        
        rows = c.fetchall()
        
        # Convert rows to a list of standard dictionaries
        data_dump = []
        for row in rows:
            record = dict(row)
            record["replicas"] = record["replicas"].split(",") if record["replicas"] else []
            data_dump.append(record)
            
        return data_dump


def sync_photos_from_dump(photo_dump: List[Dict[str, Any]]):
    """
    Inserts or replaces photo records from a full snapshot dump.
    Used by a new Replica on startup.
    """
    if not photo_dump:
        logging.info("Synchronization dump was empty. Skipping photo insertion.")
        return

    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        
        # We use INSERT OR REPLACE to handle potential duplicates or overwriting old data
        # during synchronization.
        c.executemany("""
            INSERT OR REPLACE INTO photos 
            (photo_id, logical_volume, alt_key, cookie, replicas, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (
                p['photo_id'], 
                p['logical_volume'], 
                p['alt_key'], 
                p['cookie'], 
                ",".join(p['replicas']), 
                p['status'], 
                p['created_at']
            )
            for p in photo_dump
        ])
        
        conn.commit()
    logging.info(f"Successfully synchronized and inserted/replaced {len(photo_dump)} photo records.")


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