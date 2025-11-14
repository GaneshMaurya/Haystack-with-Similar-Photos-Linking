import sqlite3
from contextlib import closing
import os

# Use relative path for local testing
DB = os.environ.get("DB_PATH", "../../data/directory.db")

def init_db():
    os.makedirs(os.path.dirname(DB) or ".", exist_ok=True)
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("""
        CREATE TABLE IF NOT EXISTS photos (
            photo_id INTEGER PRIMARY KEY,
            logical_volume TEXT,
            offset INTEGER,
            size INTEGER,
            checksum TEXT,
            cookie TEXT,
            replicas TEXT,
            status TEXT DEFAULT 'active'
        )""")
        c.execute("""
        CREATE TABLE IF NOT EXISTS volumes (
            logical_volume TEXT PRIMARY KEY,
            replicas TEXT,
            capacity_bytes INTEGER DEFAULT 1000000000,
            used_bytes INTEGER DEFAULT 0,
            is_writable INTEGER DEFAULT 1,
            status TEXT DEFAULT 'active'
        )""")
        c.execute("INSERT OR IGNORE INTO volumes (logical_volume, replicas) VALUES (?, ?)", ("lv-1", "localhost,localhost,localhost"))
        conn.commit()

def allocate_write(size):
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT logical_volume, replicas, used_bytes FROM volumes WHERE is_writable=1 LIMIT 1")
        row = c.fetchone()
        if not row:
            raise RuntimeError("no writable volume")
        name, replicas, used_bytes = row
        import uuid
        cookie = uuid.uuid4().hex[:16]
        return {"logical_volume": name, "cookie": cookie, "primary_store": "localhost:8101", "replicas": replicas.split(",")}

def commit_write(photo_id, logical_volume, offset, size, checksum, cookie, replicas):
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO photos (photo_id, logical_volume, offset, size, checksum, cookie, replicas) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (photo_id, logical_volume, offset, size, checksum, cookie, ",".join(replicas))
        )
        c.execute("UPDATE volumes SET used_bytes = used_bytes + ? WHERE logical_volume = ?", (size, logical_volume))
        conn.commit()

def get_photo(photo_id):
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT photo_id, logical_volume, offset, size, checksum, cookie, replicas, status FROM photos WHERE photo_id = ?", (photo_id,))
        row = c.fetchone()
        if not row:
            return None
        return {
            "photo_id": row[0],
            "logical_volume": row[1],
            "offset": row[2],
            "size": row[3],
            "checksum": row[4],
            "cookie": row[5],
            "replicas": row[6].split(",") if row[6] else [],
            "status": row[7]
        }
    
def list_all_photos():
    with closing(sqlite3.connect(DB)) as conn:
        c = conn.cursor()
        c.execute("SELECT photo_id, logical_volume, size, status FROM photos")
        rows = c.fetchall()
        return {
            "total": len(rows),
            "photos": [
                {
                    "photo_id": row[0],
                    "logical_volume": row[1],
                    "size": row[2],
                    "status": row[3]
                }
                for row in rows
            ]
        }