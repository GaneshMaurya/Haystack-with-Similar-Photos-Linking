"""
Haystack-style Store Service
---------------------------------------

This Store node manages one or more **physical volumes**, each corresponding
to a Haystack logical volume. Every Store maintains:

- A per-volume needle file: data/haystack_<lv>.dat
- A per-volume index file: data/<lv>.idx.json
- An in-memory index: {photo_id → {offset, size, alt_key, deleted, checksum}}

STORE BEHAVIOR:
---------------
- API Gateway passes X-Photo-ID, X-Cookie, X-Alt-Key
- Store appends:
    [4-byte header_len][header_json][data][sha256 checksum hex]
- Store computes offset locally (replicas have different offsets → OK)

ENDPOINTS:
----------
POST /volume/{lv}/append
GET  /volume/{lv}/read?photo_id=...
POST /volume/{lv}/delete
GET  /volume/{lv}/exists?photo_id=...
GET  /health
"""

import os
import json
import hashlib
from fastapi import FastAPI, Header, Request, HTTPException
from fastapi.responses import Response
from typing import Dict
import uvicorn
import logging
import os

# Logging: write to console and a service-specific logfile under ./logs
LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
STORE_LOG_FILE = os.path.join(LOG_DIR, "store.log")

# Configure root logger only if not already configured
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(STORE_LOG_FILE) for h in root_logger.handlers):
    fh = logging.FileHandler(STORE_LOG_FILE)
    fh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s [STORE]: %(message)s'))
    root_logger.addHandler(fh)

app = FastAPI()

# ---------------------------------------
# Store Identity and Data Directory
# ---------------------------------------

STORE_ID = os.environ.get("STORE_ID", "store1")
DATA_DIR = os.environ.get("DATA_DIR", "./data/store")
os.makedirs(DATA_DIR, exist_ok=True)

# index[lv][photo_id] = {offset, size, alt_key, deleted, checksum}
index: Dict[str, Dict[int, dict]] = {}

# file handles cache
files = {}


# ---------------------------------------
# Path helpers
# ---------------------------------------

def index_path_for(lv: str) -> str:
    return os.path.join(DATA_DIR, f"{lv}.idx.json")


def volume_path_for(lv: str) -> str:
    return os.path.join(DATA_DIR, f"haystack_{lv}.dat")


# ---------------------------------------
# Index load & persist
# ---------------------------------------

def load_index(lv: str):
    path = index_path_for(lv)
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                index[lv] = {int(k): v for k, v in json.load(f).items()}
        except Exception:
            index[lv] = {}
    else:
        index[lv] = {}


def persist_index(lv: str):
    path = index_path_for(lv)
    with open(path, "w") as f:
        json.dump({str(k): v for k, v in index.get(lv, {}).items()}, f)


# ---------------------------------------
# Volume file handling
# ---------------------------------------

def open_volume_file(lv: str):
    """Ensure an open append-mode file for logical volume lv."""
    if lv not in files:
        path = volume_path_for(lv)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fh = open(path, "ab+")
        files[lv] = fh
    return files[lv]


def rebuild_index_from_volume(lv: str):
    """
    Rebuild index by scanning haystack_<lv>.dat.

    Needle format:
      [4-byte header_len][header_json][data][checksum_hex]
    """
    path = volume_path_for(lv)
    idx = {}

    if not os.path.exists(path):
        index[lv] = {}
        return

    with open(path, "rb") as f:
        offset = 0
        while True:
            hlen_b = f.read(4)
            if not hlen_b or len(hlen_b) < 4:
                break

            hlen = int.from_bytes(hlen_b, "big")
            header_raw = f.read(hlen)
            if not header_raw or len(header_raw) < hlen:
                break

            try:
                header = json.loads(header_raw.decode("utf-8"))
            except Exception:
                break

            photo_id = int(header["photo_id"])
            size = header["size"]
            alt_key = header.get("alt_key", "orig")

            data = f.read(size)
            checksum_raw = f.read(64)   # 64-byte hex
            checksum = checksum_raw.decode() if checksum_raw else None

            idx[photo_id] = {
                "offset": offset,
                "size": size,
                "alt_key": alt_key,
                "deleted": False,
                "checksum": checksum,
            }

            offset += 4 + hlen + size + (len(checksum_raw) if checksum_raw else 0)

    index[lv] = idx
    persist_index(lv)


# ---------------------------------------
# Startup: load default LV (lv-1)
# ---------------------------------------

@app.on_event("startup")
def startup():
    default_lv = "lv-1"     # Enough for project; extend as needed
    if default_lv not in index:
        load_index(default_lv)
        # If empty index but file exists → rebuild
        if not index[default_lv]:
            rebuild_index_from_volume(default_lv)


# ---------------------------------------
# APPEND (write needle)
# ---------------------------------------

@app.post("/volume/{lv}/append")
async def append(
    lv: str,
    request: Request,
    x_photo_id: int = Header(None),
    x_cookie: str = Header(None),
    x_alt_key: str = Header("orig")
):
    if x_photo_id is None:
        raise HTTPException(status_code=400, detail="Missing X-Photo-ID header")

    data = await request.body()
    size = len(data)

    header = {
        "photo_id": int(x_photo_id),
        "alt_key": x_alt_key,
        "size": size,
        "cookie": x_cookie,
    }

    header_b = json.dumps(header).encode("utf-8")
    header_len = len(header_b)

    checksum = hashlib.sha256(data).hexdigest()

    fh = open_volume_file(lv)
    fh.seek(0, os.SEEK_END)
    offset = fh.tell()

    # Write needle frame
    fh.write(header_len.to_bytes(4, "big"))
    fh.write(header_b)
    fh.write(data)
    fh.write(checksum.encode("utf-8"))
    fh.flush()

    # Update in-memory index
    if lv not in index:
        index[lv] = {}

    index[lv][int(x_photo_id)] = {
        "offset": offset,
        "size": size,
        "alt_key": x_alt_key,
        "deleted": False,
        "checksum": checksum,
    }

    persist_index(lv)

    return {
        "status": "stored",
        "lv": lv,
        "offset": offset,
        "size": size,
        "checksum": checksum
    }


# ---------------------------------------
# READ (retrieve needle)
# ---------------------------------------

@app.get("/volume/{lv}/read")
def read(lv: str, photo_id: int = None):
    if photo_id is None:
        raise HTTPException(status_code=400, detail="photo_id required")

    photo_id = int(photo_id)

    if lv not in index or photo_id not in index[lv]:
        raise HTTPException(status_code=404, detail="not found")

    entry = index[lv][photo_id]
    if entry["deleted"]:
        raise HTTPException(status_code=410, detail="photo deleted")

    offset = entry["offset"]
    size = entry["size"]

    path = volume_path_for(lv)
    with open(path, "rb") as f:
        f.seek(offset)

        # Read header len
        hlen_b = f.read(4)
        if len(hlen_b) < 4:
            raise HTTPException(status_code=500, detail="corrupt volume")

        hlen = int.from_bytes(hlen_b, "big")
        header_raw = f.read(hlen)
        _ = header_raw  # parsed but unused
        data = f.read(size)
        checksum_raw = f.read(64)
        checksum = checksum_raw.decode() if checksum_raw else None

        # Optional checksum verification
        if checksum and checksum != entry["checksum"]:
            raise HTTPException(status_code=500, detail="checksum mismatch")

        return Response(content=data, media_type="application/octet-stream")


# ---------------------------------------
# EXISTS
# ---------------------------------------

@app.get("/volume/{lv}/exists")
def exists(lv: str, photo_id: int):
    if lv not in index:
        return {"exists": False}

    entry = index[lv].get(int(photo_id))
    if not entry:
        return {"exists": False}

    return {
        "exists": True,
        "offset": entry["offset"],
        "size": entry["size"],
        "deleted": entry["deleted"]
    }


# ---------------------------------------
# DELETE (soft)
# ---------------------------------------

@app.post("/volume/{lv}/delete")
def mark_delete(lv: str, payload: dict):
    photo_id = int(payload.get("photo_id"))

    if lv not in index or photo_id not in index[lv]:
        return {"status": "not_found"}

    index[lv][photo_id]["deleted"] = True
    persist_index(lv)

    # Append delete marker for compaction/audit
    fh = open_volume_file(lv)
    fh.seek(0, os.SEEK_END)

    marker = {"delete": photo_id}
    marker_b = json.dumps(marker).encode("utf-8")

    fh.write(len(marker_b).to_bytes(4, "big"))
    fh.write(marker_b)
    fh.flush()

    return {"status": "deleted", "photo_id": photo_id}


# ---------------------------------------
# HEALTH
# ---------------------------------------

@app.get("/health")
def health():
    totals = {lv: len(index.get(lv, {})) for lv in index}
    return {"store_id": STORE_ID, "volumes": totals}


if __name__ == "__main__":
    uvicorn.run("services.store.main:app", host="0.0.0.0")
