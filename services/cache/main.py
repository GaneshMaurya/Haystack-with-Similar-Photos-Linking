"""
Haystack Cache Service
----------------------

This Cache stores raw photo bytes identified by photo_id.
Cache semantics match Haystack-style caching:
- Cache stores recently-read or recently-uploaded items.
- Does NOT store offsets or anything Haystack-specific.
- TTL + LRU eviction + size limit.
- Cache is best-effort. Failures MUST NOT break the system.

Endpoints:
    GET    /cache/exists/{photo_id}
    GET    /cache/photo/{photo_id}
    POST   /cache/photo/{photo_id}
    DELETE /cache/photo/{photo_id}
    GET    /cache/stats
    GET    /health
"""

import os
import time
import json
import asyncio
import logging
from typing import Dict
from collections import OrderedDict
from fastapi import FastAPI, HTTPException, Request, Response
import uvicorn

# ------------------------------------------
# Logging
# ------------------------------------------

import os

LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
CACHE_LOG_FILE = os.path.join(LOG_DIR, "cache.log")

# Configure root logger only if not already configured
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.FileHandler) and getattr(h, 'baseFilename', None) == os.path.abspath(CACHE_LOG_FILE) for h in root_logger.handlers):
    fh = logging.FileHandler(CACHE_LOG_FILE)
    fh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s [CACHE]: %(message)s'))
    root_logger.addHandler(fh)

logger = logging.getLogger("haystack-cache")

# ------------------------------------------
# Configuration
# ------------------------------------------

DATA_DIR = os.environ.get("CACHE_DATA_DIR", "./data/cache")
os.makedirs(DATA_DIR, exist_ok=True)

MAX_CACHE_SIZE_BYTES = int(os.environ.get("MAX_CACHE_SIZE_BYTES", 10 * 1024 * 1024))  # 10 MB
MAX_ITEMS = int(os.environ.get("MAX_CACHE_ITEMS", 20))
TTL_SEC = int(os.environ.get("CACHE_TTL_SEC", 60))
SWEEP_INTERVAL = int(os.environ.get("CACHE_SWEEP_SEC", 10))

logger.info(f"Cache dir={DATA_DIR}, max_bytes={MAX_CACHE_SIZE_BYTES}, max_items={MAX_ITEMS}, ttl={TTL_SEC}")

# ------------------------------------------
# In-memory Cache: OrderedDict = LRU
# ------------------------------------------

cache_index: "OrderedDict[int, dict]" = OrderedDict()
total_cache_bytes = 0

# ------------------------------------------
# Helpers
# ------------------------------------------

def now():
    return time.time()

def file_path(photo_id: int):
    return os.path.join(DATA_DIR, f"{photo_id}.bin")

def is_expired(meta):
    return meta["expiry"] < now()

def touch(photo_id: int):
    """Refresh TTL and move to MRU."""
    try:
        meta = cache_index.pop(photo_id)
        meta["last_access"] = now()
        meta["expiry"] = now() + TTL_SEC
        cache_index[photo_id] = meta
        return True
    except KeyError:
        return False

def remove_from_cache(photo_id: int):
    """Remove metadata and file."""
    global total_cache_bytes

    meta = cache_index.pop(photo_id, None)
    if meta:
        total_cache_bytes -= meta["size"]

    path = file_path(photo_id)
    if os.path.exists(path):
        try:
            os.remove(path)
        except Exception:
            pass

def evict_if_needed():
    """Evict expired items first, then LRU/size-based."""
    global total_cache_bytes

    evicted = []

    # 1) Expired eviction
    for pid in list(cache_index.keys()):
        meta = cache_index.get(pid)
        if meta and is_expired(meta):
            remove_from_cache(pid)
            evicted.append(pid)

    # 2) Size or count eviction
    while total_cache_bytes > MAX_CACHE_SIZE_BYTES or len(cache_index) > MAX_ITEMS:
        if not cache_index:
            break
        pid, _ = cache_index.popitem(last=False)  # LRU eviction
        remove_from_cache(pid)
        evicted.append(pid)

    return evicted

# ------------------------------------------
# Background Sweeper
# ------------------------------------------

async def sweep_loop():
    logger.info("Cache sweep loop started.")
    while True:
        try:
            evicted = evict_if_needed()
            if evicted:
                logger.info(f"Swept {len(evicted)} items: {evicted}")
        except Exception as e:
            logger.exception(f"Error during sweep: {e}")
        await asyncio.sleep(SWEEP_INTERVAL)

# ------------------------------------------
# Startup & Shutdown
# ------------------------------------------

app = FastAPI(title="Haystack Cache")

@app.on_event("startup")
async def startup():
    logger.info("Cache service starting...")
    app.state.sweeper = asyncio.create_task(sweep_loop())

@app.on_event("shutdown")
async def shutdown():
    logger.info("Stopping cache service...")
    sweeper = getattr(app.state, "sweeper", None)
    if sweeper:
        sweeper.cancel()
        try:
            await sweeper
        except asyncio.CancelledError:
            pass

# ------------------------------------------
# API Endpoints
# ------------------------------------------

@app.get("/cache/exists/{photo_id}")
def exists(photo_id: int):
    meta = cache_index.get(photo_id)
    if not meta:
        return {"exists": False}
    if is_expired(meta):
        remove_from_cache(photo_id)
        return {"exists": False}
    return {"exists": True, "size": meta["size"]}

@app.get("/cache/photo/{photo_id}")
def get_photo(photo_id: int):
    meta = cache_index.get(photo_id)
    if not meta:
        raise HTTPException(status_code=404, detail="cache miss")

    if is_expired(meta):
        remove_from_cache(photo_id)
        raise HTTPException(status_code=404, detail="expired")

    # refresh TTL + move to MRU
    touch(photo_id)

    path = meta["path"]
    try:
        with open(path, "rb") as f:
            data = f.read()
        return Response(content=data, media_type="application/octet-stream")
    except Exception:
        remove_from_cache(photo_id)
        raise HTTPException(status_code=404, detail="file missing")

@app.post("/cache/photo/{photo_id}")
async def put_photo(photo_id: int, request: Request):
    global total_cache_bytes

    body = await request.body()
    size = len(body)

    if size == 0:
        raise HTTPException(status_code=400, detail="empty body")

    path = file_path(photo_id)
    tmp = path + ".tmp"

    # atomic write
    try:
        with open(tmp, "wb") as f:
            f.write(body)
        os.replace(tmp, path)
    except Exception as e:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise HTTPException(status_code=500, detail=f"write failed: {e}")

    # update in-memory index
    old = cache_index.pop(photo_id, None)
    if old:
        total_cache_bytes -= old["size"]

    meta = {
        "path": path,
        "size": size,
        "last_access": now(),
        "expiry": now() + TTL_SEC
    }
    cache_index[photo_id] = meta
    total_cache_bytes += size

    # enforce eviction
    evict_if_needed()

    return {"status": "stored", "photo_id": photo_id, "size": size}

@app.delete("/cache/photo/{photo_id}")
def delete_photo(photo_id: int):
    if photo_id not in cache_index:
        # best-effort: still attempt file removal
        path = file_path(photo_id)
        if os.path.exists(path):
            try:
                os.remove(path)
            except:
                pass
        return {"status": "not_found"}

    remove_from_cache(photo_id)
    return {"status": "deleted", "photo_id": photo_id}

@app.get("/cache/stats")
def stats():
    return {
        "items": len(cache_index),
        "bytes": total_cache_bytes,
        "max_bytes": MAX_CACHE_SIZE_BYTES,
        "max_items": MAX_ITEMS,
        "ttl": TTL_SEC
    }

@app.get("/health")
def health():
    return {"status": "ok", "items": len(cache_index)}

if __name__ == "__main__":
    logger.info("Starting Cache server...")
    uvicorn.run(app, host="0.0.0.0")
