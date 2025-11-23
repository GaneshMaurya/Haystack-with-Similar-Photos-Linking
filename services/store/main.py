import os
import json
import hashlib
import sys
from fastapi import FastAPI, Header, Request, HTTPException
from fastapi.responses import Response
from typing import Dict
import uvicorn
import logging

# Logging: write to console and a service-specific logfile under ./logs
LOG_DIR = os.environ.get("LOG_DIR", "./logs")
os.makedirs(LOG_DIR, exist_ok=True)
STORE_LOG_FILE = os.path.join(LOG_DIR, "store.log")

# Configure root logger and remove existing handlers to avoid duplicates
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
for h in root_logger.handlers[:]:
    root_logger.removeHandler(h)

# File handler
fh = logging.FileHandler(STORE_LOG_FILE)
fh.setLevel(logging.INFO)
fh.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s [STORE]: %(message)s'))
root_logger.addHandler(fh)

# Console handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
ch.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s %(name)s [STORE]: %(message)s'))
root_logger.addHandler(ch)

logger = logging.getLogger(__name__)
logger.info("=" * 60)
logger.info("Store Service Starting")
logger.info(f"Log file: {STORE_LOG_FILE}")
logger.info("=" * 60)

app = FastAPI()

# ---------------------------------------
# Store Identity and Data Directory
# ---------------------------------------

STORE_ID = os.environ.get("STORE_ID", "store1")
DATA_DIR = os.environ.get("DATA_DIR", "./data/store")
os.makedirs(DATA_DIR, exist_ok=True)

logger.info(f"STORE_ID={STORE_ID}, DATA_DIR={DATA_DIR}")

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
            logger.info(f"Loaded index for {lv} with {len(index[lv])} entries")
        except Exception as e:
            logger.error(f"Failed to load index for {lv}: {e}", exc_info=True)
            index[lv] = {}
    else:
        index[lv] = {}
        logger.info(f"No existing index for {lv}; initialized empty index")


def persist_index(lv: str):
    path = index_path_for(lv)
    try:
        with open(path, "w") as f:
            json.dump({str(k): v for k, v in index.get(lv, {}).items()}, f)
        logger.debug(f"Persisted index for {lv} ({len(index.get(lv, {}))} entries)")
    except Exception as e:
        logger.error(f"Failed to persist index for {lv}: {e}", exc_info=True)


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
        logger.info(f"Opened volume file for {lv} at {path}")
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
        logger.info(f"No volume file for {lv}; skipping rebuild")
        return

    logger.info(f"Rebuilding index for {lv} from volume file {path}")
    try:
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
        logger.info(f"Rebuild complete for {lv}; entries={len(idx)}")
    except Exception as e:
        logger.error(f"Error rebuilding index for {lv}: {e}", exc_info=True)
        index[lv] = {}
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
    logger.info("Store startup complete")


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
    logger.info(f"[APPEND] lv={lv} photo_id={x_photo_id} alt_key={x_alt_key}")
    if x_photo_id is None:
        logger.warning("[APPEND] Missing X-Photo-ID header")
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

    try:
        # Write needle frame
        fh.write(header_len.to_bytes(4, "big"))
        fh.write(header_b)
        fh.write(data)
        fh.write(checksum.encode("utf-8"))
        fh.flush()
    except Exception as e:
        logger.error(f"[APPEND] Failed to write needle for photo {x_photo_id} to {lv}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="failed to write data")

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
    logger.info(f"[APPEND] Stored photo_id={x_photo_id} at offset={offset} size={size} checksum={checksum}")

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
    logger.info(f"[READ] lv={lv} photo_id={photo_id}")
    if photo_id is None:
        logger.warning("[READ] photo_id missing in request")
        raise HTTPException(status_code=400, detail="photo_id required")

    photo_id = int(photo_id)

    if lv not in index or photo_id not in index[lv]:
        logger.warning(f"[READ] photo_id={photo_id} not found in lv={lv}")
        raise HTTPException(status_code=404, detail="not found")

    entry = index[lv][photo_id]
    if entry["deleted"]:
        logger.warning(f"[READ] photo_id={photo_id} marked deleted")
        raise HTTPException(status_code=410, detail="photo deleted")

    offset = entry["offset"]
    size = entry["size"]

    path = volume_path_for(lv)
    try:
        with open(path, "rb") as f:
            f.seek(offset)

            # Read header len
            hlen_b = f.read(4)
            if len(hlen_b) < 4:
                logger.error("[READ] corrupt volume (header len)")
                raise HTTPException(status_code=500, detail="corrupt volume")

            hlen = int.from_bytes(hlen_b, "big")
            header_raw = f.read(hlen)
            _ = header_raw  # parsed but unused
            data = f.read(size)
            checksum_raw = f.read(64)
            checksum = checksum_raw.decode() if checksum_raw else None

            # Optional checksum verification
            if checksum and checksum != entry["checksum"]:
                logger.error(f"[READ] checksum mismatch for photo {photo_id}")
                raise HTTPException(status_code=500, detail="checksum mismatch")

            logger.info(f"[READ] Serving photo_id={photo_id} size={size}")
            return Response(content=data, media_type="application/octet-stream")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[READ] Unexpected error while reading photo {photo_id} from {lv}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="read failed")


# ---------------------------------------
# EXISTS
# ---------------------------------------

@app.get("/volume/{lv}/exists")
def exists(lv: str, photo_id: int):
    logger.debug(f"[EXISTS] lv={lv} photo_id={photo_id}")
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
    logger.info(f"[DELETE] lv={lv} photo_id={photo_id}")

    if lv not in index or photo_id not in index[lv]:
        logger.warning(f"[DELETE] photo_id={photo_id} not found in lv={lv}")
        return {"status": "not_found"}

    index[lv][photo_id]["deleted"] = True
    persist_index(lv)

    # Append delete marker for compaction/audit
    fh = open_volume_file(lv)
    fh.seek(0, os.SEEK_END)

    marker = {"delete": photo_id}
    marker_b = json.dumps(marker).encode("utf-8")

    try:
        fh.write(len(marker_b).to_bytes(4, "big"))
        fh.write(marker_b)
        fh.flush()
    except Exception as e:
        logger.error(f"[DELETE] Failed to append delete marker for photo {photo_id}: {e}", exc_info=True)

    logger.info(f"[DELETE] Marked photo_id={photo_id} as deleted")
    return {"status": "deleted", "photo_id": photo_id}


# ---------------------------------------
# HEALTH
# ---------------------------------------

@app.get("/health")
def health():
    totals = {lv: len(index.get(lv, {})) for lv in index}
    return {"store_id": STORE_ID, "volumes": totals}


if __name__ == "__main__":
    logger.info("Starting Store Service with Uvicorn")
    port = int(os.environ.get("PORT", "8101"))
    uvicorn.run("services.store.main:app", host="0.0.0.0", port=port)