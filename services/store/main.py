"""
Store service (manages one or more physical volumes).

- Maintains an in-memory index: photo_id -> {offset, size, alt_key, deleted}
- Persists index to JSON (index file) for quick restart.
- Appends needles to a per-logical-volume file (haystack_lv_<lv>.dat).
- Supports:
  - POST /volume/{lv}/append  (headers: X-Photo-ID, X-Cookie, X-Alt-Key)
  - GET  /volume/{lv}/read?photo_id=...
  - GET  /volume/{lv}/exists?photo_id=...
  - POST /volume/{lv}/delete (body: {"photo_id": ...})
  - GET  /health
"""

import os
import json
import hashlib
from fastapi import FastAPI, Header, Request, HTTPException
from fastapi.responses import Response
from typing import Dict

app = FastAPI()

STORE_ID = os.environ.get("STORE_ID", "store1")          # unique store node id
DATA_DIR = os.environ.get("DATA_DIR", "./data")          # base dir for volumes/indexes
os.makedirs(DATA_DIR, exist_ok=True)

# structure: index[logical_volume][photo_id] = {"offset": int, "size": int, "alt_key": str, "deleted": bool, "checksum": str}
index: Dict[str, Dict[int, dict]] = {}
# open file descriptors cache: files[logical_volume] = open file handle
files: Dict[str, object] = {}


def index_path_for(lv: str) -> str:
    return os.path.join(DATA_DIR, f"{lv}.idx.json")


def volume_path_for(lv: str) -> str:
    # physical file for this logical volume on this store
    return os.path.join(DATA_DIR, f"haystack_{lv}.dat")


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
        # convert keys to str for JSON
        json.dump({str(k): v for k, v in index.get(lv, {}).items()}, f)


def open_volume_file(lv: str):
    # keep a single append-mode file descriptor per logical volume
    if lv not in files:
        path = volume_path_for(lv)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fh = open(path, "ab+")
        files[lv] = fh
    return files[lv]


def rebuild_index_from_volume(lv: str):
    """
    Scan the haystack_<lv>.dat file and rebuild offsets and sizes.
    Our framing format:
      [4-byte header_len][header_json][data_bytes][32-byte checksum_hex]
    header_json contains {"photo_id": int, "alt_key": str, "size": int, "cookie": str}
    """
    path = volume_path_for(lv)
    idx = {}
    if not os.path.exists(path):
        index[lv] = {}
        return
    with open(path, "rb") as f:
        offset = 0
        while True:
            # read header length
            hlen_b = f.read(4)
            if not hlen_b or len(hlen_b) < 4:
                break
            hlen = int.from_bytes(hlen_b, "big")
            header_raw = f.read(hlen)
            try:
                header = json.loads(header_raw.decode())
            except Exception:
                # corrupt header -> stop
                break
            size = header.get("size")
            photo_id = int(header.get("photo_id"))
            alt_key = header.get("alt_key", "orig")
            # read data
            data = f.read(size)
            checksum_raw = f.read(64)  # sha256 hex string length
            checksum = checksum_raw.decode() if checksum_raw else None
            # store in index
            idx[photo_id] = {"offset": offset, "size": size, "alt_key": alt_key, "deleted": False, "checksum": checksum}
            # advance offset
            offset += 4 + hlen + size + (len(checksum_raw) if checksum_raw else 0)
    index[lv] = idx
    persist_index(lv)


@app.on_event("startup")
def startup():
    # for demo: load index for lv-1 only. In prod, you would load for every physical volume present.
    # If you keep many volumes on the machine, iterate them.
    default_lv = "lv-1"
    load_index(default_lv)
    # if index missing, try to rebuild from file
    if not index.get(default_lv):
        rebuild_index_from_volume(default_lv)


@app.post("/volume/{lv}/append")
async def append(lv: str, request: Request, x_photo_id: int = Header(None), x_cookie: str = Header(None), x_alt_key: str = Header("orig")):
    """
    Append a needle to the logical volume on this Store.
    - API must pass the Directory-assigned photo_id in X-Photo-ID
    - We store header JSON + data + checksum and update in-memory index
    """
    if x_photo_id is None:
        raise HTTPException(status_code=400, detail="X-Photo-ID header required")

    data = await request.body()
    size = len(data)
    # framing
    header = {"photo_id": int(x_photo_id), "alt_key": x_alt_key, "size": size, "cookie": x_cookie}
    header_b = json.dumps(header).encode()
    header_len = len(header_b)
    checksum = hashlib.sha256(data).hexdigest()

    fh = open_volume_file(lv)
    # move to end for append
    fh.seek(0, os.SEEK_END)
    offset = fh.tell()
    # write frame
    fh.write(header_len.to_bytes(4, "big"))
    fh.write(header_b)
    fh.write(data)
    fh.write(checksum.encode())
    fh.flush()
    # update index (in-memory) and persist
    if lv not in index:
        index[lv] = {}
    index[lv][int(x_photo_id)] = {"offset": offset, "size": size, "alt_key": x_alt_key, "deleted": False, "checksum": checksum}
    persist_index(lv)
    return {"status": "stored", "lv": lv, "offset": offset, "size": size, "checksum": checksum}


@app.get("/volume/{lv}/read")
def read(lv: str, photo_id: int = None):
    """
    Read by photo_id. Store looks up in-memory index and seeks to offset.
    """
    if photo_id is None:
        raise HTTPException(status_code=400, detail="photo_id required")
    if lv not in index or photo_id not in index[lv]:
        raise HTTPException(status_code=404, detail="not found")
    entry = index[lv][photo_id]
    if entry.get("deleted"):
        raise HTTPException(status_code=410, detail="photo deleted")
    offset = entry["offset"]
    size = entry["size"]
    path = volume_path_for(lv)
    with open(path, "rb") as f:
        f.seek(offset)
        hlen_b = f.read(4)
        if not hlen_b or len(hlen_b) < 4:
            raise HTTPException(status_code=500, detail="corrupt volume")
        hlen = int.from_bytes(hlen_b, "big")
        header_raw = f.read(hlen)
        # header parsed but not used here
        _ = header_raw
        data = f.read(size)
        checksum_raw = f.read(64)
        checksum = checksum_raw.decode() if checksum_raw else None
        # optionally verify checksum
        if checksum and checksum != entry.get("checksum"):
            raise HTTPException(status_code=500, detail="checksum mismatch")
        return Response(content=data, media_type="application/octet-stream")


@app.get("/volume/{lv}/exists")
def exists(lv: str, photo_id: int):
    if lv not in index:
        return {"exists": False}
    entry = index[lv].get(int(photo_id))
    if not entry:
        return {"exists": False}
    return {"exists": True, "offset": entry["offset"], "size": entry["size"], "deleted": entry["deleted"]}


@app.post("/volume/{lv}/delete")
def mark_delete(lv: str, payload: dict):
    """
    Soft delete on this store. Mark index entry deleted and append a delete marker in the file.
    Compactor will reclaim space later.
    """
    photo_id = int(payload.get("photo_id"))
    if lv not in index or photo_id not in index[lv]:
        return {"status": "not_found"}
    index[lv][photo_id]["deleted"] = True
    persist_index(lv)
    # append delete marker for audit/compaction
    fh = open_volume_file(lv)
    fh.seek(0, os.SEEK_END)
    marker = json.dumps({"delete": photo_id}).encode()
    fh.write(len(marker).to_bytes(4, "big"))
    fh.write(marker)
    fh.flush()
    return {"status": "deleted", "photo_id": photo_id}


@app.get("/health")
def health():
    totals = {lv: len(index.get(lv, {})) for lv in index}
    return {"store_id": STORE_ID, "volumes": totals}
