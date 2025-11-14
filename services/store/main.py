import os
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel
import pathlib, hashlib, json, uvicorn, logging
from logging.handlers import RotatingFileHandler

# Setup logging
log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'store.log')
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('store')
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

app = FastAPI()
STORE_ID = os.environ.get("STORE_ID", "store1")
VOLUME_PATH = os.environ.get("VOLUME_PATH", "./data/volumes/volume_v1.dat")
INDEX_PATH = VOLUME_PATH + ".idx"

os.makedirs(os.path.dirname(VOLUME_PATH) or ".", exist_ok=True)

# in-memory index: photo_id -> {offset,size,checksum,deleted}
index = {}

# load index if exists
if os.path.exists(INDEX_PATH):
    try:
        with open(INDEX_PATH, "r") as f:
            index = json.load(f)
            index = {int(k): v for k,v in index.items()}
    except Exception:
        index = {}

def persist_index():
    with open(INDEX_PATH, "w") as f:
        json.dump({str(k): v for k,v in index.items()}, f)

class DeleteReq(BaseModel):
    photo_id: int

@app.post("/volume/{vid}/append")
async def append(vid: str, request: Request, x_photo_id: int = Header(None), x_cookie: str = Header(None)):
    logger.info(f"Append request: volume={vid}, photo_id={x_photo_id}")
    body = await request.body()
    if x_photo_id is None:
        logger.error(f"Append failed: X-Photo-ID header missing")
        raise HTTPException(status_code=400, detail="X-Photo-ID required")
    offset = os.path.getsize(VOLUME_PATH) if os.path.exists(VOLUME_PATH) else 0
    logger.debug(f"Appending {len(body)} bytes at offset {offset}")
    checksum = hashlib.sha256(body).hexdigest()
    with open(VOLUME_PATH, "ab") as f:
        header = json.dumps({"photo_id": x_photo_id, "cookie": x_cookie, "size": len(body)})
        header_b = header.encode()
        f.write(len(header_b).to_bytes(4, "big"))
        f.write(header_b)
        f.write(body)
        f.write(checksum.encode())
    index[x_photo_id] = {"offset": offset, "size": len(body), "checksum": checksum, "deleted": False}
    persist_index()
    logger.info(f"Photo appended successfully: photo_id={x_photo_id}, offset={offset}, size={len(body)}, checksum={checksum}")
    return {"vid": vid, "offset": offset, "size": len(body), "checksum": checksum}

@app.get("/volume/{vid}/read")
def read(vid: str, offset: int | None = None, photo_id: int | None = None):
    logger.info(f"Read request: volume={vid}, photo_id={photo_id}, offset={offset}")
    if photo_id is not None:
        entry = index.get(photo_id)
        if not entry or entry.get("deleted", False):
            logger.warning(f"Read failed: Photo not found or deleted, photo_id={photo_id}")
            raise HTTPException(status_code=404, detail="not found")
        offset = entry["offset"]
        size = entry["size"]
        logger.debug(f"Found photo_id={photo_id} at offset={offset}, size={size}")
    elif offset is not None:
        logger.error(f"Read by offset unsupported: offset={offset}")
        raise HTTPException(status_code=400, detail="read by offset unsupported in starter")
    else:
        logger.error(f"Read request missing both offset and photo_id")
        raise HTTPException(status_code=400, detail="either offset or photo_id required")
    try:
        with open(VOLUME_PATH, "rb") as f:
            f.seek(offset)
            lbytes = f.read(4)
            if len(lbytes) < 4:
                logger.error(f"Read failed: Volume file corrupt at offset={offset}")
                raise HTTPException(status_code=500, detail="corrupt")
            header_len = int.from_bytes(lbytes, "big")
            header = f.read(header_len)
            header_json = json.loads(header.decode())
            data = f.read(size)
            checksum = f.read(64)
            if checksum.decode() != entry["checksum"]:
                logger.error(f"Checksum mismatch for photo_id={photo_id}")
                raise HTTPException(status_code=500, detail="checksum mismatch")
            logger.info(f"Photo read successfully: photo_id={photo_id}, size={size}")
            return Response(content=data, media_type="application/octet-stream")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Read failed with exception: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/volume/{vid}/exists")
def exists(vid: str, photo_id: int):
    logger.debug(f"Exists check: volume={vid}, photo_id={photo_id}")
    entry = index.get(photo_id)
    if entry and not entry.get("deleted", False):
        logger.debug(f"Photo exists: photo_id={photo_id}")
        return {"exists": True, "offset": entry["offset"], "size": entry["size"], "checksum": entry["checksum"]}
    logger.debug(f"Photo does not exist: photo_id={photo_id}")
    return {"exists": False}

@app.post("/volume/{vid}/delete")
def mark_delete(vid: str, req: DeleteReq):
    """
    Mark a needle as deleted.
    We do NOT remove bytes.
    Only flip index flag and append a delete marker.
    """
    photo_id = req.photo_id
    logger.info(f"Delete request: volume={vid}, photo_id={photo_id}")

    if photo_id not in index:
        logger.warning(f"Delete failed: Photo not found in index, photo_id={photo_id}")
        return {"status": "not_found"}

    # mark in-memory index
    index[photo_id]["deleted"] = True
    persist_index()
    logger.debug(f"Marked photo as deleted in index: photo_id={photo_id}")

    # append a delete record into volume file (optional, but Haystack does this)
    try:
        with open(VOLUME_PATH, "ab") as f:
            marker = json.dumps({"delete": photo_id}).encode()
            f.write(len(marker).to_bytes(4, "big"))
            f.write(marker)
        logger.debug(f"Appended delete marker to volume file: photo_id={photo_id}")
    except Exception as e:
        logger.error(f"Failed to append delete marker: {e}")
        pass

    logger.info(f"Photo successfully marked as deleted: photo_id={photo_id}")
    return {"status": "deleted", "photo_id": photo_id}

@app.get("/health")
def health():
    logger.debug(f"Health check: store_id={STORE_ID}, num_index={len(index)}")
    return {"store_id": STORE_ID, "volume": VOLUME_PATH, "num_index": len(index)}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8101"))
    uvicorn.run(app, host="127.0.0.1", port=port)