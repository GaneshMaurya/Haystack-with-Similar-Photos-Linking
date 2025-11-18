from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import sqlite3
import services.directory.models as models
import uvicorn
import logging
import os
from logging.handlers import RotatingFileHandler

# Setup logging
log_dir = os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'directory.log')
os.makedirs(log_dir, exist_ok=True)

logger = logging.getLogger('directory')
logger.setLevel(logging.DEBUG)
handler = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

app = FastAPI()

models.init_db()

class AllocateReq(BaseModel):
    size: int
    alt_key: str | None = None

class CommitReq(BaseModel):
    photo_id: int
    logical_volume: str
    offset: int
    size: int
    checksum: str
    cookie: str
    replicas: list[str]

class DeleteReq(BaseModel):
    photo_id: int

@app.post("/allocate_write")
def allocate(req: AllocateReq):
    logger.info(f"Allocate write request: size={req.size}, alt_key={req.alt_key}")
    try:
        r = models.allocate_write(req.size)
        logger.debug(f"Allocation result: {r}")
        return r
    except Exception as e:
        logger.error(f"Allocate write failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/commit_write")
def commit(req: CommitReq):
    logger.info(f"Commit write request: photo_id={req.photo_id}, lv={req.logical_volume}, replicas={req.replicas}")
    try:
        models.commit_write(req.photo_id, req.logical_volume, req.offset, req.size, req.checksum, req.cookie, req.replicas)
        logger.info(f"Photo committed: photo_id={req.photo_id}")
        return {"status": "committed"}
    except Exception as e:
        logger.error(f"Commit write failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/photo/{photo_id}")
def get_photo(photo_id: int):
    logger.info(f"Get photo request: photo_id={photo_id}")
    p = models.get_photo(photo_id)
    if not p:
        logger.warning(f"Photo not found: photo_id={photo_id}")
        raise HTTPException(status_code=404, detail="photo not found")
    logger.debug(f"Photo found: {p}")
    return p

@app.get("/photos")
def list_photos():
    """Debug endpoint to list all photos in database"""
    return models.list_all_photos()

@app.post("/delete")
def delete(req: DeleteReq):
    """
    Mark photo as deleted in metadata and update status.
    This does NOT remove physical data from stores.
    """
    logger.info(f"Delete request: photo_id={req.photo_id}")
    try:
        with sqlite3.connect(models.DB) as conn:
            c = conn.cursor()
            c.execute("SELECT status FROM photos WHERE photo_id = ?", (req.photo_id,))
            row = c.fetchone()
            if not row:
                logger.warning(f"Delete failed: Photo not found, photo_id={req.photo_id}")
                raise HTTPException(status_code=404, detail="photo not found")

            # UPDATE metadata
            c.execute("UPDATE photos SET status='deleted' WHERE photo_id = ?", (req.photo_id,))
            conn.commit()
            logger.info(f"Photo marked as deleted: photo_id={req.photo_id}")

        return {"status": "deleted", "photo_id": req.photo_id}
    except Exception as e:
        logger.error(f"Delete failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/replication_update")
def replication_update(payload: dict):
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status":"ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8001)