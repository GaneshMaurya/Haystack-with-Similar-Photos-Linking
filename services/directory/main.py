"""
Directory FastAPI app.

Endpoints:
- POST /allocate_write  -> allocate a photo_id, return logical_volume, cookie, replicas
- POST /commit_write    -> mark photo active (after store append)
- GET  /photo/{photo_id} -> return logical metadata (no offsets)
- POST /delete          -> mark photo as deleted
- GET  /photos          -> debug list
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import services.directory.models as models
import os

app = FastAPI()
models.init_db()


class AllocateReq(BaseModel):
    size: int
    alt_key: str | None = "orig"


class AllocateResp(BaseModel):
    photo_id: int
    logical_volume: str
    cookie: str
    primary_store: str
    replicas: list[str]


class CommitReq(BaseModel):
    photo_id: int


class DeleteReq(BaseModel):
    photo_id: int


@app.post("/allocate_write", response_model=AllocateResp)
def allocate(req: AllocateReq):
    try:
        r = models.allocate_write(req.size, req.alt_key or "orig")
        return r
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/commit_write")
def commit(req: CommitReq):
    try:
        res = models.commit_write(req.photo_id)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/photo/{photo_id}")
def get_photo(photo_id: int):
    p = models.get_photo(photo_id)
    if not p:
        raise HTTPException(status_code=404, detail="photo not found")
    return p


@app.post("/delete")
def delete(req: DeleteReq):
    try:
        res = models.mark_deleted(req.photo_id)
        return res
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/photos")
def list_photos():
    return models.list_all_photos()

@app.get("/health")
def health():
    # simple health: DB file exists and is writable
    db_path = os.environ.get("DB_PATH", "data/directory.db")
    db_exists = os.path.exists(db_path)
    return {"service": "directory", "db_path": db_path, "db_exists": db_exists}